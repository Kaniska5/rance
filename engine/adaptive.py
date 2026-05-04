"""
RANCE — Adaptive Decision Engine

Selects the optimal compression algorithm using:
  1. Real network telemetry (RTT, BW, loss, jitter)
  2. Data-type profiling (text, JSON, binary, compressed, mixed)
  3. Weighted scoring model

score(algo) = w_ratio  * ratio_score
            + w_speed  * speed_score
            - w_cpu    * cpu_cost
            - w_loss   * ratio_score * loss_rate * 10
"""
import time
import threading
from dataclasses import dataclass, field
from collections import deque
from enum import Enum

from .algorithms import ALGORITHM_REGISTRY, BaseAlgorithm
from .probe import NetworkSnapshot, RealNetworkProbe, BaseProbe


class NetworkCondition(Enum):
    EXCELLENT = "excellent"
    GOOD      = "good"
    MODERATE  = "moderate"
    CONGESTED = "congested"
    CRITICAL  = "critical"


class DataProfile(Enum):
    TEXT       = "text"
    JSON       = "json"
    BINARY     = "binary"
    MIXED      = "mixed"
    COMPRESSED = "compressed"


@dataclass
class EngineDecision:
    algo_id:      int
    algo_name:    str
    condition:    NetworkCondition
    data_profile: DataProfile
    score:        float
    snapshot:     NetworkSnapshot
    scores:       dict  = field(default_factory=dict)
    reason:       str   = ""
    timestamp:    float = field(default_factory=time.time)
    original_size:    int = 0
    compressed_size:  int = 0
    actual_ratio:     float = 0.0
    actual_latency_ms: float = 0.0

    def to_dict(self) -> dict:
        return {
            "algo_id":           self.algo_id,
            "algo_name":         self.algo_name,
            "condition":         self.condition.value,
            "data_profile":      self.data_profile.value,
            "score":             round(self.score, 4),
            "scores":            {k: round(v, 4) for k, v in self.scores.items()},
            "reason":            self.reason,
            "timestamp":         round(self.timestamp, 3),
            "original_size":     self.original_size,
            "compressed_size":   self.compressed_size,
            "actual_ratio":      round(self.actual_ratio, 3),
            "actual_latency_ms": round(self.actual_latency_ms, 3),
            "network":           self.snapshot.to_dict(),
        }


def classify_condition(snap: NetworkSnapshot) -> NetworkCondition:
    if snap.rtt_ms < 20  and snap.bandwidth_mbps > 50  and snap.loss_rate < 0.005:
        return NetworkCondition.EXCELLENT
    if snap.rtt_ms < 50  and snap.bandwidth_mbps > 20  and snap.loss_rate < 0.02:
        return NetworkCondition.GOOD
    if snap.rtt_ms < 100 and snap.bandwidth_mbps > 5   and snap.loss_rate < 0.05:
        return NetworkCondition.MODERATE
    if snap.rtt_ms < 200 and snap.loss_rate < 0.15:
        return NetworkCondition.CONGESTED
    return NetworkCondition.CRITICAL


def profile_data(data: bytes) -> DataProfile:
    if len(data) == 0:
        return DataProfile.BINARY

    # Already-compressed magic bytes
    COMPRESSED_MAGIC = [
        b'\x1f\x8b',            # gzip
        b'PK',                   # zip
        b'\xfd7zXZ',            # xz
        b'\x28\xb5\x2f\xfd',   # zstd
        b'BZh',                  # bz2
        b'\x04\x22\x4d\x18',   # lz4 frame
        b'\xff\x06\x00\x00',   # lz4 legacy
    ]
    for magic in COMPRESSED_MAGIC:
        if data[:len(magic)] == magic:
            return DataProfile.COMPRESSED

    # JSON
    stripped = data[:20].lstrip()
    if stripped and stripped[0] in (ord('{'), ord('[')):
        return DataProfile.JSON

    # Text: high printable ASCII ratio
    printable = sum(1 for b in data[:256] if 32 <= b <= 126 or b in (9, 10, 13))
    if printable / min(len(data), 256) > 0.85:
        return DataProfile.TEXT

    # Entropy check
    freq = [0] * 256
    sample = data[:512]
    for b in sample:
        freq[b] += 1
    nonzero = sum(1 for f in freq if f > 0)
    if nonzero < 64:
        return DataProfile.BINARY

    return DataProfile.MIXED


def _compute_weights(condition: NetworkCondition, profile: DataProfile) -> dict:
    base = {
        NetworkCondition.EXCELLENT: (0.2, 0.6, 0.1, 0.1),
        NetworkCondition.GOOD:      (0.4, 0.4, 0.1, 0.1),
        NetworkCondition.MODERATE:  (0.5, 0.3, 0.1, 0.1),
        NetworkCondition.CONGESTED: (0.65, 0.2, 0.05, 0.1),
        NetworkCondition.CRITICAL:  (0.75, 0.1, 0.05, 0.1),
    }[condition]

    w_ratio, w_speed, w_cpu, w_loss = base

    if profile == DataProfile.COMPRESSED:
        w_ratio, w_speed = 0.0, 1.0
    elif profile in (DataProfile.TEXT, DataProfile.JSON):
        w_ratio += 0.1
        w_speed -= 0.1

    return {"ratio": w_ratio, "speed": w_speed, "cpu": w_cpu, "loss": w_loss}


class AdaptiveEngine:
    def __init__(self, probe: BaseProbe | None = None):
        self._probe           = probe or RealNetworkProbe()
        self._lock            = threading.Lock()
        self._history: deque[EngineDecision] = deque(maxlen=500)
        self._current_algo_id = 2
        self._switch_count    = 0
        self._running         = False
        self.switch_threshold = 0.05

    def start(self):
        self._probe.start()
        self._running = True
        return self

    def stop(self):
        self._running = False
        self._probe.stop()

    def decide(self, data: bytes) -> tuple[BaseAlgorithm, EngineDecision]:
        snap      = self._probe.snapshot()
        condition = classify_condition(snap)
        profile   = profile_data(data)
        weights   = _compute_weights(condition, profile)

        scores: dict[int, float] = {}
        for algo_id, algo in ALGORITHM_REGISTRY.items():
            score = (
                weights["ratio"] * algo.ratio_score +
                weights["speed"] * algo.speed_score -
                weights["cpu"]   * algo.cpu_cost -
                weights["loss"]  * algo.ratio_score * snap.loss_rate * 10
            )
            scores[algo_id] = score

        best_id = max(scores, key=lambda k: scores[k])

        with self._lock:
            current_score = scores.get(self._current_algo_id, 0)
            if scores[best_id] - current_score < self.switch_threshold:
                best_id = self._current_algo_id
            elif best_id != self._current_algo_id:
                self._switch_count += 1
                self._current_algo_id = best_id

        algo = ALGORITHM_REGISTRY[best_id]

        reasons = {
            NetworkCondition.EXCELLENT: "Excellent link — prioritising speed with lz4",
            NetworkCondition.GOOD:      "Good conditions — balanced ratio/speed",
            NetworkCondition.MODERATE:  "Moderate load — shifting weight to ratio",
            NetworkCondition.CONGESTED: "Congested link — maximising compression",
            NetworkCondition.CRITICAL:  "Critical link — highest ratio to save every byte",
        }
        reason = ("Data already compressed — passthrough"
                  if profile == DataProfile.COMPRESSED
                  else reasons[condition])

        decision = EngineDecision(
            algo_id      = best_id,
            algo_name    = algo.name,
            condition    = condition,
            data_profile = profile,
            score        = scores[best_id],
            snapshot     = snap,
            scores       = {ALGORITHM_REGISTRY[k].name: v for k, v in scores.items()},
            reason       = reason,
        )
        with self._lock:
            self._history.append(decision)
        return algo, decision

    def stats(self) -> dict:
        with self._lock:
            hist = list(self._history)
        if not hist:
            return {}
        latest = hist[-1]
        algo_counts: dict[str, int] = {}
        for d in hist:
            algo_counts[d.algo_name] = algo_counts.get(d.algo_name, 0) + 1
        return {
            "total_decisions":  len(hist),
            "switch_count":     self._switch_count,
            "current_algo":     latest.algo_name,
            "current_condition": latest.condition.value,
            "algo_distribution": algo_counts,
            "latest_decision":  latest.to_dict(),
        }

    def recent_decisions(self, n: int = 50) -> list[dict]:
        with self._lock:
            return [d.to_dict() for d in list(self._history)[-n:]]
