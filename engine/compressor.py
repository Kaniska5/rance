"""
RANCE — Stream Compressor & Decompressor
Wraps the adaptive engine. Returns real compression stats.
"""
import time
import threading
from dataclasses import dataclass, field
from collections import deque

from .algorithms import ALGORITHM_REGISTRY
from .adaptive import AdaptiveEngine
from .probe import RealNetworkProbe, BaseProbe, StreamBandwidthTracker
from .protocol import encode_frame, decode_frame, FrameError, FRAME_OVERHEAD


@dataclass
class CompressionStats:
    chunks:           int   = 0
    bytes_in:         int   = 0
    bytes_out:        int   = 0
    total_latency_ns: int   = 0
    algo_counts:      dict  = field(default_factory=dict)
    errors:           int   = 0
    start_time:       float = field(default_factory=time.time)

    @property
    def ratio(self) -> float:
        return self.bytes_in / max(self.bytes_out, 1)

    @property
    def savings_pct(self) -> float:
        return max(0, (1 - self.bytes_out / max(self.bytes_in, 1)) * 100)

    @property
    def avg_latency_ms(self) -> float:
        return self.total_latency_ns / max(self.chunks, 1) / 1e6

    @property
    def throughput_mbps(self) -> float:
        elapsed = time.time() - self.start_time
        return self.bytes_in / max(elapsed, 0.001) / 1e6 * 8

    def to_dict(self) -> dict:
        return {
            "chunks":         self.chunks,
            "bytes_in":       self.bytes_in,
            "bytes_out":      self.bytes_out,
            "ratio":          round(self.ratio, 3),
            "savings_pct":    round(self.savings_pct, 2),
            "avg_latency_ms": round(self.avg_latency_ms, 3),
            "throughput_mbps": round(self.throughput_mbps, 3),
            "algo_counts":    self.algo_counts,
            "errors":         self.errors,
        }


class RANCECompressor:
    def __init__(self, probe: BaseProbe | None = None):
        self._probe   = probe or RealNetworkProbe()
        self._engine  = AdaptiveEngine(probe=self._probe)
        self._stats   = CompressionStats()
        self._bw      = StreamBandwidthTracker()
        self._lock    = threading.Lock()
        self._recent: deque[dict] = deque(maxlen=200)

    def start(self):
        self._engine.start()
        return self

    def stop(self):
        self._engine.stop()

    def compress(self, data: bytes) -> bytes:
        t0 = time.perf_counter_ns()
        algo, decision = self._engine.decide(data)
        result = algo.compress(data)

        # If compression expanded the data, use passthrough
        passthrough = ALGORITHM_REGISTRY[0]
        if result.ratio < 1.0 and algo.algo_id != 0:
            algo   = passthrough
            result = passthrough.compress(data)

        framed  = encode_frame(algo.algo_id, result.data)
        elapsed = time.perf_counter_ns() - t0

        self._bw.record(len(framed))

        # Fill in actual measured values on the decision
        decision.original_size     = len(data)
        decision.compressed_size   = len(framed)
        decision.actual_ratio      = len(data) / max(len(framed), 1)
        decision.actual_latency_ms = elapsed / 1e6

        with self._lock:
            s = self._stats
            s.chunks          += 1
            s.bytes_in        += len(data)
            s.bytes_out       += len(framed)
            s.total_latency_ns += elapsed
            s.algo_counts[algo.name] = s.algo_counts.get(algo.name, 0) + 1

            self._recent.append({
                "chunk":      s.chunks,
                "algo":       algo.name,
                "algo_id":    algo.algo_id,
                "in":         len(data),
                "out":        len(framed),
                "ratio":      round(len(data) / max(len(framed), 1), 3),
                "savings_pct": round((1 - len(framed)/max(len(data),1))*100, 1),
                "latency_ms": round(elapsed / 1e6, 3),
                "condition":  decision.condition.value,
                "data_profile": decision.data_profile.value,
                "reason":     decision.reason,
                "scores":     decision.scores,
                "network":    decision.snapshot.to_dict(),
                "ts":         round(decision.timestamp, 3),
            })

        return framed

    def stats(self) -> dict:
        with self._lock:
            base = self._stats.to_dict()
            base["engine"] = self._engine.stats()
            base["live_bandwidth_mbps"] = round(self._bw.mbps(), 3)
        return base

    def recent(self, n: int = 50) -> list[dict]:
        with self._lock:
            return list(self._recent)[-n:]


class RANCEDecompressor:
    def __init__(self):
        self._total_in  = 0
        self._total_out = 0
        self._errors    = 0
        self._lock      = threading.Lock()

    def decompress(self, framed: bytes) -> bytes:
        frame = decode_frame(framed)
        algo  = ALGORITHM_REGISTRY.get(frame.algo_id)
        if algo is None:
            raise FrameError(f"Unknown algo_id: {frame.algo_id}")
        result = algo.decompress(frame.payload)
        with self._lock:
            self._total_in  += len(framed)
            self._total_out += len(result)
        return result

    def stats(self) -> dict:
        with self._lock:
            return {
                "bytes_received":    self._total_in,
                "bytes_decompressed": self._total_out,
                "expansion_ratio":   round(self._total_out / max(self._total_in, 1), 3),
            }
