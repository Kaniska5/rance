"""
RANCE — Real Network Probe

Measures actual network conditions:
  - RTT via TCP connect timing to multiple hosts
  - Bandwidth estimate via sliding window of bytes/time
  - Packet loss via failed probe tracking
  - Jitter via RTT standard deviation
"""
import time
import socket
import threading
import statistics
from collections import deque
from dataclasses import dataclass, field


@dataclass
class NetworkSnapshot:
    timestamp:      float = field(default_factory=time.time)
    rtt_ms:         float = 10.0
    bandwidth_mbps: float = 100.0
    loss_rate:      float = 0.0
    jitter_ms:      float = 0.5
    cpu_load:       float = 0.1
    probe_host:     str   = ""

    def to_dict(self) -> dict:
        return {
            "timestamp":      round(self.timestamp, 3),
            "rtt_ms":         round(self.rtt_ms, 2),
            "bandwidth_mbps": round(self.bandwidth_mbps, 2),
            "loss_rate":      round(self.loss_rate, 4),
            "jitter_ms":      round(self.jitter_ms, 2),
            "cpu_load":       round(self.cpu_load, 3),
            "probe_host":     self.probe_host,
        }


class BaseProbe:
    def snapshot(self) -> NetworkSnapshot:
        raise NotImplementedError
    def start(self): pass
    def stop(self):  pass


class RealNetworkProbe(BaseProbe):
    """
    Real probe: measures actual RTT to public DNS/HTTP servers.
    Tracks sliding window of RTTs for jitter + loss calculation.
    Estimates bandwidth from a small HTTP fetch timing.
    """

    PROBE_TARGETS = [
        ("8.8.8.8",       53),   # Google DNS
        ("1.1.1.1",       53),   # Cloudflare DNS
        ("9.9.9.9",       53),   # Quad9 DNS
        ("208.67.222.222", 53),  # OpenDNS
    ]
    BW_TEST_URL  = "http://speedtest.tele2.net/100KB.zip"
    PROBE_INTERVAL = 1.0   # seconds between probes

    def __init__(self):
        self._rtts:        deque[float] = deque(maxlen=30)
        self._attempts:    int = 0
        self._failures:    int = 0
        self._bw_mbps:     float = 100.0
        self._last_bw_ts:  float = 0.0
        self._latest:      NetworkSnapshot = NetworkSnapshot()
        self._lock         = threading.Lock()
        self._running      = False
        self._target_idx   = 0

    def _probe_rtt(self) -> float | None:
        host, port = self.PROBE_TARGETS[self._target_idx % len(self.PROBE_TARGETS)]
        self._target_idx += 1
        try:
            t0 = time.perf_counter()
            s = socket.create_connection((host, port), timeout=2.0)
            rtt = (time.perf_counter() - t0) * 1000
            s.close()
            return rtt
        except Exception:
            return None

    def _estimate_bandwidth(self) -> float:
        """
        Estimate bandwidth by timing a small TCP data transfer.
        We open a connection to a known host and measure how fast
        we can push/receive a burst of data.
        Uses HTTP HEAD to a reliable server to get at least RTT-based estimate.
        Falls back to last known value on failure.
        """
        try:
            size_bytes = 10_000
            host = "httpbin.org"
            port = 80
            t0 = time.perf_counter()
            s = socket.create_connection((host, port), timeout=3.0)
            request = (
                f"GET /bytes/{size_bytes} HTTP/1.1\r\n"
                f"Host: {host}\r\n"
                f"Connection: close\r\n\r\n"
            ).encode()
            s.sendall(request)
            received = 0
            while True:
                chunk = s.recv(4096)
                if not chunk:
                    break
                received += len(chunk)
            elapsed = time.perf_counter() - t0
            s.close()
            if elapsed > 0 and received > 100:
                mbps = (received * 8) / elapsed / 1_000_000
                return min(max(mbps, 0.1), 1000.0)
        except Exception:
            pass
        return self._bw_mbps  # keep last known

    def snapshot(self) -> NetworkSnapshot:
        with self._lock:
            return self._latest

    def start(self):
        self._running = True
        threading.Thread(target=self._probe_loop,  daemon=True).start()
        threading.Thread(target=self._bw_loop,     daemon=True).start()

    def stop(self):
        self._running = False

    def _probe_loop(self):
        while self._running:
            self._attempts += 1
            rtt = self._probe_rtt()
            with self._lock:
                if rtt is not None:
                    self._rtts.append(rtt)
                else:
                    self._failures += 1
                    self._rtts.append(500.0)  # count as high-latency miss

                rtts = list(self._rtts)
                avg_rtt = statistics.mean(rtts) if rtts else 10.0
                jitter  = statistics.stdev(rtts) if len(rtts) > 1 else 0.5
                loss    = self._failures / max(self._attempts, 1)

                self._latest = NetworkSnapshot(
                    rtt_ms         = round(avg_rtt, 2),
                    bandwidth_mbps = round(self._bw_mbps, 2),
                    loss_rate      = round(min(loss, 1.0), 4),
                    jitter_ms      = round(jitter, 2),
                    cpu_load       = 0.1,
                    probe_host     = self.PROBE_TARGETS[(self._target_idx-1) % len(self.PROBE_TARGETS)][0],
                )
            time.sleep(self.PROBE_INTERVAL)

    def _bw_loop(self):
        """Refresh bandwidth estimate every 15 seconds."""
        while self._running:
            bw = self._estimate_bandwidth()
            with self._lock:
                self._bw_mbps = bw
                if self._latest:
                    self._latest.bandwidth_mbps = round(bw, 2)
            time.sleep(15.0)


class StreamBandwidthTracker:
    """
    Tracks actual bytes flowing through the compressor to
    give a real-time bandwidth estimate based on observed throughput.
    Call record(bytes) each time a chunk is sent.
    """
    def __init__(self, window_s: float = 3.0):
        self._window = window_s
        self._events: deque[tuple[float, int]] = deque()
        self._lock = threading.Lock()

    def record(self, nbytes: int):
        now = time.time()
        with self._lock:
            self._events.append((now, nbytes))
            cutoff = now - self._window
            while self._events and self._events[0][0] < cutoff:
                self._events.popleft()

    def mbps(self) -> float:
        now = time.time()
        with self._lock:
            cutoff = now - self._window
            total = sum(n for t, n in self._events if t >= cutoff)
            return total * 8 / self._window / 1_000_000
