"""
RANCE — Real Compression Algorithm Pool
Uses actual lz4, zstandard, brotli libraries (not stdlib fallbacks).
"""
import time
import zlib

import lz4.frame
import zstandard as zstd
import brotli


class CompressionResult:
    def __init__(self, data: bytes, algo_id: int, original_size: int, elapsed_ns: int):
        self.data         = data
        self.algo_id      = algo_id
        self.original_size   = original_size
        self.compressed_size = len(data)
        self.elapsed_ns   = elapsed_ns
        self.ratio        = original_size / max(len(data), 1)

    def __repr__(self):
        return (f"<Result algo={self.algo_id} "
                f"ratio={self.ratio:.2f}x "
                f"latency={self.elapsed_ns/1e6:.2f}ms>")


class BaseAlgorithm:
    algo_id:     int   = 0
    name:        str   = "base"
    speed_score: float = 1.0
    ratio_score: float = 1.0
    cpu_cost:    float = 1.0

    def compress(self, data: bytes) -> CompressionResult:
        t0 = time.perf_counter_ns()
        compressed = self._compress(data)
        elapsed = time.perf_counter_ns() - t0
        return CompressionResult(compressed, self.algo_id, len(data), elapsed)

    def decompress(self, data: bytes) -> bytes:
        return self._decompress(data)

    def _compress(self, data: bytes) -> bytes:
        raise NotImplementedError

    def _decompress(self, data: bytes) -> bytes:
        raise NotImplementedError

    def benchmark(self, sample: bytes) -> dict:
        result = self.compress(sample)
        return {
            "name":       self.name,
            "algo_id":    self.algo_id,
            "ratio":      round(result.ratio, 3),
            "latency_ms": round(result.elapsed_ns / 1e6, 3),
            "speed_mbps": round(len(sample) / max(result.elapsed_ns, 1) * 1000, 2),
        }


class NoCompression(BaseAlgorithm):
    """Passthrough — used when data is already compressed or incompressible."""
    algo_id      = 0
    name         = "none"
    speed_score  = 5.0
    ratio_score  = 1.0
    cpu_cost     = 0.0

    def _compress(self, data: bytes) -> bytes:
        return data

    def _decompress(self, data: bytes) -> bytes:
        return data


class LZ4Fast(BaseAlgorithm):
    """
    Real LZ4 — ultra-low latency, modest ratio.
    Best when bandwidth is ample and latency matters most.
    """
    algo_id      = 1
    name         = "lz4-fast"
    speed_score  = 5.0
    ratio_score  = 1.8
    cpu_cost     = 0.2

    def _compress(self, data: bytes) -> bytes:
        if not data:
            return data
        return lz4.frame.compress(data, compression_level=0)

    def _decompress(self, data: bytes) -> bytes:
        if not data:
            return data
        return lz4.frame.decompress(data)


class ZstdBalanced(BaseAlgorithm):
    """
    Real Zstd level 3 — best balance of ratio and speed.
    The adaptive default for normal conditions.
    """
    algo_id      = 2
    name         = "zstd-balanced"
    speed_score  = 3.5
    ratio_score  = 3.2
    cpu_cost     = 0.5

    def __init__(self):
        self._cctx = zstd.ZstdCompressor(level=3)
        self._dctx = zstd.ZstdDecompressor()

    def _compress(self, data: bytes) -> bytes:
        if not data:
            return data
        return self._cctx.compress(data)

    def _decompress(self, data: bytes) -> bytes:
        if not data:
            return data
        return self._dctx.decompress(data)


class ZstdMax(BaseAlgorithm):
    """
    Real Zstd level 9 — high ratio, more CPU.
    Used when bandwidth is constrained but not critical.
    """
    algo_id      = 3
    name         = "zstd-max"
    speed_score  = 2.0
    ratio_score  = 4.0
    cpu_cost     = 1.2

    def __init__(self):
        self._cctx = zstd.ZstdCompressor(level=9)
        self._dctx = zstd.ZstdDecompressor()

    def _compress(self, data: bytes) -> bytes:
        if not data:
            return data
        return self._cctx.compress(data)

    def _decompress(self, data: bytes) -> bytes:
        if not data:
            return data
        return self._dctx.decompress(data)


class BrotliMax(BaseAlgorithm):
    """
    Real Brotli quality=6 — highest ratio, highest CPU.
    Use only when bandwidth is severely constrained.
    """
    algo_id      = 4
    name         = "brotli-max"
    speed_score  = 1.0
    ratio_score  = 5.0
    cpu_cost     = 2.5

    def _compress(self, data: bytes) -> bytes:
        if not data:
            return data
        return brotli.compress(data, quality=6)

    def _decompress(self, data: bytes) -> bytes:
        if not data:
            return data
        return brotli.decompress(data)


ALGORITHM_REGISTRY: dict[int, BaseAlgorithm] = {
    algo.algo_id: algo() for algo in [
        NoCompression, LZ4Fast, ZstdBalanced, ZstdMax, BrotliMax
    ]
}
