"""
RANCE — TCP Proxy / Socket Interceptor

Sits between sender and receiver, compresses all traffic transparently.

Architecture:
  Client → [RANCE Proxy :listen_port] ──compressed──► [RANCE Proxy :fwd_port] → Server
                                        (TCP stream)

Usage:
  # Forward mode: compress traffic to a remote host
  proxy = RANCEProxy(listen_port=9000, forward_host="example.com", forward_port=80)
  proxy.start()

  # Now connect your client to localhost:9000 instead of example.com:80
  # RANCE compresses everything automatically

  # Loopback demo: sender and receiver on same machine
  from engine.proxy import demo_loopback
  demo_loopback()
"""
import asyncio
import threading
import time
import logging
from collections import deque

from .compressor  import RANCECompressor
from .compressor import RANCEDecompressor  # see bottom of this file
from .protocol    import StreamBuffer, FrameError

log = logging.getLogger("rance.proxy")


class RANCEProxy:
    """
    Async TCP proxy that compresses data from clients before forwarding,
    and decompresses responses before returning to clients.
    """
    def __init__(self,
                 listen_port:   int  = 9000,
                 forward_host:  str  = "127.0.0.1",
                 forward_port:  int  = 9001,
                 compress_upstream:   bool = True,
                 compress_downstream: bool = False):

        self.listen_port          = listen_port
        self.forward_host         = forward_host
        self.forward_port         = forward_port
        self.compress_upstream    = compress_upstream
        self.compress_downstream  = compress_downstream

        self._compressor   = RANCECompressor().start()
        self._decompressor = _RANCEDecompressor()
        self._connections  = 0
        self._bytes_in     = 0
        self._bytes_out    = 0
        self._loop: asyncio.AbstractEventLoop | None = None
        self._thread: threading.Thread | None = None
        self._running = False

    def start(self):
        self._running = True
        self._thread  = threading.Thread(target=self._run_loop, daemon=True)
        self._thread.start()
        time.sleep(0.3)  # let the event loop start
        return self

    def stop(self):
        self._running = False
        if self._loop:
            self._loop.call_soon_threadsafe(self._loop.stop)
        self._compressor.stop()

    def _run_loop(self):
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)
        self._loop.run_until_complete(self._serve())

    async def _serve(self):
        server = await asyncio.start_server(
            self._handle_client,
            "127.0.0.1", self.listen_port
        )
        log.info(f"RANCE proxy listening on 127.0.0.1:{self.listen_port} "
                 f"→ {self.forward_host}:{self.forward_port}")
        async with server:
            await server.serve_forever()

    async def _handle_client(self,
                              client_r: asyncio.StreamReader,
                              client_w: asyncio.StreamWriter):
        self._connections += 1
        peer = client_w.get_extra_info("peername")
        log.debug(f"New connection from {peer}")

        try:
            fwd_r, fwd_w = await asyncio.open_connection(
                self.forward_host, self.forward_port
            )
        except Exception as e:
            log.warning(f"Cannot connect to {self.forward_host}:{self.forward_port}: {e}")
            client_w.close()
            return

        # Pipe both directions concurrently
        await asyncio.gather(
            self._pipe(client_r, fwd_w,    compress=self.compress_upstream,   label="↑"),
            self._pipe(fwd_r,    client_w, compress=self.compress_downstream,  label="↓"),
            return_exceptions=True
        )
        fwd_w.close()
        client_w.close()

    async def _pipe(self,
                    reader: asyncio.StreamReader,
                    writer: asyncio.StreamWriter,
                    compress: bool,
                    label: str):
        buf = StreamBuffer()
        try:
            while True:
                chunk = await reader.read(65536)
                if not chunk:
                    break
                self._bytes_in += len(chunk)

                if compress:
                    # Compress and frame the chunk
                    out = await asyncio.get_event_loop().run_in_executor(
                        None, self._compressor.compress, chunk
                    )
                else:
                    # Decompress incoming framed data
                    buf.feed(chunk)
                    parts = []
                    for frame in buf.pop_all():
                        try:
                            parts.append(
                                await asyncio.get_event_loop().run_in_executor(
                                    None, self._decompressor.decompress_frame, frame
                                )
                            )
                        except FrameError as e:
                            log.warning(f"Frame error {label}: {e}")
                    out = b"".join(parts)

                if out:
                    self._bytes_out += len(out)
                    writer.write(out)
                    await writer.drain()
        except (asyncio.IncompleteReadError, ConnectionResetError):
            pass
        except Exception as e:
            log.debug(f"Pipe error {label}: {e}")

    def stats(self) -> dict:
        cs = self._compressor.stats()
        return {
            "connections":   self._connections,
            "bytes_in":      self._bytes_in,
            "bytes_out":     self._bytes_out,
            "compressor":    cs,
        }


class _RANCEDecompressor:
    """Lightweight decompressor for proxy use."""
    from .algorithms import ALGORITHM_REGISTRY as _REG

    def decompress_frame(self, frame) -> bytes:
        from .algorithms import ALGORITHM_REGISTRY
        algo = ALGORITHM_REGISTRY.get(frame.algo_id)
        if not algo:
            raise FrameError(f"Unknown algo_id: {frame.algo_id}")
        return algo.decompress(frame.payload)


# ─────────────────────────────────────────────────────────────────────────────
# Loopback demo server — echoes back whatever it receives (decompressed)
# ─────────────────────────────────────────────────────────────────────────────

class LoopbackEchoServer:
    """
    A simple TCP server that:
      - Receives RANCE-framed compressed data
      - Decompresses and echoes back the original bytes
    Used to demonstrate the full compress→transmit→decompress cycle.
    """
    def __init__(self, port: int = 9001):
        self.port = port
        self._loop:   asyncio.AbstractEventLoop | None = None
        self._thread: threading.Thread | None = None
        self._running = False
        self._received: deque[dict] = deque(maxlen=200)
        self._lock = threading.Lock()

    def start(self):
        self._running = True
        self._thread  = threading.Thread(target=self._run, daemon=True)
        self._thread.start()
        time.sleep(0.2)
        return self

    def stop(self):
        self._running = False
        if self._loop:
            self._loop.call_soon_threadsafe(self._loop.stop)

    def _run(self):
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)
        self._loop.run_until_complete(self._serve())

    async def _serve(self):
        server = await asyncio.start_server(
            self._handle, "127.0.0.1", self.port
        )
        async with server:
            await server.serve_forever()

    async def _handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        from .algorithms import ALGORITHM_REGISTRY
        from .protocol   import StreamBuffer, FrameError
        buf = StreamBuffer()
        try:
            while True:
                data = await reader.read(65536)
                if not data:
                    break
                buf.feed(data)
                for frame in buf.pop_all():
                    algo = ALGORITHM_REGISTRY.get(frame.algo_id)
                    if algo:
                        try:
                            original = algo.decompress(frame.payload)
                            with self._lock:
                                self._received.append({
                                    "algo_id":    frame.algo_id,
                                    "frame_size": frame.wire_size,
                                    "orig_size":  len(original),
                                    "ratio":      round(len(original)/max(frame.wire_size,1), 3),
                                    "ts":         time.time(),
                                })
                            writer.write(original)
                            await writer.drain()
                        except Exception as e:
                            log.warning(f"Decompress error: {e}")
        except Exception:
            pass
        writer.close()

    def received_log(self, n: int = 20) -> list[dict]:
        with self._lock:
            return list(self._received)[-n:]
