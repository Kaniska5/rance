"""
RANCE — Wire Protocol + Stream Reassembly Buffer

Frame format (binary, big-endian):
┌──────────┬───────────┬──────────┬─────────────────┬──────────┐
│ Magic(2) │ Version(1)│ AlgoID(1)│  Payload Len(4) │ CRC32(4) │
├──────────┴───────────┴──────────┴─────────────────┴──────────┤
│                   Compressed Payload (N bytes)                 │
└───────────────────────────────────────────────────────────────┘
Header = 12 bytes total. Receiver reads header to know algo + payload size.

StreamBuffer handles real TCP streams where frames may arrive split
across multiple recv() calls, or multiple frames in one recv().
"""
import struct
import zlib
from dataclasses import dataclass

MAGIC        = b'\x52\x4E'   # 'RN'
VERSION      = 0x01
HEADER_FMT   = '>2sBBI'      # magic(2) + version(1) + algo_id(1) + payload_len(4)
HEADER_SIZE  = struct.calcsize(HEADER_FMT)   # 8
CRC_SIZE     = 4
FRAME_OVERHEAD = HEADER_SIZE + CRC_SIZE      # 12


class FrameError(Exception):
    pass


@dataclass
class Frame:
    algo_id: int
    payload: bytes

    def encode(self) -> bytes:
        header = struct.pack(HEADER_FMT, MAGIC, VERSION, self.algo_id, len(self.payload))
        crc    = zlib.crc32(header + self.payload) & 0xFFFFFFFF
        return header + self.payload + struct.pack('>I', crc)

    @staticmethod
    def decode(raw: bytes) -> 'Frame':
        if len(raw) < FRAME_OVERHEAD:
            raise FrameError(f"Too short: {len(raw)} bytes")

        header = raw[:HEADER_SIZE]
        magic, version, algo_id, payload_len = struct.unpack(HEADER_FMT, header)

        if magic != MAGIC:
            raise FrameError(f"Bad magic: {magic!r}")
        if version != VERSION:
            raise FrameError(f"Unsupported version: {version}")

        expected = HEADER_SIZE + payload_len + CRC_SIZE
        if len(raw) < expected:
            raise FrameError(f"Truncated: need {expected}, got {len(raw)}")

        payload     = raw[HEADER_SIZE: HEADER_SIZE + payload_len]
        stored_crc  = struct.unpack('>I', raw[HEADER_SIZE + payload_len:
                                               HEADER_SIZE + payload_len + CRC_SIZE])[0]
        computed    = zlib.crc32(header + payload) & 0xFFFFFFFF
        if stored_crc != computed:
            raise FrameError(f"CRC mismatch: {stored_crc:#010x} vs {computed:#010x}")

        return Frame(algo_id=algo_id, payload=payload)

    @property
    def wire_size(self) -> int:
        return HEADER_SIZE + len(self.payload) + CRC_SIZE


class StreamBuffer:
    """
    Handles real TCP fragmentation.
    Feed raw bytes from recv() into feed(); extract complete frames with pop_frame().

    TCP can deliver:
      - half a frame header
      - a frame split across two recv() calls
      - two frames concatenated in one recv()
    This handles all three cases correctly.
    """
    def __init__(self):
        self._buf = bytearray()

    def feed(self, data: bytes):
        self._buf.extend(data)

    def pop_frame(self) -> Frame | None:
        """
        Return the next complete Frame if enough bytes are buffered,
        otherwise return None. Advances internal buffer past consumed bytes.
        """
        if len(self._buf) < FRAME_OVERHEAD:
            return None

        # Parse header to learn payload_len
        magic, version, algo_id, payload_len = struct.unpack(
            HEADER_FMT, bytes(self._buf[:HEADER_SIZE])
        )
        if magic != MAGIC:
            # Corrupted stream — scan forward for next magic
            idx = bytes(self._buf).find(MAGIC, 1)
            if idx == -1:
                self._buf.clear()
            else:
                del self._buf[:idx]
            return None

        total = HEADER_SIZE + payload_len + CRC_SIZE
        if len(self._buf) < total:
            return None  # wait for more data

        raw   = bytes(self._buf[:total])
        del self._buf[:total]
        return Frame.decode(raw)

    def pop_all(self) -> list[Frame]:
        frames = []
        while True:
            f = self.pop_frame()
            if f is None:
                break
            frames.append(f)
        return frames

    def pending_bytes(self) -> int:
        return len(self._buf)


def encode_frame(algo_id: int, payload: bytes) -> bytes:
    return Frame(algo_id=algo_id, payload=payload).encode()


def decode_frame(raw: bytes) -> Frame:
    return Frame.decode(raw)
