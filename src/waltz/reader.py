"""
Pure byte cursor for parsing pgoutput / replication wire messagres.

A Reader wraps an immutable bytes buffer plus an internal offset. Each read method
pulls one value of a specific wire type from the current offset, then advances
the offset past it. Callers never touch the offset: they just call reads in the
order the message defines its fields. All the "where am I in the vuffer" bookkeeping"
libes here, instead of scattered slice math in the decoder.
"""

import struct


class Reader:
    """
    A forward-only cursor over a bytes buffer.
    """

    def __init__(self, data: bytes) -> None:
        self._data = data  # the buffer we read from; never mutated
        self._offset = 0  # how many bytes already consumed

    def _read(self, fmt: str) -> int:
        value: int = struct.unpack_from(fmt, self._data, self._offset)[0]
        # calcsize(fmt) returns the fmt's byte width; step past what we just read
        self._offset += struct.calcsize(fmt)
        return value

    def read_bytes(self, n: int) -> bytes:
        # pull n raw bytes from the cursor and advance past them.
        # Used for TupleData text value: a length-prefixed run of bytes
        # whose length we already read seperately.
        chunk = self._data[self._offset : self._offset + n]
        self._offset += n
        return chunk

    def uint8(self) -> int:
        return self._read(">B")  # B = unsigned char,    1B (column flags)

    def uint16(self) -> int:
        return self._read(">H")  # H = unsigned short,   2B (column count)

    def uint32(self) -> int:
        return self._read(">I")  # I = unsigned int,     4B (OID, xid)

    def int32(self) -> int:
        return self._read(">i")  # i = SIGNED int,       4B (atttypemod, can be -1)

    def uint64(self) -> int:
        return self._read(">Q")  # Q = unsigned long long, 8B (LSN)

    def int64(self) -> int:
        return self._read(">q")  # q = SIGNED long long, 8B (pg timestamp)

    def char(self) -> str:
        # A singl-byte tag read as a 1-char string ('B', 'R', 'I', 'C')
        value = chr(self._data[self._offset])
        self._offset += 1
        return value

    def string(self) -> str:
        # pgoutput strings are C-style which means null-terminated
        # index(0, start) finds the first 0x00 at or after the current offset
        end = self._data.index(0, self._offset)
        text = self._data[self._offset : end].decode("utf-8")
        self._offset = end + 1
        return text
