"""
Replication stream frame parsers ( the outer envelope )

Logical replication delivers COPY_BOTH frames; the first byte is a tag:
    'w'    ->    XLogData : carries a pgoutput message in its payload
    'k'    ->    Primary keepalive : server's way of asking for a reply
    'r'    ->    Standby status update from client to server

This module decodes the incoming 'w' and 'k' frames into structures records which
means bytes in dataclass out.
The pgoutput payload inside an XLogData is left untouched for the decoder to handle.

Basically, it is the inbound sibling of feedback.py which builds the outbound 'r' frame
"""

import struct
from dataclasses import dataclass

# XLogData ('w'), from server to client, Big-endian (network byte order):
#    Byte1    'w'           message tag
#    Int64    dataStart     LSN where this chunk of WAL data begins
#    Int64    walEnd        current end of WAL on the server
#    Int64    serverTime    send time, microseconds since 2000-01-01 (signed)
#    ByteN    payload       the pgoutput message itself
# The fixed header after the tag is exactly 24 bytes = 8 + 8 + 8
_XLOGDATA_HEADER = struct.Struct("QQq")

# Primary keepalive ('k'), server -> client:
#    Byte1  'k'           message tag
#    Int64  walEnd        current end of WAL on the server
#    Int64  serverTime    send time, microseconds since 2000-01-01 (signed)
#    Byte1  replyFlag     1 = "reply now or I may drop you on timeout"
# After the tag this is 17 bytes = 8 + 8 + 1.
_KEEPALIVE = struct.Struct(">QqB")


@dataclass(frozen=True, slots=True)
class XLogData:
    data_start: int
    wal_end: int
    server_time: int
    payload: bytes


@dataclass(frozen=True, slots=True)
class PrimaryKeepalive:
    wal_end: int
    server_time: int
    reply_requested: bool


def parse_xlogdata(frame: bytes) -> XLogData:
    data_start, wal_end, server_time = _XLOGDATA_HEADER.unpack_from(frame, 1)
    payload = frame[25:]
    return XLogData(data_start, wal_end, server_time, payload)


def parse_keepalive(frame: bytes) -> PrimaryKeepalive:
    wal_end, server_time, reply_flag = _KEEPALIVE.unpack_from(frame, 1)
    return PrimaryKeepalive(wal_end, server_time, reply_flag)


































