import struct

from waltz.frames import (
    PrimaryKeepalive,
    XLogData,
    parse_keepalive,
    parse_xlogdata,
)


# These two builders below produce the real wire format(big-endian) Postgres sends
def _xlogdata_frame(*, data_start: int, wal_end: int, server_time: int, payload:bytes) -> bytes:
    return b"w" + struct.pack(">QQq", data_start, wal_end, server_time) + payload


def _keepalive_frame(*, wal_end: int, server_time: int, reply_requested: bool) -> bytes:
    return b"k" + struct.pack(">QqB", wal_end, server_time, 1 if reply_requested else 0)


def test_parse_xlogdata_reads_header_and_keeps_payload():
    frame = _xlogdata_frame(
        data_start=0x16B3748,
        wal_end=0x16B3800,
        server_time=123_456,
        payload=b"I-am-an-untouched-pgoutput-message"
    )
    assert parse_xlogdata(frame) == XLogData(
        data_start=0x16B3748,
        wal_end=0x16B3800,
        server_time=123_456,
        payload=b"I-am-an-untouched-pgoutput-message",
    )


def test_parse_xlogdata_handles_empty_payload():
    # The 24-byte header ends at offset 25; an empty payload must parse cleanly.
    frame = _xlogdata_frame(data_start=1, wal_end=2, server_time=3, payload=b"")
    assert parse_xlogdata(frame).payload == b""


def test_parse_keepalive_reads_all_fields():
    frame = _keepalive_frame(wal_end=0x16B3748, server_time=123_456,
                             reply_requested=True)
    assert parse_keepalive(frame) == PrimaryKeepalive(
        wal_end=0x16B3748,
        server_time=123_456,
        reply_requested=True,
    )


def test_parse_keepalive_reply_flag_is_a_real_bool():
    # The dataclass annotates reply_requested: bool, so the parser must convert the
    # raw 0/1 int. Note: '==' would NOT catch this (1 == True), only 'is' does.
    on = parse_keepalive(_keepalive_frame(wal_end=1, server_time=2,
                                          reply_requested=True))
    off = parse_keepalive(_keepalive_frame(wal_end=1, server_time=2,
                                           reply_requested=False))
    assert on.reply_requested is True
    assert off.reply_requested is False
