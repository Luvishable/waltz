import struct

import waltz.feedback as feedback
from waltz.feedback import build_standby_status_update

# Mirror of the module's own wire layout, so the test decodes what it encodes.
_LAYOUT = struct.Struct(">cQQQqB")


def test_status_update_is_well_formed():
    msg = build_standby_status_update(
        write_lsn=0x16B3748,
        flush_lsn=0x16B3748,
        apply_lsn=0x16B3748,
        clock_micros=42,
    )
    assert len(msg) == 34
    tag, write, flush, apply, clock, reply = _LAYOUT.unpack(msg)
    assert tag == b"r"
    assert (write, flush, apply) == (0x16B3748, 0x16B3748, 0x16B3748)
    assert clock == 42
    assert reply == 0


def test_distinct_lsns_map_to_their_own_fields():
    # write/flush/apply are three separate Int64s; prove the function never swaps them.
    msg = build_standby_status_update(
        write_lsn=0xAAAA, flush_lsn=0xBBBB, apply_lsn=0xCCCC, clock_micros=0
    )
    _, write, flush, apply, _, _ = _LAYOUT.unpack(msg)
    assert (write, flush, apply) == (0xAAAA, 0xBBBB, 0xCCCC)


def test_reply_requested_flips_the_last_byte():
    on = build_standby_status_update(
        write_lsn=1, flush_lsn=1, apply_lsn=1, reply_requested=True, clock_micros=0
    )
    off = build_standby_status_update(
        write_lsn=1, flush_lsn=1, apply_lsn=1, reply_requested=False, clock_micros=0
    )
    assert on[-1] == 1
    assert off[-1] == 0


def test_default_clock_is_taken_from_now_micros(monkeypatch):
    # When clock_micros is omitted, the function calls now_micros().
    # CRUCIAL: feedback.py did `from waltz.pgtime import now_micros`, which binds the
    # name into the waltz.feedback namespace. So we patch waltz.feedback.now_micros,
    # NOT waltz.pgtime.now_micros — patching the original module would have no effect.
    monkeypatch.setattr(feedback, "now_micros", lambda: 999)
    msg = build_standby_status_update(write_lsn=0, flush_lsn=0, apply_lsn=0)
    _, _, _, _, clock, _ = _LAYOUT.unpack(msg)
    assert clock == 999
