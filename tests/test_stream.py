import struct
from unittest.mock import patch

import psycopg

from waltz.config import StreamConfig
from waltz.events import ChangeEvent, Commit
from waltz.stream import StreamManager


def _xlogdata(payload, *, data_start=0, wal_end=0, server_time=0):
    return b"w" + struct.pack(">QQq", data_start, wal_end, server_time) + payload


def _keepalive(*, wal_end, reply_requested):
    return b"k" + struct.pack(">QqB", wal_end, 0, 1 if reply_requested else 0)


def _event(lsn):
    return ChangeEvent(lsn=lsn, schema="public", table="t", op="INSERT",
                       new={"id": "1"}, old=None, commit_time=None,
                       idempotency_key=f"public.t:1:{lsn}:0")


def _commit(end_lsn):
    return Commit(end_lsn=end_lsn, commit_time=None)


class FakeDecoder:
    def __init__(self, results):
        self._results = list(results)
        self.clear_count = 0

    def feed(self, payload):
        return self._results.pop(0)

    def clear(self):
        self.clear_count += 1


class FakeCheckpoint:
    def __init__(self, log, initial=None):
        self._log = log
        self.value = initial

    def read(self):
        return self.value

    def write(self, lsn):
        self.value = lsn
        self._log.append(("checkpoint", lsn))


class FakeSink:
    def __init__(self, log):
        self._log = log

    def write(self, event):
        self._log.append(("write", event.lsn))

    def flush(self):
        self._log.append(("sink_flush",))


class FakePgConn:
    def __init__(self, log):
        self._log = log

    def put_copy_data(self, msg):
        self._log.append(("put_copy_data", msg))

    def flush(self):
        self._log.append(("flush",))


def _manager(decoder_results, *, initial_lsn=None):
    log = []
    decoder = FakeDecoder(decoder_results)
    config = StreamConfig(host="h", port=1, user="u", password="p", dbname="d",
                          slot="s", publication="pub", checkpoint_path="x",
                          sink_type="stdout", sink_url=None)
    manager = StreamManager(config, FakeCheckpoint(log, initial=initial_lsn),
                            decoder, FakeSink(log))
    manager._pgconn = FakePgConn(log)
    return manager, log, decoder


def test_structural_message_does_nothing():
    # Begin/Relation -> feed() returns None -> no write, no feedback.
    manager, log, _ = _manager([None])
    manager._handle_xlogdata(_xlogdata(b"begin"))
    assert log == []


def test_change_event_is_written_but_not_confirmed():
    # A row is handed to the sink, but progress is NOT confirmed yet: the
    # transaction is not over, so it is not safe to advance the slot past it.
    manager, log, _ = _manager([_event(100)])
    manager._handle_xlogdata(_xlogdata(b"insert"))
    assert log == [("write", 100)]
    assert manager._last_lsn == 0


def test_commit_flushes_then_confirms():
    # At the commit boundary: flush the sink, then checkpoint, then tell Postgres.
    manager, log, _ = _manager([_commit(100)])
    manager._handle_xlogdata(_xlogdata(b"commit"))
    kinds = [k for k, *_ in log]
    assert kinds == ["sink_flush", "checkpoint", "put_copy_data", "flush"]
    assert manager._last_lsn == 100


def test_checkpoint_is_written_before_feedback():
    # waltz's correctness rule: durably checkpoint, THEN tell Postgres it may advance.
    manager, log, _ = _manager([_commit(100)])
    manager._handle_xlogdata(_xlogdata(b"commit"))
    kinds = [k for k, *_ in log]
    assert kinds.index("checkpoint") < kinds.index("put_copy_data")


def test_commit_feedback_does_not_go_backwards():
    # The sink is still flushed, but nothing at/below our high-water mark is confirmed.
    manager, log, _ = _manager([_commit(100)])
    manager._last_lsn = 200
    manager._handle_xlogdata(_xlogdata(b"commit"))
    assert log == [("sink_flush",)]
    assert manager._last_lsn == 200


def test_multi_row_transaction_confirms_once_at_commit():
    # The regression: two rows then one commit. Each row is only written; the single
    # feedback comes at the commit, after BOTH rows are durably flushed. Previously we
    # confirmed after the first row and could lose later rows on a crash.
    manager, log, _ = _manager([_event(100), _event(100), _commit(100)])
    manager._handle_xlogdata(_xlogdata(b"insert1"))
    manager._handle_xlogdata(_xlogdata(b"insert2"))
    assert log == [("write", 100), ("write", 100)]  # no confirm yet

    manager._handle_xlogdata(_xlogdata(b"commit"))
    kinds = [k for k, *_ in log]
    assert kinds == ["write", "write", "sink_flush", "checkpoint", "put_copy_data", "flush"]


def test_keepalive_without_reply_advances_lsn_silently():
    manager, log, _ = _manager([])
    manager._handle_keepalive(_keepalive(wal_end=500, reply_requested=False))
    assert manager._last_lsn == 500
    assert log == []


def test_keepalive_with_reply_sends_feedback():
    manager, log, _ = _manager([])
    manager._handle_keepalive(_keepalive(wal_end=500, reply_requested=True))
    assert manager._last_lsn == 500
    assert ("checkpoint", 500) in log


def test_keepalive_lsn_never_decreases():
    manager, _, __ = _manager([])
    manager._last_lsn = 800
    manager._handle_keepalive(_keepalive(wal_end=500, reply_requested=False))
    assert manager._last_lsn == 800


# --- reconnect / retry loop tests ---

def test_run_stops_cleanly_on_keyboard_interrupt():
    # KeyboardInterrupt must exit the loop gracefully, not propagate.
    manager, _, __ = _manager([])

    def fake_connect():
        raise KeyboardInterrupt

    with patch.object(manager, "_connect_and_stream", fake_connect):
        manager.run()  # must return, not raise


def test_run_clears_decoder_on_reconnect():
    # After a connection failure, decoder.clear() must be called before the next attempt.
    manager, _, decoder = _manager([])
    call_count = 0

    def fake_connect():
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            raise psycopg.OperationalError("connection refused")
        raise KeyboardInterrupt

    with (
        patch.object(manager, "_connect_and_stream", fake_connect),
        patch("waltz.stream.time.sleep"),
    ):
        manager.run()

    assert decoder.clear_count >= 1


def test_run_backoff_doubles_on_repeated_failure():
    # Each consecutive failure should double the sleep duration up to _MAX_BACKOFF.
    manager, _, __ = _manager([])
    sleep_calls = []
    call_count = 0

    def fake_connect():
        nonlocal call_count
        call_count += 1
        if call_count >= 3:
            raise KeyboardInterrupt
        raise psycopg.OperationalError("down")

    with (
        patch.object(manager, "_connect_and_stream", fake_connect),
        patch("waltz.stream.time.sleep", side_effect=lambda s: sleep_calls.append(s)),
    ):
        manager.run()

    assert len(sleep_calls) == 2
    assert sleep_calls[1] == sleep_calls[0] * 2
