import struct

from waltz.config import StreamConfig
from waltz.events import ChangeEvent
from waltz.stream import StreamManager


def _xlogdata(payload, *, data_start=0, wal_end=0, server_time=0):
    return b"w" + struct.pack(">QQq", data_start, wal_end, server_time) + payload


def _keepalive(*, wal_end, reply_requested):
    return b"k" + struct.pack(">QqB", wal_end, 0, 1 if reply_requested else 0)


def _event(lsn):
    return ChangeEvent(lsn=lsn, schema="public", table="t", op="INSERT",
                       new={"id": "1"}, old=None, commit_time=None)


class FakeDecoder:
    def __init__(self, results):
        self._results = list(results)

    def feed(self, payload):
        return self._results.pop(0)


class FakeCheckpoint:
    def __init__(self, log, initial=None):
        self._log = log
        self.value = initial

    def read(self):
        return self.value

    def write(self, lsn):
        self.value = lsn
        self._log.append(("checkpoint", lsn))


class FakePgConn:
    def __init__(self, log):
        self._log = log

    def put_copy_data(self, msg):
        self._log.append(("put_copy_data", msg))

    def flush(self):
        self._log.append(("flush",))


def _manager(decoder_results, *, initial_lsn=None):
    log = []
    config = StreamConfig(host="h", port=1, user="u", password="p", dbname="d",
                          slot="s", publication="pub", checkpoint_path="x")
    manager = StreamManager(config, FakeCheckpoint(log, initial=initial_lsn),
                            FakeDecoder(decoder_results))
    manager._pgconn = FakePgConn(log)
    return manager, log


def test_structural_message_sends_no_feedback():
    manager, log = _manager([None])
    manager._handle_xlogdata(_xlogdata(b"begin"))
    assert log == []


def test_row_event_triggers_feedback():
    manager, log = _manager([_event(100)])
    manager._handle_xlogdata(_xlogdata(b"insert"))
    assert manager._last_lsn == 100
    assert ("checkpoint", 100) in log
    assert any(k == "put_copy_data" for k, *_ in log)


def test_feedback_does_not_go_backwards():
    manager, log = _manager([_event(100)])
    manager._last_lsn = 200
    manager._handle_xlogdata(_xlogdata(b"insert"))
    assert manager._last_lsn == 200
    assert log == []


def test_checkpoint_is_written_before_feedback():
    # waltz's correctness rule: durably checkpoint, THEN tell Postgres it may advance.
    manager, log = _manager([_event(100)])
    manager._handle_xlogdata(_xlogdata(b"insert"))
    kinds = [k for k, *_ in log]
    assert kinds.index("checkpoint") < kinds.index("put_copy_data")


def test_keepalive_without_reply_advances_lsn_silently():
    manager, log = _manager([])
    manager._handle_keepalive(_keepalive(wal_end=500, reply_requested=False))
    assert manager._last_lsn == 500
    assert log == []


def test_keepalive_with_reply_sends_feedback():
    manager, log = _manager([])
    manager._handle_keepalive(_keepalive(wal_end=500, reply_requested=True))
    assert manager._last_lsn == 500
    assert ("checkpoint", 500) in log


def test_keepalive_lsn_never_decreases():
    manager, _ = _manager([])
    manager._last_lsn = 800
    manager._handle_keepalive(_keepalive(wal_end=500, reply_requested=False))
    assert manager._last_lsn == 800
