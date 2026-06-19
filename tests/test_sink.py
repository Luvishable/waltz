import json

from waltz.events import ChangeEvent, Sentinel
from waltz.sink import StdoutSink


def _event(**over):
    base = {"lsn": 0x16B3748, "schema": "public", "table": "t", "op": "UPDATE",
            "new": {"id": "1"}, "old": {"id": "1"}, "commit_time": None}
    base.update(over)
    return ChangeEvent(**base)


def test_writes_one_json_line_per_event(capsys):
    StdoutSink().write(_event())
    out = capsys.readouterr().out
    assert out.endswith("\n")
    assert out.count("\n") == 1
    payload = json.loads(out)
    assert payload["schema"] == "public"
    assert payload["table"] == "t"
    assert payload["op"] == "UPDATE"


def test_unchanged_is_distinct_from_null(capsys):
    # A real NULL and an unchanged-TOAST column must NOT collapse to the same JSON.
    StdoutSink().write(_event(new={"a": None, "b": Sentinel.UNCHANGED}))
    payload = json.loads(capsys.readouterr().out)
    assert payload["new"]["a"] is None           # real NULL
    assert payload["new"]["b"] == "<unchanged>"  # TOASTed, not modified


def test_missing_row_is_null(capsys):
    # INSERT has no old; DELETE has no new -> the absent side serializes to null.
    StdoutSink().write(_event(op="INSERT", new={"id": "1"}, old=None))
    payload = json.loads(capsys.readouterr().out)
    assert payload["old"] is None
