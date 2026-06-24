import json
from unittest.mock import MagicMock, patch

import httpx
import pytest

from waltz.events import ChangeEvent, Sentinel
from waltz.sink import HttpSink, StdoutSink, build_sink


def _event(**over):
    base = {
        "lsn": 0x16B3748, "schema": "public", "table": "t", "op": "UPDATE",
        "new": {"id": "1"}, "old": {"id": "1"}, "commit_time": None,
        "idempotency_key": "public.t:1:0:0",
    }
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
    assert payload["idempotency_key"] == "public.t:1:0:0"


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


# ── HttpSink ──────────────────────────────────────────────────────────────────

def _mock_ok():
    mock = MagicMock()
    mock.raise_for_status.return_value = None
    return mock


def test_http_sink_write_does_not_post_immediately():
    sink = HttpSink("http://localhost/events")
    with patch("waltz.sink.httpx.post") as mock_post:
        sink.write(_event())
        mock_post.assert_not_called()


def test_http_sink_flush_sends_batch_as_json_array():
    sink = HttpSink("http://localhost/events")
    with patch("waltz.sink.httpx.post", return_value=_mock_ok()) as mock_post:
        sink.write(_event(op="INSERT"))
        sink.write(_event(op="DELETE"))
        sink.flush()
    mock_post.assert_called_once()
    payload = mock_post.call_args.kwargs["json"]
    assert len(payload) == 2
    assert payload[0]["op"] == "INSERT"
    assert payload[1]["op"] == "DELETE"
    assert all("idempotency_key" in e for e in payload)


def test_http_sink_clears_buffer_after_successful_flush():
    # A second flush must be a no-op after the first succeeds.
    sink = HttpSink("http://localhost/events")
    with patch("waltz.sink.httpx.post", return_value=_mock_ok()) as mock_post:
        sink.write(_event())
        sink.flush()
        sink.flush()
    mock_post.assert_called_once()


def test_http_sink_empty_flush_is_noop():
    sink = HttpSink("http://localhost/events")
    with patch("waltz.sink.httpx.post") as mock_post:
        sink.flush()
        mock_post.assert_not_called()


def test_http_sink_retains_buffer_on_error():
    # If the server returns an error the buffer must NOT be cleared so the
    # stream manager can retry the same batch on the next attempt.
    sink = HttpSink("http://localhost/events")
    mock_resp = MagicMock()
    mock_resp.raise_for_status.side_effect = httpx.HTTPStatusError(
        "500 Server Error", request=MagicMock(), response=mock_resp
    )
    with patch("waltz.sink.httpx.post", return_value=mock_resp):
        sink.write(_event())
        with pytest.raises(httpx.HTTPStatusError):
            sink.flush()
    assert len(sink._buffer) == 1


# ── build_sink factory ────────────────────────────────────────────────────────

def test_build_sink_stdout_returns_stdout_sink():
    assert isinstance(build_sink("stdout", None), StdoutSink)


def test_build_sink_http_returns_http_sink():
    assert isinstance(build_sink("http", "http://localhost/events"), HttpSink)


def test_build_sink_http_without_url_raises():
    with pytest.raises(RuntimeError, match=r"sink\.url"):
        build_sink("http", None)
