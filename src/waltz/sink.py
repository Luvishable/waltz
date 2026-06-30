"""
Sink is where captured changes are delivered.
A sink is the pluggable destination behind a tiny contract, so adding a new target
(HTTP, Redis, Kafka) means writing a new Sink.

The two-method contract is what makes correct delivery possible:
- write(event): hand an event over. The sink may buffer it; nothing is durable yet.
- flush(): make everything handed over so far durable / actually sent.

The stream manager calls write(...) for every row in a transaction, then flush() once
at the commit boundary, and only after a successful flush confirm progress to PG.
"""

import json
import sys
from typing import Protocol

import aiohttp

from waltz.events import ChangeEvent, Row, Sentinel
from waltz.lsn import format_lsn
from waltz.errors import ConfigError, PermanentSinkError, TransientSinkError


class Sink(Protocol):
    async def write(self, event: ChangeEvent) -> None: ...
    async def flush(self) -> None: ...


class StdoutSink:
    """
    Development sink: print each event as one JSON line to stdout.

    Not a durable target though; it exists to display the flow.
    """

    async def write(self, event: ChangeEvent) -> None:
        sys.stdout.write(json.dumps(_to_jsonable(event)) + "\n")

    async def flush(self) -> None:
        sys.stdout.flush()


class HttpSink:

    def __init__(
            self,
            url: str,
            timeout: float = 10.0
    ) -> None:
        self._url = url
        self._timeout = aiohttp.ClientTimeout(total=timeout)
        self._buffer: list[ChangeEvent] = []

    async def write(self, event: ChangeEvent) -> None:
        self._buffer.append(event)

    async def flush(self) -> None:
        if not self._buffer:
            return
        payload = [_to_jsonable(e) for e in self._buffer]
        try:
            async with aiohttp.ClientSession(timeout=self._timeout) as session:
                resp = await session.post(self._url, json=payload)
                resp.raise_for_status()
        except aiohttp.ClientResponseError as e:
            if e.status < 500:
                raise PermanentSinkError(f"HTTP {e.status}: {e.message}") from e
            raise TransientSinkError(str(e)) from e
        except aiohttp.ClientError as e:
            raise TransientSinkError(str(e)) from e
        self._buffer.clear()

def build_sink(sink_type: str, sink_url: str | None) -> Sink:
    if sink_type == "http":
        if not sink_url:
            raise ConfigError("sink.url is required when sink.type = http")
        return HttpSink(sink_url)
    return StdoutSink()


def _to_jsonable(event: ChangeEvent) -> dict[str, object]:
    # ChangeEvent holds types that JSON can't render directly: an int LSN we prefer
    # to show in PG's own hi/lo hex (matches the checkpoint file), a datetime and Rows
    return {
        "idempotency_key": event.idempotency_key,
        "lsn": format_lsn(event.lsn),
        "schema": event.schema,
        "table": event.table,
        "op": event.op,
        "new": _row_to_jsonable(event.new),
        "old": _row_to_jsonable(event.old),
        "commit_time": event.commit_time.isoformat() if event.commit_time else None,
    }

def _row_to_jsonable(row: Row | None) -> dict[str, object] | None:
    if row is None:
        return None
    return {
        name: ("<unchanged>" if value is Sentinel.UNCHANGED else value)
        for name, value in row.items()
    }


































