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

from waltz.events import ChangeEvent, Row, Sentinel
from waltz.lsn import format_lsn


class Sink(Protocol):
    def write(self, event: ChangeEvent) -> None:
        """
        Hand one event to the sink.
        """
        ...

    def flush(self) -> None:
        """
        Make everything written so fardurable. Must raise on failure.
        """
        ...


class StdoutSink:
    """
    Development sink: print each event as one JSON line to stdout.

    Not a durable target though; it exists to display the flow.
    """

    def write(self, event: ChangeEvent) -> None:
        sys.stdout.write(json.dumps(_to_jsonable(event)) + "\n")

    def flush(self) -> None:
        sys.stdout.flush()


def _to_jsonable(event: ChangeEvent) -> dict[str, object]:
    # ChangeEvent holds types that JSON can't render directly: an int LSN we prefer
    # to show in PG's own hi/lo hex (matches the checkpoint file), a datetime and Rows
    return {
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


































