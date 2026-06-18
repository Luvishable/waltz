"""
Checkpoint: durable memory of "how far have I processed".

- waltz's correctness rule: process the event durably, THEN send feedback so PostgreSQL
  may advance the slot.
- The checkpoint is the durable half of that rule on our side. It must survive a crash.
- Otherwise "I processed up to X" would be a lie and we could lose or double-count events.
"""

import os
from pathlib import Path
from typing import Protocol

from waltz.lsn import format_lsn, parse_lsn


class Checkpoint(Protocol):
    def read(self) -> int | None:
        """Last durably-stored LSN, or None if we've never checkpointed."""
        ...

    def write(self, lsn: int) -> None:
        """Durably store lsn. Crash-safe: a half-written file is forbidden."""
        ...


class FileCheckpoint:
    """
    Store the LSN in one small text file, in PostgreSQL's own "high/low" hex format
    in order to help debugging purposes.
    `cat` the file and compare it with the pg_replication_slots.confirmed_flush_lsn
    """

    def __init__(self, path: str | os.PathLike[str]) -> None:
        self._path = Path(path)

    def read(self) -> int | None:
        try:
            text = self._path.read_text().strip()
        except FileNotFoundError:
            return None  # no checkpoint yet
        if not text:
            return None
        return parse_lsn(text)

    def write(self, lsn: int) -> None:
        # Write a temp file, then atomically rename it over the real one.
        # os.replace() is atomic on POSIX: a reader sees either the whole old file
        # or the whole new one, never a half-written mix.
        tmp = self._path.with_name(self._path.name + ".tmp")
        with open(tmp, "w") as f:
            # creating python buffer and writing on it
            f.write(format_lsn(lsn))
            # data will be passed to the os ram. so basically the ownership passes to OS.
            f.flush()
            # from OS buffer (in RAM) to DISK without waiting.
            os.fsync(f.fileno())
        # atomic swap
        os.replace(tmp, self._path)
