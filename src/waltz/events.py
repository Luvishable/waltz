"""
Event vocabulary shared between the producer (decoder) and consumers (sinks).
"""

import enum
from dataclasses import dataclass
from datetime import datetime
from typing import Literal


class Sentinel(enum.Enum):
    """
    A special marker: different from both None and actual values.

    Why need that? pgoutput may mark a column as unchanged ('u') during UPDATEs. This
    means the value was not modified and was omitted from the WAL record because
    it is big not that it became NULL
    """

    UNCHANGED = "unchanged"

    def __repr__(self) -> str:
        return self.name


type Row = dict[str, str | None | Sentinel]

type Op = Literal["INSERT", "UPDATE", "DELETE"]


@dataclass(frozen=True, slots=True)
class ChangeEvent:
    """
    A single row change.
    """

    lsn: int  # commit LSN of the owning transaction
    schema: str
    table: str
    op: Op
    new: Row | None
    old: Row | None
    commit_time: datetime | None
