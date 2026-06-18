"""
PostgreSQL time helpers.

On the replication wire, PG timestamps count microseconds since 2000-01-01 UTC, not the
unix epoch (1970).
These helpers make conversions in both directions.
"""

from datetime import UTC, datetime, timedelta

PG_EPOCH = datetime(2000, 1, 1, tzinfo=UTC)


def micros_to_datetime(micros: int) -> datetime:
    return PG_EPOCH + timedelta(microseconds=micros)


def datetime_to_micros(dt: datetime) -> int:
    return (dt - PG_EPOCH) // timedelta(microseconds=1)


def now_micros() -> int:
    # current moment as microseconds since PG epoch
    return datetime_to_micros(datetime.now(UTC))
