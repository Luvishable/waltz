from datetime import UTC, datetime, timedelta

import pytest

from waltz.pgtime import (
    PG_EPOCH,
    datetime_to_micros,
    micros_to_datetime,
    now_micros,
)


def test_pg_epoch_anchors_at_zero():
    # The whole module hinges on this: 0 micros == 2000-01-01 UTC, not 1970
    assert PG_EPOCH == datetime(2000, 1, 1, tzinfo=UTC)
    assert micros_to_datetime(0) == PG_EPOCH
    assert datetime_to_micros(PG_EPOCH) == 0


def test_known_offset():
    one_second = 1_000_000
    assert micros_to_datetime(one_second) == PG_EPOCH + timedelta(seconds=1)


@pytest.mark.parametrize(
    "micros",
    [0, 1, 1_000_000, 123_456_789, 800_000_000_000_000],
)
def test_round_trip_micros(micros):
    # micros -> datetime -> micros is lossless at microsecond resolution
    assert datetime_to_micros(micros_to_datetime(micros)) == micros


def test_now_micros_is_a_positive_int_after_epoch():
    value = now_micros()
    assert isinstance(value, int)
    assert value > 0



