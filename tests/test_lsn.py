import pytest

from waltz.lsn import format_lsn, parse_lsn


@pytest.mark.parametrize(
    "lsn_int, lsn_text",
    [
        (0, "0/0"),
        (0x16B3748, "0/16B3748"),  # the doc's example LSN
        (0xFFFFFFFF, "0/FFFFFFFF"),  # max low half, high still 0
        (0x1_00000000, "1/0"),  # one unit into the high half
        (0xABCDEF12_34567890, "ABCDEF12/34567890"),
    ],
)
def test_format_matches_postgres_notation(lsn_int, lsn_text):
    assert format_lsn(lsn_int) == lsn_text


@pytest.mark.parametrize(
    "lsn",
    [0, 1, 0x16B3748, 0xFFFFFFFF, 0x1_00000000, (1 << 64) - 1],
)
def test_round_trip_int_to_text_to_int(lsn):
    assert parse_lsn(format_lsn(lsn)) == lsn


def test_parse_is_case_and_padding_insensitive():
    assert parse_lsn("0/16b3748") == parse_lsn("0/16B3748")
    assert parse_lsn("0/016B3748") == parse_lsn("0/16B3748")