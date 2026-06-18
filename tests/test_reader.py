import struct

import pytest

from waltz.reader import Reader


def test_numeric_reads_in_sequence_respect_width_and_signedness():
    data = struct.pack(">BHIiQq", 1, 2, 3, -1, 5, -6)
    reader = Reader(data)

    assert reader.uint8() == 1
    assert reader.uint16() == 2
    assert reader.uint32() == 3
    assert reader.int32() == -1
    assert reader.uint64() == 5
    assert reader.int64() == -6


def test_char_reads_one_byte_as_str():
    reader = Reader(b"B")
    assert reader.char() == "B"


def test_string_stops_at_nul_and_advances_past_it():
    # pgoutput strings are C-style: NUL terminated. After readinf "public",
    # the cursor must sit just past the NUL, ready for the next field.
    reader = Reader(b"public\x00rest")
    assert reader.string() == "public"
    assert reader.read_bytes(4) == b"rest"


def test_empty_string_is_a_lone_nul():
    reader = Reader(b"\x00X")
    assert reader.string() == ""
    assert reader.char() == "X"


def test_string_decodes_utf8():
    reader = Reader("cafe\x00".encode())
    assert reader.string() == "cafe"


def test_read_bytes_advances_the_cursor():
    reader = Reader(b"abcdef")
    assert reader.read_bytes(3) == b"abc"
    assert reader.read_bytes(3) == b"def"


def test_string_without_terminator_raises():
    reader = Reader(b"noterminator")
    with pytest.raises(ValueError):
        reader.string()


