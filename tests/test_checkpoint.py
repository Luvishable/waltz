from waltz.checkpoint import FileCheckpoint


def test_read_missing_file_returns_none(tmp_path):
    assert FileCheckpoint(tmp_path / "waltz.lsn").read() is None


def test_write_then_read_round_trip(tmp_path):
    cp = FileCheckpoint(tmp_path / "waltz.lsn")
    cp.write(0x16B3748)
    assert cp.read() == 0x16B3748


def test_file_stores_human_readable_hex(tmp_path):
    path = tmp_path / "waltz.lsn"
    FileCheckpoint(path).write(0x16B3748)
    assert path.read_text() == "0/16B3748"


def test_empty_file_reads_as_none(tmp_path):
    path = tmp_path / "waltz.lsn"
    path.write_text("")
    assert FileCheckpoint(path).read() is None


def test_overwrite_advances_value(tmp_path):
    cp = FileCheckpoint(tmp_path / "waltz.lsn")
    cp.write(1)
    cp.write(0xFFFF)
    assert cp.read() == 0xFFFF


def test_write_leaves_no_temp_file(tmp_path):
    cp = FileCheckpoint(tmp_path / "waltz.lsn")
    cp.write(1)
    assert [p.name for p in tmp_path.iterdir()] == ["waltz.lsn"]
