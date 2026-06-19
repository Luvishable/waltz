import struct
from datetime import UTC, datetime

import pytest

from waltz.decoder import Decoder
from waltz.events import ChangeEvent, Sentinel
from waltz.pgtime import datetime_to_micros

# The running example from the notes: public.employees, OID 16385,
# columns id (key, int4) and name (non-key, text).
OID = 16385
EMP_COLUMNS = [(1, "id", 23, -1), (0, "name", 25, -1)]
COMMIT_TIME = datetime(2024, 1, 15, 12, 30, tzinfo=UTC)
COMMIT_MICROS = datetime_to_micros(COMMIT_TIME)


# pgoutput message builders: the inverse of what the decoder parses.
def _cstr(s):
    return s.encode("utf-8") + b"\x00"


def begin(*, final_lsn, commit_micros=0, xid=1):
    return b"B" + struct.pack(">QqI", final_lsn, commit_micros, xid)


def commit():
    return b"C"


def relation(*, oid, namespace, name, replica_identity, columns):
    out = b"R" + struct.pack(">I", oid) + _cstr(namespace) + _cstr(name)
    out += replica_identity.encode("ascii") + struct.pack(">H", len(columns))
    for flags, col_name, type_oid, type_mod in columns:
        out += struct.pack(">B", flags) + _cstr(col_name) + struct.pack(">Ii", type_oid, type_mod)
    return out


def _tuple_data(values):
    out = struct.pack(">H", len(values))
    for kind, payload in values:
        out += kind.encode("ascii")
        if kind == "t":
            data = payload.encode("utf-8")
            out += struct.pack(">I", len(data)) + data
    return out


def insert(*, oid, values):
    return b"I" + struct.pack(">I", oid) + b"N" + _tuple_data(values)


def update(*, oid, new, old=None, old_kind="K"):
    out = b"U" + struct.pack(">I", oid)
    if old is not None:
        out += old_kind.encode("ascii") + _tuple_data(old)
    return out + b"N" + _tuple_data(new)


def delete(*, oid, old, old_kind="K"):
    return b"D" + struct.pack(">I", oid) + old_kind.encode("ascii") + _tuple_data(old)


def _emp_relation():
    return relation(oid=OID, namespace="public", name="employees",
                    replica_identity="d", columns=EMP_COLUMNS)


def test_relation_is_cached_and_exposed():
    d = Decoder()
    assert d.feed(_emp_relation()) is None
    rel = d.relations[OID]
    assert (rel.namespace, rel.name, rel.replica_identity) == ("public", "employees", "d")
    assert [c.name for c in rel.columns] == ["id", "name"]
    assert rel.columns[0].is_key is True
    assert rel.columns[1].is_key is False


def test_relations_view_is_read_only():
    d = Decoder()
    d.feed(_emp_relation())
    with pytest.raises(TypeError):
        d.relations[999] = None


def test_insert_emits_change_event_with_lsn_and_time():
    d = Decoder()
    d.feed(_emp_relation())
    d.feed(begin(final_lsn=0x16B3748, commit_micros=COMMIT_MICROS))
    event = d.feed(insert(oid=OID, values=[("t", "1"), ("t", "Ali")]))
    assert event == ChangeEvent(
        lsn=0x16B3748, schema="public", table="employees", op="INSERT",
        new={"id": "1", "name": "Ali"}, old=None, commit_time=COMMIT_TIME,
    )


def test_insert_without_begin_raises():
    d = Decoder()
    d.feed(_emp_relation())
    with pytest.raises(RuntimeError, match="no BEGIN"):
        d.feed(insert(oid=OID, values=[("t", "1"), ("t", "Ali")]))


def test_insert_for_unknown_oid_raises():
    d = Decoder()
    d.feed(begin(final_lsn=1))
    with pytest.raises(KeyError):
        d.feed(insert(oid=99999, values=[("t", "1"), ("t", "x")]))


def test_commit_resets_transaction_context():
    d = Decoder()
    d.feed(_emp_relation())
    d.feed(begin(final_lsn=1))
    assert d.feed(commit()) is None
    with pytest.raises(RuntimeError, match="no BEGIN"):
        d.feed(insert(oid=OID, values=[("t", "1"), ("t", "Ali")]))


def test_update_key_old_keeps_only_key():
    d = Decoder()
    d.feed(_emp_relation())
    d.feed(begin(final_lsn=1))
    event = d.feed(update(oid=OID, old_kind="K",
                          old=[("t", "1"), ("n", None)],
                          new=[("t", "1"), ("t", "Veli")]))
    assert event.op == "UPDATE"
    assert event.old == {"id": "1"}
    assert event.new == {"id": "1", "name": "Veli"}


def test_update_full_old_keeps_all():
    d = Decoder()
    d.feed(_emp_relation())
    d.feed(begin(final_lsn=1))
    event = d.feed(update(oid=OID, old_kind="O",
                          old=[("t", "1"), ("t", "Ali")],
                          new=[("t", "1"), ("t", "Veli")]))
    assert event.old == {"id": "1", "name": "Ali"}


def test_update_without_old():
    d = Decoder()
    d.feed(_emp_relation())
    d.feed(begin(final_lsn=1))
    event = d.feed(update(oid=OID, old=None, new=[("t", "1"), ("t", "Veli")]))
    assert event.old is None
    assert event.new == {"id": "1", "name": "Veli"}


def test_update_bad_marker_raises():
    d = Decoder()
    d.feed(_emp_relation())
    d.feed(begin(final_lsn=1))
    with pytest.raises(ValueError, match="marker"):
        d.feed(b"U" + struct.pack(">I", OID) + b"X")


def test_delete_key_old():
    d = Decoder()
    d.feed(_emp_relation())
    d.feed(begin(final_lsn=1))
    event = d.feed(delete(oid=OID, old_kind="K", old=[("t", "1"), ("n", None)]))
    assert event.op == "DELETE"
    assert event.new is None
    assert event.old == {"id": "1"}


def test_delete_full_old():
    d = Decoder()
    d.feed(_emp_relation())
    d.feed(begin(final_lsn=1))
    event = d.feed(delete(oid=OID, old_kind="O", old=[("t", "1"), ("t", "Ali")]))
    assert event.old == {"id": "1", "name": "Ali"}


def test_delete_bad_marker_raises():
    d = Decoder()
    d.feed(_emp_relation())
    d.feed(begin(final_lsn=1))
    with pytest.raises(ValueError, match="marker"):
        d.feed(b"D" + struct.pack(">I", OID) + b"N")


def test_null_value_kind():
    d = Decoder()
    d.feed(_emp_relation())
    d.feed(begin(final_lsn=1))
    event = d.feed(insert(oid=OID, values=[("t", "1"), ("n", None)]))
    assert event.new == {"id": "1", "name": None}


def test_unchanged_toast_in_update():
    d = Decoder()
    d.feed(_emp_relation())
    d.feed(begin(final_lsn=1))
    event = d.feed(update(oid=OID, old=None, new=[("t", "1"), ("u", None)]))
    assert event.new == {"id": "1", "name": Sentinel.UNCHANGED}


def test_unknown_value_kind_raises():
    d = Decoder()
    d.feed(_emp_relation())
    d.feed(begin(final_lsn=1))
    bad = b"I" + struct.pack(">I", OID) + b"N" + struct.pack(">H", 1) + b"z"
    with pytest.raises(ValueError, match="kind"):
        d.feed(bad)


def test_clear_drops_state():
    d = Decoder()
    d.feed(_emp_relation())
    d.feed(begin(final_lsn=1))
    d.clear()
    assert dict(d.relations) == {}
    with pytest.raises(KeyError):
        d.feed(insert(oid=OID, values=[("t", "1"), ("t", "Ali")]))
