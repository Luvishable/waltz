"""
pgoutput decoder: bytes in, structured ChangeEvents out.

- A decoder should be stateful:
    - pgoutput's Insert/Update/Delete messages identify their table by OID only.
    - Column names and types live in an earlier Relation message.
    - Thus, the decoder caches every Relation it sees and make use of it
      when a row change arrives (OID -> Schema)
    - It also tracks the current transaction's context (commit LSN + timestamp from Begin),
      which gets stamped onto each emitted event.
"""

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from types import MappingProxyType

from waltz.events import ChangeEvent, Commit, Op, Row, Sentinel
from waltz.pgtime import micros_to_datetime
from waltz.reader import Reader


@dataclass(frozen=True, slots=True)
class Column:
    """
    A column's shape as described by a Relation message.
    """

    name: str
    is_key: bool  # part of REPLICA IDENTITY key (matters for UPDATE/DELETE old)
    type_oid: int  # Postgres type OID
    type_modifier: int  # atttypemod, -1 = none (signed)


@dataclass(frozen=True, slots=True)
class RelationInfo:
    """
    A cached table schema, keyed by OID in the decoder.
    """

    oid: int
    namespace: str  # schema name; empty string means pg_catalog
    name: str
    replica_identity: str  # 'd' default / 'n' nothing / 'f' full / 'i' index
    columns: tuple[Column, ...]  # frozen value object must stay immutable


class Decoder:
    """
    Takes raw pgoutput payloads; emits a ChangeEvent for each row change.
    """

    def __init__(self) -> None:
        # Survives across transactions: relations are declared once, reused for every
        # following row change until the schema changes, or we reconnect.
        self._relations: dict[int, RelationInfo] = {}
        # Reset every transaction (Begin sets, Commit clears):
        self._commit_time: datetime | None = None
        self._final_lsn: int | None = None
        self._xid: int | None = None

    def feed(self, payload: bytes) -> ChangeEvent | Commit | None:
        """One pgoutput message in. A ChangeEvent for I/U/D and None for structural ones."""
        reader = Reader(payload)
        tag = reader.char()  # Consume the 1-byte message-type tag

        if tag == "B":
            self._handle_begin(reader)
            return None
        if tag == "C":
            return self._handle_commit(reader)
        if tag == "R":
            self._handle_relation(reader)
            return None
        if tag == "I":
            return self._handle_insert(reader)
        if tag == "U":
            return self._handle_update(reader)
        if tag == "D":
            return self._handle_delete(reader)

        return None

    def clear(self) -> None:
        """
        Invalidate all state. Call after a reconnect: OIDs/schemas may have changed.
        """
        self._relations.clear()
        self._reset_transaction()

    @property
    def relations(self) -> Mapping[int, RelationInfo]:
        # read-only view for observation/tests; MappingProxyType blocks mutation
        return MappingProxyType(self._relations)

    # ---------------- internals ----------------

    def _reset_transaction(self) -> None:
        self._commit_time = None
        self._final_lsn = None
        self._xid = None

    def _require_relation(self, oid: int, op: Op) -> RelationInfo:
        # Row change messages only contain a relation OID. The schema must have been
        # cached from an earlier Relation message; otherwise we cannot decode the row.
        relation = self._relations.get(oid)
        if relation is None:
            raise KeyError(f"{op} for unknown OID {oid}; no Relation cached")
        return relation

    def _row_event(
        self, op: Op, relation: RelationInfo, *, new: Row | None, old: Row | None
    ) -> ChangeEvent:
        # Attach the current transaction context (from BEGIN) to the event.
        # This is the single place where we enforce that a BEGIN must have been seen
        if self._final_lsn is None:
            raise RuntimeError(f"{op} outside a transaction: no BEGIN seen")
        return ChangeEvent(
            lsn=self._final_lsn,
            schema=relation.namespace,
            table=relation.name,
            op=op,
            new=new,
            old=old,
            commit_time=self._commit_time,
        )

    def _handle_begin(self, reader: Reader) -> None:
        # Begin (after the 'B' tag): Int64 final/commit LSN, Int64 commit time, Int32 xid.
        self._final_lsn = reader.uint64()
        self._commit_time = micros_to_datetime(reader.int64())
        self._xid = reader.uint32()

    def _handle_relation(self, reader: Reader) -> None:
        # Relation (after the 'R' tag):
        #   Int32  relation OID
        #   String namespace (NUL-terminated; empty = pg_catalog)
        #   String relation name
        #   Int8   replica identity setting ('d'/'n'/'f'/'i')   <- table level, once
        #   Int16  column count N
        #   then N columns (see _read_column)
        oid = reader.uint32()
        namespace = reader.string()
        name = reader.string()
        replica_identity = reader.char()
        column_count = reader.uint16()
        columns = tuple(self._read_column(reader) for _ in range(column_count))

        self._relations[oid] = RelationInfo(
            oid=oid,
            namespace=namespace,
            name=name,
            replica_identity=replica_identity,
            columns=columns,
        )

    def _handle_insert(self, reader: Reader) -> ChangeEvent:
        # Insert (after the 'I' tag): Int32 relation OID, Byte1 'N', then TupleData.
        oid = reader.uint32()
        relation = self._require_relation(oid, "INSERT")

        reader.char()  # the 'N' marker (new tuple): always 'N' for INSERT
        new = self._read_tuple(reader, relation)

        return self._row_event("INSERT", relation, new=new, old=None)

    def _handle_update(self, reader: Reader) -> ChangeEvent:
        # Update ('U'): Int32 relation OID, an optional old tuple, then the new tuple.
        #   'K' -> old contains only key columns (REPLICA IDENTITY DEFAULT/INDEX, key changed)
        #   'O' -> old contains the full previous row (REPLICA IDENTITY FULL)
        #   'N' -> no old tuple; the new tuple starts here
        oid = reader.uint32()
        relation = self._require_relation(oid, "UPDATE")

        marker = reader.char()
        if marker in ("K", "O"):
            old = self._read_tuple(reader, relation, key_only=(marker == "K"))
            if reader.char() != "N":
                raise ValueError("UPDATE: 'N' marker is expected after old tuple")
            new = self._read_tuple(reader, relation)
        elif marker == "N":
            old = None
            new = self._read_tuple(reader, relation)
        else:
            raise ValueError(f"Unexpected marker inside UPDATE: {marker!r}")

        return self._row_event("UPDATE", relation, new=new, old=old)

    def _handle_delete(self, reader: Reader) -> ChangeEvent:
        # Delete ('D'): Int32 relation OID, followed by a full old tuple. No new tuple.
        #   'K' -> old contains only key columns (REPLICA IDENTITY DEFAULT/INDEX)
        #   'O' -> old contains the full previous row (REPLICA IDENTITY FULL)
        oid = reader.uint32()
        relation = self._require_relation(oid, "DELETE")

        marker = reader.char()
        if marker not in ("K", "O"):
            raise ValueError(f"Unexpected marker inside DELETE: {marker!r}")
        old = self._read_tuple(reader, relation, key_only=(marker == "K"))

        return self._row_event("DELETE", relation, new=None, old=old)

    def _handle_commit(self, reader: Reader) -> Commit:
        # Commit (after 'C' tag):
        #   Int8  -> flags
        #   Int64 -> commit LSN (LSN of the commit record; matches BEGIN's final LSN)
        #   Int64 -> end LSN (transaction boundary; this is what we gonna confirm)
        #   Int64 -> commit time
        reader.uint8()
        reader.uint64()
        end_lsn = reader.uint64()
        commit_time = micros_to_datetime(reader.int64())
        self._reset_transaction()
        return Commit(end_lsn=end_lsn, commit_time=commit_time)

    @staticmethod
    def _read_column(reader: Reader) -> Column:
        # Per column: Int8 flags, String name, Int32 type OID, Int32 atttypmod.
        flags = reader.uint8()
        name = reader.string()
        type_oid = reader.uint32()
        type_modifier = reader.int32()  # -1 = "no modifier".
        return Column(
            name=name,
            is_key=bool(flags & 1),
            type_oid=type_oid,
            type_modifier=type_modifier,
        )

    @staticmethod
    def _read_tuple(reader: Reader, relation: RelationInfo, *, key_only: bool = False) -> Row:
        column_count = reader.uint16()
        values: Row = {}
        for index in range(column_count):
            column = relation.columns[index]
            kind = reader.char()
            if kind == "n":
                value: str | None | Sentinel = None
            elif kind == "u":
                # 'u' -> Unchanged TOAST value: it's not written to WAL again, just skipped.
                # However, this doesn't mean NULL so mark it as UNCHANGED
                value = Sentinel.UNCHANGED
            elif kind == "t":
                length = reader.uint32()
                value = reader.read_bytes(length).decode("utf-8")
            else:
                raise ValueError(f"Unknown TupleData value kind: {kind!r}")

            # In a key tuple (REPLICA IDENTITY DEFAULT/INDEX), the wire format still
            # includes all columns positionally, but non-identity columns are sent as NULL.
            # These are placeholders, not actual data, so skip them and keep only the key.
            if key_only and not column.is_key:
                continue
            values[column.name] = value

        return values
