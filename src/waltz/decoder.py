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
from datetime import datetime, timedelta, timezone
from types import MappingProxyType
from typing import Literal

from waltz.reader import Reader

# Postgres timestamps count microseconds from 2000-01-01 UTC, not the Unix epoch
PG_EPOCH = datetime(2000, 1, 1, tzinfo=timezone.utc)


def _pg_micros_to_datetime(micros: int) -> datetime:
    # offset the PG epoch by the message's microsecond count
    return PG_EPOCH + timedelta(microseconds=micros)


@dataclass(frozen=True, slots=True)
class Column:
    """
    A column's shape as described by a Relation message.
    """

    name: str
    is_key: bool            # part of REPLICA IDENTITY key (matters for UPDATE/DELETE old)
    type_oid: int           # Postgres type OID
    type_modifier: int      # atttypemod, -1 = none (signed)


@dataclass(frozen=True, slots=True)
class RelationInfo:
    """
    A cached table schema, keyed by OID in the decoder.
    """

    oid: int
    namespace: str                  # schema name; empty string means pg_catalog
    name: str
    replica_identity: str           # 'd' default / 'n' nothing / 'f' full / 'i' index
    columns: tuple[Column, ...]     # frozen value object must stay immutable


@dataclass(frozen=True, slots=True)
class ChangeEvent:
    """
    A single row change
    """

    lsn: int        # commit LSN of the owning transaction (from Begin)
    schema: str
    table: str
    op: Literal["INSERT", "UPDATE", "DELETE"]
    new: dict[str, str | None] | None       # values after (INSERT/UPDATE)
    old: dict[str, str | None] | None       # values before (UPDATE/DELETE)
    commit_time: datetime | None


class Decoder:
    """
    Takes raw pgoutput payloads; emits a ChangeEvent for each row change.
    """

    def __init__(self) -> None:
        # Survives across transactions: relations are declared once, reused for every
        # following row change until the schema changes or we reconnect.
        self._relations: dict[int, RelationInfo] = {}
        # Reset every transaction (Begin sets, Commit clears):
        self._commit_time: datetime | None = None
        self._final_lsn: int | None = None
        self._xid: int | None = None

    def feed(self, payload: bytes) -> ChangeEvent | None:
        """One pgoutput message in. A ChangeEvent for I/U/D and None for structural ones."""
        reader = Reader(payload)
        tag = reader.char()     # Consume the 1-byte message-type tag

        if tag == "B":
            self._handle_begin(reader)
            return None
        if tag == "C":
            self._reset_transaction()   # transaction closed; context no longer valid
            return None
        if tag == "R":
            self._handle_relation(reader)
            return None
        if tag == "I":
            return self._handle_insert(reader)

        # U / D / Y / O / T -> next chunks. Silently ignore for now.
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

    def _handle_begin(self, reader: Reader) -> None:
        # Begin (after the 'B' tag): Int64 final/commit LSN, Int64 commit time, Int32 xid.
        self._final_lsn = reader.uint64()
        self._commit_time = _pg_micros_to_datetime(reader.int64())
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

    def _handle_insert(self, reader: Reader) -> ChangeEvent:
        # Insert (after the 'I' tag): Int32 relation OID, Byte1 'N', then TupleData.
        oid = reader.uint32()
        relation = self._relations.get(oid)
        if relation is None:
            # Protocol guarantees Relation precedes its row changes; if not, our
            # cache is wrong (e.g. we connected mid-stream) and we cannot decode.
            raise KeyError(f"INSERT for unknown OID {oid}; no Relation cached")
        if self._final_lsn is None:
            raise RuntimeError("INSERT outside a transaction: no BEGIN seen")

        reader.char()   # the 'N' marker (new tuple): always 'N' for INSERT
        new = self._read_tuple(reader, relation)

        return ChangeEvent(
            lsn=self._final_lsn,
            schema=relation.namespace,
            table=relation.name,
            op="INSERT",
            new=new,
            old=None,
            commit_time=self._commit_time
        )

    @staticmethod
    def _read_tuple(reader: Reader, relation: RelationInfo) -> dict[str, str | None]:
        # the info about how many columns will be inserted:
        column_count = reader.uint16()
        # create a column_name: value dict. column_name info already caught when
        # reading the relation frame and cached in the state
        values: dict[str, str | None] = {}
        for index in range(column_count):
            # determine which column's value are being read
            column = relation.columns[index]
            # kind ('t' , 'u' , 'n') determines what will come as the value of
            # present column.
            # 't'   ->      there comes a text but it might be big so TOAST table can be utilized
            #               and the value can be read directly from the TOAST table.
            # 'n'   ->      null
            # 'u'   ->      the value didn't change. previous value can be used as the new value

            kind = reader.char()
            if kind == "n":
                values[column.name] = None
            elif kind == "u":
                # FIXME: conflated with NULL until we handle TOAST in the UPDATE step.
                values[column.name] = None
            elif kind == "t":
                length = reader.uint32()
                values[column.name] = reader.read_bytes(length).decode("utf-8")
            else:
                raise ValueError(f"Unknown TupleData value kind {kind!r}")
        return values

























































