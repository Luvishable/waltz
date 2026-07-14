"""
Admin (management-plane) database access.

waltz speaks to PostgreSQL over two kinds of connection:
- the replication connection which is used by start command to stream WAL
- and the normal connection which is an ordinary SQL session that other commands
  use to read catalogs and create objects.

This module owns that normal connection and the SQL that runs on it so the CLI
commands stay clean and the queries live in one place.
"""

import contextlib
from collections.abc import AsyncIterator
from dataclasses import dataclass

import psycopg
from psycopg import sql
from psycopg.rows import TupleRow

from waltz.config.config import StreamConfig
from waltz.errors import raise_pg_error

# connect() with no row_factory yields tuple rows. Naming the parametrized type once
# satisfies mypy --strict and keeps signatures readable
type AdminConn = psycopg.AsyncConnection[TupleRow]


@dataclass(frozen=True, slots=True)
class SlotStatus:
    """A snapshot of the pg_replication_slots for our slot"""

    slot_name: str
    plugin: str
    active: bool
    active_pid: int | None
    restart_lsn: str | None
    confirmed_flush_lsn: str | None
    lag_bytes: int | None       # has to be null when confirmed_flush_lsn is null
    wal_status: str | None      # reserved / extended / unreserved / lost (PG 13+)
    safe_wal_size: int | None   # null unless max_slot_wal_keep_size is set


async def get_slot_status(conn: AdminConn, slot: str) -> SlotStatus | None:
    """Read-only counterpart of ensure_slot. None means the slot does not exist."""
    try:
        cursor = await conn.execute(
            """
            SELECT slot_name, plugin, active, active_pid, restart_lsn,
            confirmed_flush_lsn,
            pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)::bigint,
            wal_status, safe_wal_size
            FROM pg_replication_slots
            WHERE slot_name = %s
            """,
            (slot,),
        )
        row = await cursor.fetchone()
    except psycopg.Error as e:
        raise_pg_error(e)
    if row is None:
        return None
    return SlotStatus(
        slot_name=row[0],
        plugin=row[1],
        active=row[2],
        active_pid=row[3],
        restart_lsn=row[4],
        confirmed_flush_lsn=row[5],
        lag_bytes=row[6],
        wal_status=row[7],
        safe_wal_size=row[8],
    )


@contextlib.asynccontextmanager
async def admin_connection(config: StreamConfig) -> AsyncIterator[AdminConn]:
    """
    Open a non-replication connection and always close it.
    """
    try:
        conn = await psycopg.AsyncConnection.connect(
            config.admin_conninfo(), autocommit=True
        )
    except psycopg.Error as e:
        raise_pg_error(e)
    async with conn:
        yield conn


async def ensure_publication(conn: AdminConn, publication: str) -> bool:
    try:
        cursor = await conn.execute(
            "SELECT 1 FROM pg_publication WHERE pubname = %s", (publication,)
        )
        if await cursor.fetchone() is not None:
            return False
        # Identifier() quotes the name safely
        await conn.execute(
            sql.SQL("CREATE PUBLICATION {} FOR ALL TABLES").format(
                sql.Identifier(publication)
            )
        )
        return True
    except psycopg.Error as e:
        raise_pg_error(e)


async def ensure_slot(conn: AdminConn, slot: str) -> bool:
    # create the pgoutput logical slot is missing. Returns True only when it's created
    try:
        cursor = await conn.execute(
            "SELECT 1 FROM pg_replication_slots WHERE slot_name = %s AND plugin = 'pgoutput'",
            (slot,),
        )
        if await cursor.fetchone() is not None:
            return False
        await conn.execute(
            "SELECT pg_create_logical_replication_slot(%s, 'pgoutput')", (slot,)
        )
        return True
    except psycopg.Error as e:
        raise_pg_error(e)


@dataclass(frozen=True, slots=True)
class ServerSettings:
    """
    The server-side knobs that decide wheter logical replication can work at all.
    """

    version_num: int    # e.g. 10001 -> PG 18.1
    wal_level: str
    max_replication_slots: int
    max_wal_senders: int
    max_slot_wal_keep_size: str


_SETTING_NAMES = (
    "server_version_num",
    "wal_level",
    "max_replication_slots",
    "max_wal_senders",
    "max_slot_wal_keep_size",
)


async def get_server_settings(conn: AdminConn) -> ServerSettings:
    # current_setting() returns the human form ("1GB"); the raw `setting` column
    # would return unit-relative numbers ("1024" meaning MB) that mislead operators
    try:
        cursor = await conn.execute(
            "SELECT name, current_setting(name) FROM pg_settings WHERE name = ANY(%s)",
            (list(_SETTING_NAMES),)
        )
        rows = await cursor.fetchall()
    except psycopg.Error as e:
        raise_pg_error(e)
    settings: dict[str, str] = dict(rows)
    return ServerSettings(
        version_num=int(settings["server_version_num"]),
        wal_level=settings["wal_level"],
        max_replication_slots=int(settings["max_replication_slots"]),
        max_wal_senders=int(settings["max_wal_senders"]),
        max_slot_wal_keep_size=settings["max_slot_wal_keep_size"],
    )


@dataclass(frozen=True, slots=True)
class RoleFlags:
    """
    What the connecting role may do, straight from pg_roles
    """

    name: str
    superuser: bool
    replication: bool


async def get_current_role_flags(conn: AdminConn) -> RoleFlags:
    try:
        cursor = await conn.execute(
            "SELECT rolname, rolsuper, rolreplication FROM pg_roles WHERE rolname = current_user"
        )
        row = await cursor.fetchone()
    except psycopg.Error as e:
        raise_pg_error(e)
    # current_user always has a pg_roles row; the assert narrows away None for mypy
    assert row is not None
    return RoleFlags(name=row[0], superuser=row[1], replication=row[2])


async def count_used_slots(conn: AdminConn) -> int:
    try:
        cursor = await conn.execute("SELECT count(*) FROM pg_replication_slots")
        row = await cursor.fetchone()
    except psycopg.Error as e:
        raise_pg_error(e)
    assert row is not None
    return int(row[0])


@dataclass(frozen=True, slots=True)
class PublicationInfo:
    all_tables: bool
    table_count: int


async def get_publication_info(conn: AdminConn, publication: str) -> PublicationInfo | None:
    try:
        # step 1: check the publication's existence and its configuration type.
        # the 'puballtables' column provides a boolean value indicating whether
        # this publication was created using the 'FOR ALL TABLES' rule.
        cursor = await conn.execute(
            "SELECT puballtables FROM pg_publication WHERE pubname = %s", (publication,)
        )
        row = await cursor.fetchone()
        if row is None:
            return None
        # step 2: find the total number of tables currently covered by this publication.
        # The pg_publication_tables view automatically lists all included tables, even if
        # the publication was set to 'FOR ALL TABLES'. This allows us to get the exact
        # table count in all scenarios simply by using count(*)
        cursor = await conn.execute(
            "SELECT count(*) FROM pg_publication_tables WHERE pubname = %s", (publication,)
        )
        count_row = await cursor.fetchone()
    except psycopg.Error as e:
        raise_pg_error(e)
    assert count_row is not None
    return PublicationInfo(all_tables=row[0], table_count=int(count_row[0]))


async def probe_replication_connection(config: StreamConfig) -> None:
    """
    Open and immediately close the same replication-protocol connection that
    `waltz start` uses. Catalog checks approximate; this rehearses the real path
    (REPLICATION privelege, a free walsender, pg_hba) end to end.
    """
    try:
        conn = await psycopg.AsyncConnection.connect(config.conninfo(), autocommit=True)
    except psycopg.Error as e:
        raise_pg_error(e)
    await conn.close()





























































