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

import psycopg
from psycopg import sql
from psycopg.rows import TupleRow

from waltz.config.config import StreamConfig
from waltz.errors import raise_pg_error

# connect() with no row_factory yields tuple rows. Naming the parametrized type once
# satisfies mypy --strict and keeps signatures readable
type AdminConn = psycopg.AsyncConnection[TupleRow]


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



