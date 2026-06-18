from __future__ import annotations

import os
from dataclasses import dataclass

from dotenv import load_dotenv
from psycopg.conninfo import make_conninfo


def _require_env(name: str) -> str:
    # Fail fast with a clear message instead of letting a None reach psycopg.
    value = os.getenv(name)
    if value is None or value == "":
        raise RuntimeError(f"missing required environment variable: {name}")
    return value


@dataclass(frozen=True, slots=True)
class StreamConfig:
    host: str
    port: int
    user: str
    password: str
    dbname: str
    slot: str
    publication: str
    checkpoint_path: str

    @classmethod
    def from_env(cls) -> StreamConfig:
        # Load .env into the process environment, then read it.
        load_dotenv()
        return cls(
            host=os.getenv("DB_HOST", "localhost"),
            port=int(_require_env("DB_PORT")),
            user=_require_env("DB_USER"),
            password=_require_env("POSTGRES_PASSWORD"),
            dbname=_require_env("DB_NAME"),
            slot=os.getenv("WALTZ_SLOT", "waltz_slot_pgo"),
            publication=os.getenv("WALTZ_PUBLICATION", "waltz_pub"),
            checkpoint_path=os.getenv("WALTZ_CHECKPOINT", "waltz.lsn"),
        )

    def conninfo(self) -> str:
        # Build a psycopg-ready connection string. replication=database switches
        # the connection into logical replication mode; it is fixed, not user config.
        return make_conninfo(
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            dbname=self.dbname,
            replication="database",
        )
