from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any

import yaml
from dotenv import load_dotenv
from psycopg.conninfo import make_conninfo


def _require_env(name: str) -> str:
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
    sink_type: str
    sink_url: str | None

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
            sink_type=os.getenv("WALTZ_SINK_TYPE", "stdout"),
            sink_url=os.getenv("WALTZ_SINK_URL"),
        )

    @classmethod
    def from_yaml(cls, path: str) -> StreamConfig:
        with open(path) as f:
            raw: Any = yaml.safe_load(f)
        src: Any = raw.get("source", {})
        snk: Any = raw.get("sink", {})
        ckpt: Any = raw.get("checkpoint", {})
        for key in ("port", "user", "password", "database"):
            if key not in src:
                raise RuntimeError(f"missing required YAML key: source.{key}")
        return cls(
            host=str(src.get("host", "localhost")),
            port=int(src["port"]),
            user=str(src["user"]),
            password=str(src["password"]),
            dbname=str(src["database"]),
            slot=str(src.get("slot", "waltz_slot_pgo")),
            publication=str(src.get("publication", "waltz_pub")),
            checkpoint_path=str(ckpt.get("path", "waltz.lsn")),
            sink_type=str(snk.get("type", "stdout")),
            sink_url=str(snk["url"]) if snk.get("url") else None,
        )

    def conninfo(self) -> str:
        return make_conninfo(
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            dbname=self.dbname,
            replication="database",
        )


