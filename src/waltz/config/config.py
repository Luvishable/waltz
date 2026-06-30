from __future__ import annotations

import os
from typing import Any

import yaml
from dotenv import load_dotenv
from pydantic import BaseModel, ConfigDict, ValidationError
from psycopg.conninfo import make_conninfo

from waltz.errors import ConfigError


class StreamConfig(BaseModel):
    model_config = ConfigDict(frozen=True)

    host: str = "localhost"
    port: int
    user: str
    password: str
    dbname: str
    slot: str = "waltz_slot_pgo"
    publication: str = "waltz_pub"
    checkpoint_path: str = "waltz.lsn"
    sink_type: str = "stdout"
    sink_url: str | None = None

    @classmethod
    def from_env(cls) -> StreamConfig:
        load_dotenv()
        try:
            return cls.model_validate({
                "host": os.getenv("DB_HOST", "localhost"),
                "port": os.getenv("DB_PORT"),
                "user": os.getenv("DB_USER"),
                "password": os.getenv("POSTGRES_PASSWORD"),
                "dbname": os.getenv("DB_NAME"),
                "slot": os.getenv("WALTZ_SLOT", "waltz_slot_pgo"),
                "publication": os.getenv("WALTZ_PUBLICATION", "waltz_pub"),
                "checkpoint_path": os.getenv("WALTZ_CHECKPOINT", "waltz.lsn"),
                "sink_type": os.getenv("WALTZ_SINK_TYPE", "stdout"),
                "sink_url": os.getenv("WALTZ_SINK_URL"),
            })
        except ValidationError as e:
            raise ConfigError(str(e)) from e

    @classmethod
    def from_yaml(cls, path: str) -> StreamConfig:
        with open(path) as f:
            raw: Any = yaml.safe_load(f)
        src: Any = raw.get("source", {})
        snk: Any = raw.get("sink", {})
        ckpt: Any = raw.get("checkpoint", {})
        try:
            return cls.model_validate({
                "host": src.get("host", "localhost"),
                "port": src.get("port"),
                "user": src.get("user"),
                "password": src.get("password"),
                "dbname": src.get("database"),
                "slot": src.get("slot", "waltz_slot_pgo"),
                "publication": src.get("publication", "waltz_pub"),
                "checkpoint_path": ckpt.get("path", "waltz.lsn"),
                "sink_type": snk.get("type", "stdout"),
                "sink_url": snk.get("url"),
            })
        except ValidationError as e:
            raise ConfigError(str(e)) from e

    def conninfo(self) -> str:
        return make_conninfo(
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            dbname=self.dbname,
            replication="database",
        )

    def admin_conninfo(self) -> str:
        return make_conninfo(
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            dbname=self.dbname,
        )