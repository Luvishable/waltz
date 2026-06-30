import argparse
import asyncio
import logging
import sys

import psycopg
from psycopg import sql
import structlog

from waltz.checkpoint import FileCheckpoint
from waltz.config import StreamConfig
from waltz.core.decoder import Decoder
from waltz.sink import build_sink
from waltz.replication.stream import StreamManager
from waltz.errors import (
    ConfigError,
    PermanentReplicationError,
    TransientReplicationError,
    raise_pg_error
)

logger = structlog.get_logger()


def configure_logging() -> None:
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.dev.ConsoleRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(logging.INFO),
        logger_factory=structlog.PrintLoggerFactory(),
    )


async def _init_async(config: StreamConfig) -> None:
    try:
        conn = await psycopg.AsyncConnection.connect(
            config.admin_conninfo(), autocommit=True
        )
    except psycopg.Error as e:
        raise_pg_error(e)

    async with conn:
        try:
            cursor = await conn.execute(
                "SELECT 1 FROM pg_publication WHERE pubname = %s",
                (config.publication,),
            )
            if await cursor.fetchone() is None:
                await conn.execute(
                    sql.SQL("CREATE PUBLICATION {} FOR ALL TABLES").format(
                        sql.Identifier(config.publication)
                    )
                )
                logger.info("init.publication_created", name=config.publication)
            else:
                logger.info("init_publication_exists", name=config.publication)

            cursor = await conn.execute(
                "SELECT 1 FROM pg_replication_slots(%s, 'pgoutput')",
                (config.slot,),
            )
            if await cursor.fetchone() is None:
                await conn.execute(
                    "SELECT pg_create_logical_replication_slot(%s, 'pgoutput')",
                    (config.slot,),
                )
                logger.info("init.slot_created", name=config.slot)
            else:
                logger.info("init.slot_exists", name=config.slot)

        except psycopg.Error as e:
            raise_pg_error(e)



def cmd_init(args: argparse.Namespace) -> None:
    try:
        config = StreamConfig.from_yaml(args.config) if args.config else StreamConfig.from_env()
        asyncio.run(_init_async(config))
    except (ConfigError, PermanentReplicationError, TransientReplicationError) as e:
        logger.error("waltz.init_error", error=str(e))
        sys.exit(1)


def cmd_start(args: argparse.Namespace) -> None:
    try:
        config = StreamConfig.from_yaml(args.config) if args.config else StreamConfig.from_env()
        sink = build_sink(config.sink_type, config.sink_url)
        asyncio.run(
            StreamManager(
                config,
                FileCheckpoint(config.checkpoint_path),
                Decoder(),
                sink,
            ).run()
        )
    except ConfigError as e:
        logger.error("waltz.config_error", error=str(e))
        sys.exit(1)
    except PermanentReplicationError as e:
        logger.error("waltz.permanent_error", error=str(e))
        sys.exit(1)


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="waltz",
        description="PostgreSQL CDC service - capture and stream WAL changes",
    )
    sub = parser.add_subparsers(dest="command", metavar="COMMAND")

    start_cmd = sub.add_parser("start", help="start the CDC stream")
    start_cmd.add_argument(
        "--config", metavar="FILE",
        help="YAML config file; if omitted, falls back to environment variables",
    )

    init_cmd = sub.add_parser("init", help="create replication slot and publication")
    init_cmd.add_argument(
        "--config", metavar="FILE",
        help="YAML config file; if omitted, falls back to environment variables",
    )

    args = parser.parse_args()
    configure_logging()
    if args.command == "start":
        cmd_start(args)
    elif args.command == "init":
        cmd_init(args)
    else:
        parser.print_help()
        sys.exit(1)
