import argparse
import asyncio
import sys
import logging

import structlog

from waltz.checkpoint import FileCheckpoint
from waltz.config import StreamConfig
from waltz.decoder import Decoder
from waltz.sink import build_sink
from waltz.stream import StreamManager


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


def cmd_start(args: argparse.Namespace) -> None:
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

    args = parser.parse_args()
    configure_logging()
    if args.command == "start":
        cmd_start(args)
    else:
        parser.print_help()
        sys.exit(1)
