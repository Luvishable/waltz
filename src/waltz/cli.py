import argparse
import sys

from waltz.checkpoint import FileCheckpoint
from waltz.config import StreamConfig
from waltz.decoder import Decoder
from waltz.sink import build_sink
from waltz.stream import StreamManager


def cmd_start(args: argparse.Namespace) -> None:
    config = StreamConfig.from_yaml(args.config) if args.config else StreamConfig.from_env()
    sink = build_sink(config.sink_type, config.sink_url)
    StreamManager(
        config,
        FileCheckpoint(config.checkpoint_path),
        Decoder(),
        sink,
    ).run()


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
    if args.command == "start":
        cmd_start(args)
    else:
        parser.print_help()
        sys.exit(1)
