import asyncio
from pathlib import Path
from typing import Annotated

import structlog
import typer

from waltz.checkpoint.checkpoint import FileCheckpoint
from waltz.config.config import StreamConfig
from waltz.core.decoder import Decoder
from waltz.errors import ConfigError, PermanentReplicationError
from waltz.replication.stream import StreamManager
from waltz.sink.sink import build_sink

logger = structlog.get_logger()

def start(
        config: Annotated[
            Path | None,
            typer.Option(help="YAML config file; falls back to env vars if omitted"),
        ] = None,
) -> None:
    """Start the CDC stream"""
    try:
        cfg = StreamConfig.load(config)
        sink = build_sink(cfg.sink_type, cfg.sink_url)
        asyncio.run(
            StreamManager(
                cfg,
                FileCheckpoint(cfg.checkpoint_path),
                Decoder(),
                sink,
            ).run()
        )
    except ConfigError as e:
        logger.error("waltz.config_error", error=str(e))
        raise typer.Exit(code=1) from e
    except PermanentReplicationError as e:
        logger.error("waltz.permanent_error", error=str(e))
        raise typer.Exit(code=1) from e

