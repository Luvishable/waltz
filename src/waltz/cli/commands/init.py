import asyncio
from pathlib import Path
from typing import Annotated

import structlog
import typer

from waltz.admin.queries import admin_connection, ensure_publication, ensure_slot
from waltz.config.config import StreamConfig
from waltz.errors import (
    ConfigError,
    PermanentReplicationError,
    TransientReplicationError,
)

logger = structlog.get_logger()


async def _init(config: StreamConfig) -> None:
    async with admin_connection(config) as conn:
        created = await ensure_publication(conn, config.publication)
        logger.info("init.publication_created" if created else "init.publication_exists",
                    name=config.publication,
        )
        created = await ensure_slot(conn, config.slot)
        logger.info(
            "init.slot_created" if created else "init.slot_exists",
            name=config.slot,
        )


def init(
        config: Annotated[
            Path | None,
            typer.Option(help="YAML config file; falls back to env vars if omitted"),
        ] = None,
) -> None:
    """Create the replication slot and publication (idempotent)."""
    try:
        cfg = StreamConfig.load(config)
        asyncio.run(_init(cfg))
    except (ConfigError, PermanentReplicationError, TransientReplicationError) as e:
        logger.error("waltz.init_error", error=str(e))
        raise typer.Exit(code=1) from e
