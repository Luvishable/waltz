import asyncio
from pathlib import Path
from typing import Annotated

import structlog
import typer
from rich.console import Console
from rich.table import Table

from waltz.admin.queries import SlotStatus, admin_connection, get_slot_status
from waltz.config.config import StreamConfig
from waltz.errors import (
    ConfigError,
    PermanentReplicationError,
    TransientReplicationError,
)

logger = structlog.get_logger()


async def _status(config: StreamConfig) -> SlotStatus | None:
    async with admin_connection(config) as conn:
        return await get_slot_status(conn, config.slot)


def _render(s: SlotStatus) -> None:
    table = Table(title="waltz replication status")
    table.add_column("slot")
    table.add_column("active")
    table.add_column("restart_lsn")
    table.add_column("confirmed_flush_lsn")
    table.add_column("lag", justify="right")
    table.add_row(
        s.slot_name,
        "[green]yes[/green]" if s.active else "[red]no[/red]",
        s.restart_lsn or "-",
        s.confirmed_flush_lsn or "-",
        f"{s.lag_bytes:,} B" if s.lag_bytes is not None else "-",
    )
    Console().print(table)


def status(
        config: Annotated[
            Path | None,
            typer.Option(help="YAML config file; falls back to env vars if omitted."),
        ] = None,
) -> None:
    """Show slot health: activity, LSN positions, replication lag"""
    try:
        cfg = StreamConfig.load(config)
        slot_status = asyncio.run(_status(cfg))
    except (ConfigError, PermanentReplicationError, TransientReplicationError) as e:
        logger.error("waltz.status_error", error=str(e))
        raise typer.Exit(code=1) from e
    if slot_status is None:
        # missing slot is not an exception but health verdict. Thus, report and exit non-zero
        logger.error("status.slot_missing", slot=cfg.slot, hint="run `waltz init` first")
        raise typer.Exit(code=1)
    _render(slot_status)
