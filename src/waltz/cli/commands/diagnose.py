import asyncio
from pathlib import Path
from typing import Annotated

import structlog
import typer
from rich.console import Console
from rich.table import Table

from waltz.admin.diagnostics import CheckOutcome, CheckResult, run_diagnostics
from waltz.config.config import StreamConfig
from waltz.errors import ConfigError

logger = structlog.get_logger()

_OUTCOME_MARKUP = {
    CheckOutcome.PASS: "[green]pass[/green]",
    CheckOutcome.WARN: "[yellow]warn[/yellow]",
    CheckOutcome.FAIL: "[red]fail[/red]",
    CheckOutcome.SKIP: "[dim]skip[/dim]",
}


def _render(results: list[CheckResult]) -> None:
    table = Table(title="waltz diagnose")
    table.add_column("check")
    table.add_column("outcome")
    table.add_column("observed", overflow="fold")
    table.add_column("hint", overflow="fold")
    for result in results:
        table.add_row(
            result.name,
            _OUTCOME_MARKUP[result.outcome],
            result.observed,
            result.hint or "",
        )
    Console().print(table)


def diagnose(
        config: Annotated[
            Path | None,
            typer.Option(help="YAML config file; falls back to env vars if omitted."),
        ] = None,
) -> None:
    """
    Run health checks and explain how to fix whatever would break the stream
    """
    # Only ConfigError can escape: replication failures are diagnostic findings,
    # not exceptions
    try:
        cfg = StreamConfig.load(config)
    except ConfigError as e:
        logger.error("waltz.diagnose_error", error=str(e))
        raise typer.Exit(code=1) from e
    results = asyncio.run(run_diagnostics(cfg))
    _render(results)
    if any(r.outcome is CheckOutcome.FAIL for r in results):
        raise typer.Exit(code=1)
