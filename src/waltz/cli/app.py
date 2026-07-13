import logging

import structlog
import typer

from waltz.cli.commands import diagnose, init, start, status

app = typer.Typer(
    name="waltz",
    help="PostgreSQL CDC service - capture and stream WAL changes.",
    no_args_is_help=True,
    add_completion=False,
)

# Register each module's command function on the shared app.
app.command(name="start")(start.start)
app.command(name="init")(init.init)
app.command(name="status")(status.status)
app.command(name="diagnose")(diagnose.diagnose)


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


@app.callback()
def _root() -> None:
    # Runs before whatever command was selectedİ the place for global setup
    configure_logging()


def main() -> None:
    app()

