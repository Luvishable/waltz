# Changelog

## [Unreleased]

---

## [0.2.0] — 2026-07-13

### Added
- `waltz init` command: idempotent creation of the replication slot and publication
- `waltz status` command: slot activity, LSN positions and replication lag as a Rich table
- `waltz diagnose` command: twelve pass/fail health checks with fix hints — connection,
  server version, `wal_level`, replication privilege, slot capacity/plugin/holder,
  WAL retention (`wal_status`), lag, publication coverage, checkpoint-file sanity and
  a real replication-protocol probe; exits non-zero when any check fails
- Structured logging via `structlog`: JSON-compatible log output with slot and
  publication bound as context on every log line
- ISO timestamp and log level on every log entry

### Changed
- CLI migrated from `argparse` to `Typer` + `Rich`; each command lives in its own
  module under `cli/commands/`
- Modules organized into subpackages (`core`, `replication`, `sink`, `checkpoint`,
  `config`, `admin`, `cli`) with the import rule "outer → inner"; entry point moved
  to `waltz.cli.app:main`
- `StreamConfig`: dataclass → Pydantic v2 model (frozen, validated)
- Slot queries no longer filter on `plugin = 'pgoutput'`; `SlotStatus` now carries
  `plugin`, `active_pid`, `wal_status` and `safe_wal_size`, so a same-named slot with
  a foreign plugin is reported instead of appearing missing
- Supported PostgreSQL baseline is now 13+ (slot queries read `wal_status` and
  `safe_wal_size`)

---

## [0.1.0] — 2026-06-26

### Added
- Logical replication stream using `psycopg3 AsyncConnection` and the COPY_BOTH
  protocol; non-blocking frame reads via `asyncio.Event` and `loop.add_reader`
- SIGTERM graceful shutdown
- Exponential backoff reconnect loop (1 s → 30 s)
- `Sink` protocol with `async write / flush`; `StdoutSink` (development) and
  `HttpSink` (batched POST per transaction commit) implementations
- `build_sink` factory driven by config
- `StreamConfig`: YAML file (`from_yaml`) and environment variable (`from_env`) loading
- `FileCheckpoint`: fsync-safe atomic LSN persistence
- pgoutput decoder: Begin, Commit, Relation, Insert, Update, Delete
- Idempotency key per event: `schema.table:pk:lsn:seq`
- `waltz start [--config FILE]` CLI entry point

### Changed
- HTTP sink: `httpx` (sync) → `aiohttp` (async)
