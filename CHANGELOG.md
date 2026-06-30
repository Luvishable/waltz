# Changelog

## [Unreleased]

### Added
- Structured logging via `structlog`: JSON-compatible log output with slot and
  publication bound as context on every log line
- ISO timestamp and log level on every log entry

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
