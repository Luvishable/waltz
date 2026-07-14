"""
Turning raw catalog facts into pass/fail verdicts with a fix hint

queries.py answers "what is there"; this module answers "is that healthy,
and if not, what should the operator do". This split enables every verdict to be
a pure function: values in, CheckResult out, no I/O.

Unlike the other commands, a failure here is not an exception but the product:
a dead connection becomes a FAIL row, everything it blocks becomes SKIP, and the
report still renders.
"""

from dataclasses import dataclass
from enum import StrEnum

from waltz.admin.queries import (
    PublicationInfo,
    RoleFlags,
    ServerSettings,
    SlotStatus,
    admin_connection,
    count_used_slots,
    get_current_role_flags,
    get_publication_info,
    get_server_settings,
    get_slot_status,
    probe_replication_connection,
)
from waltz.checkpoint.checkpoint import FileCheckpoint
from waltz.config.config import StreamConfig
from waltz.core.lsn import format_lsn, parse_lsn
from waltz.errors import ReplicationError

_MIN_SUPPORT_VERSION = 130000   # wal_status / safe_wal_size appeared in PG 13


class CheckOutcome(StrEnum):
    PASS = "pass"
    WARN = "warn"   # works today, who knows it will tomorrow
    FAIL = "fail"
    SKIP = "skip"   # prerequisite failed or check is not applicable


@dataclass(frozen=True, slots=True)
class CheckResult:
    name: str
    outcome: CheckOutcome
    observed: str               # what we actually saw
    hint: str | None = None     # only for non-PASS outcomes


# Must mirror execution order: when a query dies mid-run, the first name without
# a result is the check that was in flight (it takes the FAIL) and the rest were
# never reached (they take SKIP)
_CATALOG_CHECKS = (
    "connection",
    "server_version",
    "wal_level",
    "replication_privilege",
    "slot_capacity",
    "slot",
    "slot_holder",
    "slot_wal_status",
    "lag",
    "publication",
)


def check_server_version(settings: ServerSettings) -> CheckResult:
    major = settings.version_num // 10000
    if settings.version_num < _MIN_SUPPORT_VERSION:
        return CheckResult(
            "server_version", CheckOutcome.FAIL, f"PostgreSQL {major}",
            "waltz relies on PG 13+ slot columns (wal_status, safe_wal_size); upgrade the server",
        )
    return CheckResult("server_version", CheckOutcome.PASS, f"PostgreSQL {major}")


def check_wal_level(settings: ServerSettings) -> CheckResult:
    if settings.wal_level != "logical":
        return CheckResult(
            "wal_level", CheckOutcome.FAIL, settings.wal_level,
            "set wal_level = logical in postgresql.conf and restart (reload is not enough)",
        )
    return CheckResult(
        "wal_level", CheckOutcome.PASS, "logical")


def check_replication_privilege(role: RoleFlags) -> CheckResult:
    if role.superuser or role.replication:
        via = "superuser" if role.superuser else "replication"
        return CheckResult("replication_privilege", CheckOutcome.PASS, f"{role.name} ({via})")
    return CheckResult(
        "replication_privilege", CheckOutcome.FAIL, f"{role.name} lacks REPLICATION",
        f'ALTER ROLE "{role.name}" REPLICATION',
    )


def check_slot_capacity(settings: ServerSettings, used: int, slot_exists: bool) -> CheckResult:
    if slot_exists:
        return CheckResult("slot_capacity", CheckOutcome.SKIP, "slot already exists")
    observed = f"{used}/{settings.max_replication_slots} slots in use"
    if used >= settings.max_replication_slots:
        return CheckResult(
            "slot_capacity", CheckOutcome.FAIL, observed,
            "`waltz init` will fail: drop an unused slot or raise max_replication_slots (restart)",
        )
    # if slot not exist and there is room for the slot that waltz will need
    return CheckResult("slot_capacity", CheckOutcome.PASS, observed)


def check_slot(slot: SlotStatus | None, expected: str) -> CheckResult:
    if slot is None:
        return CheckResult(
            "slot", CheckOutcome.FAIL, f"{expected!r} does not exist", "run `waltz init`"
        )
    if slot.plugin != "pgoutput":
        return CheckResult(
            "slot", CheckOutcome.FAIL, f"exists, but plugin is {slot.plugin!r}",
            "waltz decodes pgoutput only: drop the slot, then run `waltz init`",
        )
    return CheckResult("slot", CheckOutcome.PASS, "exists, plugin pgoutput")


def check_slot_holder(slot: SlotStatus) -> CheckResult:
    if slot.active:
        return CheckResult(
            "slot_holder", CheckOutcome.WARN, f"held by backend PID {slot.active_pid}",
            "fine if that is your running `waltz start`; otherwise another consumer"
            "owns the slot"
        )
    return CheckResult("slot_holder", CheckOutcome.PASS, "not in use")


def check_slot_wal_status(slot: SlotStatus) -> CheckResult:
    if slot.wal_status == "lost":
        return CheckResult(
            "slot_wal_status", CheckOutcome.FAIL,
            "lost: required WAL segments were removed",
            "the slot can never resume: drop it, run `waltz init`, resync the sink "
            "from scratch"
        )
    if slot.wal_status == "unreserved":
        safe = f"{slot.safe_wal_size:,} B left" if slot.safe_wal_size is not None else "limit near"
        return CheckResult(
            "slot_wal_status", CheckOutcome.WARN, f"unreserved ({safe})",
            "invalidation is close: start the consumer or raise max_slot_wal_keep_size",
        )
    return CheckResult("slot_wal_status", CheckOutcome.PASS, slot.wal_status or "unknown")


def check_lag(slot: SlotStatus, settings: ServerSettings) -> CheckResult:
    # Intentionally avoiding a hardcoded byte limit. We rely on PG's 'wal_status'
    # to determine the risk. The lag size is only displayed for informational purposes.
    if slot.lag_bytes is None:
        return CheckResult(
            "lag", CheckOutcome.WARN, "not measurable (slot has no confirmed_flush_lsn)",
            "run `waltz start` once so the slot records a consumed position"
        )
    return CheckResult(
        "lag", CheckOutcome.PASS,
        f"{slot.lag_bytes:,} B (max_slot_wal_keep_size={settings.max_slot_wal_keep_size})",
    )


def check_publication(pub: PublicationInfo | None, expected: str) -> CheckResult:
    if pub is None:
        return CheckResult(
            "publication", CheckOutcome.FAIL, f"{expected!r} does not exist", "run `waltz init`"
        )
    if pub.table_count == 0:
        return CheckResult(
            "publication", CheckOutcome.WARN, "exists, but convers 0 tables",
            "the stream will silently publish nothing; create tables or widen the publication",
        )
    scope = "ALL TABLES" if pub.all_tables else "explicit list"
    return CheckResult(
        "publication", CheckOutcome.PASS, f"exists, {scope}, {pub.table_count} tables",
    )


def check_checkpoint_file(path: str, slot: SlotStatus | None) -> CheckResult:
    # Deliberately reuse FileCheckpoint: diagnose must rehearse the exact code path
    # `waltz start` runs, not a lookalike reimplementation.
    try:
        lsn = FileCheckpoint(path).read()
    except ValueError:
        return CheckResult(
            "checkpoint_file", CheckOutcome.FAIL, f"{path}: content is not an LSN",
            "delete the file; the stream resumes from the slot position"
            "(duplicates are expected and safe under at-least-once)",
        )
    except OSError as e:
        return CheckResult(
            "checkpoint_file", CheckOutcome.FAIL, f"unreadable: {e}", "fix file permissions",
        )
    if lsn is None:
        return CheckResult("checkpoint_file", CheckOutcome.PASS, "no checkpoint yet (fresh start)")
    if slot is None or slot.confirmed_flush_lsn is None:
        return CheckResult(
            "checkpoint_file", CheckOutcome.PASS,
            f"{format_lsn(lsn)} (no slot position to compare against)",
        )
    if lsn < parse_lsn(slot.confirmed_flush_lsn):
        return CheckResult(
            "checkpoint_file", CheckOutcome.WARN,
            f"file {format_lsn(lsn)} is behind slot {slot.confirmed_flush_lsn}",
            "waltz checkpoints before feedback, so this should not happen: "
            "stale file from a backup, or a foreign consumer advanced the slot"
        )
    return CheckResult(
        "checkpoint_file", CheckOutcome.PASS,
        f"{format_lsn(lsn)} (slot at {slot.confirmed_flush_lsn})"
    )


def _skip(name: str, reason: str) -> CheckResult:
    return CheckResult(name, CheckOutcome.SKIP, reason)


async def run_diagnostics(config: StreamConfig) -> list[CheckResult]:
    results, slot = await _catalog_checks(config)
    results.append(check_checkpoint_file(config.checkpoint_path, slot))
    results.append(await _probe_check(config))
    return results


async def _catalog_checks(config: StreamConfig) -> tuple[list[CheckResult], SlotStatus | None]:
    results: list[CheckResult] = []
    slot: SlotStatus | None = None
    try:
        async with admin_connection(config) as conn:
            # if the connection is successful which means you have conn object now,
            # append it in results. Remember the flow follows _CATALOG_CHECKS!
            results.append(CheckResult(
                "connection", CheckOutcome.PASS,
                f"{config.host}:{config.port}/{config.dbname}",
            ))
            # check version but if it's not the version waltz expects, then skip
            # other checks as they become unnecessary to check.
            settings = await get_server_settings(conn)
            version = check_server_version(settings)
            results.append(version)
            if version.outcome is CheckOutcome.FAIL:
                results.extend(_skip(n, "unsupported server version") for n in _CATALOG_CHECKS[2:])
                return results, None

            # check wal_level. should be logical for waltz
            results.append(check_wal_level(settings))

            # the current role must allow to connect in replication mode
            results.append(check_replication_privilege(await get_current_role_flags(conn)))

            # the checks above required settings but now we need info about slot as
            # the checks below is about the health of slot itself
            slot = await get_slot_status(conn, config.slot)
            used = await count_used_slots(conn)

            results.append(check_slot_capacity(settings, used, slot is not None))
            results.append(check_slot(slot, config.slot))

            # check_slot_holder, check_slot_wal_status, check_lag strictly dependant
            # on the existence of slot object.
            if slot is None:
                results.extend(
                    _skip(n, "slot does not exist")
                    for n in ("slot_holder", "slot_wal_status", "lag")
                )
            else:
                results.append(check_slot_holder(slot))
                results.append(check_slot_wal_status(slot))
                results.append(check_lag(slot, settings))

            # finally check publication
            pub = await get_publication_info(conn, config.publication)
            results.append(check_publication(pub, config.publication))

    except ReplicationError as e:
        done = {r.name for r in results}
        pending = [n for n in _CATALOG_CHECKS if n not in done]
        # hint will be transformed according to the connection success
        hint = (
            "check host/port, credentials and that PG is running"
            if pending[0] == "connection"
            else "the session died mid-diagnosis; re-run `waltz diagnose`"
        )
        # program run accross the problem while handling the pending[0] so mark
        # it as a FAIL and the rest has to be skipped
        results.append(CheckResult(pending[0], CheckOutcome.FAIL, str(e).strip(), hint))
        results.extend(_skip(n, "a prerequisite check failed") for n in pending[1:])
    return results, slot


async def _probe_check(config: StreamConfig) -> CheckResult:
    try:
        await probe_replication_connection(config)
    except ReplicationError as e:
        return CheckResult(
            "replication_probe", CheckOutcome.FAIL, str(e).strip(),
            "this is the exact connection `waltz start` opends: check the REPLICATION "
            "privilege, free max_wal_senders capacity and pg_hba.conf",
        )
    return CheckResult("replication_probe", CheckOutcome.PASS, "replication connection accepted")













































































