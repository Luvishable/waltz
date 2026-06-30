from __future__ import annotations

from typing import Never

import psycopg
import psycopg.errors as pg_errors


class WaltzError(Exception):
    """
    Root of all waltz-specific exceptions.
    """


class ConfigError(WaltzError):
    """
    Invalid or missiıng configuration; never retry.
    """


class ReplicationError(WaltzError):
    """
    Base for all PostgreSQL replication errors.
    """


class TransientReplicationError(ReplicationError):
    """
    Temporary condition; reconnect and retry such as connection refused,
    network drop, PG restart.
    """


class PermanentReplicationError(ReplicationError):
    """
    Fatal conditionin which retrying will not help. Such as slot invalidated,
    insufficient privilege.
    """


class SinkError(WaltzError):
    """
    Base for all delivery errors.
    """


class TransientSinkError(SinkError):
    """
    Temporary delivery failure; retry after backoff. Such as HTTP 503,
    connection timeout.
    """


class PermanentSinkError(SinkError):
    """
    Irretrievable delivery failure; stop the process. Such as HTTP 400/401/403,
    schema rejection.
    """


def raise_pg_error(exc: psycopg.Error) -> Never:
    """Wrap a psycopg exception into the waltz hierarchy."""
    if isinstance(exc, (
        pg_errors.InsufficientPrivilege,
        pg_errors.ObjectNotInPrerequisiteState,
        pg_errors.UndefinedObject,
        pg_errors.InvalidAuthorizationSpecification,
        pg_errors.ConfigurationLimitExceeded,
    )):
        raise PermanentReplicationError(str(exc)) from exc
    raise TransientReplicationError(str(exc)) from exc
