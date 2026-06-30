"""
Feedback: the messages waltz sends back to Postgres over the replication stream.

Logical replication is a two-way conversation on one socket (COPY_BOTH):
- Postgres -> use:  XLogData ('w', decoded changes) and Primary keepalive ('k').
- us -> Postgres:   Standby status update ('r'), telling the server how far we got.

This module only builds that 'r' message as raw bytes.

Why the server needs it:
    - The 'r' message's flush LSN is what advances the slot's confirmed_flush_lsn.
      Postgres uses this to delete old WAL that are no longer needed. Therefore, we must
      only report an LSN as "flushed" after it has been safely persisted on our side.
      Always perform c checkpoint before sending the feedback.
    - If the server hears nothing within wal_sender_timeout (default 60s) it drops
      the connection. Sending this status updates prevents timeouts. Furthermore, we
      must explicitly reply when a keepalive message includes the 'reply requested'
      flag to keep the connection alive.
"""

import struct

from waltz.replication.pgtime import now_micros

# Standby status update ('r'), client -> server. Big-endian (network byte order):
#   Byte1  'r'              message tag
#   Int64  write LSN        last WAL byte + 1 received and written by us
#   Int64  flush LSN        last WAL byte + 1 made durable by us  <- advances the slot
#   Int64  apply LSN        last WAL byte + 1 applied by us
#   Int64  clock            client time, microseconds since 2000-01-01
#   Byte1  replyRequested   1 = "please reply now" (used to ping the server)
# Total 34 bytes. struct.Struct precompiles the format once for repeated packing.
_STANDBY_STATUS = struct.Struct(">cQQQqB")


def build_standby_status_update(
    *,
    write_lsn: int,
    flush_lsn: int,
    apply_lsn: int,
    reply_requested: bool = False,
    clock_micros: int | None = None,
) -> bytes:
    """
    Build one Standby status update message.

    The three LSNs are usually the same value for a CDC consumer which means
    "everything up to here is durably handled"; they stay seperate to mirror the wire
    format and to leave room for better reporting later. clock_micros is injectable so
    this stays a pure, deterministic function in tests.
    """
    if clock_micros is None:
        clock_micros = now_micros()
    return _STANDBY_STATUS.pack(
        b"r",
        write_lsn,
        flush_lsn,
        apply_lsn,
        clock_micros,
        1 if reply_requested else 0,
    )
