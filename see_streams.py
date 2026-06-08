import os
import struct
from datetime import datetime, timedelta, timezone

import psycopg
from dotenv import load_dotenv
from psycopg import pq

load_dotenv()

PASSWORD = os.getenv("POSTGRES_PASSWORD")
DB_USER = os.getenv("DB_USER")
DB_NAME = os.getenv("DB_NAME")
DB_PORT = os.getenv("DB_PORT")

DB_PARAMS = {
    "host": "localhost",
    "port": DB_PORT,
    "user": DB_USER,
    "password": PASSWORD,
    "dbname": DB_NAME,
    "replication": "database",  # logical replication mode
}

SLOT_NAME = "waltz_slot_pgo"
PUBLICATION_NAME = "waltz_pub"

# Postgres timestamps count microseconds from 2000-01-01 UTC, NOT the Unix epoch(1970)
PG_EPOCH = datetime(2000, 1, 1, tzinfo=timezone.utc)


def format_lsn(lsn: int) -> str:
    # An LSN is one 64-bit number, but Postgres prints it as two 32-bit halves in hex.
    #   lsn >> 32        -> shift the top 32 bits down   = "high" half
    #   lsn & 0xFFFFFFFF -> keep only the bottom 32 bits = "low" half
    #   :X               -> uppercase hex, no zero padding (same as Postgres)
    return f"{lsn >> 32:X}/{lsn & 0xFFFFFFFF:X}"


def format_pgtime(micros: int) -> str:
    # Add the microsecond offset onto Postgres' own epoch to get a real datetime.
    return (PG_EPOCH + timedelta(microseconds=micros)).isoformat()


def handle_payload(payload: bytes) -> None:
    # the XLogData payload is exactly one pgoutput message.
    # Like the outer COPY frame, its first byte is a type tag.
    # payload[0] is an int; chr() turns it into its character ('B' , 'C' , ... )
    tag = chr(payload[0])
    if tag == "B":
        decode_begin(payload)
    elif tag == "C":
        decode_commit(payload)
    else:
        # R / I / U / D / Y / O / T -> coming in the next steps. Show tag + hex for now.
        print(f"    [{tag}] (not parsed yet)    {len(payload)}B    {payload.hex()}")


def decode_begin(payload: bytes) -> None:
    # Begin message (pgoutput proto v1), after the 1-byte 'B' tag:
    #   [1:9]   Int64 final LSN         - this transaction's commit LSN
    #   [9:17]   Int64  commit time     - microseconds since 2000-01-01
    #   [17:21]  Int32  xid             - transaction id
    # ">QqI": Q=final LSN (8B unsigned), q=commit time (8B signed), I=xid (4B unsigned).
    # payload[1:21] is exactly 20 bytes = 8 + 8 + 4.
    final_lsn, commit_time, xid = struct.unpack(">QqI", payload[1:21])

    print(
        f"    [B] Begin   xid={xid}  finalLSN={format_lsn(final_lsn)}  "
        f"committedAt={format_pgtime(commit_time)}"
    )

def decode_commit(payload: bytes) -> None:
    # Commit message (pgoutput proto v1), after the 1-byte 'C' tag:
    #   [1]      Int8   flags         - unused in v1, always 0
    #   [2:10]   Int64  commit LSN    - LSN of this transaction's commit record
    #   [10:18]  Int64  end LSN       - position just PAST the transaction (next record)
    #   [18:26]  Int64  commit time   - microseconds since 2000-01-01
    # ">BQQq": B=flags (1B), Q=commit LSN (8B), Q=end LSN (8B), q=commit time (8B).
    # payload[1:26] is exactly 25 bytes = 1 + 8 + 8 + 8.
    flags, commit_lsn, end_lsn, commit_time = struct.unpack(">BQQq", payload[1:26])
    print(
        f"    [C] Commit  commitLSN={format_lsn(commit_lsn)}  "
        f"endLSN={format_lsn(end_lsn)}  committedAt={format_pgtime(commit_time)}"
    )

def handle_xlogdata(frame: bytes) -> None:
    # 'w' (XLogData) layout, byte by byte:
    # [0]              'w'                (1byte) the type tag, already read by the dispathcer
    # [1:9]            dataStart LSN      (8byte) wher this chunk of WAL data begins
    # [9:17]           walEnd LSN         (8byte) the current end of WAL on the server
    # [17:25]          serverTİME         (8byte) send time, microseconds since 2000-01-01
    # [25:]            payload            (Nbyte) the actual pgoutput message
    #
    # struct.unpack(format, buffer) turns raw bytes into numbers and returns a tuple.
    #   ">" big-endian (network byte order: most significant byte first approach)
    #   "Q" unsigned 64-bit int (8byte) - an LSN is a position, cant be negative
    #   "q" signed 64-bit int   (8byte) - postgre timestamps are signed int64
    # frame[1:25] is exactly 24 bytes = 8 + 8 + 8 so we read al three fields at once
    data_start, wal_end, server_time = struct.unpack(">QQq", frame[1:25])
    payload = frame[25:]    # everything after the 25-byte header is the inner message

    print("-" * 60)
    print("[w] XLogData")
    print(f"    dataStart = {format_lsn(data_start)}")
    print(f"    walEnd    = {format_lsn(wal_end)}")
    print(f"    sentAt    = {format_pgtime(server_time)}")
    handle_payload(payload)


def handle_keepalive(frame: bytes) -> None:
    # 'k' (Primary keepalive) layout:
    #   [0]             'k'             (1B)
    #   [1:9]           walEnd LSN      (8B) the current end of WAL on the server
    #   [9:17]          serverTime      (8B)
    #   [17]            replyFlag       (1B)  1 = "reply now or I may drop you on timeout"
    # ">QqB": Q=walEnd (8B), q=serverTime (8B), B=replyFlag (1B unsigned char).
    # frame[1:18] is 17 bytes = 8 + 8 + 1.
    wal_end, server_time, reply_flag = struct.unpack(">QqB", frame[1:18])

    print("-" * 60)
    print("[k] Keepalive")
    print(f"    walEnd          = {format_lsn(wal_end)}")
    print(f"    replyRequested  = {reply_flag}")


def start_binary_stream() -> None:
    print("Connecting in replication mode...")
    conn = psycopg.connect(**DB_PARAMS, autocommit=True)

    with conn:
        # as replication streaming is a low-level job, normal cursor won't do.
        # pgconn is the raw libpq connection object underneath psycopg.
        pgconn = conn.pgconn

        # start logical replication on our slot, ask pgoutput for our publication.
        start_cmd = (
            f"START_REPLICATION SLOT {SLOT_NAME} LOGICAL 0/0 "
            f"(proto_version '1', publication_names '{PUBLICATION_NAME}')"
        ).encode()  # exec_ expects bytes

        res = pgconn.exec_(start_cmd)
        if res.status != pq.ExecStatus.COPY_BOTH:
            print(f"Stream did not start: {pgconn.error_message.decode()}")
            return  # nothing to read, no need to go on and enter the loop

        # replication protocol uses COPY_BOTH as it needs two-way data flow:
        # postgresql sends wal rows
        # client can send keepalive messages and feedback messages
        print("Stream started. Raw frames below (CTRL + C to stop).\n")

        try:
            while True:
                # parameter 0(zero) blokcs and returns one complete frame per call,
                # this saves us from seeing half frame and reassemble buffers.
                nbytes, data = pgconn.get_copy_data(0)
                if nbytes == -1:
                    print("Stream ended")
                    break
                if nbytes == -2:
                    print(f"Stream error: {pgconn.error_message.decode()}")
                    break

                # copy the memoryview into immutable bytes so the frame remains independant
                # of libpq's internal buffer and can be safely sliced.
                frame = bytes(data)

                # first byte indicate frame type. frame[0] is an int and
                # chr() turns it into its character ('w').
                msg_type = chr(frame[0])
                if msg_type == "w":
                    handle_xlogdata(frame)
                elif msg_type == "k":
                    handle_keepalive(frame)
                else:
                    print(f"Unknown frame {msg_type!r}: {frame.hex()}")
        except KeyboardInterrupt:
            print("\nStopped by user")


if __name__ == "__main__":
    start_binary_stream()


























