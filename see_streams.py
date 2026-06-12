import os
import struct
from datetime import datetime, timedelta, timezone

import psycopg
from dotenv import load_dotenv
from psycopg import pq

from waltz.decoder import Decoder
from waltz.lsn import format_lsn

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


def format_pgtime(micros: int) -> str:
    # Add the microsecond offset onto Postgres' own epoch to get a real datetime.
    return (PG_EPOCH + timedelta(microseconds=micros)).isoformat()


def handle_payload(payload: bytes, decoder: Decoder) -> None:
    # Feed the raw pgoutput message to the decoder. Structural messages
    # (Begin/Commit/Relation) return None; row changes return a ChangeEvent.
    tag = chr(payload[0])
    event = decoder.feed(payload)
    if event is not None:
        print(f"    [{tag}] {event}")
    else:
        print(f"    [{tag}] (structural, cached {len(decoder.relations)} relation(s))")


def handle_xlogdata(frame: bytes, decoder: Decoder) -> None:
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
    handle_payload(payload, decoder)


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

        decoder = Decoder()

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
                    handle_xlogdata(frame, decoder)
                elif msg_type == "k":
                    handle_keepalive(frame)
                else:
                    print(f"Unknown frame {msg_type!r}: {frame.hex()}")
        except KeyboardInterrupt:
            print("\nStopped by user")


if __name__ == "__main__":
    start_binary_stream()


























