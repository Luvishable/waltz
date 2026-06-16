import os
import struct
from datetime import datetime, timedelta, timezone

import psycopg
from dotenv import load_dotenv
from psycopg import pq

from waltz.checkpoint import FileCheckpoint
from waltz.feedback import build_standby_status_update
from waltz.decoder import ChangeEvent, Decoder
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
    "options": "-c wal_sender_timeout=0"    # DEBUG ONLY
}

SLOT_NAME = "waltz_slot_pgo"
PUBLICATION_NAME = "waltz_pub"
CHECKPOINT_PATH = "waltz.lsn"   # "how far did I process" recorder

# Postgres timestamps count microseconds from 2000-01-01 UTC, NOT the Unix epoch(1970)
PG_EPOCH = datetime(2000, 1, 1, tzinfo=timezone.utc)


def format_pgtime(micros: int) -> str:
    # Add the microsecond offset onto Postgres' own epoch to get a real datetime.
    return (PG_EPOCH + timedelta(microseconds=micros)).isoformat()


def handle_payload(payload: bytes, decoder: Decoder) -> ChangeEvent | None:
    # feed the raw pgoutput messsage to the decoder. Structural messages
    # (Begin/Commit/Relation) return None; row changes return a ChangeEvent
    tag = chr(payload[0])
    event = decoder.feed(payload)
    if event is not None:
        print(f"    [{tag}] {event}")
    else:
        print(f"    [{tag}] (structural, cached {len(decoder.relations)} relation(s))")
    return event


def handle_xlogdata(frame: bytes, decoder: Decoder) -> ChangeEvent | None:
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

    print(f"    sentAt    = {format_pgtime(server_time)}")
    # The payload is a *decoded* pgoutput message; its length is unrelated to any LSN.
    # So the loop confirms progress from the event's commit LSN, not from this frame.
    return handle_payload(payload, decoder)


def handle_keepalive(frame: bytes) -> tuple[int, bool]:
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
    # The keepalive's walEnd is a safe point to advance to: it means "nothing for you
    # below here". reply_flag=1 means the server wants an answer now or it may drop us.
    return wal_end, bool(reply_flag)


def send_feedback(pgconn: pq.PGconn, checkpoint: FileCheckpoint, lsn: int) -> None:
    # 1) make it durable on OUR side first (the checkpoint file is fsync'd).
    checkpoint.write(lsn)
    # 2) only AFTER that, tell Postgres it may release WAL up to here.
    msg = build_standby_status_update(write_lsn=lsn, flush_lsn=lsn, apply_lsn=lsn)
    pgconn.put_copy_data(msg)
    # flush pushes our buffered message onto the socket; 0 means fully sent.
    pgconn.flush()
    print(f"    -> feedback flush={format_lsn(lsn)}")


def start_binary_stream() -> None:
    print("Connecting in replication mode...")
    conn = psycopg.connect(**DB_PARAMS, autocommit=True)

    with conn:
        # as replication streaming is a low-level job, normal cursor won't do.
        # pgconn is the raw libpq connection object underneath psycopg.
        pgconn = conn.pgconn

        decoder = Decoder()
        checkpoint = FileCheckpoint(CHECKPOINT_PATH)

        # Resume: read our durable record. None -> first run, start at 0/0 which means
        # "use the slot's own confirmed position". last_lsn tracks the highest point we
        # have confirmed so feedback never goes backwards.
        resume_lsn = checkpoint.read()
        last_lsn = resume_lsn or 0
        start_at = format_lsn(resume_lsn) if resume_lsn is not None else "0/0"
        print(f"Resuming from {start_at}")

        # start logical replication on our slot, ask pgoutput for our publication.
        start_cmd = (
            f"START_REPLICATION SLOT {SLOT_NAME} LOGICAL {start_at} "
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
                    event = handle_xlogdata(frame, decoder)
                    # Confirm only at a transaction boundary we fully handled: the
                    # event's commit LSN. Events arrive in commit order, so this only
                    # moves forward.
                    if event is not None and event.lsn > last_lsn:
                        last_lsn = event.lsn
                        send_feedback(pgconn, checkpoint, last_lsn)
                elif msg_type == "k":
                    wal_end, reply_now = handle_keepalive(frame)
                    last_lsn = max(last_lsn, wal_end)
                    # Reply when asked (heartbeat) so wal_sender_timeout won't drop us;
                    # this also lets the slot advance over WAL that isn't ours.
                    if reply_now:
                        send_feedback(pgconn, checkpoint, last_lsn)
                else:
                    print(f"Unknown frame {msg_type!r}: {frame.hex()}")
        except KeyboardInterrupt:
            print("\nStopped by user")


if __name__ == "__main__":
    start_binary_stream()


























