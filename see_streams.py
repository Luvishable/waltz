import os
import sys

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


def start_binary_stream() -> None:
    print("Connecting in replication mode...")
    conn = psycopg.connect(**DB_PARAMS, autocommit=True)

    with conn:
        # as replication streaming is a low-level job, normal cursor is not enough.
        # That's why we need underlying C lib which is libpq and pgconn represents raw
        # connection object
        pgconn = conn.pgconn

        # start a logical replication, clarify the slot that will be used,
        # request the WAL(write-ahead log) changes
        start_cmd = (
            f"START_REPLICATION SLOT {SLOT_NAME} LOGICAL 0/0 "
            f"(proto_version '1', publication_names '{PUBLICATION_NAME}')"
        ).encode()

        # exec_ expects bytes
        res = pgconn.exec_(start_cmd)
        if res.status != pq.ExecStatus.COPY_BOTH:
            print(f"Stream did not start: {pgconn.error_message.decode()}")

        # replication protocol uses COPY_BOTH as it needs two-way data flow:
        # postgresql sends wal rows
        # client can send keepalive messages and feedback messages
        print("Stream started. Raw frames below (CTRL + C to stop).\n")

        try:
            while True:
                nbytes, data = pgconn.get_copy_data(0)
                if nbytes == -1:
                    print("Stream ended")
                    break
                if nbytes == -2:
                    print(f"Stream error: {pgconn.error_message.decode()}")
                    break

                msg_type = chr(data[0])
                print("-" * 60)
                print(f"type={msg_type!r} size={nbytes}B")
                print(f"hex: {data.hex()}")
        except KeyboardInterrupt:
            print("\nStopped by user")

if __name__ == "__main__":
    start_binary_stream()


























