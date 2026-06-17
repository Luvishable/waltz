"""
Stream manager: the heart of waltz.

- Connects to PG in replication mode
- Starts logical replication on our slot
- Read raw COPY_BOTH frames
- Feeds pgoutput payloads to the decoder
- and drives the LSN feedback loop.

It ties the pure pieces (frames, decoder, feedback, config) to the one stateful,
I/O-bound job: keep the stream alive and confirm progress only after it is durable.

Being correct means for stream manager: process durably (checkpoint) before sending
feedback, so a crash can only replay events, never lose them (at-least once principal)
"""

import psycopg
from psycopg import pq

from waltz import checkpoint
from waltz.checkpoint import Checkpoint
from waltz.config import StreamConfig
from waltz.decoder import Decoder
from waltz.events import ChangeEvent
from waltz.feedback import build_standby_status_update
from waltz.frames import parse_keepalive, parse_xlogdata
from waltz.lsn import format_lsn


class StreamManager:
    """
    connect, read, decode, confirm
    """

    def __init__(
            self,
            config: StreamConfig,
            checkpoint: Checkpoint,
            decoder: Decoder,
    ) -> None:
        self._config = config
        self._checkpoint = checkpoint
        self._decoder = decoder
        self._pgconn: pq.PGconn | None = None
        # highest LSN we have confirmed; feedback must never go backwards.
        self._last_lsn = 0

    def run(self) -> None:
        """
        Open the connection, start replication, and loop until stopped.
        """
        conn = psycopg.connect(**self._config.libpq_params(), autocommit=True)
        with conn:
            # replication streaming is a low-level job; the raw libpq connection
            # (pgconn) underneath psycopg is what speaks COPY_BOTH
            self._pgconn = conn.pgconn
            if not self._start_replication():
                return
            self._consume()

    def _start_replication(self) -> bool:
        assert self._pgconn is not None
        # Resume from our durable record. If it is None in the first run, then
        # start at 0/0, which means "use the slot's own confirmed position"
        resume_lsn = checkpoint.read()
        self._last_lsn = resume_lsn
        start_at = format_lsn(resume_lsn) if resume_lsn is not None else "0/0"
        print(f"Resuming from {start_at}")

        start_cmd = (
            f"START_REPLICATION SLOT {self._config.slot} LOGICAL {start_at} "
            f"(proto_version '1', publication names '{self._config.publication}')"
        ).encode()

        res = self._pgconn.exec_(start_cmd)
        if res.status != pq.ExecStatus.COPY_BOTH:
            print(f"Stream did not start: {self._pgconn.error_message.decode()}")
            return False

        # COPY_BOTH is two-way flow: server sends WAL rows, we send feedback
        print("Stream started (CTRL + C to stop.\n")
        return True

    # -------------------------- MAIN LOOP --------------------------

    def _consume(self) -> None:
        assert self._pgconn is not None
        try:
            while True:
                # 0 blocks and returns exactly one complete frame, so we never
                # see a half frame or have to reassemble buffers ourselves
                nbytes, data = self._pgconn.get_copy_data(0)
                if nbytes == -1:
                    print("Stream ended")
                    break
                if nbytes == -2:
                    print(f"Stream error: {self._pgconn.error_message.decode()}")
                    break

                # Copy the memoryview into immutable bytes so the frame is independent
                # of libpq's internal buffer and safe to slice
                frame = bytes(data)
                tag = chr(frame[0])

                if tag == "w":
                    self._handle_xlogdata(frame)
                elif tag == "k":
                    self._handle_keepalive(frame)
                else:
                    print(f"Unknown frame {tag!r}: {frame.hex()}")
        except KeyboardInterrupt:
            print("\nStopped by user")

    # --------------------- FRAME HANDLERS ----------------------

    def _handle_xlogdata(self, frame: bytes) -> None:
        xlog = parse_xlogdata(frame)
        event = self._decoder.feed(xlog.payload)
        if event is None:
            return    # structural message (Begin/Commit/Relation); nothing to emit
        self._emit(event)
        if event.lsn > self._last_lsn:
            self._last_lsn = event.lsn
            self._send_feedback(self._last_lsn)

    def _handle_keepalive(self, frame: bytes) -> None:
        keepalive = parse_keepalive(frame)
        # walEnd is a safe point: nothing below it is for us
        self._last_lsn = max(self._last_lsn, keepalive.wal_end)
        # Reply when asked so wal_sender_timeout won't drop us; this also lets slot
        # advance over WAL that isn't ours.
        if keepalive.reply_requested:
            self._send_feedback(self._last_lsn)

    # ---------------------- OUTPUT & FEEDBACK ----------------------

    def _emit(self, event: ChangeEvent) -> None:
        # Placeholder output. The sink layer will replace this body next step
        print(f"    {event}")

    def _send_feedback(self, lsn: int) -> None:
        assert self._pgconn is not None
        # 1) make it durable on our side first (the checkpoint file is fsync'd)
        self._checkpoint.write(lsn)
        # 2) only after that, tell PG it may release WAL up to here
        msg = build_standby_status_update(write_lsn=lsn, flush_lsn=lsn, apply_lsn=lsn)
        # flush pushes our buffered message onto the socket
        self._pgconn.flush()
        print(f"    -> feedback flush={format_lsn(lsn)}")
































































































