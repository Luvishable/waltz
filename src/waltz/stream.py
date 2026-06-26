"""
- Connects to PG in replication mode via psycopg3 AsyncConnection
- Starts logical replication on our slot
- Reads frames via non-blocking get_copy_data + asyncio socket waiting
- Feeds pgoutput payloads to the decoder
- Drives the LSN feedback loop
"""

import asyncio
import signal

import psycopg
from psycopg import pq

from waltz.checkpoint import Checkpoint
from waltz.config import StreamConfig
from waltz.decoder import Decoder
from waltz.events import ChangeEvent, Commit
from waltz.feedback import build_standby_status_update
from waltz.frames import parse_keepalive, parse_xlogdata
from waltz.lsn import format_lsn
from waltz.sink import Sink

_INITIAL_BACKOFF = 1.0
_MAX_BACKOFF = 30.0


class StreamManager:

    def __init__(
            self,
            config: StreamConfig,
            checkpoint:Checkpoint,
            decoder: Decoder,
            sink: Sink,
    ) -> None:
        self._config = config
        self._checkpoint = checkpoint
        self._decoder = decoder
        self._sink = sink
        self._pgconn: pq.abc.PGconn | None = None
        self._last_lsn = 0
        self._stop = False

    async def run(self) -> None:
        loop = asyncio.get_running_loop()
        loop.add_signal_handler(signal.SIGTERM, self._request_stop)

        backoff = _INITIAL_BACKOFF
        while not self._stop:
            try:
                await self._connect_and_stream()
                backoff = _INITIAL_BACKOFF
            except KeyboardInterrupt:
                break
            except Exception as e:
                print(f"Stream error: {e}. Reconnecting in {backoff:.0f}s...")
                await asyncio.sleep(backoff)
                backoff = min(backoff*2, _MAX_BACKOFF)
            self._decoder.clear()

    def _request_stop(self) -> None:
        self._stop = True

    async def _connect_and_stream(self) -> None:
        conn = await psycopg.AsyncConnection.connect(
            self._config.conninfo(), autocommit=True
        )
        async with conn:
            self._pgconn = conn.pgconn
            if not self._start_replication():
                return
            await self._consume()

    def _start_replication(self) -> bool:
        assert self._pgconn is not None
        resume_lsn = self._checkpoint.read()
        self._last_lsn = resume_lsn or 0
        start_at = format_lsn(resume_lsn) if resume_lsn is not None else "0/0"
        print(f"Resuming from {start_at}")

        start_cmd = (
            f"START REPLICATION SLOT {self._config.slot} LOGICAL {start_at}"
            f"(proto_version '1', publication_names '{self._config.publication}')"
        ).encode()

        res = self._pgconn.exec_(start_cmd)
        if res.status != pq.ExecStatus.COPY_BOTH:
            print(f"Stream did not start: {self._pgconn.error_message.decode()}")
            return False

        print("Stream started (CTRL + C to stop)\n")
        return True

    async def _consume(self) -> None:
        assert self._pgconn is not None
        loop = asyncio.get_running_loop()
        fd = self._pgconn.socket

        while not self._stop:
            n_bytes, data = self._pgconn.get_copy_data(1)    # 1 = non-blocking

            if n_bytes == -1:
                print("Stream ended")
                break
            if n_bytes == -2:
                print(f"Stream error: {self._pgconn.error_message.decode()}")
                break
            if n_bytes == 0:
                # No data yet. Register a reader so that the event loop wakes us when
                # the socket becomes readable.
                readable = asyncio.Event()
                # call readable.set when the socket becomes readable that is, when
                # PG sends data
                loop.add_reader(fd, readable.set)

                try:
                    await asyncio.wait_for(readable.wait(), timeout=1.0)
                except TimeoutError:
                    pass
                finally:
                    loop.remove_reader(fd)
                continue

            frame = bytes(data)
            tag = chr(frame[0])

            if tag == "w":
                await self._handle_xlogdata(frame)
            elif tag == "k":
                self._handle_keepalive(frame)
            else:
                print(f"Unknown frame {tag!r}: {frame.hex()}")

    async def _handle_xlogdata(self, frame: bytes) -> None:
        xlog = parse_xlogdata(frame)
        result = self._decoder.feed(xlog.payload)

        if isinstance(result, ChangeEvent):
            await self._sink.write(result)
        elif isinstance(result, Commit):
            await self._sink.flush()
            if result.end_lsn > self._last_lsn:
                self._last_lsn = result.end_lsn
                self._send_feedback(self._last_lsn)

    def _handle_keepalive(self, frame: bytes) -> None:
        keepalive = parse_keepalive(frame)
        self._last_lsn = max(self._last_lsn, keepalive.wal_end)
        if keepalive.reply_requested:
            self._send_feedback(self._last_lsn)

    def _send_feedback(self, lsn: int) -> None:
        assert self._pgconn is not None
        # Checkpoint first: durable on our side before telling PG ro release WAL
        self._checkpoint.write(lsn)
        msg = build_standby_status_update(write_lsn=lsn, flush_lsn=lsn, apply_lsn=lsn)
        # put the data into the output buffer of libpq
        self._pgconn.put_copy_data(msg)
        # empty the buffer and send them via socket
        self._pgconn.flush()

































