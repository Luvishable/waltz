"""
waltz: a pure Python CDC service for PostgreSQL logical replication.
"""

from waltz.checkpoint import FileCheckpoint
from waltz.config import StreamConfig
from waltz.decoder import Decoder
from waltz.sink import HttpSink, Sink, StdoutSink
from waltz.stream import StreamManager


def main() -> None:
    """
    Composition root: build the pieces, wire them, run the stream.
    """
    config = StreamConfig.from_env()
    checkpoint = FileCheckpoint(config.checkpoint_path)
    decoder = Decoder()
    sink: Sink
    if config.sink_type == "http":
        if not config.sink_url:
            raise RuntimeError("WALTZ_SINK_URL is required when WALTZ_SINK_TYPE=http")
        sink = HttpSink(config.sink_url)
    else:
        sink = StdoutSink()

    manager = StreamManager(config, checkpoint, decoder, sink)
    manager.run()
