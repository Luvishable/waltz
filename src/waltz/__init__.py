"""
waltz: a pure Python CDC service for PostgreSQL logical replication.
"""

from waltz.checkpoint import FileCheckpoint
from waltz.config import StreamConfig
from waltz.decoder import Decoder
from waltz.sink import build_sink
from waltz.stream import StreamManager


def main() -> None:
    config = StreamConfig.from_env()
    checkpoint = FileCheckpoint(config.checkpoint_path)
    decoder = Decoder()
    sink = build_sink(config.sink_type, config.sink_url)
    StreamManager(config, checkpoint, decoder, sink).run()
