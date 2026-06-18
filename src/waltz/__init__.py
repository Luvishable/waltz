"""
waltz: a pure Python CDC service for PostgreSQL logical replication.
"""

from waltz.checkpoint import FileCheckpoint
from waltz.config import StreamConfig
from waltz.decoder import Decoder
from waltz.stream import StreamManager


def main() -> None:
    """
    Composition root: build the pieces, wire them, run the stream.
    """
    config = StreamConfig.from_env()
    checkpoint = FileCheckpoint(config.checkpoint_path)
    decoder = Decoder()
    manager = StreamManager(config, checkpoint, decoder)
    manager.run()
