"""
LSN (Log Sequence Number) helpers.

- An LSN is a 64-bit position in the WAL.
- Internally, we keep it as a plain int, because that's what comparisons and arithmetic need.
- On the wire and in the logs, PostgreSQL prints it as two 32-bit halves in hex: "high/low" (e.g. 0/16B3748)
- These two function convert between the representations.
"""


def format_lsn(lsn: int) -> str:
    return f"{lsn >> 32:X}/{lsn & 0xFFFFFFFF:X}"


def parse_lsn(text: str) -> int:
    # inverse of format_lsn: "high/low" hex -> 64-bit int
    high_str, low_str = text.split("/")
    return (int(high_str, 16) << 32) | int(low_str, 16)

