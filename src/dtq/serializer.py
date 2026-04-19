"""Pickle wrapper with a hard size guard.

Keeping serialization in one place lets us swap protocols (or add compression)
later without touching producers or workers. The 8 MiB ceiling protects Redis
from accidentally accepting multi-hundred-megabyte arguments.
"""

from __future__ import annotations

import pickle
from typing import Any


PICKLE_PROTOCOL = pickle.HIGHEST_PROTOCOL
MAX_PAYLOAD_BYTES = 8 * 1024 * 1024  # 8 MiB - generous but bounded.


class PayloadTooLarge(ValueError):
    """Raised when a serialized payload exceeds :data:`MAX_PAYLOAD_BYTES`."""


def dumps(obj: Any) -> bytes:
    """Pickle ``obj`` and enforce the size ceiling."""
    blob = pickle.dumps(obj, protocol=PICKLE_PROTOCOL)
    if len(blob) > MAX_PAYLOAD_BYTES:
        raise PayloadTooLarge(
            f"Serialized payload is {len(blob):,} bytes, exceeds {MAX_PAYLOAD_BYTES:,} byte limit"
        )
    return blob


def loads(blob: bytes) -> Any:
    """Inverse of :func:`dumps`. Returns the unpickled object."""
    return pickle.loads(blob)
