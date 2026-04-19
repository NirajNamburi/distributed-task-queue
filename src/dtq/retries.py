"""Exponential backoff with jitter.

Used by the worker when a task fails and there are retries remaining. We use
"full jitter" (Marc Brooker / AWS Architecture Blog) which is the cheapest
form of jitter that avoids thundering herd: each retry waits a random
duration in ``[0, exponential_band)`` capped at a ceiling.
"""

from __future__ import annotations

import random


def backoff_seconds(attempt: int, base: float = 2.0, cap: float = 60.0) -> float:
    """Return the delay (seconds) to wait before the ``attempt``-th retry.

    ``attempt`` is 1-indexed: ``backoff_seconds(1)`` is the wait before the
    first retry, after the original attempt failed.
    """
    if attempt < 1:
        attempt = 1
    raw = min(base * (2 ** (attempt - 1)), cap)
    return random.uniform(0.0, raw)
