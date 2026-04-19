"""Task model and shared constants.

A ``Task`` is the in-Python representation of work to be executed. Its
authoritative on-the-wire form is a Redis hash at ``dtq:task:<id>`` with
fields described in :class:`TaskField`. The pending queue itself only ever
stores the task_id (a UUID4 string) to keep queue payloads tiny and the
hash holds everything else.
"""

from __future__ import annotations

import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class TaskState(str, Enum):
    """Lifecycle states. Stored as the ``state`` hash field."""

    QUEUED = "QUEUED"
    RUNNING = "RUNNING"
    RETRYING = "RETRYING"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"


class TaskField:
    """Hash field names. Centralized so Lua scripts and Python agree."""

    ID = "id"
    FUNC = "func"
    ARGS = "args"
    KWARGS = "kwargs"
    STATE = "state"
    ATTEMPTS = "attempts"
    MAX_RETRIES = "max_retries"
    ENQUEUED_AT = "enqueued_at"
    CLAIMED_AT = "claimed_at"
    COMPLETED_AT = "completed_at"
    WORKER_ID = "worker_id"
    RESULT = "result"
    ERROR = "error"
    ERROR_TYPE = "error_type"
    NEXT_RUN_AT = "next_run_at"


@dataclass
class Task:
    """An enqueueable unit of work.

    ``func`` is a dotted path resolvable via :mod:`dtq.registry`; ``args`` and
    ``kwargs`` are pickled when persisted. We deliberately avoid pickling the
    function itself - that's the Celery footgun (cross-version pickles, missing
    modules at the worker, etc).
    """

    func: str
    args: tuple = ()
    kwargs: dict[str, Any] = field(default_factory=dict)
    id: str = field(default_factory=lambda: uuid.uuid4().hex)
    max_retries: int = 3
    enqueued_at: float = field(default_factory=time.time)
    attempts: int = 0
