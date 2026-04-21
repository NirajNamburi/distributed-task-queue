"""Compact logging setup for dtq.

One plain human-readable formatter, configurable log level, and a
``LoggerAdapter`` that stamps every record with a ``component`` and
``host_pid`` field so multi-process output is still easy to read.
"""

from __future__ import annotations

import logging
import os
import sys


class _DtqFormatter(logging.Formatter):
    """Base format plus any ``extra=`` kwargs appended as ``key=value``.

    Example: ``log.info("claimed", extra={"task_id": "abc", "worker_id": "w1"})``
    renders as ``... claimed task_id=abc worker_id=w1``.
    """

    _STD_ATTRS = {
        "name", "msg", "args", "levelname", "levelno", "pathname", "filename",
        "module", "exc_info", "exc_text", "stack_info", "lineno", "funcName",
        "created", "msecs", "relativeCreated", "thread", "threadName",
        "processName", "process", "message", "asctime", "taskName",
    }

    def __init__(self) -> None:
        super().__init__(
            fmt="%(asctime)s %(levelname)-5s [pid=%(process)d] %(name)s: %(message)s",
            datefmt="%H:%M:%S",
        )

    def format(self, record: logging.LogRecord) -> str:
        base = super().format(record)
        extras = []
        for k, v in record.__dict__.items():
            if k in self._STD_ATTRS or k.startswith("_") or k in ("component", "host_pid"):
                continue
            extras.append(f"{k}={v!r}")
        return f"{base} {' '.join(extras)}" if extras else base


def setup_logging(level: str = "INFO") -> None:
    """Idempotently configure the root logger.

    Safe to call from multiple processes; we re-configure on each call inside
    a child process because forked log handlers can carry parent FDs that we'd
    rather not share.
    """
    root = logging.getLogger()
    root.setLevel(level.upper())
    for h in list(root.handlers):
        root.removeHandler(h)

    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(_DtqFormatter())
    root.addHandler(handler)

    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("redis").setLevel(logging.WARNING)


def get_logger(name: str) -> logging.LoggerAdapter:
    """Return a logger bound with ``component`` + ``host_pid`` defaults."""
    base = logging.getLogger(name)
    return logging.LoggerAdapter(base, {"component": name, "host_pid": os.getpid()})
