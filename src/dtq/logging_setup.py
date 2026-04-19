"""Structured JSON logging via the stdlib ``logging`` module.

We avoid third-party logging libraries to keep the dependency surface tight.
The formatter emits one JSON object per line so the output is friendly to
log shippers (Vector, Fluent Bit, Loki) without any further parsing.
"""

from __future__ import annotations

import json
import logging
import os
import sys
import time
from typing import Any


_STD_ATTRS = {
    "name", "msg", "args", "levelname", "levelno", "pathname", "filename",
    "module", "exc_info", "exc_text", "stack_info", "lineno", "funcName",
    "created", "msecs", "relativeCreated", "thread", "threadName",
    "processName", "process", "message", "asctime", "taskName",
}


class JsonFormatter(logging.Formatter):
    """Render every log record as a single line of JSON.

    Any extra fields passed via ``logger.info("...", extra={"k": "v"})`` are
    merged into the top-level object. This is the killer feature for
    structured logs: you can correlate by ``task_id`` or ``worker_id`` later.
    """

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "ts": time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(record.created)) + f".{int(record.msecs):03d}Z",
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
            "pid": record.process,
            "thread": record.threadName,
        }
        for k, v in record.__dict__.items():
            if k in _STD_ATTRS or k.startswith("_"):
                continue
            try:
                json.dumps(v)
                payload[k] = v
            except (TypeError, ValueError):
                payload[k] = repr(v)

        if record.exc_info:
            payload["exc"] = self.formatException(record.exc_info)
        if record.stack_info:
            payload["stack"] = self.formatStack(record.stack_info)
        return json.dumps(payload, ensure_ascii=False)


class HumanFormatter(logging.Formatter):
    """A compact, color-free human formatter for local dev when JSON is too noisy."""

    def __init__(self) -> None:
        super().__init__(
            fmt="%(asctime)s %(levelname)-5s [pid=%(process)d] %(name)s: %(message)s",
            datefmt="%H:%M:%S",
        )


_CONFIGURED = False


def setup_logging(level: str = "INFO", json_mode: bool = True) -> None:
    """Idempotently configure the root logger.

    Safe to call from multiple processes; we re-configure on each call inside
    a child process because forked log handlers can carry parent FDs that we'd
    rather not share.
    """
    global _CONFIGURED
    root = logging.getLogger()
    root.setLevel(level.upper())
    for h in list(root.handlers):
        root.removeHandler(h)

    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(JsonFormatter() if json_mode else HumanFormatter())
    root.addHandler(handler)

    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("redis").setLevel(logging.WARNING)
    _CONFIGURED = True


def get_logger(name: str) -> logging.LoggerAdapter:
    """Return a logger bound with a default ``component`` attribute."""
    base = logging.getLogger(name)
    return logging.LoggerAdapter(base, {"component": name, "host_pid": os.getpid()})
