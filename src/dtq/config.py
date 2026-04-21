"""Environment-driven configuration for dtq.

All knobs are read from environment variables with safe defaults so the system
can be booted with zero configuration against a local Redis on 6379.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field, fields


def _env_str(name: str, default: str) -> str:
    return os.environ.get(name, default)


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    try:
        return int(raw)
    except ValueError as exc:
        raise ValueError(f"Environment variable {name}={raw!r} is not an int") from exc


def _env_float(name: str, default: float) -> float:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    try:
        return float(raw)
    except ValueError as exc:
        raise ValueError(f"Environment variable {name}={raw!r} is not a float") from exc


def _default_processes() -> int:
    return os.cpu_count() or 1


@dataclass(frozen=True)
class Settings:
    """Immutable runtime settings.

    Use :meth:`from_env` to construct from environment variables, or instantiate
    directly in tests to override any field.
    """

    redis_url: str = field(default_factory=lambda: _env_str("DTQ_REDIS_URL", "redis://localhost:6379/0"))

    # Redis key names. Centralizing them avoids string-typo bugs across modules.
    pending_queue: str = field(default_factory=lambda: _env_str("DTQ_PENDING_QUEUE", "dtq:pending"))
    processing_prefix: str = field(default_factory=lambda: _env_str("DTQ_PROCESSING_PREFIX", "dtq:processing:"))
    dlq: str = field(default_factory=lambda: _env_str("DTQ_DLQ", "dtq:dead_letter"))
    retry_zset: str = field(default_factory=lambda: _env_str("DTQ_RETRY_ZSET", "dtq:retries"))
    heartbeat_hash: str = field(default_factory=lambda: _env_str("DTQ_HEARTBEAT_HASH", "dtq:workers:heartbeats"))
    task_hash_prefix: str = field(default_factory=lambda: _env_str("DTQ_TASK_HASH_PREFIX", "dtq:task:"))

    # Behavior knobs.
    max_retries: int = field(default_factory=lambda: _env_int("DTQ_MAX_RETRIES", 3))
    backoff_base_s: float = field(default_factory=lambda: _env_float("DTQ_BACKOFF_BASE_S", 2.0))
    backoff_cap_s: float = field(default_factory=lambda: _env_float("DTQ_BACKOFF_CAP_S", 60.0))

    # Heartbeat / liveness.
    heartbeat_interval_s: float = field(default_factory=lambda: _env_float("DTQ_HEARTBEAT_INTERVAL_S", 5.0))
    worker_timeout_s: float = field(default_factory=lambda: _env_float("DTQ_WORKER_TIMEOUT_S", 60.0))

    # Worker pool.
    worker_processes: int = field(default_factory=lambda: _env_int("DTQ_WORKER_PROCESSES", _default_processes()))
    claim_block_s: float = field(default_factory=lambda: _env_float("DTQ_CLAIM_BLOCK_S", 5.0))
    shutdown_grace_s: float = field(default_factory=lambda: _env_float("DTQ_SHUTDOWN_GRACE_S", 15.0))

    # Reaper.
    reaper_interval_s: float = field(default_factory=lambda: _env_float("DTQ_REAPER_INTERVAL_S", 5.0))

    # Logging.
    log_level: str = field(default_factory=lambda: _env_str("DTQ_LOG_LEVEL", "INFO"))

    @classmethod
    def from_env(cls) -> "Settings":
        """Construct a Settings by reading environment variables."""
        return cls()

    def task_hash_key(self, task_id: str) -> str:
        return f"{self.task_hash_prefix}{task_id}"

    def processing_key(self, worker_id: str) -> str:
        return f"{self.processing_prefix}{worker_id}"

    def as_dict(self) -> dict:
        return {f.name: getattr(self, f.name) for f in fields(self)}
