"""The Redis facade.

Every Redis interaction in dtq goes through :class:`Broker`. That single
choke-point makes it easy to (a) audit which keys we touch, (b) swap
fakeredis in for tests, and (c) load all Lua scripts exactly once per
process. State-mutating operations are implemented as Lua scripts so the
``LREM processing + HSET task hash`` pair is atomic - workers can crash
mid-finalize without leaving the system in a torn state.
"""

from __future__ import annotations

import time
from importlib import resources
from typing import Any, Iterable

import redis

from dtq import serializer
from dtq.config import Settings
from dtq.task import Task, TaskField, TaskState


_LUA_SCRIPTS = (
    "mark_running",
    "complete",
    "fail_retry",
    "fail_dlq",
    "requeue_dead",
    "promote_due",
)


def _load_lua(name: str) -> str:
    """Read a Lua script bundled inside the ``dtq.lua`` package."""
    return resources.files("dtq.lua").joinpath(f"{name}.lua").read_text(encoding="utf-8")


class Broker:
    """Thin, opinionated Redis wrapper used by clients, workers, and the reaper."""

    def __init__(self, settings: Settings, client: redis.Redis | None = None) -> None:
        self.settings = settings
        # Note: decode_responses=False so we can store binary pickle blobs in
        # hash fields without UTF-8 decoding errors.
        self.r: redis.Redis = client if client is not None else redis.Redis.from_url(
            settings.redis_url, decode_responses=False
        )
        self._scripts = {name: self.r.register_script(_load_lua(name)) for name in _LUA_SCRIPTS}

    # ------------------------------------------------------------------
    # Connectivity
    # ------------------------------------------------------------------

    def ping(self) -> bool:
        return bool(self.r.ping())

    def close(self) -> None:
        try:
            self.r.close()
        except Exception:
            pass

    # ------------------------------------------------------------------
    # Producer side
    # ------------------------------------------------------------------

    def enqueue(self, task: Task) -> None:
        """Persist the task hash and push its id onto the pending queue."""
        s = self.settings
        task_key = s.task_hash_key(task.id)
        mapping = {
            TaskField.ID: task.id,
            TaskField.FUNC: task.func,
            TaskField.ARGS: serializer.dumps(task.args),
            TaskField.KWARGS: serializer.dumps(task.kwargs),
            TaskField.STATE: TaskState.QUEUED.value,
            TaskField.ATTEMPTS: str(task.attempts),
            TaskField.MAX_RETRIES: str(task.max_retries),
            TaskField.ENQUEUED_AT: repr(task.enqueued_at),
        }
        pipe = self.r.pipeline(transaction=True)
        pipe.hset(task_key, mapping=mapping)
        pipe.lpush(s.pending_queue, task.id)
        pipe.execute()

    # ------------------------------------------------------------------
    # Worker side
    # ------------------------------------------------------------------

    def claim(self, worker_id: str, block_s: float) -> str | None:
        """Block (up to ``block_s`` seconds) waiting for a task.

        Implements the reliable-queue pattern: BLMOVE atomically pops the
        task_id from the pending queue and pushes it into a per-worker
        processing list, so a crash here cannot lose the task. After the
        BLMOVE succeeds we run the ``mark_running`` Lua script to update
        the task hash state.
        """
        s = self.settings
        processing_key = s.processing_key(worker_id)
        raw = self.r.blmove(
            s.pending_queue,
            processing_key,
            timeout=block_s,
            src="RIGHT",
            dest="LEFT",
        )
        if raw is None:
            return None
        task_id = raw.decode("utf-8") if isinstance(raw, (bytes, bytearray)) else str(raw)
        task_key = s.task_hash_key(task_id)
        existed = self._scripts["mark_running"](
            keys=[task_key],
            args=[worker_id, repr(time.time())],
        )
        if int(existed) == 0:
            # Task hash was wiped between enqueue and claim - drop it so the
            # processing list doesn't accumulate orphans.
            self.r.lrem(processing_key, 0, task_id)
            return None
        return task_id

    def fetch_task(self, task_id: str) -> tuple[str, tuple, dict, int, int] | None:
        """Read the executable payload for a claimed task."""
        s = self.settings
        raw = self.r.hmget(
            s.task_hash_key(task_id),
            [
                TaskField.FUNC,
                TaskField.ARGS,
                TaskField.KWARGS,
                TaskField.ATTEMPTS,
                TaskField.MAX_RETRIES,
            ],
        )
        func_raw, args_raw, kwargs_raw, attempts_raw, max_retries_raw = raw
        if func_raw is None:
            return None
        func = func_raw.decode("utf-8") if isinstance(func_raw, (bytes, bytearray)) else func_raw
        args = serializer.loads(args_raw) if args_raw else ()
        kwargs = serializer.loads(kwargs_raw) if kwargs_raw else {}
        attempts = int(attempts_raw or 0)
        max_retries = int(max_retries_raw or self.settings.max_retries)
        return func, args, kwargs, attempts, max_retries

    def complete(self, worker_id: str, task_id: str, result: Any) -> None:
        s = self.settings
        self._scripts["complete"](
            keys=[s.processing_key(worker_id), s.task_hash_key(task_id)],
            args=[task_id, repr(time.time()), serializer.dumps(result)],
        )

    def fail_retry(
        self,
        worker_id: str,
        task_id: str,
        attempts: int,
        run_at: float,
        error: str,
        error_type: str,
    ) -> None:
        s = self.settings
        self._scripts["fail_retry"](
            keys=[s.processing_key(worker_id), s.task_hash_key(task_id), s.retry_zset],
            args=[task_id, repr(time.time()), repr(run_at), str(attempts), error, error_type],
        )

    def fail_dlq(
        self,
        worker_id: str,
        task_id: str,
        attempts: int,
        error: str,
        error_type: str,
    ) -> None:
        s = self.settings
        self._scripts["fail_dlq"](
            keys=[s.processing_key(worker_id), s.task_hash_key(task_id), s.dlq],
            args=[task_id, repr(time.time()), str(attempts), error, error_type],
        )

    # ------------------------------------------------------------------
    # Heartbeats / reaper
    # ------------------------------------------------------------------

    def heartbeat(self, worker_id: str) -> None:
        self.r.hset(self.settings.heartbeat_hash, worker_id, repr(time.time()))

    def remove_heartbeat(self, worker_id: str) -> None:
        self.r.hdel(self.settings.heartbeat_hash, worker_id)

    def list_workers(self) -> dict[str, float]:
        """Return ``{worker_id: last_heartbeat_epoch}`` for all live entries."""
        raw = self.r.hgetall(self.settings.heartbeat_hash)
        out: dict[str, float] = {}
        for k, v in raw.items():
            wid = k.decode("utf-8") if isinstance(k, (bytes, bytearray)) else k
            try:
                out[wid] = float(v)
            except (TypeError, ValueError):
                continue
        return out

    def dead_workers(self, now: float, ttl_s: float) -> list[str]:
        return [wid for wid, ts in self.list_workers().items() if (now - ts) > ttl_s]

    def requeue_dead_worker(self, worker_id: str) -> tuple[int, int]:
        """Return ``(requeued, dlq'd)`` counts."""
        s = self.settings
        result = self._scripts["requeue_dead"](
            keys=[s.processing_key(worker_id), s.pending_queue, s.heartbeat_hash],
            args=[
                worker_id,
                s.task_hash_prefix,
                repr(time.time()),
                str(s.max_retries),
                s.dlq,
            ],
        )
        if isinstance(result, list) and len(result) == 2:
            return int(result[0]), int(result[1])
        return 0, 0

    def promote_due_retries(self, now: float | None = None, limit: int = 1000) -> int:
        s = self.settings
        n = self._scripts["promote_due"](
            keys=[s.retry_zset, s.pending_queue, s.task_hash_prefix],
            args=[repr(now if now is not None else time.time()), str(limit), s.task_hash_prefix],
        )
        return int(n)

    # ------------------------------------------------------------------
    # Introspection (CLI / tests)
    # ------------------------------------------------------------------

    def queue_depth(self, key: str) -> int:
        return int(self.r.llen(key))

    def zset_size(self, key: str) -> int:
        return int(self.r.zcard(key))

    def pending_depth(self) -> int:
        return self.queue_depth(self.settings.pending_queue)

    def dlq_depth(self) -> int:
        return self.queue_depth(self.settings.dlq)

    def retry_depth(self) -> int:
        return self.zset_size(self.settings.retry_zset)

    def in_flight_count(self) -> int:
        """Sum of the lengths of all per-worker processing lists."""
        s = self.settings
        prefix = s.processing_prefix.encode() if isinstance(s.processing_prefix, str) else s.processing_prefix
        total = 0
        for key in self.r.scan_iter(match=prefix + b"*"):
            total += int(self.r.llen(key))
        return total

    def get_task_state(self, task_id: str) -> str | None:
        raw = self.r.hget(self.settings.task_hash_key(task_id), TaskField.STATE)
        if raw is None:
            return None
        return raw.decode("utf-8") if isinstance(raw, (bytes, bytearray)) else raw

    def get_task_hash(self, task_id: str) -> dict[str, Any]:
        raw = self.r.hgetall(self.settings.task_hash_key(task_id))
        out: dict[str, Any] = {}
        for k, v in raw.items():
            key = k.decode("utf-8") if isinstance(k, (bytes, bytearray)) else k
            if key in (TaskField.RESULT, TaskField.ARGS, TaskField.KWARGS):
                try:
                    out[key] = serializer.loads(v)
                    continue
                except Exception:
                    pass
            if isinstance(v, (bytes, bytearray)):
                try:
                    out[key] = v.decode("utf-8")
                except UnicodeDecodeError:
                    out[key] = v
            else:
                out[key] = v
        return out

    def get_task_result(self, task_id: str) -> Any:
        raw = self.r.hget(self.settings.task_hash_key(task_id), TaskField.RESULT)
        if raw is None:
            return None
        return serializer.loads(raw)

    def replay_dlq(self, task_ids: Iterable[str] | None = None) -> int:
        """Move tasks from the DLQ back to pending. ``None`` replays all."""
        s = self.settings
        if task_ids is None:
            count = 0
            while True:
                raw = self.r.rpop(s.dlq)
                if raw is None:
                    break
                task_id = raw.decode("utf-8") if isinstance(raw, (bytes, bytearray)) else raw
                self._reset_for_replay(task_id)
                self.r.lpush(s.pending_queue, task_id)
                count += 1
            return count

        count = 0
        for task_id in task_ids:
            removed = self.r.lrem(s.dlq, 0, task_id)
            if removed:
                self._reset_for_replay(task_id)
                self.r.lpush(s.pending_queue, task_id)
                count += int(removed)
        return count

    def _reset_for_replay(self, task_id: str) -> None:
        task_key = self.settings.task_hash_key(task_id)
        if self.r.exists(task_key):
            self.r.hset(task_key, mapping={
                TaskField.STATE: TaskState.QUEUED.value,
                TaskField.ATTEMPTS: "0",
            })
            self.r.hdel(task_key, TaskField.ERROR, TaskField.ERROR_TYPE, TaskField.NEXT_RUN_AT)

    def purge(self, key: str) -> int:
        """DEL a key. Returns the number of keys removed (0 or 1)."""
        return int(self.r.delete(key))
