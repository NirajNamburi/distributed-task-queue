"""The multiprocessing worker pool.

This is where we genuinely bypass the GIL. Each worker is a separate OS
process spawned via :mod:`multiprocessing`, with its own Python interpreter
and its own Redis connection - so a CPU-bound task on one worker does not
stall any other worker. The master process owns the SIGTERM/SIGINT handlers
and the reaper thread; children only own their loop.

Lifecycle:

  master.start()
      │
      ├─ start Reaper thread
      ├─ for i in range(N): spawn Process(target=worker_loop)
      └─ install SIGTERM/SIGINT -> set stop_event, join with deadline

  worker_loop:
      while not stop_event.is_set():
          heartbeat()
          task_id = broker.claim(worker_id, block_s)   # blocking BLMOVE + Lua
          if task_id: execute -> complete | fail_retry | fail_dlq

  shutdown:
      stop_event.set() -> blocking claim returns within block_s ->
      worker exits -> master joins (deadline shutdown_grace_s) -> SIGKILL stragglers
"""

from __future__ import annotations

import multiprocessing as mp
import os
import signal
import socket
import sys
import threading
import time
import traceback
import uuid
from typing import Optional

from dtq.broker import Broker
from dtq.config import Settings
from dtq.logging_setup import get_logger, setup_logging
from dtq.reaper import Reaper
from dtq.registry import UnknownTaskError, resolve
from dtq.retries import backoff_seconds


log = get_logger(__name__)


# ----------------------------------------------------------------------
# Child entrypoint
# ----------------------------------------------------------------------


def worker_loop(
    worker_id: str,
    settings: Settings,
    stop_event,
    *,
    max_iterations: Optional[int] = None,
    broker: Optional[Broker] = None,
) -> None:
    """Run the per-process worker loop until ``stop_event`` is set.

    ``max_iterations`` is for tests: when set, the loop exits after that many
    claim attempts even if work remains. ``broker`` is also for tests; in
    production we construct a fresh Broker so each child gets its own
    connection (forking shared connections is undefined behavior).
    """
    setup_logging(settings.log_level)
    own_broker = False
    if broker is None:
        broker = Broker(settings)
        own_broker = True

    log_w = get_logger(f"dtq.worker.{worker_id}")
    log_w.info(
        "worker started",
        extra={"worker_id": worker_id, "pid": os.getpid()},
    )

    # In a child, ignore the parent's signal-handling intent: the master sets
    # stop_event directly, so we want default Python behavior here. We still
    # install our own minimal handlers to flip the local stop event so SIGTERM
    # to a single child (e.g. via `kill <pid>`) still drains gracefully.
    # signal.signal() only works in the main thread; tests that drive
    # worker_loop from a thread must skip the install.
    if threading.current_thread() is threading.main_thread():
        def _sig(_signum, _frame):
            stop_event.set()

        try:
            signal.signal(signal.SIGTERM, _sig)
            signal.signal(signal.SIGINT, _sig)
        except (ValueError, OSError):
            pass

    last_heartbeat = 0.0
    iterations = 0
    try:
        while not stop_event.is_set():
            now = time.time()
            if (now - last_heartbeat) >= settings.heartbeat_interval_s:
                broker.heartbeat(worker_id)
                last_heartbeat = now

            try:
                task_id = broker.claim(worker_id, block_s=settings.claim_block_s)
            except Exception:
                log_w.exception("claim failed; sleeping briefly before retry")
                _sleep_interruptible(stop_event, 1.0)
                continue

            if task_id is not None:
                _execute_one(worker_id, broker, settings, task_id, log_w)

            iterations += 1
            if max_iterations is not None and iterations >= max_iterations:
                break
    finally:
        try:
            broker.remove_heartbeat(worker_id)
        except Exception:
            pass
        if own_broker:
            broker.close()
        log_w.info("worker stopped", extra={"worker_id": worker_id})


def _execute_one(worker_id: str, broker: Broker, settings: Settings, task_id: str, log_w) -> None:
    payload = broker.fetch_task(task_id)
    if payload is None:
        log_w.warning("claimed task vanished from hash; dropping", extra={"task_id": task_id})
        broker.complete(worker_id, task_id, None)
        return
    func_path, args, kwargs, attempts, max_retries = payload

    log_w.info(
        "task started",
        extra={"task_id": task_id, "func": func_path, "attempt": attempts + 1, "max_retries": max_retries},
    )
    started = time.perf_counter()
    try:
        func = resolve(func_path)
        result = func(*args, **kwargs)
    except UnknownTaskError as exc:
        # Unresolvable task is a config/deploy bug; do not waste retries on it.
        broker.fail_dlq(
            worker_id,
            task_id,
            attempts + 1,
            error=str(exc),
            error_type=type(exc).__name__,
        )
        log_w.error(
            "unknown task -> DLQ",
            extra={"task_id": task_id, "func": func_path, "error": str(exc)},
        )
        return
    except BaseException as exc:
        new_attempts = attempts + 1
        tb = traceback.format_exc(limit=20)
        if new_attempts > max_retries:
            broker.fail_dlq(
                worker_id,
                task_id,
                new_attempts,
                error=tb,
                error_type=type(exc).__name__,
            )
            log_w.error(
                "task exhausted retries -> DLQ",
                extra={
                    "task_id": task_id,
                    "func": func_path,
                    "attempt": new_attempts,
                    "max_retries": max_retries,
                    "error_type": type(exc).__name__,
                },
            )
        else:
            delay = backoff_seconds(
                new_attempts,
                base=settings.backoff_base_s,
                cap=settings.backoff_cap_s,
            )
            run_at = time.time() + delay
            broker.fail_retry(
                worker_id,
                task_id,
                new_attempts,
                run_at,
                error=tb,
                error_type=type(exc).__name__,
            )
            log_w.warning(
                "task failed -> retry",
                extra={
                    "task_id": task_id,
                    "func": func_path,
                    "attempt": new_attempts,
                    "max_retries": max_retries,
                    "delay_s": round(delay, 3),
                    "error_type": type(exc).__name__,
                },
            )
        return

    elapsed = time.perf_counter() - started
    broker.complete(worker_id, task_id, result)
    log_w.info(
        "task succeeded",
        extra={
            "task_id": task_id,
            "func": func_path,
            "duration_s": round(elapsed, 4),
        },
    )


def _sleep_interruptible(stop_event, total: float) -> None:
    end = time.time() + total
    while time.time() < end and not stop_event.is_set():
        time.sleep(min(0.1, end - time.time()))


# ----------------------------------------------------------------------
# Master pool
# ----------------------------------------------------------------------


class WorkerPool:
    """Owns N child worker processes plus a Reaper thread."""

    def __init__(self, settings: Settings | None = None) -> None:
        self.settings = settings or Settings.from_env()
        self.broker = Broker(self.settings)
        # mp.Event is a multiprocessing primitive that's safe to share with
        # spawned children. We pass it as the stop signal.
        self._mp_ctx = mp.get_context("spawn")
        self._stop_event = self._mp_ctx.Event()
        self._procs: list[mp.process.BaseProcess] = []
        self._reaper: Reaper | None = None
        self._reaper_stop = __import__("threading").Event()
        self._installed_signals = False

    def _make_worker_id(self, idx: int) -> str:
        host = socket.gethostname().split(".")[0]
        return f"{host}-{os.getpid()}-{idx}-{uuid.uuid4().hex[:6]}"

    def start(self, processes: int | None = None, block: bool = True) -> None:
        """Spawn the workers and (optionally) block until shutdown."""
        n = processes or self.settings.worker_processes
        setup_logging(self.settings.log_level)
        log.info(
            "worker pool starting",
            extra={"processes": n, "redis_url": self.settings.redis_url},
        )

        # Best-effort connection check so we fail fast on a bad REDIS_URL.
        try:
            self.broker.ping()
        except Exception as exc:
            log.error("cannot reach redis", extra={"error": str(exc), "redis_url": self.settings.redis_url})
            raise

        for i in range(n):
            worker_id = self._make_worker_id(i)
            p = self._mp_ctx.Process(
                target=worker_loop,
                name=f"dtq-worker-{i}",
                args=(worker_id, self.settings, self._stop_event),
                daemon=False,
            )
            p.start()
            self._procs.append(p)
            log.info("spawned worker", extra={"worker_id": worker_id, "child_pid": p.pid, "slot": i})

        self._reaper = Reaper(self.settings, self.broker, self._reaper_stop)
        self._reaper.start()

        if block:
            self._install_signals()
            self._wait_for_shutdown()

    def _install_signals(self) -> None:
        if self._installed_signals:
            return

        def _handler(signum, _frame):
            log.info("signal received -> initiating graceful shutdown", extra={"signum": int(signum)})
            self.shutdown()

        signal.signal(signal.SIGTERM, _handler)
        signal.signal(signal.SIGINT, _handler)
        self._installed_signals = True

    def _wait_for_shutdown(self) -> None:
        # Idle until any child exits or a signal flips the stop event. We poll
        # so the master process is responsive to signals on every platform
        # (including Windows, where signal delivery on a blocked join is iffy).
        try:
            while True:
                if self._stop_event.is_set():
                    break
                alive = [p for p in self._procs if p.is_alive()]
                if not alive:
                    log.warning("all workers exited; shutting down pool")
                    break
                time.sleep(0.5)
        finally:
            self.shutdown()

    def shutdown(self, kill_after_s: float | None = None) -> None:
        """Signal children to stop and join them with a deadline."""
        if self._stop_event.is_set() and not any(p.is_alive() for p in self._procs):
            return
        deadline = (kill_after_s if kill_after_s is not None else self.settings.shutdown_grace_s) + time.time()
        self._stop_event.set()
        self._reaper_stop.set()

        for p in self._procs:
            remaining = max(0.1, deadline - time.time())
            p.join(timeout=remaining)

        for p in self._procs:
            if p.is_alive():
                log.warning("worker did not exit in grace period; terminating", extra={"child_pid": p.pid})
                try:
                    p.terminate()
                    p.join(timeout=2.0)
                except Exception:
                    pass

        for p in self._procs:
            if p.is_alive():
                log.error("worker still alive after terminate(); killing", extra={"child_pid": p.pid})
                try:
                    p.kill()
                except Exception:
                    pass

        if self._reaper is not None:
            self._reaper.join(timeout=2.0)

        try:
            self.broker.close()
        except Exception:
            pass
        log.info("worker pool stopped")


def _main_entry() -> None:
    """Helper for `python -m dtq.worker`."""
    settings = Settings.from_env()
    pool = WorkerPool(settings)
    pool.start(block=True)


if __name__ == "__main__":
    sys.exit(_main_entry())
