"""The reaper thread.

A single background thread, run inside the master process of the worker pool,
performs two janitorial duties on a fixed cadence:

1. **Promote due retries.** Tasks scheduled for delayed retry live in the
   ``dtq:retries`` ZSET with a score equal to their ``run_at`` epoch. When the
   wall clock catches up, the reaper moves them back into the pending queue.

2. **Recover dead workers.** Each worker writes its heartbeat into a Redis
   hash. If the heartbeat goes stale (older than ``worker_timeout_s``), the
   reaper assumes the worker died and drains its per-worker processing list
   back into pending - bumping each task's attempt counter so a worker that
   loops crashing on the same poison-pill task eventually trips the DLQ.

The reaper is a thread (not a separate process) because both jobs are I/O
bound on Redis and the GIL is irrelevant. Running it inside the pool master
also means the reaper dies with the pool on SIGTERM, which is the desired
behavior - the OS PID lifecycle is the truth of "is dtq running?".
"""

from __future__ import annotations

import threading
import time
from typing import Callable

from dtq.broker import Broker
from dtq.config import Settings
from dtq.logging_setup import get_logger


log = get_logger(__name__)


class Reaper(threading.Thread):
    """Background sweeper for delayed retries and dead-worker recovery."""

    def __init__(
        self,
        settings: Settings,
        broker: Broker,
        stop_event: threading.Event,
        clock: Callable[[], float] = time.time,
    ) -> None:
        super().__init__(name="dtq-reaper", daemon=True)
        self.settings = settings
        self.broker = broker
        self._stop = stop_event
        self._clock = clock

    def run(self) -> None:
        log.info(
            "reaper started",
            extra={
                "interval_s": self.settings.reaper_interval_s,
                "worker_timeout_s": self.settings.worker_timeout_s,
            },
        )
        while not self._stop.is_set():
            try:
                self.tick()
            except Exception:
                log.exception("reaper tick failed")
            # Sleep in small increments so a SIGTERM doesn't have to wait the
            # full reaper_interval_s before the master can join us.
            slept = 0.0
            while slept < self.settings.reaper_interval_s and not self._stop.is_set():
                time.sleep(0.25)
                slept += 0.25
        log.info("reaper stopped")

    def tick(self) -> None:
        """Run one promote+requeue pass. Public for tests."""
        promoted = self.broker.promote_due_retries(now=self._clock())
        if promoted:
            log.info("promoted due retries", extra={"count": promoted})

        dead = self.broker.dead_workers(self._clock(), self.settings.worker_timeout_s)
        for worker_id in dead:
            requeued, dlq_count = self.broker.requeue_dead_worker(worker_id)
            if requeued or dlq_count:
                log.warning(
                    "recovered dead worker",
                    extra={
                        "dead_worker_id": worker_id,
                        "requeued": requeued,
                        "dlq": dlq_count,
                    },
                )
            else:
                log.info("cleared dead worker with empty processing list", extra={"dead_worker_id": worker_id})
