"""Backoff math + retry promotion via the reaper."""

from __future__ import annotations

import time

import pytest

from dtq.client import TaskClient
from dtq.reaper import Reaper
from dtq.retries import backoff_seconds
from dtq.task import TaskField, TaskState


def test_backoff_caps_at_ceiling():
    samples = [backoff_seconds(att, base=2.0, cap=10.0) for att in range(1, 20) for _ in range(50)]
    assert all(0.0 <= s <= 10.0 for s in samples)


def test_backoff_stays_within_exponential_band():
    for attempt in range(1, 6):
        cap_for_attempt = min(2.0 * (2 ** (attempt - 1)), 60.0)
        samples = [backoff_seconds(attempt, base=2.0, cap=60.0) for _ in range(200)]
        assert all(0.0 <= s <= cap_for_attempt for s in samples)


def test_promote_due_moves_only_due_tasks(broker, settings):
    producer = TaskClient(settings, broker=broker)
    early_id = producer.delay("dtq.tasks.fetch_flaky_api", 1)
    late_id = producer.delay("dtq.tasks.fetch_flaky_api", 2)

    broker.claim("w1", block_s=0.5)
    broker.fail_retry("w1", early_id, attempts=1, run_at=time.time() - 5, error="x", error_type="E")
    broker.claim("w1", block_s=0.5)
    broker.fail_retry("w1", late_id, attempts=1, run_at=time.time() + 60, error="x", error_type="E")

    promoted = broker.promote_due_retries()
    assert promoted == 1

    pending = [v.decode() for v in broker.r.lrange(settings.pending_queue, 0, -1)]
    assert early_id in pending
    assert late_id not in pending

    info = producer.get_info(early_id)
    assert info[TaskField.STATE] == TaskState.QUEUED.value
    assert TaskField.NEXT_RUN_AT not in info


def test_reaper_tick_promotes_due_retries(broker, settings):
    producer = TaskClient(settings, broker=broker)
    tid = producer.delay("dtq.tasks.fetch_flaky_api", 7)
    broker.claim("w1", block_s=0.5)
    broker.fail_retry("w1", tid, attempts=1, run_at=time.time() - 1, error="x", error_type="E")

    import threading
    stop = threading.Event()
    reaper = Reaper(settings, broker, stop)
    reaper.tick()

    pending = [v.decode() for v in broker.r.lrange(settings.pending_queue, 0, -1)]
    assert tid in pending
