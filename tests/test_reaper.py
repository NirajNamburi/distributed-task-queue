"""Reaper recovery of dead workers."""

from __future__ import annotations

import threading
import time

from dtq.client import TaskClient
from dtq.reaper import Reaper
from dtq.task import TaskField, TaskState


def test_dead_worker_processing_list_is_drained_back_to_pending(broker, settings):
    producer = TaskClient(settings, broker=broker)
    tid = producer.delay("dtq.tasks.calculate_primes", 11)

    claimed = broker.claim("ghost", block_s=0.5)
    assert claimed == tid

    # Backdate the heartbeat well past worker_timeout_s.
    broker.r.hset(settings.heartbeat_hash, "ghost", repr(time.time() - 9999.0))
    assert broker.dead_workers(time.time(), settings.worker_timeout_s) == ["ghost"]

    stop = threading.Event()
    Reaper(settings, broker, stop).tick()

    assert broker.r.llen(settings.processing_key("ghost")) == 0
    pending = [v.decode() for v in broker.r.lrange(settings.pending_queue, 0, -1)]
    assert tid in pending

    info = producer.get_info(tid)
    assert info[TaskField.STATE] == TaskState.QUEUED.value
    assert int(info[TaskField.ATTEMPTS]) == 1
    assert "WorkerDied" in info.get(TaskField.ERROR_TYPE, "")

    assert broker.r.hexists(settings.heartbeat_hash, "ghost") == 0


def test_dead_worker_with_too_many_attempts_goes_to_dlq(broker, settings):
    producer = TaskClient(settings, broker=broker)
    tid = producer.delay("dtq.tasks.calculate_primes", 13, max_retries=1)
    broker.r.hset(settings.task_hash_key(tid), TaskField.ATTEMPTS, "2")

    broker.claim("ghost", block_s=0.5)
    broker.r.hset(settings.heartbeat_hash, "ghost", repr(time.time() - 9999.0))

    stop = threading.Event()
    Reaper(settings, broker, stop).tick()

    assert broker.r.llen(settings.dlq) == 1
    assert broker.r.rpop(settings.dlq).decode() == tid

    info = producer.get_info(tid)
    assert info[TaskField.STATE] == TaskState.FAILED.value


def test_live_worker_is_not_reaped(broker, settings):
    producer = TaskClient(settings, broker=broker)
    tid = producer.delay("dtq.tasks.calculate_primes", 17)
    broker.claim("alive", block_s=0.5)
    broker.heartbeat("alive")

    stop = threading.Event()
    Reaper(settings, broker, stop).tick()

    assert broker.r.llen(settings.processing_key("alive")) == 1
    assert producer.get_info(tid)[TaskField.STATE] == TaskState.RUNNING.value
