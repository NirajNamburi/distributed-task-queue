"""Atomicity tests for the Lua-backed state transitions."""

from __future__ import annotations

import threading

from dtq.client import TaskClient
from dtq.task import TaskField, TaskState


def test_two_workers_cannot_claim_the_same_task(broker_factory, settings):
    """Concurrent claims on a single-task queue: exactly one wins."""
    producer = TaskClient(settings, broker=broker_factory())
    task_id = producer.delay("dtq.tasks.calculate_primes", 5)

    results: list[str | None] = []
    barrier = threading.Barrier(2)

    def _claim(worker_id: str) -> None:
        b = broker_factory()
        barrier.wait()
        results.append(b.claim(worker_id, block_s=0.5))

    t1 = threading.Thread(target=_claim, args=("worker-A",))
    t2 = threading.Thread(target=_claim, args=("worker-B",))
    t1.start(); t2.start()
    t1.join(); t2.join()

    winners = [r for r in results if r is not None]
    losers = [r for r in results if r is None]
    assert winners == [task_id]
    assert len(losers) == 1

    info = producer.get_info(task_id)
    assert info[TaskField.STATE] == TaskState.RUNNING.value
    assert info[TaskField.WORKER_ID] in {"worker-A", "worker-B"}


def test_complete_atomically_clears_processing_and_writes_result(broker, settings):
    producer = TaskClient(settings, broker=broker)
    task_id = producer.delay("dtq.tasks.calculate_primes", 7)

    claimed = broker.claim("worker-1", block_s=0.5)
    assert claimed == task_id
    assert broker.r.llen(settings.processing_key("worker-1")) == 1

    broker.complete("worker-1", task_id, result={"answer": 4})

    assert broker.r.llen(settings.processing_key("worker-1")) == 0
    info = producer.get_info(task_id)
    assert info[TaskField.STATE] == TaskState.SUCCESS.value
    assert info[TaskField.RESULT] == {"answer": 4}


def test_fail_retry_atomically_schedules_in_zset(broker, settings):
    producer = TaskClient(settings, broker=broker)
    task_id = producer.delay("dtq.tasks.fetch_flaky_api", 1)

    broker.claim("worker-1", block_s=0.5)
    broker.fail_retry(
        worker_id="worker-1",
        task_id=task_id,
        attempts=1,
        run_at=999.0,
        error="boom",
        error_type="ConnectionError",
    )

    assert broker.r.llen(settings.processing_key("worker-1")) == 0
    score = broker.r.zscore(settings.retry_zset, task_id)
    assert score == 999.0

    info = producer.get_info(task_id)
    assert info[TaskField.STATE] == TaskState.RETRYING.value
    assert info[TaskField.ERROR] == "boom"
    assert info[TaskField.ERROR_TYPE] == "ConnectionError"


def test_fail_dlq_atomically_pushes_to_dead_letter(broker, settings):
    producer = TaskClient(settings, broker=broker)
    task_id = producer.delay("dtq.tasks.fetch_flaky_api", 2)

    broker.claim("worker-1", block_s=0.5)
    broker.fail_dlq(
        worker_id="worker-1",
        task_id=task_id,
        attempts=4,
        error="permanent",
        error_type="ConnectionError",
    )

    assert broker.r.llen(settings.processing_key("worker-1")) == 0
    assert broker.r.llen(settings.dlq) == 1
    assert broker.r.rpop(settings.dlq).decode() == task_id

    info = producer.get_info(task_id)
    assert info[TaskField.STATE] == TaskState.FAILED.value
    assert int(info[TaskField.ATTEMPTS]) == 4
