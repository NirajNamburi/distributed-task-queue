"""End-to-end test of the worker loop running in-process against fakeredis."""

from __future__ import annotations

import threading
import time

from dtq.client import TaskClient
from dtq.task import TaskField, TaskState
from dtq.worker import worker_loop


def test_worker_loop_processes_a_simple_task(broker_factory, settings):
    producer_broker = broker_factory()
    worker_broker = broker_factory()
    producer = TaskClient(settings, broker=producer_broker)

    task_id = producer.delay("dtq.tasks.calculate_primes", 50)

    stop = threading.Event()
    t = threading.Thread(
        target=worker_loop,
        args=("solo", settings, stop),
        kwargs={"broker": worker_broker, "max_iterations": 5},
        daemon=True,
    )
    t.start()
    t.join(timeout=10.0)
    stop.set()

    info = producer.get_info(task_id)
    assert info[TaskField.STATE] == TaskState.SUCCESS.value
    assert isinstance(info[TaskField.RESULT], int)
    assert info[TaskField.RESULT] == 15  # primes <= 50


def test_worker_loop_retries_then_succeeds(broker_factory, settings, monkeypatch):
    """Force a task to fail twice then succeed; assert state machine transitions."""
    import dtq.tasks as tasks

    calls = {"n": 0}

    def flaky(_uid: int) -> dict:
        calls["n"] += 1
        if calls["n"] <= 2:
            raise ConnectionError("boom")
        return {"ok": True, "calls": calls["n"]}

    monkeypatch.setattr(tasks, "fetch_flaky_api", flaky)
    # Bust the registry cache so resolve() picks up the patched function.
    from dtq import registry
    registry._RESOLVE_CACHE.pop("dtq.tasks.fetch_flaky_api", None)

    producer = TaskClient(settings, broker=broker_factory())
    tid = producer.delay("dtq.tasks.fetch_flaky_api", 1, max_retries=5)

    worker_broker = broker_factory()
    stop = threading.Event()

    deadline = time.time() + 30.0
    while time.time() < deadline:
        # Run the worker for one claim; if it sees no task, we promote retries.
        t = threading.Thread(
            target=worker_loop,
            args=("retrier", settings, stop),
            kwargs={"broker": worker_broker, "max_iterations": 1},
            daemon=True,
        )
        t.start()
        t.join(timeout=5.0)
        producer.broker.promote_due_retries(now=time.time() + 3600)
        state = producer.get_state(tid)
        if state == TaskState.SUCCESS.value:
            break
    stop.set()

    info = producer.get_info(tid)
    assert info[TaskField.STATE] == TaskState.SUCCESS.value
    assert int(info[TaskField.ATTEMPTS]) == 2  # number of failed attempts before success
    assert info[TaskField.RESULT] == {"ok": True, "calls": 3}


def test_worker_loop_dlqs_after_exhausting_retries(broker_factory, settings, monkeypatch):
    import dtq.tasks as tasks

    def always_fail(_uid: int) -> dict:
        raise ConnectionError("never works")

    monkeypatch.setattr(tasks, "fetch_flaky_api", always_fail)
    from dtq import registry
    registry._RESOLVE_CACHE.pop("dtq.tasks.fetch_flaky_api", None)

    producer = TaskClient(settings, broker=broker_factory())
    tid = producer.delay("dtq.tasks.fetch_flaky_api", 1, max_retries=2)

    worker_broker = broker_factory()
    stop = threading.Event()

    for _ in range(10):
        t = threading.Thread(
            target=worker_loop,
            args=("loser", settings, stop),
            kwargs={"broker": worker_broker, "max_iterations": 1},
            daemon=True,
        )
        t.start()
        t.join(timeout=5.0)
        producer.broker.promote_due_retries(now=time.time() + 3600)
        if producer.get_state(tid) == TaskState.FAILED.value:
            break
    stop.set()

    info = producer.get_info(tid)
    assert info[TaskField.STATE] == TaskState.FAILED.value
    assert int(info[TaskField.ATTEMPTS]) == 3  # initial + 2 retries
    assert "ConnectionError" in info[TaskField.ERROR_TYPE]
    assert producer.broker.dlq_depth() == 1
