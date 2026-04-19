"""TaskClient producer-side behavior."""

from __future__ import annotations

import pytest

from dtq.client import TaskClient
from dtq.task import TaskField, TaskState


def test_delay_persists_task_hash_and_pushes_id(broker, settings):
    client = TaskClient(settings, broker=broker)
    task_id = client.delay("dtq.tasks.calculate_primes", 100)

    info = client.get_info(task_id)
    assert info[TaskField.STATE] == TaskState.QUEUED.value
    assert info[TaskField.FUNC] == "dtq.tasks.calculate_primes"
    assert info[TaskField.ARGS] == (100,)
    assert info[TaskField.KWARGS] == {}
    assert int(info[TaskField.ATTEMPTS]) == 0
    assert int(info[TaskField.MAX_RETRIES]) == settings.max_retries

    pending_id = broker.r.rpop(settings.pending_queue)
    assert pending_id.decode() == task_id


def test_delay_accepts_callable_and_kwargs(broker, settings):
    from dtq.tasks import fetch_flaky_api

    client = TaskClient(settings, broker=broker)
    tid = client.delay(fetch_flaky_api, 42, fail_rate=0.0)

    info = client.get_info(tid)
    assert info[TaskField.FUNC] == "dtq.tasks.fetch_flaky_api"
    assert info[TaskField.ARGS] == (42,)
    assert info[TaskField.KWARGS] == {"fail_rate": 0.0}


def test_delay_rejects_lambda(broker, settings):
    client = TaskClient(settings, broker=broker)
    with pytest.raises(ValueError):
        client.delay(lambda x: x, 1)


def test_get_state_returns_none_for_missing_task(broker, settings):
    client = TaskClient(settings, broker=broker)
    assert client.get_state("does-not-exist") is None
