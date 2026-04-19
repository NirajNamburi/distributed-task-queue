"""Shared pytest fixtures.

We use fakeredis as a stand-in for a real Redis server. The ``[lua]`` extra
gives fakeredis a Lua interpreter so all of dtq's atomic state-transition
scripts execute through the same code paths as production.
"""

from __future__ import annotations

import uuid

import fakeredis
import pytest

from dtq.broker import Broker
from dtq.config import Settings


@pytest.fixture
def fake_server() -> fakeredis.FakeServer:
    """A FakeServer is shared across multiple FakeRedis clients (like a real redis-server)."""
    return fakeredis.FakeServer()


@pytest.fixture
def settings(tmp_path) -> Settings:
    """A unique-prefixed Settings so accidental cross-test bleed is impossible."""
    suffix = uuid.uuid4().hex[:8]
    return Settings(
        redis_url="redis://unused",
        pending_queue=f"test:{suffix}:pending",
        processing_prefix=f"test:{suffix}:processing:",
        dlq=f"test:{suffix}:dlq",
        retry_zset=f"test:{suffix}:retries",
        heartbeat_hash=f"test:{suffix}:hb",
        task_hash_prefix=f"test:{suffix}:task:",
        max_retries=3,
        backoff_base_s=2.0,
        backoff_cap_s=60.0,
        heartbeat_interval_s=0.1,
        worker_timeout_s=1.0,
        worker_processes=2,
        claim_block_s=0.1,
        shutdown_grace_s=2.0,
        reaper_interval_s=0.05,
        metrics_enabled=False,
        metrics_port=0,
        log_level="WARNING",
        log_json=False,
    )


@pytest.fixture
def fake_redis(fake_server):
    return fakeredis.FakeRedis(server=fake_server, decode_responses=False)


@pytest.fixture
def broker(settings, fake_redis) -> Broker:
    return Broker(settings, client=fake_redis)


@pytest.fixture
def broker_factory(settings, fake_server):
    """Yield a callable that returns a fresh Broker bound to the same fake server.

    Lets tests model multiple Worker connections against one Redis instance.
    """
    created: list[Broker] = []

    def _make() -> Broker:
        client = fakeredis.FakeRedis(server=fake_server, decode_responses=False)
        b = Broker(settings, client=client)
        created.append(b)
        return b

    yield _make

    for b in created:
        b.close()
