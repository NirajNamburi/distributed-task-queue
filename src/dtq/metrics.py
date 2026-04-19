"""Prometheus metrics surface.

Each worker process owns its own metric registry (Prometheus client metrics
are process-local), and the master process runs the HTTP exporter on
``:9090/metrics``. We deliberately do NOT use ``multiprocess.MultiProcessCollector``
to keep the dependency surface tight and the moving parts visible: in this
demo, worker-side counters increment local registries and the master scrapes
queue depths directly from Redis on each scrape.

If metrics are disabled (``DTQ_METRICS_ENABLED=0``) every helper here is a
no-op so production code paths don't need to branch.
"""

from __future__ import annotations

import threading
from typing import Callable

from prometheus_client import (
    CollectorRegistry,
    Counter,
    Gauge,
    Histogram,
    start_http_server,
)


_LOCK = threading.Lock()
_STATE: dict = {"started": False, "registry": None, "metrics": None, "broker": None, "settings": None}


def _make_metrics(registry: CollectorRegistry) -> dict:
    return {
        "enqueued": Counter(
            "dtq_tasks_enqueued_total",
            "Tasks enqueued by the producer.",
            ["task"],
            registry=registry,
        ),
        "completed": Counter(
            "dtq_tasks_completed_total",
            "Tasks reaching a terminal state.",
            ["task", "status"],
            registry=registry,
        ),
        "retried": Counter(
            "dtq_tasks_retried_total",
            "Task attempts that failed and were rescheduled.",
            ["task"],
            registry=registry,
        ),
        "dlq": Counter(
            "dtq_tasks_dlq_total",
            "Tasks moved to the dead-letter queue.",
            ["task"],
            registry=registry,
        ),
        "duration": Histogram(
            "dtq_task_duration_seconds",
            "Wall-clock task execution latency.",
            ["task"],
            buckets=(0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 5, 10, 30, 60, 300),
            registry=registry,
        ),
        "queue_depth": Gauge(
            "dtq_queue_depth",
            "Current depth of a Redis-backed queue (refreshed on scrape).",
            ["queue"],
            registry=registry,
        ),
        "workers_alive": Gauge(
            "dtq_workers_alive",
            "Number of workers with a fresh heartbeat.",
            registry=registry,
        ),
        "reaper_recovered": Counter(
            "dtq_reaper_recovered_total",
            "Tasks recovered from dead workers by the reaper.",
            registry=registry,
        ),
        "reaper_promoted": Counter(
            "dtq_reaper_promoted_total",
            "Retry-scheduled tasks promoted back to pending.",
            registry=registry,
        ),
    }


def _enabled() -> bool:
    settings = _STATE.get("settings")
    if settings is None:
        return False
    return bool(getattr(settings, "metrics_enabled", True))


def init_metrics(settings, broker=None) -> None:
    """Initialize a process-local Prometheus registry. Idempotent per process."""
    with _LOCK:
        _STATE["settings"] = settings
        if _STATE["registry"] is None:
            registry = CollectorRegistry()
            _STATE["registry"] = registry
            _STATE["metrics"] = _make_metrics(registry)
        if broker is not None:
            _STATE["broker"] = broker


def start_exporter(settings, broker=None) -> None:
    """Start the Prometheus HTTP exporter (master process only)."""
    init_metrics(settings, broker)
    if not _enabled():
        return
    with _LOCK:
        if _STATE["started"]:
            return
        _bind_dynamic_collectors()
        start_http_server(settings.metrics_port, registry=_STATE["registry"])
        _STATE["started"] = True


def _bind_dynamic_collectors() -> None:
    """Wire queue-depth and worker-alive gauges to live Redis reads."""
    metrics = _STATE["metrics"]
    broker = _STATE.get("broker")
    settings = _STATE["settings"]
    if broker is None:
        return

    def _refresh() -> None:
        try:
            metrics["queue_depth"].labels(queue=settings.pending_queue).set(broker.pending_depth())
            metrics["queue_depth"].labels(queue=settings.dlq).set(broker.dlq_depth())
            metrics["queue_depth"].labels(queue=settings.retry_zset).set(broker.retry_depth())
            metrics["queue_depth"].labels(queue="processing:*").set(broker.in_flight_count())
            metrics["workers_alive"].set(len(broker.list_workers()))
        except Exception:
            pass

    _STATE["refresh"] = _refresh

    def _refresh_loop() -> None:
        import time as _t
        while True:
            _refresh()
            _t.sleep(2.0)

    t = threading.Thread(target=_refresh_loop, name="dtq-metrics-refresh", daemon=True)
    t.start()


def _with_metric(name: str, fn: Callable) -> None:
    if not _enabled():
        return
    metrics = _STATE.get("metrics")
    if metrics is None:
        return
    try:
        fn(metrics[name])
    except Exception:
        pass


def inc_enqueued(task: str) -> None:
    _with_metric("enqueued", lambda m: m.labels(task=task).inc())


def inc_completed(task: str, status: str) -> None:
    _with_metric("completed", lambda m: m.labels(task=task, status=status).inc())


def inc_retried(task: str) -> None:
    _with_metric("retried", lambda m: m.labels(task=task).inc())


def inc_dlq(task: str) -> None:
    _with_metric("dlq", lambda m: m.labels(task=task).inc())


def inc_reaper_recovered(n: int = 1) -> None:
    _with_metric("reaper_recovered", lambda m: m.inc(n))


def inc_reaper_promoted(n: int = 1) -> None:
    _with_metric("reaper_promoted", lambda m: m.inc(n))


def observe_duration(task: str, seconds: float) -> None:
    _with_metric("duration", lambda m: m.labels(task=task).observe(seconds))
