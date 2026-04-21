"""GIL-bypass benchmark.

Goal: prove with one number that ``dtq`` actually parallelizes CPU-bound work
across processes. We enqueue ``--jobs`` copies of ``calculate_primes(n)``,
spin up the worker pool with ``--processes 1``, time wall-clock to drain, then
do it again with ``--processes`` set to the host's CPU count. The ratio is the
measured speedup.

Caveats / why this is honest:

* ``calculate_primes`` is pure-Python CPU work (no I/O, no numpy, no native
  threads), so a single-threaded interpreter is GIL-pinned and cannot scale on
  threads. Any speedup we observe therefore must come from real parallel
  processes, which is the property we are claiming.
* Speedup is sub-linear in practice (load on the box, Redis round trips,
  Python startup costs amortized over the wall clock). Expect roughly
  ``cpu_count * 0.7-0.9`` on a quiet machine.

Usage:

    python -m benchmarks.gil_bypass

Defaults: ``--jobs os.cpu_count()`` and ``--n 1_000_000``. The workload is
sized to dominate pool-startup and Redis round-trip overhead so the measured
ratio reflects real CPU parallelism, not bookkeeping noise.

Requires a live Redis (e.g. ``docker compose up -d``).
"""

from __future__ import annotations

import argparse
import os
import statistics
import sys
import time
from typing import Iterable

from dtq.broker import Broker
from dtq.client import TaskClient
from dtq.config import Settings
from dtq.logging_setup import setup_logging
from dtq.task import TaskState
from dtq.worker import WorkerPool


def _drain(client: TaskClient, task_ids: Iterable[str], deadline: float) -> tuple[int, int]:
    """Block until every task reaches a terminal state. Returns (success, failed)."""
    success = failed = 0
    pending = list(task_ids)
    while pending and time.time() < deadline:
        still: list[str] = []
        for tid in pending:
            state = client.get_state(tid)
            if state == TaskState.SUCCESS.value:
                success += 1
            elif state == TaskState.FAILED.value:
                failed += 1
            else:
                still.append(tid)
        pending = still
        if pending:
            time.sleep(0.05)
    if pending:
        raise TimeoutError(f"{len(pending)} tasks did not finish before deadline")
    return success, failed


def _purge(broker: Broker) -> None:
    s = broker.settings
    broker.purge(s.pending_queue)
    broker.purge(s.dlq)
    broker.purge(s.retry_zset)
    broker.purge(s.heartbeat_hash)
    for key in broker.r.scan_iter(match=(s.processing_prefix + "*").encode()):
        broker.r.delete(key)
    for key in broker.r.scan_iter(match=(s.task_hash_prefix + "*").encode()):
        broker.r.delete(key)


def _wait_for_pool_warm(broker: Broker, expected: int, deadline: float) -> None:
    """Block until ``expected`` workers have written a heartbeat.

    Guarantees every trial starts measurement with a fully-warmed pool so the
    wall-clock numbers are comparable across process counts. Without this, the
    N-process trial pays 100-500 ms per Windows ``spawn()`` call inside the
    measurement (N times), which can swamp the actual compute cost.
    """
    while time.time() < deadline:
        if len(broker.list_workers()) >= expected:
            return
        time.sleep(0.05)
    raise TimeoutError(f"pool never warmed ({len(broker.list_workers())}/{expected} heartbeats)")


def _run_one(processes: int, jobs: int, n: int, settings: Settings) -> tuple[float, int, int]:
    """Run a single (processes x jobs) trial. Returns (wall_seconds, success, failed).

    Measurement protocol:

    1. Purge Redis (fresh slate).
    2. Start the pool in-process and wait for every worker to heartbeat once
       (guarantees each worker has finished its Python startup + Lua
       registration + first BLMOVE call).
    3. Enqueue all ``jobs`` tasks.
    4. Record ``t0`` and drain until every task is terminal.
    5. Record wall-clock and shut the pool down cleanly.

    We run the pool in-process with ``block=False`` and call ``shutdown()``
    between trials. This matters on Windows: ``Process.terminate()`` maps to
    ``TerminateProcess`` and does not run signal handlers, which would leak
    worker children across trials and let them steal tasks from the next
    measurement.
    """
    client = TaskClient(settings)
    _purge(client.broker)

    pool_settings = Settings(**{**settings.as_dict(), "worker_processes": processes})
    pool = WorkerPool(pool_settings)
    pool.start(block=False)

    try:
        _wait_for_pool_warm(client.broker, expected=processes, deadline=time.time() + 60.0)

        task_ids = [client.delay("dtq.tasks.calculate_primes", n) for _ in range(jobs)]
        t0 = time.perf_counter()
        success, failed = _drain(client, task_ids, deadline=time.time() + 1200.0)
        wall = time.perf_counter() - t0
    finally:
        pool.shutdown()

    return wall, success, failed


def _fmt(seconds: float) -> str:
    return f"{seconds:6.2f}s"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="dtq GIL-bypass benchmark")
    parser.add_argument("--jobs", type=int, default=max(2, os.cpu_count() or 2),
                        help="Number of calculate_primes tasks to enqueue per trial (default: cpu_count)")
    parser.add_argument("--n", type=int, default=1_000_000,
                        help="Upper bound for prime calculation (default: 1_000_000)")
    parser.add_argument("--processes", type=int, nargs="+", default=None,
                        help="Process counts to compare (default: 1 and os.cpu_count())")
    parser.add_argument("--repeats", type=int, default=1, help="Number of trials per process count")
    args = parser.parse_args(argv)

    settings = Settings.from_env()
    setup_logging(settings.log_level)

    process_counts = args.processes or [1, max(2, os.cpu_count() or 2)]
    print(f"\nGIL-bypass benchmark: {args.jobs} x calculate_primes({args.n})")
    print(f"Redis: {settings.redis_url}")
    print(f"CPU count: {os.cpu_count()}\n")
    print(f"{'processes':>10}  {'wall':>8}  {'mean':>8}  {'speedup':>8}  status")
    print("-" * 55)

    baselines: dict[int, float] = {}
    for procs in process_counts:
        trials: list[float] = []
        for _ in range(args.repeats):
            wall, success, failed = _run_one(procs, args.jobs, args.n, settings)
            trials.append(wall)
        mean = statistics.mean(trials)
        baselines[procs] = mean
        speedup = baselines[process_counts[0]] / mean if mean > 0 else float("inf")
        status = "ok" if failed == 0 else f"FAIL ({failed})"
        print(f"{procs:>10}  {_fmt(trials[-1])}  {_fmt(mean)}  {speedup:>7.2f}x  {status}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
