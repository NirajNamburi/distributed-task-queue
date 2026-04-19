"""Demo driver from the project spec.

Run this in one terminal AFTER starting `dtq worker` in another:

    python -m dtq.main

It enqueues a mix of I/O, CPU, and flaky tasks, then polls Redis until they
all reach a terminal state and prints a per-task summary. The interesting
console story is in the worker terminal: you should see the four
``calculate_primes`` jobs running on four different child PIDs simultaneously,
the flaky API calls firing the retry path, and one or two ending up in the DLQ.
"""

from __future__ import annotations

import time
from typing import Any

from dtq.client import TaskClient, TaskFailed, TaskTimeout
from dtq.config import Settings
from dtq.logging_setup import get_logger, setup_logging
from dtq.task import TaskField


log = get_logger(__name__)


def _short(v: Any, n: int = 80) -> str:
    s = repr(v)
    return s if len(s) <= n else s[: n - 1] + "\u2026"


def main() -> int:
    settings = Settings.from_env()
    setup_logging(settings.log_level, json_mode=False)
    client = TaskClient(settings)

    log.info("=" * 70)
    log.info("dtq demo: enqueueing the spec workload")
    log.info("=" * 70)

    enqueued: list[tuple[str, str, tuple, dict]] = []

    csv_id = client.delay("dtq.tasks.process_sales_csv", "data/sales.csv")
    enqueued.append((csv_id, "process_sales_csv", ("data/sales.csv",), {}))

    for n in (200_000, 200_000, 200_000, 200_000):
        tid = client.delay("dtq.tasks.calculate_primes", n)
        enqueued.append((tid, "calculate_primes", (n,), {}))

    for uid in range(101, 107):
        tid = client.delay("dtq.tasks.fetch_flaky_api", uid)
        enqueued.append((tid, "fetch_flaky_api", (uid,), {}))

    log.info("enqueued %d tasks; waiting for completion...", len(enqueued))

    deadline = time.time() + 300.0
    summaries: list[dict] = []
    for tid, name, args, kwargs in enqueued:
        timeout = max(5.0, deadline - time.time())
        try:
            result = client.wait(tid, timeout=timeout)
            info = client.get_info(tid)
            summaries.append({
                "task_id": tid,
                "func": name,
                "args": args,
                "state": info.get(TaskField.STATE),
                "attempts": info.get(TaskField.ATTEMPTS),
                "result": _short(result),
            })
        except TaskFailed as exc:
            info = client.get_info(tid)
            summaries.append({
                "task_id": tid,
                "func": name,
                "args": args,
                "state": "FAILED",
                "attempts": info.get(TaskField.ATTEMPTS),
                "error_type": exc.error_type,
                "error": _short(exc.error, 200),
            })
        except TaskTimeout:
            summaries.append({
                "task_id": tid,
                "func": name,
                "args": args,
                "state": "TIMEOUT",
            })

    log.info("=" * 70)
    log.info("dtq demo: results")
    log.info("=" * 70)
    for s in summaries:
        log.info(
            "  %-22s id=%s state=%-7s attempts=%s result=%s",
            s["func"],
            s["task_id"][:8],
            s.get("state", "?"),
            s.get("attempts", "?"),
            s.get("result") or s.get("error") or "-",
        )

    pending = client.broker.pending_depth()
    dlq = client.broker.dlq_depth()
    retries = client.broker.retry_depth()
    in_flight = client.broker.in_flight_count()
    log.info(
        "broker depths: pending=%d in_flight=%d retries=%d dlq=%d",
        pending, in_flight, retries, dlq,
    )

    client.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
