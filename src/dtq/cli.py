"""``dtq`` CLI: operate the queue from a terminal.

Subcommands:

    dtq worker       - start a worker pool (default: os.cpu_count() processes)
    dtq enqueue      - push a single task by dotted path
    dtq stats        - print queue depths, in-flight count, live workers
    dtq replay-dlq   - move tasks from the DLQ back to pending
    dtq purge        - DEL a queue (requires --yes)

Argument parsing is stdlib argparse - no Click, no Typer - so the project has
zero soft dependencies on extra libraries.
"""

from __future__ import annotations

import argparse
import json
import sys
from typing import Sequence

from dtq.broker import Broker
from dtq.client import TaskClient
from dtq.config import Settings
from dtq.logging_setup import setup_logging
from dtq.worker import WorkerPool


def _parse_kw(items: Sequence[str]) -> dict:
    """Convert ``["k=v", "n=3"]`` into ``{"k": "v", "n": 3}``.

    Values are JSON-decoded if possible (so ``--kw n=3`` becomes int 3,
    ``--kw enabled=true`` becomes bool True), else kept as strings.
    """
    out: dict = {}
    for item in items:
        if "=" not in item:
            raise SystemExit(f"--kw expects key=value, got {item!r}")
        k, v = item.split("=", 1)
        try:
            out[k] = json.loads(v)
        except json.JSONDecodeError:
            out[k] = v
    return out


def _parse_arg(raw: str):
    """JSON-decode a positional task arg, falling back to the raw string."""
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return raw


def _cmd_worker(args, settings: Settings) -> int:
    if args.processes is not None:
        settings = Settings(**{**settings.as_dict(), "worker_processes": args.processes})
    pool = WorkerPool(settings)
    pool.start(block=True)
    return 0


def _cmd_enqueue(args, settings: Settings) -> int:
    client = TaskClient(settings)
    pos = [_parse_arg(a) for a in (args.args or [])]
    kw = _parse_kw(args.kw or [])
    task_id = client.delay(args.task_path, *pos, max_retries=args.max_retries, **kw)
    print(task_id)
    if args.wait:
        try:
            result = client.wait(task_id, timeout=args.wait_timeout)
            print(json.dumps({"task_id": task_id, "state": "SUCCESS", "result": repr(result)}))
        except Exception as exc:
            print(json.dumps({"task_id": task_id, "state": "FAILED", "error": str(exc)}))
            return 1
    return 0


def _cmd_stats(args, settings: Settings) -> int:
    broker = Broker(settings)
    out = {
        "redis_url": settings.redis_url,
        "pending": broker.pending_depth(),
        "in_flight": broker.in_flight_count(),
        "retries": broker.retry_depth(),
        "dlq": broker.dlq_depth(),
        "workers": broker.list_workers(),
    }
    print(json.dumps(out, indent=2, default=str))
    return 0


def _cmd_replay_dlq(args, settings: Settings) -> int:
    broker = Broker(settings)
    if args.task_id:
        moved = broker.replay_dlq([args.task_id])
    elif args.all:
        moved = broker.replay_dlq(None)
    else:
        print("error: pass --all or --task-id ID", file=sys.stderr)
        return 2
    print(json.dumps({"replayed": moved}))
    return 0


def _cmd_purge(args, settings: Settings) -> int:
    if not args.yes:
        print("refusing to purge without --yes", file=sys.stderr)
        return 2
    broker = Broker(settings)
    removed = broker.purge(args.queue)
    print(json.dumps({"deleted": args.queue, "removed_keys": removed}))
    return 0


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="dtq", description="Distributed task queue CLI")
    sub = p.add_subparsers(dest="cmd", required=True)

    sw = sub.add_parser("worker", help="Run a worker pool until SIGTERM/SIGINT")
    sw.add_argument("--processes", type=int, default=None, help="Override worker_processes (default: os.cpu_count())")
    sw.set_defaults(func=_cmd_worker)

    se = sub.add_parser("enqueue", help="Enqueue a task by dotted path")
    se.add_argument("task_path", metavar="FUNC", help="Dotted path, e.g. dtq.tasks.calculate_primes")
    se.add_argument("args", nargs="*", help="Positional args (each JSON-decoded; falls back to string)")
    se.add_argument("--kw", action="append", default=[], help="Keyword arg as k=v (value JSON-decoded)")
    se.add_argument("--max-retries", type=int, default=None)
    se.add_argument("--wait", action="store_true", help="Block until terminal state")
    se.add_argument("--wait-timeout", type=float, default=60.0)
    se.set_defaults(func=_cmd_enqueue)

    ss = sub.add_parser("stats", help="Print queue depths and live workers")
    ss.set_defaults(func=_cmd_stats)

    sr = sub.add_parser("replay-dlq", help="Re-enqueue dead-letter tasks")
    g = sr.add_mutually_exclusive_group()
    g.add_argument("--all", action="store_true", help="Replay every task in the DLQ")
    g.add_argument("--task-id", help="Replay a single task by id")
    sr.set_defaults(func=_cmd_replay_dlq)

    sp = sub.add_parser("purge", help="DEL a Redis key (queue, zset, etc.)")
    sp.add_argument("queue", help="Redis key to delete")
    sp.add_argument("--yes", action="store_true", help="Required confirmation")
    sp.set_defaults(func=_cmd_purge)

    return p


def main(argv: Sequence[str] | None = None) -> int:
    settings = Settings.from_env()
    setup_logging(settings.log_level)
    args = build_parser().parse_args(argv)
    return int(args.func(args, settings) or 0)


if __name__ == "__main__":
    raise SystemExit(main())
