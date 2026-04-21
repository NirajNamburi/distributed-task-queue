"""Regression tests for the CLI argument parser.

These are deliberately tiny - they just prove that argparse wires every
subcommand's dispatch function correctly and that positional arguments do
not shadow the ``func`` attribute used for command dispatch.
"""

from __future__ import annotations

import pytest

from dtq.cli import build_parser


def test_every_subcommand_has_a_callable_func():
    parser = build_parser()
    subcommands = {
        "worker": [],
        "enqueue": ["dtq.tasks.calculate_primes", "100"],
        "stats": [],
        "replay-dlq": ["--all"],
        "purge": ["dtq:pending", "--yes"],
    }
    for cmd, extra in subcommands.items():
        args = parser.parse_args([cmd, *extra])
        assert callable(args.func), f"`dtq {cmd}` -> args.func is not callable: {args.func!r}"


def test_enqueue_positional_does_not_shadow_dispatch_func():
    """Regression: the enqueue subparser used to name its positional 'func',
    which argparse then stored in args.func - clobbering the callable set by
    set_defaults(func=_cmd_enqueue). The dispatch then failed with
    `'str' object is not callable`. Make sure that never comes back.
    """
    parser = build_parser()
    args = parser.parse_args(["enqueue", "dtq.tasks.calculate_primes", "7", "--kw", "k=1"])
    assert callable(args.func)
    assert args.task_path == "dtq.tasks.calculate_primes"
    assert args.args == ["7"]
    assert args.kw == ["k=1"]


def test_enqueue_requires_task_path():
    parser = build_parser()
    with pytest.raises(SystemExit):
        parser.parse_args(["enqueue"])
