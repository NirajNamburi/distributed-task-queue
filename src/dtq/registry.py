"""Resolve dotted-path strings to callable Python objects.

Tasks travel across process and machine boundaries as ``"pkg.mod.func"`` strings
rather than pickled function references. The worker re-imports the module on
demand and looks up the attribute. This is the pattern used by Celery, RQ, and
basically every production task queue, because pickling functions across
Python versions or git commits is fragile and unsafe.
"""

from __future__ import annotations

import importlib
from typing import Callable


class UnknownTaskError(LookupError):
    """Raised when a task's dotted path cannot be resolved."""


_RESOLVE_CACHE: dict[str, Callable] = {}


def resolve(dotted: str) -> Callable:
    """Look up a callable by dotted path. Cached after the first resolution.

    Examples:
        >>> resolve("dtq.tasks.calculate_primes")  # doctest: +SKIP
        <function calculate_primes at ...>
    """
    cached = _RESOLVE_CACHE.get(dotted)
    if cached is not None:
        return cached

    if ":" in dotted:
        module_path, attr = dotted.split(":", 1)
    elif "." not in dotted:
        raise UnknownTaskError(f"Task path {dotted!r} must include a module (e.g. 'pkg.mod.fn')")
    else:
        module_path, attr = dotted.rsplit(".", 1)

    try:
        module = importlib.import_module(module_path)
    except ImportError as exc:
        raise UnknownTaskError(f"Cannot import module {module_path!r}: {exc}") from exc

    try:
        func = getattr(module, attr)
    except AttributeError as exc:
        raise UnknownTaskError(f"Module {module_path!r} has no attribute {attr!r}") from exc

    if not callable(func):
        raise UnknownTaskError(f"{dotted!r} resolved to {type(func).__name__}, not a callable")

    _RESOLVE_CACHE[dotted] = func
    return func


def callable_to_path(func: Callable) -> str:
    """Inverse of :func:`resolve`: derive a dotted path from a callable.

    Round-trips for module-level functions; raises for lambdas / closures.
    """
    module = getattr(func, "__module__", None)
    qualname = getattr(func, "__qualname__", None)
    if not module or not qualname:
        raise ValueError(f"Cannot derive a dotted path for {func!r}")
    if "<locals>" in qualname or "<lambda>" in qualname:
        raise ValueError(
            f"Cannot enqueue {func!r}: only module-level functions are supported "
            "(no lambdas or closures)"
        )
    return f"{module}.{qualname}"
