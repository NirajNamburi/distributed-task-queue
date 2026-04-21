"""Producer-side API.

Application code talks to the queue exclusively through :class:`TaskClient`.
The client never touches Redis directly - it builds a :class:`Task`, hands it
to the :class:`Broker`, and returns the task id. Inspection helpers
(``get_state``, ``get_result``, ``wait``) round-trip through the broker too.
"""

from __future__ import annotations

import time
from typing import Any, Callable

from dtq.broker import Broker
from dtq.config import Settings
from dtq.logging_setup import get_logger
from dtq.registry import callable_to_path
from dtq.task import Task, TaskField, TaskState


log = get_logger(__name__)


class TaskNotFound(KeyError):
    """Raised by :meth:`TaskClient.wait` and friends when the task hash is gone."""


class TaskTimeout(TimeoutError):
    """Raised by :meth:`TaskClient.wait` when the deadline elapses."""


class TaskFailed(RuntimeError):
    """Raised by :meth:`TaskClient.wait` when the task ends in FAILED."""

    def __init__(self, task_id: str, error: str, error_type: str) -> None:
        super().__init__(f"Task {task_id} failed ({error_type}): {error}")
        self.task_id = task_id
        self.error = error
        self.error_type = error_type


_TERMINAL = {TaskState.SUCCESS.value, TaskState.FAILED.value}


class TaskClient:
    """Producer that enqueues tasks and inspects their state."""

    def __init__(self, settings: Settings | None = None, broker: Broker | None = None) -> None:
        self.settings = settings or Settings.from_env()
        self.broker = broker or Broker(self.settings)

    def delay(
        self,
        func_or_path: Callable | str,
        *args: Any,
        max_retries: int | None = None,
        **kwargs: Any,
    ) -> str:
        """Enqueue ``func(*args, **kwargs)`` for asynchronous execution.

        ``func_or_path`` may be a real callable (must be importable by the
        worker via its ``__module__``/``__qualname__``) or a dotted path
        string of the form ``"pkg.mod.func"``.

        Returns the generated task id.
        """
        if isinstance(func_or_path, str):
            dotted = func_or_path
        elif callable(func_or_path):
            dotted = callable_to_path(func_or_path)
        else:
            raise TypeError(f"delay() expects a callable or dotted path, got {type(func_or_path).__name__}")

        task = Task(
            func=dotted,
            args=tuple(args),
            kwargs=dict(kwargs),
            max_retries=self.settings.max_retries if max_retries is None else int(max_retries),
        )
        self.broker.enqueue(task)
        log.info(
            "task enqueued",
            extra={"task_id": task.id, "func": dotted, "max_retries": task.max_retries},
        )
        return task.id

    # ------------------------------------------------------------------
    # Inspection helpers
    # ------------------------------------------------------------------

    def get_state(self, task_id: str) -> str | None:
        return self.broker.get_task_state(task_id)

    def get_result(self, task_id: str) -> Any:
        return self.broker.get_task_result(task_id)

    def get_info(self, task_id: str) -> dict[str, Any]:
        return self.broker.get_task_hash(task_id)

    def wait(
        self,
        task_id: str,
        timeout: float = 30.0,
        poll_interval: float = 0.1,
        raise_on_failure: bool = True,
    ) -> Any:
        """Block until the task reaches a terminal state or the timeout expires.

        Implementation note: this is a polling wait, which is fine for the
        producer-side ergonomics we want here. A pubsub-based wait is feasible
        but adds a moving part for very little real benefit at small N.
        """
        deadline = time.time() + timeout
        backoff = poll_interval
        while True:
            info = self.broker.get_task_hash(task_id)
            if not info:
                raise TaskNotFound(task_id)
            state = info.get(TaskField.STATE)
            if state in _TERMINAL:
                if state == TaskState.FAILED.value and raise_on_failure:
                    raise TaskFailed(
                        task_id,
                        info.get(TaskField.ERROR, ""),
                        info.get(TaskField.ERROR_TYPE, "Exception"),
                    )
                return info.get(TaskField.RESULT)
            if time.time() >= deadline:
                raise TaskTimeout(f"Task {task_id} did not complete within {timeout}s (state={state})")
            time.sleep(backoff)
            backoff = min(backoff * 1.5, 1.0)

    def close(self) -> None:
        self.broker.close()
