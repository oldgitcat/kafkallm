from __future__ import annotations

import logging
import time
from collections.abc import Callable


class RetryTimeout(RuntimeError):
    """A retry loop reached its deadline without succeeding."""

    def __init__(self, description: str, last_error: Exception | None) -> None:
        super().__init__(f"{description} did not become ready in time")
        self.description = description
        self.last_error = last_error


def retry_until[T](
    deadline: float,
    attempt: Callable[[], T],
    *,
    description: str,
    logger: logging.Logger,
    initial_delay: float = 1.0,
    max_delay: float = 10.0,
) -> T:
    """Call ``attempt`` with exponential backoff until it succeeds or time is up.

    ``attempt`` signals failure by raising. The caller translates
    :class:`RetryTimeout` into its own error type when it needs a specific
    message or exception class.
    """
    delay = initial_delay
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        try:
            return attempt()
        except Exception as exc:
            last_error = exc
            remaining = max(0.0, deadline - time.monotonic())
            if remaining <= 0:
                break
            logger.warning(
                "%s not ready yet (%s); retrying in %.1fs",
                description,
                exc,
                min(delay, remaining),
            )
            time.sleep(min(delay, remaining))
            delay = min(delay * 2, max_delay)
    raise RetryTimeout(description, last_error)
