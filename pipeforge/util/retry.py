from __future__ import annotations

import logging
import random
import time
from typing import Callable, TypeVar

log = logging.getLogger("pipeforge")
T = TypeVar("T")

_TRANSIENT_MARKERS = (
    "connection refused", "connection reset", "connectexception", "sockettimeout",
    "timed out", "timeout", "could not connect", "terminating connection",
    "too many connections", "the connection attempt failed", "broken pipe",
    "slowdown", "throttl", "service unavailable", "temporarily",
)


def is_transient(exc: BaseException) -> bool:
    """Heuristic: network-ish failures are retried, auth/syntax failures are not."""
    text = f"{type(exc).__name__} {exc}".lower()
    return any(m in text for m in _TRANSIENT_MARKERS)


def retry_call(fn: Callable[[], T], *, attempts: int = 3, base_delay: float = 1.0,
               max_delay: float = 30.0,
               should_retry: Callable[[BaseException], bool] = is_transient,
               sleep: Callable[[float], None] = time.sleep, what: str = "operation") -> T:
    """Call ``fn`` with exponential backoff and jitter. Re-raises the last error."""
    attempts = max(1, attempts)
    for attempt in range(1, attempts + 1):
        try:
            return fn()
        except Exception as exc:  # noqa: BLE001
            if attempt >= attempts or not should_retry(exc):
                raise
            delay = min(max_delay, base_delay * (2 ** (attempt - 1))) * (0.5 + random.random() / 2)
            log.warning("%s failed (attempt %d/%d): %s; retrying in %.1fs",
                        what, attempt, attempts, exc, delay)
            sleep(delay)
    raise AssertionError("unreachable")
