"""Batching of incoming ticks before they are written to Parquet.

Writing one file per tick would bury the raw layer in tiny files, so ticks are held
until either the batch is large enough or enough time has passed. The time bound
matters for quiet pairs, which would otherwise never reach the size bound.
"""

from __future__ import annotations

import time
from collections.abc import Callable
from typing import Any

DEFAULT_MAX_TICKS = 5_000
DEFAULT_MAX_SECONDS = 300.0


class TickBuffer:
    def __init__(
        self,
        max_ticks: int = DEFAULT_MAX_TICKS,
        max_seconds: float = DEFAULT_MAX_SECONDS,
        clock: Callable[[], float] | None = None,
    ):
        if max_ticks <= 0:
            raise ValueError("max_ticks must be positive")
        if max_seconds <= 0:
            raise ValueError("max_seconds must be positive")

        self.max_ticks = max_ticks
        self.max_seconds = max_seconds
        self._clock = clock or time.monotonic
        self._ticks: list[dict[str, Any]] = []
        self._opened_at = self._clock()

    def __len__(self) -> int:
        return len(self._ticks)

    def add(self, tick: dict[str, Any]) -> None:
        self._ticks.append(tick)

    def should_flush(self) -> bool:
        if not self._ticks:
            return False
        if len(self._ticks) >= self.max_ticks:
            return True
        return (self._clock() - self._opened_at) >= self.max_seconds

    def drain(self) -> list[dict[str, Any]]:
        """Hand over the buffered ticks and start a new window."""
        batch, self._ticks = self._ticks, []
        self._opened_at = self._clock()
        return batch
