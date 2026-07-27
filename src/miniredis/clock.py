from __future__ import annotations

import asyncio
import time
from collections.abc import Callable
from typing import Protocol


class Clock(Protocol):
    def now_ms(self) -> int: ...


class SystemClock:
    def now_ms(self) -> int:
        return time.time_ns() // 1_000_000


class ScheduledHandle(Protocol):
    def cancel(self) -> None:
        raise NotImplementedError


class TimerScheduler(Protocol):
    def call_at_ms(
        self,
        deadline_ms: int,
        callback: Callable[[], None],
    ) -> ScheduledHandle:
        raise NotImplementedError


class AsyncioTimerScheduler:
    def __init__(self, clock: Clock) -> None:
        self._clock = clock

    def call_at_ms(
        self,
        deadline_ms: int,
        callback: Callable[[], None],
    ) -> asyncio.TimerHandle:
        delay = max(0, deadline_ms - self._clock.now_ms()) / 1000
        return asyncio.get_running_loop().call_later(delay, callback)
