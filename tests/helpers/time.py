import heapq
from collections.abc import Callable
from dataclasses import dataclass, field


class FakeClock:
    def __init__(self, now_ms: int = 0) -> None:
        self.value = now_ms

    def now_ms(self) -> int:
        return self.value

    def advance(self, milliseconds: int) -> None:
        self.value += milliseconds


@dataclass(order=True)
class ManualHandle:
    deadline_ms: int
    order: int
    callback: Callable[[], None] = field(compare=False)
    cancelled: bool = field(default=False, compare=False)

    def cancel(self) -> None:
        self.cancelled = True


class ManualScheduler:
    def __init__(self, clock: FakeClock) -> None:
        self.clock = clock
        self._next_order = 0
        self._calls: list[ManualHandle] = []

    def call_at_ms(
        self,
        deadline_ms: int,
        callback: Callable[[], None],
    ) -> ManualHandle:
        handle = ManualHandle(deadline_ms, self._next_order, callback)
        self._next_order += 1
        heapq.heappush(self._calls, handle)
        return handle

    def fire_due(self) -> int:
        fired = 0
        while self._calls and self._calls[0].deadline_ms <= self.clock.now_ms():
            handle = heapq.heappop(self._calls)
            if not handle.cancelled:
                handle.callback()
                fired += 1
        return fired

    @property
    def pending_count(self) -> int:
        return sum(not handle.cancelled for handle in self._calls)
