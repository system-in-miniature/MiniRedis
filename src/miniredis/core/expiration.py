from collections.abc import Callable

from miniredis.clock import Clock, ScheduledHandle, TimerScheduler
from miniredis.core.commit import DeleteKey, DeleteReason
from miniredis.core.database import Entry


def is_expired(entry: Entry, now_ms: int) -> bool:
    return entry.expire_at_ms is not None and entry.expire_at_ms <= now_ms


def expiry_delete(key: bytes) -> DeleteKey:
    return DeleteKey(key, DeleteReason.EXPIRED)


class ActiveExpireProducer:
    def __init__(
        self,
        clock: Clock,
        scheduler: TimerScheduler,
        interval_ms: int,
        post_control: Callable[[object], bool],
        tick_factory: Callable[[int], object],
    ) -> None:
        self._clock = clock
        self._scheduler = scheduler
        self._interval_ms = interval_ms
        self._post_control = post_control
        self._tick_factory = tick_factory
        self._running = False
        self._handle: ScheduledHandle | None = None

    def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._schedule_next()

    def _schedule_next(self) -> None:
        deadline = self._clock.now_ms() + self._interval_ms
        self._handle = self._scheduler.call_at_ms(deadline, self._fire)

    def _fire(self) -> None:
        if not self._running:
            return
        self._post_control(self._tick_factory(self._clock.now_ms()))
        if self._running:
            self._schedule_next()

    async def quiesce(self) -> None:
        self._running = False
        if self._handle is not None:
            self._handle.cancel()
            self._handle = None
