from __future__ import annotations

import asyncio
from collections import deque


class EventLoopMailbox[T]:
    """A single-event-loop mailbox with separate user and control admission."""

    def __init__(self, max_pending_users: int) -> None:
        if max_pending_users <= 0:
            raise ValueError("max_pending_users must be positive")
        self._max_pending_users = max_pending_users
        self._items: deque[tuple[bool, T]] = deque()
        self._pending_users = 0
        self._ready = asyncio.Event()
        self._changed = asyncio.Event()
        self._user_open = True
        self._control_open = True

    @property
    def pending_users(self) -> int:
        return self._pending_users

    @property
    def accepting_users(self) -> bool:
        return self._user_open

    @property
    def pending_items(self) -> int:
        return len(self._items)

    def admit_user(self, item: T) -> bool:
        if not self._user_open or self._pending_users >= self._max_pending_users:
            return False
        self._items.append((True, item))
        self._pending_users += 1
        self._ready.set()
        self._changed.set()
        return True

    def post_control(self, item: T) -> bool:
        if not self._control_open:
            return False
        self._items.append((False, item))
        self._ready.set()
        self._changed.set()
        return True

    async def take(self) -> T:
        while not self._items:
            self._ready.clear()
            if self._items:
                continue
            await self._ready.wait()

        is_user, item = self._items.popleft()
        if is_user:
            self._pending_users -= 1
            self._changed.set()
        return item

    async def wait_pending_at_least(self, count: int) -> None:
        while self._pending_users < count:
            self._changed.clear()
            if self._pending_users >= count:
                return
            await self._changed.wait()

    async def wait_items_at_least(self, count: int) -> None:
        while len(self._items) < count:
            self._changed.clear()
            if len(self._items) >= count:
                return
            await self._changed.wait()

    def drain(self) -> tuple[T, ...]:
        items = tuple(item for _is_user, item in self._items)
        self._items.clear()
        self._pending_users = 0
        self._ready.clear()
        self._changed.set()
        return items

    def close_user_admission(self) -> None:
        self._user_open = False
        self._changed.set()

    def open_user_admission(self) -> None:
        if not self._control_open:
            raise RuntimeError("cannot reopen user admission after control close")
        self._user_open = True
        self._changed.set()

    def close_control_admission(self) -> None:
        self._control_open = False
        self._changed.set()

    def close_admissions(self) -> None:
        self.close_user_admission()
        self.close_control_admission()
