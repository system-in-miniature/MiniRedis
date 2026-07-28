from __future__ import annotations

from collections import defaultdict, deque
from collections.abc import Callable
from dataclasses import dataclass
from enum import Enum
from typing import Protocol

from miniredis.core.commit import (
    CommitOperation,
    DeleteKey,
    DeleteReason,
    PutEntry,
    StoredEntry,
    StoredList,
)
from miniredis.core.outbound import RequestToken


class CancelHandle(Protocol):
    def cancel(self) -> None:
        raise NotImplementedError


@dataclass(frozen=True, slots=True)
class WaiterId:
    value: int


class WaiterState(str, Enum):
    ACTIVE = "active"
    FULFILLED = "fulfilled"
    TIMED_OUT = "timed_out"
    CANCELLED = "cancelled"
    CLOSED = "closed"


@dataclass(slots=True)
class BlockingWaiter:
    waiter_id: WaiterId
    generation: int
    token: RequestToken
    session_id: int
    keys: tuple[bytes, ...]
    deadline_ms: int | None
    left: bool
    state: WaiterState = WaiterState.ACTIVE
    timer: CancelHandle | None = None


@dataclass(frozen=True, slots=True)
class WaiterWakeup:
    waiter_id: WaiterId
    generation: int
    key: bytes
    item: bytes


class WaiterRegistry:
    def __init__(self, on_debug_change: Callable[[], None]) -> None:
        self._next_id = 1
        self._by_id: dict[WaiterId, BlockingWaiter] = {}
        self._by_key: dict[bytes, deque[WaiterId]] = defaultdict(deque)
        self._by_session: dict[int, set[WaiterId]] = defaultdict(set)
        self._on_debug_change = on_debug_change

    def register(
        self,
        token: RequestToken,
        session_id: int,
        keys: tuple[bytes, ...],
        deadline_ms: int | None,
        left: bool,
    ) -> BlockingWaiter:
        waiter = BlockingWaiter(
            WaiterId(self._next_id),
            1,
            token,
            session_id,
            keys,
            deadline_ms,
            left,
        )
        self._next_id += 1
        self._by_id[waiter.waiter_id] = waiter
        self._by_session[session_id].add(waiter.waiter_id)
        for key in dict.fromkeys(keys):
            self._by_key[key].append(waiter.waiter_id)
        self._on_debug_change()
        return waiter

    def peek(
        self,
        key: bytes,
        excluded: set[WaiterId],
    ) -> BlockingWaiter | None:
        for waiter_id in self._by_key.get(key, ()):
            waiter = self._by_id.get(waiter_id)
            if (
                waiter is not None
                and waiter.state is WaiterState.ACTIVE
                and waiter_id not in excluded
            ):
                return waiter
        return None

    def transition(
        self,
        waiter_id: WaiterId,
        generation: int,
        state: WaiterState,
    ) -> BlockingWaiter | None:
        waiter = self._by_id.get(waiter_id)
        if (
            waiter is None
            or waiter.generation != generation
            or waiter.state is not WaiterState.ACTIVE
        ):
            return None
        waiter.state = state
        if waiter.timer is not None:
            waiter.timer.cancel()
        del self._by_id[waiter_id]
        session_index = self._by_session.get(waiter.session_id)
        if session_index is not None:
            session_index.discard(waiter_id)
            if not session_index:
                del self._by_session[waiter.session_id]
        for key in dict.fromkeys(waiter.keys):
            index = self._by_key[key]
            self._by_key[key] = deque(item for item in index if item != waiter_id)
            if not self._by_key[key]:
                del self._by_key[key]
        self._on_debug_change()
        return waiter

    def for_session(self, session_id: int) -> tuple[BlockingWaiter, ...]:
        return tuple(
            self._by_id[item]
            for item in tuple(self._by_session.get(session_id, ()))
            if item in self._by_id
        )

    def for_token(self, token: RequestToken) -> BlockingWaiter | None:
        return next(
            (waiter for waiter in self._by_id.values() if waiter.token == token),
            None,
        )

    def active(self) -> tuple[BlockingWaiter, ...]:
        return tuple(self._by_id.values())

    @property
    def active_count(self) -> int:
        return len(self._by_id)

    @property
    def timer_count(self) -> int:
        return sum(waiter.timer is not None for waiter in self._by_id.values())

    def ids_for_key(self, key: bytes) -> tuple[WaiterId, ...]:
        return tuple(
            waiter_id
            for waiter_id in self._by_key.get(key, ())
            if waiter_id in self._by_id
        )

    @property
    def index_counts(self) -> tuple[int, int, int]:
        return len(self._by_id), len(self._by_key), len(self._by_session)


def prepare_list_wakeups(
    key: bytes,
    pushed: PutEntry,
    waiters: WaiterRegistry,
) -> tuple[CommitOperation, tuple[WaiterWakeup, ...]]:
    stored = pushed.entry.value
    if not isinstance(stored, StoredList):
        raise TypeError("push operation must contain StoredList")
    remaining = deque(stored.items)
    reserved: set[WaiterId] = set()
    wakeups: list[WaiterWakeup] = []
    while remaining:
        waiter = waiters.peek(key, reserved)
        if waiter is None:
            break
        reserved.add(waiter.waiter_id)
        wakeups.append(
            WaiterWakeup(
                waiter.waiter_id,
                waiter.generation,
                key,
                remaining.popleft() if waiter.left else remaining.pop(),
            )
        )
    if remaining:
        final: CommitOperation = PutEntry(
            key,
            StoredEntry(
                StoredList(tuple(remaining)),
                pushed.entry.expire_at_ms,
                pushed.entry.mutation_version,
            ),
        )
    else:
        final = DeleteKey(key, DeleteReason.CLIENT)
    return final, tuple(wakeups)
