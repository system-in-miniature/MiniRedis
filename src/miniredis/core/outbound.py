from __future__ import annotations

import asyncio
from collections import deque
from collections.abc import Callable
from dataclasses import dataclass
from typing import TypeAlias

from miniredis.core.reply import Reply


@dataclass(frozen=True, slots=True)
class RequestToken:
    value: int


@dataclass(frozen=True, slots=True)
class Replied:
    reply: Reply | None


@dataclass(frozen=True, slots=True)
class Abandoned:
    pass


@dataclass(frozen=True, slots=True)
class TransportClosed:
    pass


@dataclass(frozen=True, slots=True)
class RuntimeClosed:
    pass


@dataclass(frozen=True, slots=True)
class RuntimeFailed:
    reason: str


RequestOutcome: TypeAlias = (
    Replied | Abandoned | TransportClosed | RuntimeClosed | RuntimeFailed
)


@dataclass(frozen=True, slots=True)
class ReplyMessage:
    request_id: RequestToken
    reply: Reply


@dataclass(frozen=True, slots=True)
class SubscriptionAck:
    kind: str
    channel: bytes | None
    subscription_count: int


@dataclass(frozen=True, slots=True)
class PubSubMessage:
    channel: bytes
    payload: bytes


@dataclass(frozen=True, slots=True)
class PubSubPong:
    payload: bytes


@dataclass(frozen=True, slots=True)
class ServerClosed:
    reason: str


Outbound: TypeAlias = (
    ReplyMessage | SubscriptionAck | PubSubMessage | PubSubPong | ServerClosed
)


class OutboxClosed(RuntimeError):
    pass


class CloseAwareOutbox:
    def __init__(
        self,
        capacity: int,
        on_overflow: Callable[[], None] | None = None,
    ) -> None:
        if capacity <= 0:
            raise ValueError("outbox capacity must be positive")
        self._capacity = capacity
        self._items: deque[Outbound] = deque()
        self._changed = asyncio.Event()
        self._empty = asyncio.Event()
        self._empty.set()
        self._closed = False
        self._reason = ""
        self._overflow_notified = False
        self._on_overflow = on_overflow

    @property
    def closed(self) -> bool:
        return self._closed

    @property
    def close_reason(self) -> str:
        return self._reason

    @property
    def pending_count(self) -> int:
        return len(self._items)

    def offer(self, item: Outbound) -> bool:
        if self._closed:
            return False
        if len(self._items) == self._capacity:
            self.abort("outbox full")
            if not self._overflow_notified:
                self._overflow_notified = True
                if self._on_overflow is not None:
                    self._on_overflow()
            return False
        self._items.append(item)
        self._empty.clear()
        self._changed.set()
        return True

    def offer_best_effort(self, item: Outbound) -> bool:
        if self._closed or len(self._items) == self._capacity:
            return False
        self._items.append(item)
        self._empty.clear()
        self._changed.set()
        return True

    async def receive(self) -> Outbound:
        while True:
            if self._items:
                item = self._items.popleft()
                if not self._items:
                    self._empty.set()
                return item
            if self._closed:
                raise OutboxClosed(self._reason)
            self._changed.clear()
            if self._items or self._closed:
                continue
            await self._changed.wait()

    def begin_close(self, reason: str) -> None:
        if self._closed:
            return
        self._closed = True
        self._reason = reason
        self._changed.set()

    def abort(self, reason: str) -> None:
        if not self._closed:
            self._closed = True
            self._reason = reason
        self._items.clear()
        self._empty.set()
        self._changed.set()

    async def wait_empty(self) -> None:
        await self._empty.wait()


class SessionEndpoint:
    def __init__(
        self,
        session_id: int,
        capacity: int,
        reply_via_outbox: bool,
        on_slow: Callable[[int, str], None],
        close_transport: Callable[[str], None],
    ) -> None:
        self.session_id = session_id
        self.reply_via_outbox = reply_via_outbox
        self._on_slow = on_slow
        self._close_transport = close_transport
        self._transport_close_requested = False
        self.outbox = CloseAwareOutbox(capacity, self._overflow)

    def _overflow(self) -> None:
        self.request_transport_close("outbox full")
        self._on_slow(self.session_id, "outbox full")

    def offer(self, item: Outbound) -> bool:
        return self.outbox.offer(item)

    def offer_best_effort(self, item: Outbound) -> bool:
        return self.outbox.offer_best_effort(item)

    async def receive(self) -> Outbound:
        return await self.outbox.receive()

    def request_transport_close(self, reason: str) -> None:
        if self._transport_close_requested:
            return
        self._transport_close_requested = True
        self._close_transport(reason)
