from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

from miniredis.commands.request import CommandRequest
from miniredis.core.executor import AbandonRequest, SubmittedRequest
from miniredis.core.outbound import (
    Abandoned,
    Replied,
    RuntimeClosed,
    RuntimeFailed,
    TransportClosed,
)
from miniredis.core.reply import Failure, Reply

if TYPE_CHECKING:
    from miniredis.runtime import MiniRedis


class DirectClient:
    def __init__(self, runtime: MiniRedis, session_id: int) -> None:
        self._runtime = runtime
        self.session_id = session_id
        self._closed = False

    @property
    def closed(self) -> bool:
        return self._closed

    async def execute(self, request: CommandRequest) -> Reply | None:
        if self._closed:
            return Failure("CLOSED", "client is closed")
        parsed = self._runtime.parse(request)
        if isinstance(parsed, Failure):
            return parsed
        submitted = self._runtime.executor.submit(
            session_id=self.session_id, command=parsed
        )
        if isinstance(submitted, Failure):
            return submitted
        assert isinstance(submitted, SubmittedRequest), (
            f"unexpected submission: {submitted!r}"
        )

        try:
            outcome = await asyncio.shield(submitted.future)
        except asyncio.CancelledError:
            self._runtime.executor.post_control(AbandonRequest(submitted.token))
            raise
        match outcome:
            case Replied(reply=reply):
                return reply
            case RuntimeClosed():
                return Failure("CLOSED", "runtime closed before reply")
            case TransportClosed():
                return Failure("CLOSED", "session closed")
            case RuntimeFailed(reason):
                return Failure("ERR", f"runtime failed: {reason}")
            case Abandoned():
                return Failure("ERR", "request abandoned")
        raise AssertionError(f"unknown request outcome: {outcome!r}")

    async def receive(self) -> Reply:
        raise NotImplementedError("DirectClient.receive is unavailable in Phase 1")

    async def close(self) -> None:
        self._closed = True
