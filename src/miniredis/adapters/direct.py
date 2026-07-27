from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

from miniredis.commands.request import CommandRequest
from miniredis.commands.model import BlPop
from miniredis.core.executor import (
    AbandonRequest,
    SessionClosed,
    SubmittedRequest,
)
from miniredis.core.outbound import (
    Abandoned,
    Outbound,
    Replied,
    RuntimeClosed,
    RuntimeFailed,
    SessionEndpoint,
    TransportClosed,
)
from miniredis.core.reply import Bytes, Failure, Reply

if TYPE_CHECKING:
    from miniredis.runtime import MiniRedis


class DirectClient:
    def __init__(self, runtime: MiniRedis, endpoint: SessionEndpoint) -> None:
        self._runtime = runtime
        self.endpoint = endpoint
        self._closed = False
        self._close_task: asyncio.Task[None] | None = None

    @property
    def session_id(self) -> int:
        return self.endpoint.session_id

    @property
    def closed(self) -> bool:
        return self._closed

    async def execute(self, request: CommandRequest) -> Reply | None:
        if self._closed:
            return Failure("CLOSED", "client is closed")
        if not self._runtime.accepting_commands:
            if self._runtime.normal_shutdown_started:
                return Failure("CLOSED", "runtime is not accepting commands")
            return Failure("CLOSED", "runtime is closed")
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
            case TransportClosed() if isinstance(parsed, BlPop):
                return Bytes(None)
            case TransportClosed():
                return Failure("CLOSED", "session closed")
            case RuntimeFailed(reason):
                return Failure("ERR", f"runtime failed: {reason}")
            case Abandoned():
                return Failure("ERR", "request abandoned")
        raise AssertionError(f"unknown request outcome: {outcome!r}")

    async def receive(self) -> Outbound:
        return await self.endpoint.receive()

    async def close(self) -> None:
        self._closed = True
        if self._close_task is None:
            self._close_task = asyncio.create_task(
                self._close_once(),
                name=f"miniredis:direct-close:{self.session_id}",
            )
        await asyncio.shield(self._close_task)

    async def _close_once(self) -> None:
        completion = asyncio.get_running_loop().create_future()
        if not self._runtime.executor.post_control(
            SessionClosed(self.session_id, completion)
        ):
            self.endpoint.outbox.abort("runtime closed")
            return
        await asyncio.shield(completion)
