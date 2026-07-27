from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

from miniredis.commands.parser import CommandParseError, parse_command_request
from miniredis.commands.request import CommandRequest
from miniredis.core.executor import Replied, RuntimeClosed, SubmittedRequest
from miniredis.core.reply import Failure, Reply

if TYPE_CHECKING:
    from miniredis.runtime import MiniRedis


class DirectClient:
    def __init__(self, runtime: MiniRedis, session_id: int) -> None:
        self.runtime = runtime
        self.session_id = session_id
        self.closed = False

    async def execute(self, request: CommandRequest) -> Reply:
        if self.closed:
            return Failure("CLOSED", "client is closed")
        try:
            command = parse_command_request(request)
        except CommandParseError as error:
            return Failure("ERR", str(error))

        submitted = self.runtime.executor.submit(
            session_id=self.session_id, command=command
        )
        if isinstance(submitted, Failure):
            return submitted
        assert isinstance(submitted, SubmittedRequest), (
            f"unexpected submission: {submitted!r}"
        )

        outcome = await asyncio.shield(submitted.future)
        match outcome:
            case Replied(reply=reply):
                return reply
            case RuntimeClosed():
                return Failure("CLOSED", "runtime closed before reply")
            case _:
                raise AssertionError(f"unexpected request outcome: {outcome!r}")

    async def receive(self) -> Reply:
        raise NotImplementedError("DirectClient.receive is unavailable in Phase 1")

    async def close(self) -> None:
        self.closed = True
