from __future__ import annotations

import asyncio
from enum import Enum
from typing import Any, Self

from miniredis.adapters.direct import DirectClient
from miniredis.clock import Clock, SystemClock
from miniredis.config import MiniRedisConfig
from miniredis.core.commit import CommitBatch
from miniredis.core.database import Database
from miniredis.core.executor import (
    CommandExecutor,
    CommitBarrier,
    NullCommitBarrier,
)
from miniredis.core.planner import CommandPlanner


class RuntimeState(str, Enum):
    STARTING = "starting"
    RUNNING = "running"
    DRAINING = "draining"
    CLOSED = "closed"


class MiniRedis:
    def __init__(
        self,
        config: MiniRedisConfig,
        *,
        clock: Clock,
        commit_barrier: CommitBarrier,
    ) -> None:
        self.config = config
        self.clock = clock
        self.commit_barrier = commit_barrier
        self.database = Database()
        self.planner = CommandPlanner(config)
        self.executor = CommandExecutor(
            database=self.database,
            planner=self.planner,
            clock=clock,
            commit_barrier=commit_barrier,
            max_pending_commands=config.max_pending_commands,
            on_terminal_failure=self._on_executor_terminal_failure,
        )
        self.state = RuntimeState.STARTING
        self._next_session_id = 0
        self._start_task: asyncio.Task[None] | None = None
        self._close_task: asyncio.Task[None] | None = None

    @classmethod
    def open(
        cls,
        config: MiniRedisConfig | None = None,
        **options: Any,
    ) -> MiniRedis:
        if config is not None and options:
            raise TypeError("config cannot be combined with keyword options")
        resolved = config if config is not None else MiniRedisConfig(**options)
        return cls(
            resolved,
            clock=SystemClock(),
            commit_barrier=NullCommitBarrier(),
        )

    @classmethod
    def _for_test(
        cls,
        config: MiniRedisConfig | None = None,
        *,
        clock: Clock | None = None,
        commit_barrier: CommitBarrier | None = None,
        **options: Any,
    ) -> MiniRedis:
        if config is not None and options:
            raise TypeError("config cannot be combined with keyword options")
        resolved = config if config is not None else MiniRedisConfig(**options)
        return cls(
            resolved,
            clock=clock if clock is not None else SystemClock(),
            commit_barrier=(
                commit_barrier if commit_barrier is not None else NullCommitBarrier()
            ),
        )

    async def start(self) -> None:
        if self.state is RuntimeState.RUNNING:
            return
        if self.state in {RuntimeState.DRAINING, RuntimeState.CLOSED}:
            raise RuntimeError("runtime is closed")
        if self._start_task is None:
            self._start_task = asyncio.create_task(
                self._start_once(), name="miniredis:runtime-start"
            )
        await asyncio.shield(self._start_task)

    async def _start_once(self) -> None:
        await self.executor.start()
        if self.state is RuntimeState.STARTING:
            self.state = RuntimeState.RUNNING

    def direct_client(self) -> DirectClient:
        if self.state in {RuntimeState.DRAINING, RuntimeState.CLOSED}:
            raise RuntimeError("runtime is closed")
        self._next_session_id += 1
        return DirectClient(self, self._next_session_id)

    async def close(self) -> None:
        if self._close_task is None:
            self._close_task = asyncio.create_task(
                self._close(), name="miniredis:runtime-close"
            )
        await asyncio.shield(self._close_task)

    async def _close(self) -> None:
        if self.state is RuntimeState.CLOSED:
            return
        self.state = RuntimeState.DRAINING
        await self.executor.close()
        self.state = RuntimeState.CLOSED

    def _on_executor_terminal_failure(self, failure: BaseException) -> None:
        del failure
        self.state = RuntimeState.CLOSED

    async def __aenter__(self) -> Self:
        await self.start()
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        await self.close()

    @property
    def debug_commit_seq(self) -> int:
        return self.database.commit_seq

    @property
    def debug_applied_batches(self) -> tuple[CommitBatch, ...]:
        return self.executor.debug_applied_batches

    def debug_pause_executor(self) -> None:
        self.executor.debug_pause()

    def debug_resume_executor(self) -> None:
        self.executor.debug_resume()

    async def debug_wait_accepted_at_least(self, count: int) -> None:
        await self.executor.debug_wait_accepted_at_least(count)
