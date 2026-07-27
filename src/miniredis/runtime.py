from __future__ import annotations

import asyncio
import itertools
from collections.abc import Callable
from dataclasses import dataclass
from enum import Enum
from typing import Any, Self

from miniredis.adapters.direct import DirectClient
from miniredis.clock import (
    AsyncioTimerScheduler,
    Clock,
    SystemClock,
    TimerScheduler,
)
from miniredis.config import MiniRedisConfig
from miniredis.commands.model import Command
from miniredis.commands.parser import CommandParseError, parse_command_request
from miniredis.commands.request import CommandRequest
from miniredis.core.blocking import WaiterId
from miniredis.core.commit import CommitBatch, StoredEntry
from miniredis.core.database import Database
from miniredis.core.executor import (
    CommandExecutor,
    CommitBarrier,
    NullCommitBarrier,
    SessionClosed,
)
from miniredis.core.outbound import RequestToken, SessionEndpoint
from miniredis.core.planner import CommandPlanner
from miniredis.core.reply import Failure


class RuntimeState(str, Enum):
    STARTING = "starting"
    RUNNING = "running"
    DRAINING = "draining"
    CLOSED = "closed"


@dataclass(frozen=True, slots=True)
class RuntimeStats:
    accepted_requests: int
    pending_futures: int
    waiters: int
    subscriptions: int
    sessions: int
    timer_handles: int
    owned_tasks: int


def _direct_transport_close(_reason: str) -> None:
    return None


class MiniRedis:
    def __init__(
        self,
        config: MiniRedisConfig,
        *,
        clock: Clock,
        commit_barrier: CommitBarrier,
        scheduler: TimerScheduler | None,
    ) -> None:
        self.config = config
        self.clock = clock
        self.scheduler = (
            AsyncioTimerScheduler(clock) if scheduler is None else scheduler
        )
        self.commit_barrier = commit_barrier
        self.database = Database()
        self.planner = CommandPlanner(config)
        self._debug_changed = asyncio.Event()
        self.executor = CommandExecutor(
            database=self.database,
            planner=self.planner,
            clock=clock,
            commit_barrier=commit_barrier,
            max_pending_commands=config.max_pending_commands,
            active_expire_sample_size=config.active_expire_sample_size,
            scheduler=self.scheduler,
            on_debug_change=self._debug_notify,
            on_terminal_failure=self._on_executor_terminal_failure,
        )
        self.state = RuntimeState.STARTING
        self._session_ids = itertools.count(1)
        self._start_task: asyncio.Task[None] | None = None
        self._close_task: asyncio.Task[None] | None = None

    @classmethod
    def open(
        cls,
        config: MiniRedisConfig | None = None,
        *,
        clock: Clock | None = None,
        scheduler: TimerScheduler | None = None,
        commit_barrier: CommitBarrier | None = None,
        **options: Any,
    ) -> MiniRedis:
        if config is not None and options:
            raise TypeError("config cannot be combined with keyword options")
        resolved = config if config is not None else MiniRedisConfig(**options)
        return cls(
            resolved,
            clock=clock if clock is not None else SystemClock(),
            scheduler=scheduler,
            commit_barrier=(
                commit_barrier if commit_barrier is not None else NullCommitBarrier()
            ),
        )

    @classmethod
    def _for_test(
        cls,
        config: MiniRedisConfig | None = None,
        *,
        clock: Clock | None = None,
        scheduler: TimerScheduler | None = None,
        commit_barrier: CommitBarrier | None = None,
        **options: Any,
    ) -> MiniRedis:
        return cls.open(
            config,
            clock=clock,
            scheduler=scheduler,
            commit_barrier=commit_barrier,
            **options,
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

    def parse(self, request: CommandRequest) -> Command | Failure:
        try:
            return parse_command_request(request)
        except CommandParseError as error:
            return Failure("ERR", str(error))

    def direct_client(self) -> DirectClient:
        if self.state in {RuntimeState.DRAINING, RuntimeState.CLOSED}:
            raise RuntimeError("runtime is closed")
        session_id = next(self._session_ids)
        endpoint = SessionEndpoint(
            session_id=session_id,
            capacity=self.config.outbox_limit,
            reply_via_outbox=False,
            on_slow=self._session_became_slow,
            close_transport=_direct_transport_close,
        )
        self.executor.register_endpoint(endpoint)
        return DirectClient(self, endpoint)

    def _session_became_slow(self, session_id: int, _reason: str) -> None:
        self.executor.post_control(SessionClosed(session_id))

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
    def debug_physical_key_count(self) -> int:
        return len(self.database.entries)

    async def debug_active_expire_once(self) -> int:
        if self.state is not RuntimeState.RUNNING:
            return 0
        return await self.executor.active_expire_once()

    def debug_applied_batches(self) -> tuple[CommitBatch, ...]:
        return self.executor.debug_applied_batches()

    def debug_logical_items(self) -> tuple[tuple[bytes, StoredEntry], ...]:
        return self.database.logical_items()

    def debug_pause_executor(self) -> None:
        self.executor.debug_pause()

    def debug_resume_executor(self) -> None:
        self.executor.debug_resume()

    async def debug_wait_accepted_at_least(self, count: int) -> None:
        await self.executor.debug_wait_accepted_at_least(count)

    @property
    def debug_accepted_tokens(self) -> tuple[RequestToken, ...]:
        return self.executor.accepted_tokens

    def debug_stats(self) -> RuntimeStats:
        return RuntimeStats(
            accepted_requests=self.executor.accepted_request_count,
            pending_futures=self.executor.pending_request_count,
            waiters=self.executor.waiters.active_count,
            subscriptions=self.executor.pubsub.membership_count,
            sessions=self.executor.endpoint_count,
            timer_handles=self.executor.waiters.timer_count,
            owned_tasks=0,
        )

    def _debug_notify(self) -> None:
        self._debug_changed.set()

    async def _debug_wait(self, predicate: Callable[[], bool]) -> None:
        while not predicate():
            self._debug_changed.clear()
            if predicate():
                return
            await self._debug_changed.wait()

    async def debug_wait_until_queued(self, count: int) -> None:
        await self._debug_wait(lambda: self.executor.accepted_request_count >= count)

    async def debug_wait_until_idle(self) -> None:
        await self._debug_wait(lambda: self.executor.idle)

    async def debug_wait_for_sessions(self, count: int) -> None:
        await self._debug_wait(lambda: self.executor.endpoint_count == count)

    async def debug_wait_for_waiters(self, count: int) -> None:
        await self._debug_wait(lambda: self.executor.waiters.active_count == count)

    def debug_waiter_ids(self, key: bytes) -> tuple[WaiterId, ...]:
        return self.executor.waiters.ids_for_key(key)

    @property
    def debug_waiter_index_counts(self) -> tuple[int, int, int]:
        return self.executor.waiters.index_counts
