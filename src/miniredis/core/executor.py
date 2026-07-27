from __future__ import annotations

import asyncio
import itertools
from bisect import bisect_right
from collections.abc import Callable
from dataclasses import dataclass
from typing import Protocol

from miniredis.clock import Clock
from miniredis.commands.model import BlPop, Command
from miniredis.core.blocking import WaiterRegistry, WaiterState
from miniredis.core.commit import CommitBatch, CommitOperation, CommitTrigger
from miniredis.core.database import Database
from miniredis.core.expiration import expiry_delete, is_expired
from miniredis.core.mailbox import EventLoopMailbox
from miniredis.core.outbound import (
    Abandoned,
    ReplyMessage,
    RequestOutcome,
    RequestToken,
    Replied,
    RuntimeClosed,
    SessionEndpoint,
    TransportClosed,
)
from miniredis.core.reply import Failure, Reply


@dataclass(slots=True)
class ExecuteRequest:
    token: RequestToken
    session_id: int
    command: Command
    future: asyncio.Future[RequestOutcome]


@dataclass(frozen=True, slots=True)
class SubmittedRequest:
    token: RequestToken
    future: asyncio.Future[RequestOutcome]


@dataclass(frozen=True, slots=True)
class AbandonRequest:
    token: RequestToken


@dataclass(frozen=True, slots=True)
class SessionClosed:
    session_id: int


@dataclass(frozen=True, slots=True)
class ExecutionPlan:
    reply: Reply | None
    operations: tuple[CommitOperation, ...] = ()
    touch_keys: tuple[bytes, ...] = ()
    trigger: CommitTrigger = CommitTrigger.CLIENT


class CommitBarrier(Protocol):
    async def append(self, batch: CommitBatch) -> None: ...


class NullCommitBarrier:
    async def append(self, batch: CommitBatch) -> None:
        del batch


class Planner(Protocol):
    def plan(
        self, command: Command, database: Database, now_ms: int
    ) -> ExecutionPlan: ...


@dataclass(frozen=True, slots=True)
class _StopExecutor:
    pass


@dataclass(slots=True)
class ActiveExpireTick:
    now_ms: int
    future: asyncio.Future[int] | None = None


type ExecutorMessage = (
    ExecuteRequest | AbandonRequest | ActiveExpireTick | _StopExecutor | object
)


class CommandExecutor:
    def __init__(
        self,
        *,
        database: Database,
        planner: Planner,
        clock: Clock,
        commit_barrier: CommitBarrier,
        max_pending_commands: int,
        active_expire_sample_size: int = 20,
        on_debug_change: Callable[[], None],
        on_terminal_failure: Callable[[BaseException], None] | None = None,
    ) -> None:
        self.database = database
        self.planner = planner
        self.clock = clock
        self.commit_barrier = commit_barrier
        self.max_pending_commands = max_pending_commands
        if active_expire_sample_size <= 0:
            raise ValueError("active_expire_sample_size must be positive")
        self.active_expire_sample_size = active_expire_sample_size
        self._active_expire_cursor: bytes | None = None
        self.mailbox: EventLoopMailbox[ExecutorMessage] = EventLoopMailbox(
            max_pending_commands
        )
        self._on_debug_change = on_debug_change
        self._on_terminal_failure = on_terminal_failure
        self.waiters = WaiterRegistry(self._on_debug_change)

        self._worker_task: asyncio.Task[None] | None = None
        self._worker_started_or_done = asyncio.Event()
        self._worker_entered = False
        self._close_task: asyncio.Task[None] | None = None
        self._run_gate = asyncio.Event()
        self._run_gate.set()
        self._request_tokens = itertools.count(1)
        self._requests: dict[RequestToken, ExecuteRequest] = {}
        self._accepted_tokens: list[RequestToken] = []
        self._endpoints: dict[int, SessionEndpoint] = {}
        self._accepted_changed = asyncio.Event()
        self._applied_batches: list[CommitBatch] = []
        self._handling_message = False
        self._failure: BaseException | None = None
        self._terminal_cleanup_complete = False
        self._stopping = False
        self._started = False

    async def start(self) -> None:
        if self._started:
            if self._stopping:
                raise RuntimeError("executor is stopping")
            await self._worker_started_or_done.wait()
            return
        if self._stopping:
            raise RuntimeError("executor is stopping")
        self._started = True
        self._worker_task = asyncio.create_task(self._run(), name="miniredis:executor")
        self._worker_task.add_done_callback(self._on_worker_done)
        await self._worker_started_or_done.wait()

    def _on_worker_done(self, task: asyncio.Task[None]) -> None:
        try:
            if not self._worker_entered:
                try:
                    task.result()
                except asyncio.CancelledError as error:
                    self._complete_terminal_failure(error)
                except Exception as error:  # noqa: BLE001 - startup is terminal
                    self._complete_terminal_failure(error)
        finally:
            self._worker_started_or_done.set()

    def submit(self, session_id: int, command: Command) -> SubmittedRequest | Failure:
        if (
            not self._started
            or self._stopping
            or not self.mailbox.accepting_users
            or (self._worker_task is not None and self._worker_task.cancelling() != 0)
            or (self._worker_task is not None and self._worker_task.done())
        ):
            return Failure("CLOSED", "runtime is closed")
        if len(self._requests) >= self.max_pending_commands:
            return Failure("BUSY", "command queue is full")

        token = RequestToken(next(self._request_tokens))
        future: asyncio.Future[RequestOutcome] = (
            asyncio.get_running_loop().create_future()
        )
        request = ExecuteRequest(token, session_id, command, future)
        self._requests[token] = request
        if not self.mailbox.admit_user(request):
            del self._requests[token]
            if not self.mailbox.accepting_users:
                return Failure("CLOSED", "runtime is closed")
            return Failure("BUSY", "command queue is full")
        self._accepted_tokens.append(token)
        self._accepted_changed.set()
        self._on_debug_change()
        return SubmittedRequest(token, future)

    def _finish_request(
        self,
        token: RequestToken,
        outcome: RequestOutcome,
    ) -> bool:
        message = self._requests.pop(token, None)
        if message is None:
            return False
        if message.future.done():
            raise RuntimeError(f"executor-owned Future already done: {token.value}")
        message.future.set_result(outcome)
        self._accepted_changed.set()
        self._on_debug_change()
        return True

    def _finish_reply(
        self,
        token: RequestToken,
        reply: Reply | None,
    ) -> bool:
        request = self._requests.get(token)
        if request is None:
            return False
        endpoint = self._endpoints.get(request.session_id)
        if endpoint is None:
            return self._finish_request(token, TransportClosed())
        if reply is not None and endpoint.reply_via_outbox:
            if not endpoint.offer(ReplyMessage(token, reply)):
                return self._finish_request(token, TransportClosed())
        return self._finish_request(token, Replied(reply))

    def post_control(self, message: object) -> bool:
        posted = self.mailbox.post_control(message)
        if posted:
            self._on_debug_change()
        return posted

    async def _run(self) -> None:
        failure: BaseException | None = None
        self._worker_entered = True
        self._worker_started_or_done.set()
        try:
            while True:
                message = await self.mailbox.take()
                await self._run_gate.wait()
                self._handling_message = True
                self._on_debug_change()
                try:
                    if isinstance(message, _StopExecutor):
                        return
                    await self._dispatch(message)
                finally:
                    self._handling_message = False
                    self._on_debug_change()
        except asyncio.CancelledError as error:
            failure = error
        except Exception as error:  # noqa: BLE001 - worker failures are terminal
            failure = error
        finally:
            if failure is not None:
                self._complete_terminal_failure(failure)
            else:
                for token in tuple(self._requests):
                    self._finish_request(token, RuntimeClosed())
            self._on_debug_change()

    async def _dispatch(self, message: object) -> None:
        if isinstance(message, ExecuteRequest):
            await self._execute(message)
        elif isinstance(message, AbandonRequest):
            self._abandon(message)
        elif isinstance(message, ActiveExpireTick):
            deleted = await self._active_expire_once(message.now_ms)
            if message.future is not None and not message.future.done():
                message.future.set_result(deleted)
        elif isinstance(message, SessionClosed):
            self._close_session(message.session_id)
        else:
            raise AssertionError(f"unknown executor message: {message!r}")

    def _abandon(self, event: AbandonRequest) -> None:
        waiter = self.waiters.for_token(event.token)
        if waiter is not None:
            transitioned = self.waiters.transition(
                waiter.waiter_id,
                waiter.generation,
                WaiterState.CANCELLED,
            )
            if transitioned is not None:
                self._finish_request(event.token, Abandoned())
                return
        self._finish_request(event.token, Abandoned())

    def _close_session(self, session_id: int) -> None:
        self._endpoints.pop(session_id, None)
        for token, request in tuple(self._requests.items()):
            if request.session_id == session_id:
                self._finish_request(token, TransportClosed())
        self._on_debug_change()

    def _complete_terminal_failure(self, failure: BaseException) -> None:
        if self._terminal_cleanup_complete:
            return
        self._terminal_cleanup_complete = True
        self._failure = failure
        self._stopping = True
        self.mailbox.close_user_admission()
        self.mailbox.drain()
        for token in tuple(self._requests):
            self._finish_request(token, RuntimeClosed())
        self.mailbox.close_control_admission()
        self._on_debug_change()
        if self._on_terminal_failure is not None:
            self._on_terminal_failure(failure)

    async def _execute(self, request: ExecuteRequest) -> None:
        now_ms = self.clock.now_ms()
        if isinstance(request.command, BlPop):
            plan = self.planner.plan_blpop_now(request.command, self.database, now_ms)
            if plan is None:
                deadline = (
                    None
                    if request.command.timeout_ms == 0
                    else now_ms + request.command.timeout_ms
                )
                self.waiters.register(
                    request.token,
                    request.session_id,
                    request.command.keys,
                    deadline,
                )
                return
        else:
            plan = self.planner.plan(request.command, self.database, now_ms)
        await self._apply_plan(request, plan, now_ms)

    async def _apply_plan(
        self,
        request: ExecuteRequest,
        plan: ExecutionPlan,
        now_ms: int,
    ) -> None:
        if plan.operations:
            batch = CommitBatch(
                self.database.commit_seq + 1,
                plan.operations,
                plan.trigger,
            )
            await self.commit_barrier.append(batch)
            self.database.apply_batch(
                batch,
                track_access=plan.trigger is CommitTrigger.CLIENT,
            )
            self._applied_batches.append(batch)

        for key in dict.fromkeys(plan.touch_keys):
            self.database.touch_if_live(key, now_ms)

        self._finish_reply(request.token, plan.reply)

    async def active_expire_once(self) -> int:
        if self._worker_task is None or self._stopping:
            return 0
        future: asyncio.Future[int] = asyncio.get_running_loop().create_future()
        tick = ActiveExpireTick(self.clock.now_ms(), future)
        if not self.post_control(tick):
            return 0
        return await asyncio.shield(future)

    async def _active_expire_once(self, now_ms: int) -> int:
        keys = sorted(
            key
            for key, entry in self.database.entries.items()
            if entry.expire_at_ms is not None
        )
        if not keys:
            self._active_expire_cursor = None
            return 0
        start = (
            0
            if self._active_expire_cursor is None
            else bisect_right(keys, self._active_expire_cursor)
        )
        ordered_keys = keys[start:] + keys[:start]
        candidate_keys = ordered_keys[: self.active_expire_sample_size]
        self._active_expire_cursor = candidate_keys[-1]
        operations = tuple(
            expiry_delete(key)
            for key in candidate_keys
            if is_expired(self.database.entries[key], now_ms)
        )
        if not operations:
            return 0
        batch = CommitBatch(
            self.database.commit_seq + 1,
            operations,
            CommitTrigger.ACTIVE_EXPIRE,
        )
        await self.commit_barrier.append(batch)
        self.database.apply_batch(batch, track_access=False)
        self._applied_batches.append(batch)
        return len(operations)

    async def close(self) -> None:
        if self._close_task is None:
            self._close_task = asyncio.create_task(
                self._close_once(), name="miniredis:executor-close"
            )
        await asyncio.shield(self._close_task)

    async def _close_once(self) -> None:
        self._stopping = True
        self.mailbox.close_user_admission()
        self._run_gate.set()
        try:
            if self._worker_task is not None and not self._worker_task.done():
                self.post_control(_StopExecutor())
                await self._worker_task
        except Exception as error:  # noqa: BLE001 - close must finish cleanup
            if self._failure is None:
                self._failure = error
        finally:
            self.mailbox.drain()
            for token in tuple(self._requests):
                self._finish_request(token, RuntimeClosed())
            self.mailbox.close_control_admission()
            self._on_debug_change()

    def debug_pause(self) -> None:
        self._run_gate.clear()

    def debug_resume(self) -> None:
        self._run_gate.set()

    async def debug_wait_accepted_at_least(self, count: int) -> None:
        while len(self._requests) < count:
            self._accepted_changed.clear()
            if len(self._requests) >= count:
                return
            await self._accepted_changed.wait()

    @property
    def debug_accepted_count(self) -> int:
        return len(self._requests)

    @property
    def accepted_tokens(self) -> tuple[RequestToken, ...]:
        return tuple(self._accepted_tokens)

    @property
    def accepted_request_count(self) -> int:
        return len(self._requests)

    @property
    def pending_request_count(self) -> int:
        return sum(not request.future.done() for request in self._requests.values())

    @property
    def idle(self) -> bool:
        return (
            not self._handling_message
            and self.mailbox.pending_items == 0
            and not self._requests
        )

    def register_endpoint(self, endpoint: SessionEndpoint) -> None:
        if endpoint.session_id in self._endpoints:
            raise ValueError(f"duplicate session: {endpoint.session_id}")
        self._endpoints[endpoint.session_id] = endpoint
        self._on_debug_change()

    def endpoint(self, session_id: int) -> SessionEndpoint | None:
        return self._endpoints.get(session_id)

    def endpoints(self) -> tuple[SessionEndpoint, ...]:
        return tuple(self._endpoints.values())

    @property
    def endpoint_count(self) -> int:
        return len(self._endpoints)

    @property
    def debug_failure(self) -> BaseException | None:
        return self._failure

    def debug_applied_batches(self) -> tuple[CommitBatch, ...]:
        return tuple(self._applied_batches)
