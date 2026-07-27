from __future__ import annotations

import asyncio
import itertools
from bisect import bisect_right
from collections.abc import Callable
from dataclasses import dataclass, replace
from typing import Protocol

from miniredis.clock import Clock, TimerScheduler
from miniredis.commands.model import (
    BlPop,
    Command,
    ListPush,
    Ping,
    Publish,
    Subscribe,
    Unsubscribe,
)
from miniredis.core.blocking import (
    WaiterId,
    WaiterRegistry,
    WaiterState,
    WaiterWakeup,
    prepare_list_wakeups,
)
from miniredis.core.commit import (
    CommitBatch,
    CommitOperation,
    CommitTrigger,
    PutEntry,
    StoredList,
)
from miniredis.core.database import Database
from miniredis.core.expiration import expiry_delete, is_expired
from miniredis.core.mailbox import EventLoopMailbox
from miniredis.core.outbound import (
    Abandoned,
    PubSubMessage,
    PubSubPong,
    ReplyMessage,
    RequestOutcome,
    RequestToken,
    Replied,
    RuntimeClosed,
    RuntimeFailed,
    ServerClosed,
    SessionEndpoint,
    SubscriptionAck,
    TransportClosed,
)
from miniredis.core.pubsub import PubSubRegistry
from miniredis.core.reply import Bytes, Failure, Items, Number, Reply


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


@dataclass(slots=True)
class SessionClosed:
    session_id: int
    completion: asyncio.Future[None] | None = None


@dataclass(frozen=True, slots=True)
class TimeoutWaiter:
    waiter_id: WaiterId
    generation: int


@dataclass(slots=True)
class BeginShutdown:
    outcome: RequestOutcome
    completion: asyncio.Future[None]


@dataclass(frozen=True, slots=True)
class ExecutionPlan:
    reply: Reply | None
    operations: tuple[CommitOperation, ...] = ()
    touch_keys: tuple[bytes, ...] = ()
    trigger: CommitTrigger = CommitTrigger.CLIENT
    waiter_wakeups: tuple[WaiterWakeup, ...] = ()


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
        scheduler: TimerScheduler,
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
        self.pubsub = PubSubRegistry(self._on_debug_change)
        self.scheduler = scheduler

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
        self._stop_after_current_message = False
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
                    if self._stop_after_current_message:
                        return
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
        elif isinstance(message, TimeoutWaiter):
            self._timeout_waiter(message)
        elif isinstance(message, SessionClosed):
            self._close_session(message)
        elif isinstance(message, BeginShutdown):
            self._begin_shutdown(message)
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

    def _timeout_waiter(self, event: TimeoutWaiter) -> None:
        waiter = self.waiters.transition(
            event.waiter_id,
            event.generation,
            WaiterState.TIMED_OUT,
        )
        if waiter is not None:
            self._finish_reply(waiter.token, Bytes(None))

    def _close_session(self, event: SessionClosed) -> None:
        for waiter in self.waiters.for_session(event.session_id):
            closed = self.waiters.transition(
                waiter.waiter_id,
                waiter.generation,
                WaiterState.CLOSED,
            )
            if closed is not None:
                self._finish_request(closed.token, TransportClosed())
        self.pubsub.remove_session(event.session_id)
        endpoint = self._endpoints.pop(event.session_id, None)
        if endpoint is not None:
            endpoint.outbox.abort("session closed")
            endpoint.request_transport_close("session closed")
        if event.completion is not None and not event.completion.done():
            event.completion.set_result(None)
        self._on_debug_change()

    def _begin_shutdown(self, event: BeginShutdown) -> None:
        self.mailbox.close_control_admission()
        for waiter in self.waiters.active():
            closed = self.waiters.transition(
                waiter.waiter_id,
                waiter.generation,
                WaiterState.CLOSED,
            )
            if closed is not None:
                self._finish_request(closed.token, event.outcome)
        self.pubsub.clear()
        for token in tuple(self._requests):
            self._finish_request(token, event.outcome)
        for endpoint in self._endpoints.values():
            endpoint.offer_best_effort(ServerClosed("runtime closed"))
        if not event.completion.done():
            event.completion.set_result(None)
        self._stop_after_current_message = True

    def _complete_terminal_failure(self, failure: BaseException) -> None:
        if self._terminal_cleanup_complete:
            return
        self._terminal_cleanup_complete = True
        self._failure = failure
        self._stopping = True
        self.mailbox.close_user_admission()
        self.mailbox.drain()
        for token in tuple(self._requests):
            waiter = self.waiters.for_token(token)
            if waiter is not None:
                self.waiters.transition(
                    waiter.waiter_id,
                    waiter.generation,
                    WaiterState.CLOSED,
                )
                self._finish_request(
                    token,
                    RuntimeFailed(str(failure) or type(failure).__name__),
                )
            else:
                self._finish_request(token, RuntimeClosed())
        self.pubsub.clear()
        self.mailbox.close_control_admission()
        self._on_debug_change()
        if self._on_terminal_failure is not None:
            self._on_terminal_failure(failure)

    async def _execute(self, request: ExecuteRequest) -> None:
        command = request.command
        if self.pubsub.count(request.session_id) > 0 and not isinstance(
            command, (Ping, Subscribe, Unsubscribe)
        ):
            self._finish_reply(
                request.token,
                Failure(
                    "ERR",
                    "only PING, SUBSCRIBE and UNSUBSCRIBE are allowed "
                    "in subscribed mode",
                ),
            )
            return
        if isinstance(command, Subscribe):
            self._subscribe(request, command)
            return
        if isinstance(command, Unsubscribe):
            self._unsubscribe(request, command)
            return
        if isinstance(command, Publish):
            self._publish(request, command)
            return
        if isinstance(command, Ping) and self.pubsub.count(request.session_id) > 0:
            self._subscribed_ping(request, command)
            return

        now_ms = self.clock.now_ms()
        if isinstance(command, BlPop):
            plan = self.planner.plan_blpop_now(command, self.database, now_ms)
            if plan is None:
                deadline = (
                    None if command.timeout_ms == 0 else now_ms + command.timeout_ms
                )
                waiter = self.waiters.register(
                    request.token,
                    request.session_id,
                    command.keys,
                    deadline,
                )
                if waiter.deadline_ms is not None:
                    waiter.timer = self.scheduler.call_at_ms(
                        waiter.deadline_ms,
                        lambda: self.post_control(
                            TimeoutWaiter(
                                waiter.waiter_id,
                                waiter.generation,
                            )
                        ),
                    )
                    self._on_debug_change()
                return
        else:
            plan = self.planner.plan(command, self.database, now_ms)
            plan = self._attach_push_wakeups(command, plan)
        await self._apply_plan(request, plan, now_ms)

    def _subscribe(self, request: ExecuteRequest, command: Subscribe) -> None:
        endpoint = self._endpoints[request.session_id]
        for channel in command.channels:
            count = self.pubsub.subscribe(request.session_id, channel)
            if not endpoint.offer(SubscriptionAck("subscribe", channel, count)):
                self._finish_request(request.token, TransportClosed())
                return
        self._finish_request(request.token, Replied(None))

    def _unsubscribe(self, request: ExecuteRequest, command: Unsubscribe) -> None:
        endpoint = self._endpoints[request.session_id]
        for channel in self.pubsub.unsubscribe_targets(
            request.session_id,
            command.channels,
        ):
            count = (
                self.pubsub.count(request.session_id)
                if channel is None
                else self.pubsub.unsubscribe(request.session_id, channel)
            )
            if not endpoint.offer(SubscriptionAck("unsubscribe", channel, count)):
                self._finish_request(request.token, TransportClosed())
                return
        self._finish_request(request.token, Replied(None))

    def _publish(self, request: ExecuteRequest, command: Publish) -> None:
        delivered = 0
        for session_id in self.pubsub.subscribers(command.channel):
            endpoint = self._endpoints.get(session_id)
            if endpoint is not None and endpoint.offer(
                PubSubMessage(command.channel, command.payload)
            ):
                delivered += 1
        self._finish_reply(request.token, Number(delivered))

    def _subscribed_ping(self, request: ExecuteRequest, command: Ping) -> None:
        payload = b"" if command.message is None else command.message
        endpoint = self._endpoints[request.session_id]
        if endpoint.offer(PubSubPong(payload)):
            self._finish_request(request.token, Replied(None))
        else:
            self._finish_request(request.token, TransportClosed())

    def _attach_push_wakeups(
        self,
        command: Command,
        plan: ExecutionPlan,
    ) -> ExecutionPlan:
        if not isinstance(command, ListPush) or isinstance(plan.reply, Failure):
            return plan
        operations = list(plan.operations)
        for index, operation in enumerate(operations):
            if (
                isinstance(operation, PutEntry)
                and operation.key == command.key
                and isinstance(operation.entry.value, StoredList)
            ):
                final, wakeups = prepare_list_wakeups(
                    command.key,
                    operation,
                    self.waiters,
                )
                operations[index] = final
                return replace(
                    plan,
                    operations=tuple(operations),
                    waiter_wakeups=wakeups,
                )
        raise AssertionError("successful list push has no target PutEntry")

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

        for wakeup in plan.waiter_wakeups:
            waiter = self.waiters.transition(
                wakeup.waiter_id,
                wakeup.generation,
                WaiterState.FULFILLED,
            )
            if waiter is not None:
                self._finish_reply(
                    waiter.token,
                    Items((Bytes(wakeup.key), Bytes(wakeup.item))),
                )
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
    def worker_task(self) -> asyncio.Task[None] | None:
        return self._worker_task

    @property
    def worker_done(self) -> bool:
        return self._worker_task is None or self._worker_task.done()

    async def join(self) -> None:
        if self._worker_task is not None:
            await asyncio.gather(self._worker_task, return_exceptions=True)

    def fallback_terminalize(self, outcome: RequestOutcome) -> None:
        if not self.worker_done:
            raise RuntimeError(
                "fallback terminalization requires a stopped worker"
            )
        self.mailbox.close_control_admission()
        for waiter in self.waiters.active():
            closed = self.waiters.transition(
                waiter.waiter_id,
                waiter.generation,
                WaiterState.CLOSED,
            )
            if closed is not None:
                self._finish_request(closed.token, outcome)
        self.pubsub.clear()
        for token in tuple(self._requests):
            self._finish_request(token, outcome)
        for endpoint in self._endpoints.values():
            endpoint.outbox.abort("runtime stopped")

    def release_endpoints(self) -> None:
        self._endpoints.clear()
        self._on_debug_change()

    @property
    def debug_failure(self) -> BaseException | None:
        return self._failure

    def debug_applied_batches(self) -> tuple[CommitBatch, ...]:
        return tuple(self._applied_batches)
