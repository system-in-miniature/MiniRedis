from __future__ import annotations

import asyncio
import itertools
from bisect import bisect_right
from collections.abc import Callable
from dataclasses import dataclass, replace
from typing import TYPE_CHECKING, Protocol

from miniredis.clock import Clock, TimerScheduler
from miniredis.commands.model import (
    BlPop,
    Command,
    ListPush,
    Ping,
    Publish,
    Subscribe,
    Unsubscribe,
    is_dataset_mutating,
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
    PreparedCommit,
    PutEntry,
    SnapshotImage,
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
    SessionEndpoint,
    SubscriptionAck,
    TransportClosed,
)
from miniredis.core.pubsub import PubSubRegistry
from miniredis.core.reply import Bytes, Failure, Items, Number, Reply
from miniredis.persistence.aof import (
    AofAppendFailed,
    AofAppendOk,
    AofAppendOutcome,
)
from miniredis.replication.sink import ReplicaAttachment

if TYPE_CHECKING:
    from miniredis.replication.sink import ReplicaSink


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


@dataclass(slots=True)
class SnapshotBarrier:
    future: asyncio.Future[SnapshotImage]


@dataclass(slots=True)
class AttachReplica:
    sink: ReplicaSink
    future: asyncio.Future[ReplicaAttachment]


@dataclass(slots=True)
class DetachReplica:
    generation: int
    future: asyncio.Future[bool] | None


@dataclass(slots=True)
class InstallReplicaSnapshot:
    sink: ReplicaSink
    generation: int
    image: SnapshotImage
    future: asyncio.Future[bool]


@dataclass(slots=True)
class ApplyReplicaBatch:
    generation: int
    batch: CommitBatch
    future: asyncio.Future[bool]


@dataclass(frozen=True, slots=True)
class PromotionResult:
    applied_seq: int
    writable: bool


@dataclass(slots=True)
class PromoteReplica:
    generation: int
    future: asyncio.Future[PromotionResult]


@dataclass(frozen=True, slots=True)
class ExecutionPlan:
    reply: Reply | None
    operations: tuple[CommitOperation, ...] = ()
    touch_keys: tuple[bytes, ...] = ()
    trigger: CommitTrigger = CommitTrigger.CLIENT
    waiter_wakeups: tuple[WaiterWakeup, ...] = ()

    @property
    def prepared_commit(self) -> PreparedCommit | None:
        if not self.operations:
            return None
        return PreparedCommit(self.operations, self.trigger)


class CommitBarrier(Protocol):
    async def append(self, batch: CommitBatch) -> AofAppendOutcome:
        raise NotImplementedError


class NullCommitBarrier:
    async def append(self, batch: CommitBatch) -> AofAppendOutcome:
        return AofAppendOk(batch.seq)


class DurabilityFailure(RuntimeError):
    pass


class _InjectedExecutorFailure(RuntimeError):
    pass


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
        on_fatal: Callable[[str], None] | None = None,
        replica_apply_failure: BaseException | None = None,
        replica_apply_entered: asyncio.Event | None = None,
        replica_apply_release: asyncio.Event | None = None,
        allow_failure_injection: bool = False,
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
        self._on_fatal = on_fatal or (lambda _reason: None)
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
        self._replica_sinks: dict[int, ReplicaSink] = {}
        self._next_replica_generation = 1
        self._active_source_generation: int | None = None
        self._replica_read_only = False
        self._replica_apply_failure = replica_apply_failure
        if (replica_apply_entered is None) != (replica_apply_release is None):
            raise ValueError(
                "replica apply gate requires both entered and release events"
            )
        self._replica_apply_entered = replica_apply_entered
        self._replica_apply_release = replica_apply_release
        self._handling_message = False
        self._failure: BaseException | None = None
        self._terminal_cleanup_complete = False
        self._shutdown_barrier_held = False
        self._shutdown_release = asyncio.Event()
        self._stopping = False
        self._started = False
        self._allow_failure_injection = allow_failure_injection

    def install_database_before_start(self, database: Database) -> None:
        if self._started:
            raise RuntimeError("database can only be installed before start")
        self.database = database

    def set_commit_barrier_before_start(
        self,
        commit_barrier: CommitBarrier,
    ) -> None:
        if self._started:
            raise RuntimeError("commit barrier can only be installed before start")
        self.commit_barrier = commit_barrier

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

        token = self.new_request_token()
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

    def new_request_token(self) -> RequestToken:
        return RequestToken(next(self._request_tokens))

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
                    if self._shutdown_barrier_held:
                        await self._shutdown_release.wait()
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
        elif isinstance(message, _InjectedExecutorFailure):
            raise message
        elif isinstance(message, SnapshotBarrier):
            try:
                image = self.database.snapshot_image(self.clock.now_ms())
            except BaseException as exc:
                if not message.future.done():
                    message.future.set_exception(exc)
                self._on_fatal(str(exc) or type(exc).__name__)
            else:
                if not message.future.done():
                    message.future.set_result(image)
        elif isinstance(message, AttachReplica):
            generation = self._next_replica_generation
            self._next_replica_generation += 1
            image = self.database.snapshot_image(self.clock.now_ms())
            attachment = ReplicaAttachment(generation, image)
            message.sink.register_attachment(attachment)
            self._replica_sinks[generation] = message.sink
            message.future.set_result(attachment)
        elif isinstance(message, DetachReplica):
            removed = self._replica_sinks.pop(message.generation, None) is not None
            if message.future is not None and not message.future.done():
                message.future.set_result(removed)
        elif isinstance(message, InstallReplicaSnapshot):
            if not message.sink.install_allowed(message.generation):
                message.future.set_result(False)
                return
            self.database.install_snapshot(
                message.image,
                now_ms=self.clock.now_ms(),
            )
            self._active_source_generation = message.generation
            self._replica_read_only = True
            message.future.set_result(True)
        elif isinstance(message, ApplyReplicaBatch):
            if message.generation != self._active_source_generation:
                message.future.set_result(False)
                return
            try:
                if self._replica_apply_entered is not None:
                    assert self._replica_apply_release is not None
                    self._replica_apply_entered.set()
                    await self._replica_apply_release.wait()
                if self._replica_apply_failure is not None:
                    error = self._replica_apply_failure
                    self._replica_apply_failure = None
                    raise error
                self.database.apply_batch(message.batch, track_access=False)
            except BaseException as exc:
                message.future.set_exception(exc)
            else:
                message.future.set_result(True)
        elif isinstance(message, PromoteReplica):
            if message.generation != self._active_source_generation:
                message.future.set_result(
                    PromotionResult(self.database.commit_seq, False)
                )
                return
            self._active_source_generation = None
            self._replica_read_only = False
            message.future.set_result(PromotionResult(self.database.commit_seq, True))
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
        self._replica_sinks.clear()
        for token in tuple(self._requests):
            self._finish_request(token, event.outcome)
        if not event.completion.done():
            event.completion.set_result(None)
        self._shutdown_barrier_held = True

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
        if self._replica_read_only and is_dataset_mutating(command):
            self._finish_reply(
                request.token,
                Failure("READONLY", "replica is read only"),
            )
            return
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
        plan = self._attach_push_wakeups(request.command, plan)
        try:
            if plan.prepared_commit is not None:
                await self._commit_prepared(plan.prepared_commit)
        except DurabilityFailure as exc:
            self._finish_reply(
                request.token,
                Failure("ERR", f"durability failure: {exc}"),
            )
            self._on_fatal(str(exc))
            return

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

    async def _commit_prepared(
        self,
        prepared: PreparedCommit,
    ) -> CommitBatch:
        batch = prepared.to_batch(self.database.commit_seq + 1)
        outcome = await self.commit_barrier.append(batch)
        if isinstance(outcome, AofAppendFailed):
            raise DurabilityFailure(outcome.message)
        if outcome != AofAppendOk(batch.seq):
            raise DurabilityFailure("AOF acknowledged the wrong sequence")

        self.database.apply_batch(
            batch,
            track_access=prepared.trigger is CommitTrigger.CLIENT,
        )
        self._applied_batches.append(batch)
        self._offer_replica_batch(batch)
        return batch

    def _offer_replica_batch(self, batch: CommitBatch) -> None:
        for generation, sink in tuple(self._replica_sinks.items()):
            if not sink.offer(batch):
                self._replica_sinks.pop(generation, None)

    async def active_expire_once(self) -> int:
        if self._worker_task is None or self._stopping:
            return 0
        future: asyncio.Future[int] = asyncio.get_running_loop().create_future()
        tick = ActiveExpireTick(self.clock.now_ms(), future)
        if not self.post_control(tick):
            return 0
        return await asyncio.shield(future)

    async def capture_snapshot(self) -> SnapshotImage:
        future: asyncio.Future[SnapshotImage] = (
            asyncio.get_running_loop().create_future()
        )
        if not self.post_control(SnapshotBarrier(future)):
            raise RuntimeError("executor control admission is closed")
        return await asyncio.shield(future)

    async def attach_replica(self, sink: ReplicaSink) -> ReplicaAttachment:
        future: asyncio.Future[ReplicaAttachment] = (
            asyncio.get_running_loop().create_future()
        )
        if not self.post_control(AttachReplica(sink, future)):
            raise RuntimeError("executor control admission is closed")
        return await asyncio.shield(future)

    async def detach_replica(self, generation: int) -> bool:
        future: asyncio.Future[bool] = asyncio.get_running_loop().create_future()
        if not self.post_control(DetachReplica(generation, future)):
            return False
        return await asyncio.shield(future)

    async def install_replica_snapshot(
        self,
        sink: ReplicaSink,
        generation: int,
        image: SnapshotImage,
    ) -> bool:
        future: asyncio.Future[bool] = asyncio.get_running_loop().create_future()
        if not self.post_control(
            InstallReplicaSnapshot(sink, generation, image, future)
        ):
            return False
        return await asyncio.shield(future)

    async def apply_replica_batch(
        self,
        generation: int,
        batch: CommitBatch,
    ) -> bool:
        future: asyncio.Future[bool] = asyncio.get_running_loop().create_future()
        if not self.post_control(ApplyReplicaBatch(generation, batch, future)):
            return False
        return await asyncio.shield(future)

    async def promote_replica(self, generation: int) -> PromotionResult:
        future: asyncio.Future[PromotionResult] = (
            asyncio.get_running_loop().create_future()
        )
        if not self.post_control(PromoteReplica(generation, future)):
            raise RuntimeError("executor control admission is closed")
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
        try:
            prepared = PreparedCommit(
                operations,
                CommitTrigger.ACTIVE_EXPIRE,
            )
            await self._commit_prepared(prepared)
        except DurabilityFailure as exc:
            self._on_fatal(str(exc))
            return 0
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

    async def stop_after_shutdown_barrier(self) -> None:
        self._stopping = True
        self._shutdown_release.set()
        await self.join()

    def debug_fail(self, failure: BaseException) -> None:
        if not self._allow_failure_injection:
            raise RuntimeError("executor failure injection is test-only")
        if not self.post_control(_InjectedExecutorFailure(str(failure))):
            raise RuntimeError("executor control admission is closed")

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
    def replica_link_count(self) -> int:
        return len(self._replica_sinks)

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
            raise RuntimeError("fallback terminalization requires a stopped worker")
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
