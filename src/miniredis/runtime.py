from __future__ import annotations

import asyncio
import itertools
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from enum import Enum
from typing import Any, Self

from miniredis.adapters.direct import DirectClient, DirectPipeline
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
    ActiveExpireTick,
    AbandonRequest,
    BeginShutdown,
    CommandExecutor,
    CommitBarrier,
    NullCommitBarrier,
    SessionClosed,
    SubmittedRequest,
)
from miniredis.core.expiration import ActiveExpireProducer
from miniredis.core.outbound import (
    ReplyMessage,
    RequestOutcome,
    RequestToken,
    RuntimeClosed,
    RuntimeFailed,
    SessionEndpoint,
)
from miniredis.core.planner import CommandPlanner
from miniredis.core.reply import Failure
from miniredis.persistence.aof import AofFileOps, AofWriter
from miniredis.persistence.recovery import recover_database
from miniredis.persistence.snapshot import (
    SnapshotFailed,
    SnapshotFileOps,
    SnapshotManager,
    SnapshotOutcome,
)
from miniredis.replication.sink import ReplicaSink, ReplicaStatus


class RuntimeState(str, Enum):
    STARTING = "starting"
    RUNNING = "running"
    DRAINING = "draining"
    CLOSED = "closed"
    FAILED = "failed"


@dataclass(frozen=True, slots=True)
class RuntimeStats:
    accepted_requests: int
    pending_futures: int
    waiters: int
    subscriptions: int
    sessions: int
    timer_handles: int
    owned_tasks: int
    replica_links: int
    accepting_users: bool
    snapshot_jobs: int
    aof_tasks: int
    control_producers: int
    executor_tasks: int
    replica_tasks: int
    tcp_servers: int
    tcp_sessions: int
    tcp_tasks: int


@dataclass(slots=True)
class _RuntimeTestHooks:
    aof_appender: CommitBarrier | None = None
    snapshot_ops: SnapshotFileOps | None = None
    replica_apply_failure: BaseException | None = None
    replica_apply_entered: asyncio.Event | None = None
    replica_apply_release: asyncio.Event | None = None
    aof_ops: AofFileOps | None = None
    aof_sleep: Callable[[float], Awaitable[None]] | None = None
    lifecycle_trace: list[str] | None = None


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
        test_hooks: _RuntimeTestHooks | None = None,
    ) -> None:
        self.config = config
        self.clock = clock
        self.scheduler = (
            AsyncioTimerScheduler(clock) if scheduler is None else scheduler
        )
        self._test_hooks = test_hooks
        actual_barrier = (
            test_hooks.aof_appender
            if test_hooks is not None and test_hooks.aof_appender is not None
            else commit_barrier
        )
        self.commit_barrier = actual_barrier
        self._aof_writer: AofWriter | None = None
        self.database = Database()
        self.planner = CommandPlanner(config)
        self._debug_changed = asyncio.Event()
        self.executor = CommandExecutor(
            database=self.database,
            planner=self.planner,
            clock=clock,
            commit_barrier=actual_barrier,
            max_pending_commands=config.max_pending_commands,
            active_expire_sample_size=config.active_expire_sample_size,
            scheduler=self.scheduler,
            on_debug_change=self._debug_notify,
            on_terminal_failure=self._on_executor_terminal_failure,
            on_fatal=self._transition_failed,
            replica_apply_failure=(
                None
                if self._test_hooks is None
                else self._test_hooks.replica_apply_failure
            ),
            replica_apply_entered=(
                None
                if self._test_hooks is None
                else self._test_hooks.replica_apply_entered
            ),
            replica_apply_release=(
                None
                if self._test_hooks is None
                else self._test_hooks.replica_apply_release
            ),
            allow_failure_injection=self._test_hooks is not None,
        )
        self.executor.mailbox.close_user_admission()
        self._snapshot_manager = (
            SnapshotManager(
                config.snapshot_path,
                self.executor.capture_snapshot,
                ops=(
                    None if self._test_hooks is None else self._test_hooks.snapshot_ops
                ),
            )
            if config.snapshot_path is not None
            else None
        )
        self.state = RuntimeState.STARTING
        self._session_ids = itertools.count(1)
        self._start_task: asyncio.Task[None] | None = None
        self._lifecycle_lock = asyncio.Lock()
        self._shutdown_task: asyncio.Task[None] | None = None
        self._control_producers: set[object] = set()
        self._owned_tasks: set[asyncio.Task[object]] = set()
        self._failure_reason: str | None = None
        self._shutdown_complete = False
        self._owned_replica_sinks: set[ReplicaSink] = set()
        self._tcp_servers: set[Any] = set()

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
            test_hooks=None,
        )

    @classmethod
    def _for_test(
        cls,
        config: MiniRedisConfig | None = None,
        *,
        clock: Clock | None = None,
        scheduler: TimerScheduler | None = None,
        commit_barrier: CommitBarrier | None = None,
        test_hooks: _RuntimeTestHooks | None = None,
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
            test_hooks=test_hooks,
        )

    async def start(self) -> None:
        if self.state is RuntimeState.RUNNING:
            return
        if self.state in {
            RuntimeState.DRAINING,
            RuntimeState.CLOSED,
            RuntimeState.FAILED,
        }:
            raise RuntimeError("runtime is closed")
        if self._start_task is None:
            self._start_task = asyncio.create_task(
                self._start_owned(), name="miniredis:runtime-start"
            )
        await asyncio.shield(self._start_task)

    async def _start_owned(self) -> None:
        async with self._lifecycle_lock:
            if self.state is not RuntimeState.STARTING:
                return
            try:
                recovered = await asyncio.to_thread(
                    recover_database,
                    snapshot_path=self.config.snapshot_path,
                    aof_path=self.config.aof_path,
                    now_ms=self.clock.now_ms(),
                    repair_truncated_tail=self.config.aof_repair_truncated_tail,
                )
                self.database = recovered
                self.executor.install_database_before_start(recovered)
                if self.config.aof_path is not None:
                    hooks = self._test_hooks
                    self._aof_writer = AofWriter(
                        self.config.aof_path,
                        self.config.aof_policy,
                        fsync_interval_seconds=(self.config.aof_fsync_interval_seconds),
                        ops=None if hooks is None else hooks.aof_ops,
                        sleep=(
                            asyncio.sleep
                            if hooks is None or hooks.aof_sleep is None
                            else hooks.aof_sleep
                        ),
                        on_failure=self._aof_worker_failed,
                    )
                    await self._aof_writer.start()
                    self.commit_barrier = self._aof_writer
                    self.executor.set_commit_barrier_before_start(self._aof_writer)
                await self.executor.start()
                if self.state is not RuntimeState.STARTING:
                    return
                worker = self.executor.worker_task
                if worker is None:
                    raise RuntimeError("executor did not create its worker")
                self._track_owned_task(worker)
                worker.add_done_callback(self._executor_stopped)
                producer = ActiveExpireProducer(
                    self.clock,
                    self.scheduler,
                    self.config.active_expire_interval_ms,
                    self.executor.post_control,
                    lambda now_ms: ActiveExpireTick(now_ms, None),
                )
                self._control_producers.add(producer)
                producer.start()
                self.executor.mailbox.open_user_admission()
                self._set_state(RuntimeState.RUNNING)
            except BaseException as exc:
                self._failure_reason = str(exc) or type(exc).__name__
                self._set_state(RuntimeState.FAILED)
                self.executor.mailbox.close_user_admission()
                await self._cleanup_failed_start()
                raise

    async def _cleanup_failed_start(self) -> None:
        outcome = RuntimeFailed(self._failure_reason or "startup failed")
        if self.executor.worker_done:
            self.executor.fallback_terminalize(outcome)
        else:
            completion = asyncio.get_running_loop().create_future()
            if self.executor.post_control(BeginShutdown(outcome, completion)):
                worker = self.executor.worker_task
                assert worker is not None
                await asyncio.wait(
                    (completion, worker),
                    return_when=asyncio.FIRST_COMPLETED,
                )
                if completion.done():
                    await self.executor.stop_after_shutdown_barrier()
                else:
                    self.executor.fallback_terminalize(outcome)
            else:
                await self.executor.join()
                self.executor.fallback_terminalize(outcome)
        self.executor.release_endpoints()
        if self._snapshot_manager is not None:
            await self._snapshot_manager.close()
        if self._aof_writer is not None:
            await self._aof_writer.close()
        self._control_producers.clear()

    def _aof_worker_failed(self, failure: BaseException) -> None:
        self._transition_failed(str(failure) or type(failure).__name__)

    def parse(self, request: CommandRequest) -> Command | Failure:
        try:
            return parse_command_request(request)
        except CommandParseError as error:
            return Failure("ERR", str(error))

    def direct_client(self) -> DirectClient:
        if self.state in {
            RuntimeState.DRAINING,
            RuntimeState.CLOSED,
            RuntimeState.FAILED,
        }:
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

    def direct_pipeline(self) -> DirectPipeline:
        return DirectPipeline(self.direct_client())

    def _session_became_slow(self, session_id: int, reason: str) -> None:
        endpoint = self.executor.endpoint(session_id)
        if endpoint is not None:
            endpoint.request_transport_close(reason)
        self.executor.post_control(SessionClosed(session_id))

    def new_session_id(self) -> int:
        return next(self._session_ids)

    def register_session(self, endpoint: SessionEndpoint) -> None:
        self.executor.register_endpoint(endpoint)

    def request_session_close(self, session_id: int) -> None:
        self.executor.post_control(SessionClosed(session_id))

    async def close_session(self, session_id: int) -> None:
        endpoint = self.executor.endpoint(session_id)
        if endpoint is None:
            return
        completion = asyncio.get_running_loop().create_future()
        if not self.executor.post_control(SessionClosed(session_id, completion)):
            endpoint.outbox.abort("runtime closed")
            return
        await asyncio.shield(completion)

    def session_became_slow(
        self,
        session_id: int,
        reason: str,
    ) -> None:
        self._session_became_slow(session_id, reason)

    async def execute_for_session(
        self,
        session_id: int,
        request: CommandRequest,
    ) -> None:
        submitted = self.submit_request(session_id, request)
        endpoint = self.executor.endpoint(session_id)
        if endpoint is None:
            return
        if isinstance(submitted, Failure):
            token = self.executor.new_request_token()
            endpoint.offer(ReplyMessage(token, submitted))
            return
        await self.wait_for_session_submission(submitted)

    def submit_request(
        self,
        session_id: int,
        request: CommandRequest,
    ) -> SubmittedRequest | Failure:
        parsed = self.parse(request)
        if isinstance(parsed, Failure):
            return self.executor.submit_rejection(session_id, parsed)
        return self.executor.submit(session_id, parsed)

    async def wait_for_session_submission(
        self,
        submitted: SubmittedRequest,
    ) -> None:
        try:
            await asyncio.shield(submitted.future)
        except asyncio.CancelledError:
            self.executor.post_control(AbandonRequest(submitted.token))
            raise

    async def start_tcp(self, host: str, port: int) -> Any:
        from miniredis.adapters.tcp import TcpServer

        if self.state is not RuntimeState.RUNNING:
            raise RuntimeError("runtime is not running")
        server = TcpServer(self, host, port, self.config.outbox_limit)
        await server.start()
        self._tcp_servers.add(server)
        self._control_producers.add(server)
        return server

    def unregister_tcp_server(self, server: Any) -> None:
        self._tcp_servers.discard(server)
        self._control_producers.discard(server)

    async def close(self) -> None:
        async with self._lifecycle_lock:
            if self._shutdown_task is None:
                self._shutdown_task = asyncio.create_task(
                    self._shutdown_once(),
                    name="miniredis:shutdown",
                )
                self._track_owned_task(self._shutdown_task)
            task = self._shutdown_task
        await asyncio.shield(task)
        if self.state is RuntimeState.FAILED:
            self._set_state(RuntimeState.CLOSED)

    async def _shutdown_once(self, crash: bool = False) -> None:
        if self._shutdown_complete:
            return
        failure = self._failure_reason
        preserve_failed_state = self.state is RuntimeState.FAILED
        if self.state not in {RuntimeState.CLOSED, RuntimeState.FAILED}:
            self._set_state(RuntimeState.DRAINING)
        self.executor.mailbox.close_user_admission()
        await asyncio.gather(
            *(
                producer.quiesce()  # type: ignore[attr-defined]
                for producer in tuple(self._control_producers)
            )
        )
        outcome: RequestOutcome = (
            RuntimeFailed(failure) if failure is not None else RuntimeClosed()
        )
        if self.executor.worker_done:
            self.executor.fallback_terminalize(outcome)
        else:
            completion = asyncio.get_running_loop().create_future()
            if self.executor.post_control(BeginShutdown(outcome, completion)):
                worker = self.executor.worker_task
                assert worker is not None
                await asyncio.wait(
                    (completion, worker),
                    return_when=asyncio.FIRST_COMPLETED,
                )
                if not completion.done():
                    self.executor.fallback_terminalize(outcome)
            else:
                await self.executor.join()
                self.executor.fallback_terminalize(outcome)

        endpoints = self.executor.endpoints()
        for endpoint in endpoints:
            endpoint.outbox.begin_close("runtime closed")
        drainers = (
            [endpoint.outbox.wait_empty() for endpoint in endpoints]
            if not crash
            else []
        )
        if drainers:
            try:
                async with asyncio.timeout(self.config.outbox_drain_grace_ms / 1000):
                    await asyncio.gather(*drainers)
            except TimeoutError:
                pass
        for endpoint in endpoints:
            endpoint.outbox.abort("runtime closed")
            endpoint.request_transport_close("runtime closed")
        await asyncio.gather(
            *(server.finish_runtime_close() for server in tuple(self._tcp_servers)),
            return_exceptions=False,
        )
        self._tcp_servers.clear()
        self.executor.release_endpoints()

        if self._snapshot_manager is not None:
            await self._snapshot_manager.close()
        self._trace_lifecycle("snapshot-job-done")

        if self._aof_writer is not None:
            if crash:
                await asyncio.shield(self._aof_writer.crash_close())
            else:
                await asyncio.shield(self._aof_writer.close())
        self._trace_lifecycle("aof-closed")

        for sink in tuple(self._owned_replica_sinks):
            if crash:
                await sink.source_crashed()
            else:
                await sink.drain_and_stop(self.config.replica_drain_grace_ms)
        self._owned_replica_sinks.clear()
        self._trace_lifecycle("replicas-stopped")

        if self.executor.worker_done:
            await self.executor.join()
        else:
            await self.executor.stop_after_shutdown_barrier()
        self._trace_lifecycle("executor-stopped")

        self._control_producers.clear()
        current = asyncio.current_task()
        for owned in tuple(self._owned_tasks):
            if owned.done() or owned is current:
                self._owned_tasks.discard(owned)
        self._shutdown_complete = True
        self._set_state(
            RuntimeState.FAILED if preserve_failed_state else RuntimeState.CLOSED
        )

    def _on_executor_terminal_failure(self, failure: BaseException) -> None:
        reason = str(failure) or type(failure).__name__
        self._failure_reason = reason
        self._set_state(RuntimeState.CLOSED)
        self.executor.mailbox.close_user_admission()
        if self._shutdown_task is None:
            self._shutdown_task = asyncio.create_task(
                self._shutdown_once(),
                name="miniredis:failed-shutdown",
            )
            self._track_owned_task(self._shutdown_task)

    def _executor_stopped(self, task: asyncio.Task[None]) -> None:
        if self.state in {
            RuntimeState.DRAINING,
            RuntimeState.CLOSED,
            RuntimeState.FAILED,
        }:
            return
        if task.cancelled():
            reason = "executor worker cancelled"
        else:
            error = task.exception()
            reason = (
                "executor worker stopped"
                if error is None
                else f"executor worker failed: {error}"
            )
        self._transition_failed(reason)

    def _transition_failed(self, reason: str) -> None:
        if self.state in {RuntimeState.DRAINING, RuntimeState.CLOSED}:
            return
        was_starting = self.state is RuntimeState.STARTING
        self._failure_reason = reason
        self._set_state(RuntimeState.FAILED)
        self.executor.mailbox.close_user_admission()
        if was_starting:
            return
        if self._shutdown_task is None:
            self._shutdown_task = asyncio.create_task(
                self._shutdown_once(),
                name="miniredis:failed-shutdown",
            )
            self._track_owned_task(self._shutdown_task)

    def _trace_lifecycle(self, event: str) -> None:
        if (
            self._test_hooks is not None
            and self._test_hooks.lifecycle_trace is not None
        ):
            self._test_hooks.lifecycle_trace.append(event)

    def _set_state(self, state: RuntimeState) -> None:
        self.state = state
        self._debug_notify()

    def _track_owned_task(self, task: asyncio.Task[object]) -> None:
        self._owned_tasks.add(task)
        task.add_done_callback(self._owned_tasks.discard)

    async def attach_replica(self, sink: ReplicaSink) -> ReplicaStatus:
        if self.state is not RuntimeState.RUNNING:
            raise RuntimeError("primary is not running")
        self._owned_replica_sinks.add(sink)
        task = asyncio.create_task(
            sink.attach(self),
            name="miniredis:replica-attach",
        )
        task.add_done_callback(
            lambda completed: self._replica_attach_done(sink, completed)
        )
        try:
            return await asyncio.shield(task)
        except BaseException:
            if task.done() and (task.cancelled() or task.exception() is not None):
                self._owned_replica_sinks.discard(sink)
            raise

    def _replica_attach_done(
        self,
        sink: ReplicaSink,
        task: asyncio.Task[ReplicaStatus],
    ) -> None:
        if task.cancelled() or task.exception() is not None:
            self._owned_replica_sinks.discard(sink)

    def _release_replica_sink(self, sink: ReplicaSink) -> None:
        self._owned_replica_sinks.discard(sink)

    async def simulate_crash(self) -> None:
        async with self._lifecycle_lock:
            if self._shutdown_task is None:
                self._shutdown_task = asyncio.create_task(
                    self._shutdown_once(crash=True),
                    name="miniredis:simulated-crash",
                )
                self._track_owned_task(self._shutdown_task)
            task = self._shutdown_task
        await asyncio.shield(task)

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

    @property
    def closed(self) -> bool:
        return self.state is RuntimeState.CLOSED

    @property
    def accepting_commands(self) -> bool:
        return (
            self.state is RuntimeState.RUNNING and self.executor.mailbox.accepting_users
        )

    @property
    def normal_shutdown_started(self) -> bool:
        return (
            self._failure_reason is None
            and self.executor.debug_failure is None
            and self.state in {RuntimeState.DRAINING, RuntimeState.CLOSED}
        )

    async def debug_active_expire_once(self) -> int:
        if self.state is not RuntimeState.RUNNING:
            return 0
        return await self.executor.active_expire_once()

    async def save_snapshot(self) -> SnapshotOutcome:
        if self.state is not RuntimeState.RUNNING:
            return SnapshotFailed("runtime is not running")
        if self._snapshot_manager is None:
            return SnapshotFailed("snapshot_path is not configured")
        return await self._snapshot_manager.save()

    def debug_applied_batches(self) -> tuple[CommitBatch, ...]:
        return self.executor.debug_applied_batches()

    def debug_logical_items(self) -> tuple[tuple[bytes, StoredEntry], ...]:
        return self.database.export_stored_entries(self.clock.now_ms())

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
        servers = tuple(self._tcp_servers)
        sinks = tuple(self._owned_replica_sinks)
        worker = self.executor.worker_task
        return RuntimeStats(
            accepted_requests=self.executor.accepted_request_count,
            pending_futures=self.executor.pending_request_count,
            waiters=self.executor.waiters.active_count,
            subscriptions=self.executor.pubsub.membership_count,
            sessions=self.executor.endpoint_count,
            timer_handles=self.executor.waiters.timer_count,
            owned_tasks=sum(
                not task.done()
                for task in self._owned_tasks
                if task is not asyncio.current_task()
            ),
            replica_links=self.executor.replica_link_count,
            accepting_users=self.executor.mailbox.accepting_users,
            snapshot_jobs=int(
                self._snapshot_manager is not None
                and self._snapshot_manager.active_job is not None
            ),
            aof_tasks=(
                0 if self._aof_writer is None else self._aof_writer.owned_task_count
            ),
            control_producers=len(self._control_producers),
            executor_tasks=int(worker is not None and not worker.done()),
            replica_tasks=sum(sink.owned_task_count for sink in sinks),
            tcp_servers=len(servers),
            tcp_sessions=sum(server.session_count for server in servers),
            tcp_tasks=sum(server.owned_task_count for server in servers),
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

    async def debug_wait_for_state(self, value: str) -> None:
        await self._debug_wait(lambda: self.state.value == value)

    async def debug_wait_for_failure(self) -> None:
        await self._debug_wait(lambda: self._failure_reason is not None)

    def debug_fail_executor(self, failure: BaseException) -> None:
        self.executor.debug_fail(failure)

    def debug_lifecycle_trace(self) -> tuple[str, ...]:
        if self._test_hooks is None or self._test_hooks.lifecycle_trace is None:
            return ()
        return tuple(self._test_hooks.lifecycle_trace)

    def debug_register_control_producer(self, producer: object) -> None:
        if self.state is not RuntimeState.RUNNING:
            raise RuntimeError("control producers register only while running")
        self._control_producers.add(producer)
        self._debug_notify()

    def debug_waiter_ids(self, key: bytes) -> tuple[WaiterId, ...]:
        return self.executor.waiters.ids_for_key(key)

    @property
    def debug_waiter_index_counts(self) -> tuple[int, int, int]:
        return self.executor.waiters.index_counts
