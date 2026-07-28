import asyncio
import threading
from collections.abc import Awaitable, Callable
from pathlib import Path

from miniredis.persistence.snapshot import (
    PosixSnapshotFileOps,
    SnapshotFileOps,
)
from miniredis.persistence.aof import AofFileOps, PosixAofFileOps
from miniredis.runtime import MiniRedis, _RuntimeTestHooks


class GateSnapshotFileOps:
    def __init__(self, loop: asyncio.AbstractEventLoop) -> None:
        self.entered = asyncio.Event()
        self.release = threading.Event()
        self._loop = loop
        self._delegate: SnapshotFileOps = PosixSnapshotFileOps()

    def write_atomic(
        self,
        destination: Path,
        temporary: Path,
        data: bytes,
    ) -> None:
        self._loop.call_soon_threadsafe(self.entered.set)
        self.release.wait()
        self._delegate.write_atomic(destination, temporary, data)


class GateAofRewriteOps(PosixAofFileOps):
    def __init__(self, loop: asyncio.AbstractEventLoop) -> None:
        self.entered = asyncio.Event()
        self.release = threading.Event()
        self._loop = loop
        self._rewrite_fd: int | None = None

    def open_rewrite(self, path: Path) -> int:
        fd = super().open_rewrite(path)
        self._rewrite_fd = fd
        return fd

    def write_all(self, fd: int, data: bytes) -> None:
        if fd == self._rewrite_fd:
            self._loop.call_soon_threadsafe(self.entered.set)
            self.release.wait()
        super().write_all(fd, data)


class TestMiniRedis(MiniRedis):
    debug_snapshot_write_entered: asyncio.Event
    debug_snapshot_write_release: threading.Event
    debug_replica_apply_entered: asyncio.Event
    debug_replica_apply_release: asyncio.Event
    debug_aof_rewrite_entered: asyncio.Event
    debug_aof_rewrite_release: threading.Event


async def open_test_runtime(
    *,
    clock=None,
    scheduler=None,
    aof_appender=None,
    config=None,
    snapshot_write_gate: bool = False,
    replica_apply_failure: BaseException | None = None,
    replica_apply_gate: bool = False,
    aof_ops: AofFileOps | None = None,
    aof_sleep: Callable[[float], Awaitable[None]] | None = None,
    aof_rewrite_gate: bool = False,
    replication_id_factory: Callable[[], str] | None = None,
    lifecycle_trace: bool = False,
) -> TestMiniRedis:
    loop = asyncio.get_running_loop()
    snapshot_gate = GateSnapshotFileOps(loop) if snapshot_write_gate else None
    if aof_rewrite_gate and aof_ops is not None:
        raise ValueError("AOF rewrite gate cannot be combined with aof_ops")
    rewrite_gate = GateAofRewriteOps(loop) if aof_rewrite_gate else None
    apply_entered = asyncio.Event() if replica_apply_gate else None
    apply_release = asyncio.Event() if replica_apply_gate else None
    runtime = TestMiniRedis._for_test(
        config=config,
        clock=clock,
        scheduler=scheduler,
        test_hooks=_RuntimeTestHooks(
            aof_appender=aof_appender,
            snapshot_ops=snapshot_gate,
            replica_apply_failure=replica_apply_failure,
            replica_apply_entered=apply_entered,
            replica_apply_release=apply_release,
            aof_ops=rewrite_gate if rewrite_gate is not None else aof_ops,
            aof_sleep=aof_sleep,
            lifecycle_trace=[] if lifecycle_trace else None,
            replication_id_factory=replication_id_factory,
        ),
    )
    if snapshot_gate is not None:
        runtime.debug_snapshot_write_entered = snapshot_gate.entered
        runtime.debug_snapshot_write_release = snapshot_gate.release
    if apply_entered is not None and apply_release is not None:
        runtime.debug_replica_apply_entered = apply_entered
        runtime.debug_replica_apply_release = apply_release
    if rewrite_gate is not None:
        runtime.debug_aof_rewrite_entered = rewrite_gate.entered
        runtime.debug_aof_rewrite_release = rewrite_gate.release
    await runtime.start()
    return runtime
