import asyncio
import threading
from collections.abc import Awaitable, Callable
from pathlib import Path

from miniredis.persistence.snapshot import (
    PosixSnapshotFileOps,
    SnapshotFileOps,
)
from miniredis.persistence.aof import AofFileOps
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


class TestMiniRedis(MiniRedis):
    debug_snapshot_write_entered: asyncio.Event
    debug_snapshot_write_release: threading.Event
    debug_replica_apply_entered: asyncio.Event
    debug_replica_apply_release: asyncio.Event


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
    lifecycle_trace: bool = False,
) -> TestMiniRedis:
    loop = asyncio.get_running_loop()
    snapshot_gate = GateSnapshotFileOps(loop) if snapshot_write_gate else None
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
            aof_ops=aof_ops,
            aof_sleep=aof_sleep,
            lifecycle_trace=[] if lifecycle_trace else None,
        ),
    )
    if snapshot_gate is not None:
        runtime.debug_snapshot_write_entered = snapshot_gate.entered
        runtime.debug_snapshot_write_release = snapshot_gate.release
    if apply_entered is not None and apply_release is not None:
        runtime.debug_replica_apply_entered = apply_entered
        runtime.debug_replica_apply_release = apply_release
    await runtime.start()
    return runtime
