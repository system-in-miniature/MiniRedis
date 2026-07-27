import asyncio
import threading
from pathlib import Path

from miniredis.persistence.snapshot import (
    PosixSnapshotFileOps,
    SnapshotFileOps,
)
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


async def open_test_runtime(
    *,
    clock=None,
    scheduler=None,
    aof_appender=None,
    config=None,
    snapshot_write_gate: bool = False,
    replica_apply_failure: BaseException | None = None,
) -> TestMiniRedis:
    loop = asyncio.get_running_loop()
    snapshot_gate = GateSnapshotFileOps(loop) if snapshot_write_gate else None
    runtime = TestMiniRedis._for_test(
        config=config,
        clock=clock,
        scheduler=scheduler,
        test_hooks=_RuntimeTestHooks(
            aof_appender=aof_appender,
            snapshot_ops=snapshot_gate,
            replica_apply_failure=replica_apply_failure,
        ),
    )
    if snapshot_gate is not None:
        runtime.debug_snapshot_write_entered = snapshot_gate.entered
        runtime.debug_snapshot_write_release = snapshot_gate.release
    await runtime.start()
    return runtime
