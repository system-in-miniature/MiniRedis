import asyncio
import os

import pytest

from miniredis.core.commit import SnapshotImage
from miniredis.persistence.snapshot import (
    SnapshotBusy,
    SnapshotFailed,
    SnapshotManager,
    SnapshotSaved,
)


@pytest.mark.asyncio
async def test_second_save_is_busy_before_another_capture_starts(tmp_path):
    capture_entered = asyncio.Event()
    release_capture = asyncio.Event()
    captures = 0

    async def capture() -> SnapshotImage:
        nonlocal captures
        captures += 1
        capture_entered.set()
        await release_capture.wait()
        return SnapshotImage(0, ())

    manager = SnapshotManager(tmp_path / "dump.mrsnap", capture)
    first = asyncio.create_task(manager.save())
    await capture_entered.wait()

    assert await manager.save() == SnapshotBusy()
    assert captures == 1

    release_capture.set()
    assert await first == SnapshotSaved(tmp_path / "dump.mrsnap", 0)
    await manager.close()


@pytest.mark.asyncio
async def test_cancelled_save_caller_does_not_cancel_owned_job(tmp_path):
    capture_entered = asyncio.Event()
    release_capture = asyncio.Event()

    async def capture() -> SnapshotImage:
        capture_entered.set()
        await release_capture.wait()
        return SnapshotImage(3, ())

    manager = SnapshotManager(tmp_path / "dump.mrsnap", capture)
    caller = asyncio.create_task(manager.save())
    await capture_entered.wait()
    caller.cancel()
    with pytest.raises(asyncio.CancelledError):
        await caller

    release_capture.set()
    await manager.close()

    assert (tmp_path / "dump.mrsnap").exists()
    assert manager.active_job is None


@pytest.mark.asyncio
async def test_failure_before_replace_preserves_last_snapshot(
    tmp_path,
    monkeypatch,
):
    destination = tmp_path / "dump.mrsnap"
    destination.write_bytes(b"last-good")
    real_replace = os.replace

    def fail_replace(source, target):
        assert target == destination
        raise OSError("replace denied")

    monkeypatch.setattr(os, "replace", fail_replace)

    async def capture() -> SnapshotImage:
        return SnapshotImage(4, ())

    manager = SnapshotManager(destination, capture)
    outcome = await manager.save()

    assert outcome == SnapshotFailed("replace denied")
    assert destination.read_bytes() == b"last-good"
    assert tuple(tmp_path.glob(".dump.mrsnap.tmp.*")) == ()
    monkeypatch.setattr(os, "replace", real_replace)
    await manager.close()
