import asyncio

import pytest

from miniredis import CommandRequest
from miniredis.config import MiniRedisConfig
from miniredis.core.reply import Ok
from miniredis.persistence.codec import decode_snapshot_file
from miniredis.persistence.snapshot import SnapshotSaved
from tests.helpers.time import FakeClock
from tests.helpers.runtime import open_test_runtime


@pytest.mark.asyncio
async def test_snapshot_barrier_captures_seq_and_only_logically_live_keys(
    tmp_path,
):
    clock = FakeClock(now_ms=1000)
    path = tmp_path / "dump.mrsnap"
    runtime = await open_test_runtime(
        clock=clock,
        config=MiniRedisConfig(snapshot_path=path),
    )
    client = runtime.direct_client()
    await client.execute(CommandRequest(b"SET", (b"live", b"1")))
    await client.execute(
        CommandRequest(b"SET", (b"expired", b"2", b"PX", b"5"))
    )
    clock.advance(5)

    outcome = await runtime.save_snapshot()
    image = decode_snapshot_file(path.read_bytes())

    assert outcome == SnapshotSaved(path, 2)
    assert image.checkpoint_seq == 2
    assert tuple(key for key, _entry in image.entries) == (b"live",)
    await runtime.close()


@pytest.mark.asyncio
async def test_commands_continue_after_capture_while_file_write_is_blocked(
    tmp_path,
):
    runtime = await open_test_runtime(
        config=MiniRedisConfig(snapshot_path=tmp_path / "dump.mrsnap"),
        snapshot_write_gate=True,
    )
    client = runtime.direct_client()
    await client.execute(CommandRequest(b"SET", (b"before", b"1")))
    write_entered = runtime.debug_snapshot_write_entered
    release_write = runtime.debug_snapshot_write_release

    save = asyncio.create_task(runtime.save_snapshot())
    await write_entered.wait()
    reply = await client.execute(
        CommandRequest(b"SET", (b"after", b"2"))
    )

    assert reply == Ok()
    assert runtime.debug_commit_seq == 2
    release_write.set()
    outcome = await save
    assert outcome.checkpoint_seq == 1
    await runtime.close()
