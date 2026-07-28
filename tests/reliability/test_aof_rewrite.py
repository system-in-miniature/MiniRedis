import asyncio

import pytest

from miniredis import CommandRequest, MiniRedis
from miniredis.config import MiniRedisConfig
from miniredis.core.reply import Bytes, Items, Ok
from miniredis.persistence.aof import (
    AofPolicy,
    AofRewriteBusy,
    AofRewriteFailed,
    AofRewriteSaved,
    load_aof,
)
from tests.helpers.runtime import open_test_runtime


@pytest.mark.asyncio
async def test_write_during_paused_base_survives_rewrite_and_restart(
    tmp_path,
):
    config = MiniRedisConfig(
        aof_path=tmp_path / "appendonly.mraof",
        aof_policy=AofPolicy.ALWAYS,
    )
    runtime = await open_test_runtime(
        config=config,
        aof_rewrite_gate=True,
    )
    client = runtime.direct_client()
    assert await client.execute(
        CommandRequest(b"SET", (b"before", b"1"))
    ) == Ok()

    rewriting = asyncio.create_task(runtime.rewrite_aof())
    await runtime.debug_aof_rewrite_entered.wait()
    assert await client.execute(
        CommandRequest(b"SET", (b"during", b"2"))
    ) == Ok()
    stats = runtime.debug_stats()
    assert stats.aof_rewrite_active is True
    assert stats.aof_rewrite_delta_bytes > 0
    assert stats.aof_rewrite_checkpoint_seq == 1

    runtime.debug_aof_rewrite_release.set()
    assert isinstance(await rewriting, AofRewriteSaved)
    await runtime.close()

    recovered = MiniRedis.open(config)
    await recovered.start()
    assert await recovered.direct_client().execute(
        CommandRequest(b"MGET", (b"before", b"during"))
    ) == Items((Bytes(b"1"), Bytes(b"2")))
    await recovered.close()


@pytest.mark.asyncio
async def test_runtime_reports_busy_for_concurrent_rewrite(tmp_path):
    runtime = await open_test_runtime(
        config=MiniRedisConfig(
            aof_path=tmp_path / "appendonly.mraof",
            aof_policy=AofPolicy.ALWAYS,
        ),
        aof_rewrite_gate=True,
    )
    first = asyncio.create_task(runtime.rewrite_aof())
    await runtime.debug_aof_rewrite_entered.wait()

    assert await runtime.rewrite_aof() == AofRewriteBusy()

    runtime.debug_aof_rewrite_release.set()
    assert isinstance(await first, AofRewriteSaved)
    await runtime.close()


@pytest.mark.asyncio
async def test_runtime_rewrite_without_aof_is_disabled():
    runtime = await open_test_runtime()

    assert await runtime.rewrite_aof() == AofRewriteFailed(
        "aof_path is not configured"
    )

    await runtime.close()


@pytest.mark.asyncio
async def test_rewrite_delta_overflow_preserves_old_aof_history(tmp_path):
    config = MiniRedisConfig(
        aof_path=tmp_path / "appendonly.mraof",
        aof_policy=AofPolicy.ALWAYS,
        aof_rewrite_delta_limit_bytes=1,
    )
    runtime = await open_test_runtime(
        config=config,
        aof_rewrite_gate=True,
    )
    client = runtime.direct_client()
    assert await client.execute(
        CommandRequest(b"SET", (b"before", b"1"))
    ) == Ok()
    rewriting = asyncio.create_task(runtime.rewrite_aof())
    await runtime.debug_aof_rewrite_entered.wait()

    assert await client.execute(
        CommandRequest(b"SET", (b"during", b"2"))
    ) == Ok()
    assert await rewriting == AofRewriteFailed(
        "AOF rewrite delta limit exceeded"
    )
    runtime.debug_aof_rewrite_release.set()
    await runtime.close()

    recovered = MiniRedis.open(config)
    await recovered.start()
    assert await recovered.direct_client().execute(
        CommandRequest(b"MGET", (b"before", b"during"))
    ) == Items((Bytes(b"1"), Bytes(b"2")))
    await recovered.close()


@pytest.mark.asyncio
async def test_newer_rewrite_base_wins_over_older_snapshot(tmp_path):
    config = MiniRedisConfig(
        aof_path=tmp_path / "appendonly.mraof",
        aof_policy=AofPolicy.ALWAYS,
        snapshot_path=tmp_path / "dump.mrsnap",
    )
    runtime = await open_test_runtime(config=config)
    client = runtime.direct_client()
    await client.execute(CommandRequest(b"SET", (b"k", b"snapshot")))
    await runtime.save_snapshot()
    await client.execute(CommandRequest(b"SET", (b"k", b"rewrite")))

    assert isinstance(await runtime.rewrite_aof(), AofRewriteSaved)
    await runtime.close()

    recovered = MiniRedis.open(config)
    await recovered.start()
    assert await recovered.direct_client().execute(
        CommandRequest(b"GET", (b"k",))
    ) == Bytes(b"rewrite")
    await recovered.close()


@pytest.mark.asyncio
async def test_graceful_close_waits_for_runtime_rewrite(tmp_path):
    runtime = await open_test_runtime(
        config=MiniRedisConfig(
            aof_path=tmp_path / "appendonly.mraof",
            aof_policy=AofPolicy.ALWAYS,
        ),
        aof_rewrite_gate=True,
    )
    rewriting = asyncio.create_task(runtime.rewrite_aof())
    await runtime.debug_aof_rewrite_entered.wait()

    closing = asyncio.create_task(runtime.close())
    await asyncio.sleep(0)
    assert not closing.done()
    runtime.debug_aof_rewrite_release.set()
    assert isinstance(await rewriting, AofRewriteSaved)
    await closing
    assert runtime.debug_stats().aof_tasks == 0


@pytest.mark.asyncio
async def test_simulated_crash_before_rename_keeps_old_aof(tmp_path):
    config = MiniRedisConfig(
        aof_path=tmp_path / "appendonly.mraof",
        aof_policy=AofPolicy.ALWAYS,
    )
    runtime = await open_test_runtime(
        config=config,
        aof_rewrite_gate=True,
    )
    assert await runtime.direct_client().execute(
        CommandRequest(b"SET", (b"k", b"old"))
    ) == Ok()
    rewriting = asyncio.create_task(runtime.rewrite_aof())
    await runtime.debug_aof_rewrite_entered.wait()

    crashing = asyncio.create_task(runtime.simulate_crash())
    await asyncio.sleep(0)
    runtime.debug_aof_rewrite_release.set()
    await crashing
    assert await rewriting == AofRewriteFailed(
        "AOF writer crashed during rewrite"
    )

    recovered = MiniRedis.open(config)
    await recovered.start()
    assert await recovered.direct_client().execute(
        CommandRequest(b"GET", (b"k",))
    ) == Bytes(b"old")
    await recovered.close()


@pytest.mark.asyncio
async def test_successful_rewrite_compacts_history_to_base_only(tmp_path):
    path = tmp_path / "appendonly.mraof"
    runtime = await open_test_runtime(
        config=MiniRedisConfig(
            aof_path=path,
            aof_policy=AofPolicy.ALWAYS,
        )
    )
    client = runtime.direct_client()
    for value in (b"1", b"2", b"3"):
        await client.execute(CommandRequest(b"SET", (b"k", value)))

    assert len(load_aof(path, repair_truncated_tail=False).batches) == 3
    assert isinstance(await runtime.rewrite_aof(), AofRewriteSaved)
    log = load_aof(path, repair_truncated_tail=False)
    assert log.state_base is not None
    assert log.state_base.checkpoint_seq == 3
    assert log.batches == ()
    await runtime.close()


@pytest.mark.asyncio
async def test_immediate_write_after_rewrite_request_has_no_capture_gap(
    tmp_path,
):
    path = tmp_path / "appendonly.mraof"
    config = MiniRedisConfig(
        aof_path=path,
        aof_policy=AofPolicy.ALWAYS,
    )
    runtime = await open_test_runtime(config=config)
    client = runtime.direct_client()
    rewriting = asyncio.create_task(runtime.rewrite_aof())
    writing = asyncio.create_task(
        client.execute(CommandRequest(b"SET", (b"k", b"v")))
    )

    assert isinstance(await rewriting, AofRewriteSaved)
    assert await writing == Ok()
    await runtime.close()

    recovered = MiniRedis.open(config)
    await recovered.start()
    assert await recovered.direct_client().execute(
        CommandRequest(b"GET", (b"k",))
    ) == Bytes(b"v")
    await recovered.close()
