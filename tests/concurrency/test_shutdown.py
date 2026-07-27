import asyncio

import pytest

from miniredis import CommandRequest, MiniRedis, RuntimeState
from miniredis.core.reply import Failure, Ok


class GateProducer:
    def __init__(self) -> None:
        self.quiesce_started = asyncio.Event()
        self.release = asyncio.Event()
        self.quiesced = False

    async def quiesce(self) -> None:
        self.quiesce_started.set()
        await self.release.wait()
        self.quiesced = True


@pytest.mark.asyncio
async def test_cancelled_close_caller_does_not_cancel_cleanup():
    runtime = MiniRedis.open(outbox_drain_grace_ms=0)
    await runtime.start()
    producer = GateProducer()
    runtime.debug_register_control_producer(producer)
    closing = asyncio.create_task(runtime.close())
    await producer.quiesce_started.wait()
    closing.cancel()
    with pytest.raises(asyncio.CancelledError):
        await closing
    producer.release.set()
    await runtime.close()
    assert producer.quiesced
    assert runtime.closed
    assert runtime.debug_stats().owned_tasks == 0


@pytest.mark.asyncio
async def test_shutdown_barrier_bypasses_full_user_admission():
    runtime = MiniRedis.open(
        max_pending_commands=1,
        outbox_drain_grace_ms=0,
    )
    await runtime.start()
    client = runtime.direct_client()
    runtime.debug_pause_executor()
    accepted = asyncio.create_task(client.execute(CommandRequest(b"PING")))
    await runtime.debug_wait_until_queued(1)
    closing = asyncio.create_task(runtime.close())
    await runtime.debug_wait_for_state(RuntimeState.DRAINING.value)
    assert await client.execute(CommandRequest(b"PING")) == Failure(
        "CLOSED", "runtime is not accepting commands"
    )
    runtime.debug_resume_executor()
    assert await accepted == Ok(b"PONG")
    await closing
    assert runtime.debug_stats().pending_futures == 0


@pytest.mark.asyncio
async def test_failed_runtime_terminalizes_infinite_waiter():
    runtime = MiniRedis.open(outbox_drain_grace_ms=0)
    await runtime.start()
    blocked = asyncio.create_task(
        runtime.direct_client().execute(
            CommandRequest(b"BLPOP", (b"q", b"0"))
        )
    )
    await runtime.debug_wait_for_waiters(1)
    runtime._transition_failed("injected worker failure")
    assert await blocked == Failure(
        "ERR", "runtime failed: injected worker failure"
    )
    await runtime.close()
    assert runtime.debug_stats().pending_futures == 0


@pytest.mark.asyncio
async def test_abrupt_worker_stop_uses_supervisor_fallback_once():
    runtime = MiniRedis.open(outbox_drain_grace_ms=0)
    await runtime.start()
    blocked = asyncio.create_task(
        runtime.direct_client().execute(
            CommandRequest(b"BLPOP", (b"q", b"0"))
        )
    )
    await runtime.debug_wait_for_waiters(1)
    worker = runtime.executor.worker_task
    assert worker is not None
    worker.cancel()
    outcome = await blocked
    assert isinstance(outcome, Failure)
    assert outcome.code == "ERR"
    await runtime.close()
    assert runtime.debug_stats().pending_futures == 0
