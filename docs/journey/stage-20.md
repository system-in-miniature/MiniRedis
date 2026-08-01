# Stage 20 · TCP runtime parity

### Goal

Expose the same serialized MiniRedis semantics through real TCP sessions, with per-connection ordering, bounded buffering, slow-client isolation, and fully owned shutdown.

??? note "Deliverable files"
    - `pyproject.toml`
    - `src/miniredis/adapters/tcp.py`
    - `src/miniredis/config.py`
    - `src/miniredis/core/executor.py`
    - `src/miniredis/runtime.py`
    - `tests/adapters/test_direct_resp_parity.py`
    - `tests/adapters/test_tcp_async_semantics.py`
    - `tests/adapters/test_tcp_smoke.py`
    - `tests/interop/test_redis_py_resp2.py`
    - `uv.lock`

### The problem at this point

A codec is not a server. Each socket introduces concurrent reader, writer, command, and close work. Commands on one connection must remain ordered without letting a blocking command stop another connection; all replies and Pub/Sub events must share one ordered outbox; EOF, protocol error, slow writers, server close, and runtime close must converge on one session outcome.

### Test contract

#### See the failure first

Submitting every decoded frame concurrently can reorder pipelined commands. Awaiting a blocking command in the read loop can stop unrelated frames or connections. Writing replies directly from producers can reorder Pub/Sub and replies. EOF can leave BLPOP waiters alive, while close races can leak reader/writer tasks or spend the drain grace period twice.

??? note "File diff: tests/adapters/test_direct_resp_parity.py"
    ```diff
    diff --git a/tests/adapters/test_direct_resp_parity.py b/tests/adapters/test_direct_resp_parity.py
    new file mode 100644
    index 0000000000000000000000000000000000000000..fcbe8a3e0fb7d271a688fb7c9093d56bc14e9fe1
    --- /dev/null
    +++ b/tests/adapters/test_direct_resp_parity.py
    @@ -0,0 +1,69 @@
    +import asyncio
    +
    +import pytest
    +
    +from miniredis import MiniRedis
    +from miniredis.adapters.resp2 import (
    +    RespArray,
    +    RespBulk,
    +    encode_frame,
    +    encode_outbound,
    +)
    +from miniredis.commands.request import CommandRequest
    +
    +
    +COMMAND_SEQUENCE = (
    +    CommandRequest(b"SET", (b"k", b"1")),
    +    CommandRequest(b"INCR", (b"k",)),
    +    CommandRequest(b"GET", (b"k",)),
    +    CommandRequest(b"HSET", (b"h", b"f", b"v")),
    +    CommandRequest(b"HGET", (b"h", b"f")),
    +    CommandRequest(b"HGETALL", (b"h",)),
    +    CommandRequest(b"RPUSH", (b"l", b"a", b"b")),
    +    CommandRequest(b"LRANGE", (b"l", b"0", b"-1")),
    +    CommandRequest(b"SADD", (b"s", b"a", b"b")),
    +    CommandRequest(b"SMEMBERS", (b"s",)),
    +    CommandRequest(b"ZADD", (b"z", b"1", b"a")),
    +    CommandRequest(b"ZRANGE", (b"z", b"0", b"-1")),
    +    CommandRequest(b"TYPE", (b"z",)),
    +    CommandRequest(b"GET", (b"h",)),
    +    CommandRequest(b"DEL", (b"k", b"missing")),
    +)
    +
    +
    +def request_wire(request: CommandRequest) -> bytes:
    +    return encode_frame(
    +        RespArray(
    +            (
    +                RespBulk(request.name),
    +                *(RespBulk(arg) for arg in request.args),
    +            )
    +        )
    +    )
    +
    +
    +@pytest.mark.asyncio
    +async def test_selected_sequence_has_state_and_reply_parity():
    +    async with (
    +        MiniRedis.open() as direct_runtime,
    +        MiniRedis.open() as tcp_runtime,
    +    ):
    +        direct = direct_runtime.direct_client()
    +        server = await tcp_runtime.start_tcp("127.0.0.1", 0)
    +        reader, writer = await asyncio.open_connection(*server.address)
    +        try:
    +            for request in COMMAND_SEQUENCE:
    +                expected = encode_outbound(await direct.execute(request))
    +                writer.write(request_wire(request))
    +                await writer.drain()
    +                assert await reader.readexactly(len(expected)) == expected
    +
    +            assert (
    +                tcp_runtime.debug_logical_items()
    +                == direct_runtime.debug_logical_items()
    +            )
    +            assert tcp_runtime.debug_commit_seq == direct_runtime.debug_commit_seq
    +        finally:
    +            writer.close()
    +            await writer.wait_closed()
    +            await server.close()
    ```

**What this test locks**

It locks reply bytes, logical state, and commit sequence parity between direct and TCP adapters.

**How it constructs the counterexample**

It runs one mixed command sequence through two runtimes and compares each encoded reply plus final state.

**Key test statement**

```python
assert tcp_runtime.debug_logical_items() == direct_runtime.debug_logical_items()
```

**What a failure means**

The network adapter has invented semantics instead of transporting the same executor behavior.

??? note "File diff: tests/adapters/test_tcp_async_semantics.py"
    ```diff
    diff --git a/tests/adapters/test_tcp_async_semantics.py b/tests/adapters/test_tcp_async_semantics.py
    new file mode 100644
    index 0000000000000000000000000000000000000000..4ee11c8acd8ee276a4ffcf485866ee8d5c6edc46
    --- /dev/null
    +++ b/tests/adapters/test_tcp_async_semantics.py
    @@ -0,0 +1,184 @@
    +import asyncio
    +
    +import pytest
    +
    +from miniredis import MiniRedis
    +from miniredis.commands.request import CommandRequest
    +from miniredis.core.reply import Bytes, Number
    +
    +
    +async def send(writer, wire):
    +    writer.write(wire)
    +    await writer.drain()
    +
    +
    +async def expect(reader, wire):
    +    assert await reader.readexactly(len(wire)) == wire
    +
    +
    +async def close_writers(*writers):
    +    for writer in writers:
    +        writer.close()
    +    await asyncio.gather(
    +        *(writer.wait_closed() for writer in writers),
    +        return_exceptions=True,
    +    )
    +
    +
    +class CloseReleasedWriter:
    +    def __init__(self, inner) -> None:
    +        self._inner = inner
    +        self.drain_started = asyncio.Event()
    +        self._closed = asyncio.Event()
    +
    +    def write(self, data: bytes) -> None:
    +        self._inner.write(data)
    +
    +    async def drain(self) -> None:
    +        self.drain_started.set()
    +        await self._closed.wait()
    +        raise ConnectionError("transport closed")
    +
    +    def close(self) -> None:
    +        self._inner.close()
    +        self._closed.set()
    +
    +    async def wait_closed(self) -> None:
    +        await self._inner.wait_closed()
    +
    +    def force_release(self) -> None:
    +        self._closed.set()
    +
    +
    +@pytest.mark.asyncio
    +async def test_blpop_does_not_block_another_connection():
    +    async with MiniRedis.open() as redis:
    +        server = await redis.start_tcp("127.0.0.1", 0)
    +        r1, w1 = await asyncio.open_connection(*server.address)
    +        r2, w2 = await asyncio.open_connection(*server.address)
    +        await send(w1, b"*3\r\n$5\r\nBLPOP\r\n$1\r\nq\r\n$1\r\n0\r\n")
    +        await send(w2, b"*3\r\n$5\r\nRPUSH\r\n$1\r\nq\r\n$1\r\nx\r\n")
    +        await expect(r2, b":1\r\n")
    +        await expect(r1, b"*2\r\n$1\r\nq\r\n$1\r\nx\r\n")
    +        await close_writers(w1, w2)
    +        await server.close()
    +
    +
    +@pytest.mark.asyncio
    +async def test_infinite_blpop_eof_closes_waiter_before_later_push():
    +    async with MiniRedis.open() as redis:
    +        server = await redis.start_tcp("127.0.0.1", 0)
    +        reader, writer = await asyncio.open_connection(*server.address)
    +        await send(writer, b"*3\r\n$5\r\nBLPOP\r\n$1\r\nq\r\n$1\r\n0\r\n")
    +        await redis.debug_wait_for_waiters(1)
    +        writer.close()
    +        await writer.wait_closed()
    +        await redis.debug_wait_for_waiters(0)
    +        await redis.debug_wait_for_sessions(0)
    +        producer = redis.direct_client()
    +        assert await producer.execute(CommandRequest(b"RPUSH", (b"q", b"x"))) == Number(
    +            1
    +        )
    +        assert await producer.execute(CommandRequest(b"LPOP", (b"q",))) == Bytes(b"x")
    +        assert await reader.read() == b""
    +        await server.close()
    +
    +
    +@pytest.mark.asyncio
    +async def test_subscribe_message_ping_unsubscribe_share_one_ordered_outbox():
    +    async with MiniRedis.open() as redis:
    +        server = await redis.start_tcp("127.0.0.1", 0)
    +        sub_r, sub_w = await asyncio.open_connection(*server.address)
    +        pub_r, pub_w = await asyncio.open_connection(*server.address)
    +        await send(sub_w, b"*2\r\n$9\r\nSUBSCRIBE\r\n$1\r\nc\r\n")
    +        await expect(
    +            sub_r,
    +            b"*3\r\n$9\r\nsubscribe\r\n$1\r\nc\r\n:1\r\n",
    +        )
    +        await send(
    +            pub_w,
    +            b"*3\r\n$7\r\nPUBLISH\r\n$1\r\nc\r\n$1\r\nm\r\n",
    +        )
    +        await expect(pub_r, b":1\r\n")
    +        await expect(
    +            sub_r,
    +            b"*3\r\n$7\r\nmessage\r\n$1\r\nc\r\n$1\r\nm\r\n",
    +        )
    +        await send(sub_w, b"*2\r\n$4\r\nPING\r\n$1\r\nx\r\n")
    +        await expect(sub_r, b"*2\r\n$4\r\npong\r\n$1\r\nx\r\n")
    +        await send(
    +            sub_w,
    +            b"*2\r\n$11\r\nUNSUBSCRIBE\r\n$1\r\nc\r\n",
    +        )
    +        await expect(
    +            sub_r,
    +            b"*3\r\n$11\r\nunsubscribe\r\n$1\r\nc\r\n:0\r\n",
    +        )
    +        await send(sub_w, b"*1\r\n$4\r\nPING\r\n")
    +        await expect(sub_r, b"+PONG\r\n")
    +        await close_writers(sub_w, pub_w)
    +        await server.close()
    +
    +
    +@pytest.mark.asyncio
    +async def test_full_tcp_outbox_closes_only_the_slow_subscriber():
    +    async with MiniRedis.open(outbox_limit=1) as redis:
    +        server = await redis.start_tcp("127.0.0.1", 0)
    +        server.debug_pause_new_writers()
    +        slow_r, slow_w = await asyncio.open_connection(*server.address)
    +        await redis.debug_wait_for_sessions(1)
    +        server.debug_resume_new_writers()
    +        fast_r, fast_w = await asyncio.open_connection(*server.address)
    +        pub_r, pub_w = await asyncio.open_connection(*server.address)
    +        await redis.debug_wait_for_sessions(3)
    +
    +        await send(slow_w, b"*2\r\n$9\r\nSUBSCRIBE\r\n$1\r\nc\r\n")
    +        await redis.debug_wait_until_idle()
    +        await send(fast_w, b"*2\r\n$9\r\nSUBSCRIBE\r\n$1\r\nc\r\n")
    +        await expect(
    +            fast_r,
    +            b"*3\r\n$9\r\nsubscribe\r\n$1\r\nc\r\n:1\r\n",
    +        )
    +        await send(
    +            pub_w,
    +            b"*3\r\n$7\r\nPUBLISH\r\n$1\r\nc\r\n$1\r\nm\r\n",
    +        )
    +        await expect(pub_r, b":1\r\n")
    +        await expect(
    +            fast_r,
    +            b"*3\r\n$7\r\nmessage\r\n$1\r\nc\r\n$1\r\nm\r\n",
    +        )
    +        await redis.debug_wait_for_sessions(2)
    +        assert await slow_r.read() == b""
    +
    +        await close_writers(fast_w, pub_w, slow_w)
    +        await server.close()
    +
    +
    +@pytest.mark.asyncio
    +async def test_runtime_close_does_not_spend_outbox_grace_twice():
    +    redis = MiniRedis.open(outbox_drain_grace_ms=60_000)
    +    await redis.start()
    +    server = await redis.start_tcp("127.0.0.1", 0)
    +    reader, client_writer = await asyncio.open_connection(*server.address)
    +    await redis.debug_wait_for_sessions(1)
    +    session = server.debug_sessions()[0]
    +    gated = CloseReleasedWriter(session.writer)
    +    session.writer = gated
    +
    +    await send(client_writer, b"*1\r\n$4\r\nPING\r\n")
    +    await gated.drain_started.wait()
    +    assert session.endpoint.outbox.pending_count == 0
    +    try:
    +        async with asyncio.timeout(1):
    +            await redis.close()
    +    finally:
    +        gated.force_release()
    +        await redis.close()
    +
    +    assert redis.closed
    +    assert server.closed
    +    assert server.owned_task_count == 0
    +    assert await reader.read() == b"+PONG\r\n"
    +    client_writer.close()
    +    await client_writer.wait_closed()
    ```

**What this test locks**

It locks cross-connection progress, EOF waiter cleanup, Pub/Sub ordering, slow-subscriber isolation, and one bounded runtime-close drain.

**How it constructs the counterexample**

It blocks BLPOP, disconnects clients, pauses one writer with a tiny outbox, and gates transport drain during runtime shutdown.

**Key test statement**

```python
assert server.owned_task_count == 0
assert await reader.read() == b"+PONG\r\n"
```

**What a failure means**

Async session ownership leaks, ordering diverges, or one slow transport controls healthy clients/runtime shutdown.

??? note "File diff: tests/adapters/test_tcp_smoke.py"
    ```diff
    diff --git a/tests/adapters/test_tcp_smoke.py b/tests/adapters/test_tcp_smoke.py
    new file mode 100644
    index 0000000000000000000000000000000000000000..9d05b0cb0ef351fa3ea37d6e9b09b96ed29346b8
    --- /dev/null
    +++ b/tests/adapters/test_tcp_smoke.py
    @@ -0,0 +1,92 @@
    +import asyncio
    +
    +import pytest
    +
    +from miniredis import MiniRedis
    +
    +
    +async def expect(reader, wire):
    +    assert await reader.readexactly(len(wire)) == wire
    +
    +
    +@pytest.mark.asyncio
    +async def test_tcp_fragmentation_multiple_commands_and_close():
    +    async with MiniRedis.open() as redis:
    +        server = await redis.start_tcp("127.0.0.1", 0)
    +        reader, writer = await asyncio.open_connection(*server.address)
    +        writer.write(b"*1\r\n$4\r\nPI")
    +        await writer.drain()
    +        writer.write(
    +            b"NG\r\n"
    +            b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n"
    +            b"*2\r\n$3\r\nGET\r\n$1\r\nk\r\n"
    +        )
    +        await writer.drain()
    +        await expect(reader, b"+PONG\r\n")
    +        await expect(reader, b"+OK\r\n")
    +        await expect(reader, b"$1\r\nv\r\n")
    +        writer.close()
    +        await writer.wait_closed()
    +        await server.close()
    +        await server.close()
    +        assert server.closed
    +
    +
    +@pytest.mark.asyncio
    +async def test_protocol_error_is_written_by_outbox_writer_then_closes():
    +    async with MiniRedis.open() as redis:
    +        server = await redis.start_tcp("127.0.0.1", 0)
    +        reader, writer = await asyncio.open_connection(*server.address)
    +        writer.write(b"+not-a-command\r\n")
    +        await writer.drain()
    +        assert await reader.readline() == (
    +            b"-CLOSED protocol error: command must be a non-empty array\r\n"
    +        )
    +        assert await reader.read() == b""
    +        writer.close()
    +        await writer.wait_closed()
    +        await server.close()
    +
    +
    +@pytest.mark.asyncio
    +async def test_truncated_frame_at_eof_uses_the_protocol_error_path():
    +    async with MiniRedis.open() as redis:
    +        server = await redis.start_tcp("127.0.0.1", 0)
    +        reader, writer = await asyncio.open_connection(*server.address)
    +        writer.write(b"*2\r\n$3\r\nGET\r\n$1\r\n")
    +        await writer.drain()
    +        writer.write_eof()
    +        await writer.drain()
    +        assert await reader.readline() == (
    +            b"-CLOSED protocol error: truncated RESP frame\r\n"
    +        )
    +        assert await reader.read() == b""
    +        writer.close()
    +        await writer.wait_closed()
    +        await server.close()
    +
    +
    +@pytest.mark.asyncio
    +async def test_server_close_settles_reader_writer_and_registration():
    +    async with MiniRedis.open() as redis:
    +        server = await redis.start_tcp("127.0.0.1", 0)
    +        reader, writer = await asyncio.open_connection(*server.address)
    +        await redis.debug_wait_for_sessions(1)
    +        await server.close()
    +        assert await reader.read() == b""
    +        assert server.closed
    +        assert server.owned_task_count == 0
    +        assert redis.debug_stats().sessions == 0
    +        writer.close()
    +        await writer.wait_closed()
    +
    +
    +@pytest.mark.asyncio
    +async def test_start_tcp_rejects_non_running_runtime_before_bind():
    +    redis = MiniRedis.open()
    +    with pytest.raises(RuntimeError, match="runtime is not running"):
    +        await redis.start_tcp("127.0.0.1", 0)
    +    await redis.start()
    +    await redis.close()
    +    with pytest.raises(RuntimeError, match="runtime is not running"):
    +        await redis.start_tcp("127.0.0.1", 0)
    ```

**What this test locks**

It locks fragmentation, multiple commands, protocol-error delivery, EOF truncation, idempotent server close, and admission only on a running runtime.

**How it constructs the counterexample**

It splits PING across writes, coalesces SET/GET, sends invalid/truncated frames, and closes live sessions and servers repeatedly.

**Key test statement**

```python
assert await reader.readline() == b"-CLOSED protocol error: truncated RESP frame\r\n"
```

**What a failure means**

Socket lifecycle or protocol errors bypass the ordered session boundary.

??? note "File diff: tests/interop/test_redis_py_resp2.py"
    ```diff
    diff --git a/tests/interop/test_redis_py_resp2.py b/tests/interop/test_redis_py_resp2.py
    new file mode 100644
    index 0000000000000000000000000000000000000000..164eda4fdfb4a5c7854f117a8728e073fda6830e
    --- /dev/null
    +++ b/tests/interop/test_redis_py_resp2.py
    @@ -0,0 +1,28 @@
    +import pytest
    +import redis.asyncio as redis_async
    +
    +from miniredis import MiniRedis
    +
    +
    +@pytest.mark.interop
    +@pytest.mark.asyncio
    +async def test_redis_py_resp2_string_and_hash_smoke():
    +    async with MiniRedis.open() as runtime:
    +        server = await runtime.start_tcp("127.0.0.1", 0)
    +        host, port = server.address
    +        client = redis_async.Redis(
    +            host=host,
    +            port=port,
    +            protocol=2,
    +            decode_responses=False,
    +            driver_info=None,
    +        )
    +        try:
    +            assert await client.ping() is True
    +            assert await client.set(b"k", b"1")
    +            assert await client.incr(b"k") == 2
    +            assert await client.hset(b"h", b"f", b"v") == 1
    +            assert await client.hget(b"h", b"f") == b"v"
    +        finally:
    +            await client.aclose()
    +            await server.close()
    ```

**What this test locks**

It locks basic compatibility with an independent Redis client implementation.

**How it constructs the counterexample**

redis-py connects in RESP2 binary mode and performs string and hash operations.

**Key test statement**

```python
assert await client.incr(b"k") == 2
assert await client.hget(b"h", b"f") == b"v"
```

**What a failure means**

In-process codec tests missed a real client handshake or wire-behavior mismatch.

### Basic concepts

A TCP session owns one decoder/read pump, one ordered command-at-a-time submission chain, one outbox/write pump, and one idempotent close task. Pipelining means reading more frames before earlier replies finish; it does not permit per-session execution reorder. Slow-client isolation means outbox overflow closes that endpoint without blocking executor progress.

### Why this mechanism is necessary

The network boundary multiplies lifecycle races but must not fork database semantics. Reusing the executor endpoint/outbox contract preserves ordering and backpressure behavior across direct and TCP access. Explicit session ownership makes EOF and shutdown release blocked commands, subscriptions, transports, and tasks together.

### Runtime mental model

The server accepts a socket and registers a `SessionEndpoint`. The reader decodes into a bounded frame deque and starts at most one command task. Executor outcomes enter the endpoint outbox. The writer alone serializes outbound values and drains the transport. Any terminal path quiesces the reader, closes the executor session, aborts/drains the outbox as appropriate, joins tasks, closes the socket, and unregisters the session.

### Mechanism blocks

#### Owned TCP session pumps

Give each connection one incremental reader, one ordered outbox writer, bounded decoded-frame buffering, and one idempotent close path.

??? note "File diff: src/miniredis/adapters/tcp.py"
    ```diff
    diff --git a/src/miniredis/adapters/tcp.py b/src/miniredis/adapters/tcp.py
    new file mode 100644
    index 0000000000000000000000000000000000000000..581f719c84ee55a75b9bdb2960d06e8b02fec2e0
    --- /dev/null
    +++ b/src/miniredis/adapters/tcp.py
    @@ -0,0 +1,468 @@
    +from __future__ import annotations
    +
    +import asyncio
    +from collections import deque
    +from dataclasses import dataclass
    +from typing import TYPE_CHECKING, Callable
    +
    +from miniredis.adapters.resp2 import (
    +    RespDecoder,
    +    RespProtocolError,
    +    encode_outbound,
    +    frame_to_request,
    +)
    +from miniredis.commands.request import CommandRequest
    +from miniredis.core.outbound import (
    +    OutboxClosed,
    +    ServerClosed,
    +    SessionEndpoint,
    +)
    +
    +if TYPE_CHECKING:
    +    from miniredis.runtime import MiniRedis
    +
    +
    +@dataclass(frozen=True, slots=True)
    +class TcpAddress:
    +    host: str
    +    port: int
    +
    +
    +class TcpSession:
    +    def __init__(
    +        self,
    +        runtime: MiniRedis,
    +        reader: asyncio.StreamReader,
    +        writer: asyncio.StreamWriter,
    +        outbox_limit: int,
    +        max_buffered_frames: int,
    +        on_closed: Callable[[TcpSession], None],
    +        writer_starts_paused: bool,
    +    ) -> None:
    +        self.runtime = runtime
    +        self.reader = reader
    +        self.writer = writer
    +        self.decoder = RespDecoder()
    +        self.session_id = runtime.new_session_id()
    +        self._frames: deque[CommandRequest] = deque()
    +        self._max_buffered_frames = max_buffered_frames
    +        self._on_closed = on_closed
    +        self._reader_task: asyncio.Task[None] | None = None
    +        self._writer_task: asyncio.Task[None] | None = None
    +        self._pending_command: asyncio.Task[None] | None = None
    +        self._close_task: asyncio.Task[None] | None = None
    +        self._reader_quiescing = False
    +        self._reader_quiesced = asyncio.Event()
    +        self._transport_finishing = False
    +        self._transport_finished = asyncio.Event()
    +        self._writer_allowed = asyncio.Event()
    +        if not writer_starts_paused:
    +            self._writer_allowed.set()
    +        self._closed = False
    +        self.endpoint = SessionEndpoint(
    +            session_id=self.session_id,
    +            capacity=outbox_limit,
    +            reply_via_outbox=True,
    +            on_slow=runtime.session_became_slow,
    +            close_transport=self._request_transport_close,
    +        )
    +
    +    @property
    +    def reader_task(self) -> asyncio.Task[None]:
    +        if self._reader_task is None:
    +            raise RuntimeError("TCP session is not started")
    +        return self._reader_task
    +
    +    async def start(self) -> None:
    +        self.runtime.register_session(self.endpoint)
    +        self._writer_task = asyncio.create_task(
    +            self._write_loop(),
    +            name=f"miniredis:tcp-writer:{self.session_id}",
    +        )
    +        self._reader_task = asyncio.create_task(
    +            self._read_loop(),
    +            name=f"miniredis:tcp-reader:{self.session_id}",
    +        )
    +        self._reader_task.add_done_callback(self._reader_done)
    +
    +    def _reader_done(self, task: asyncio.Task[None]) -> None:
    +        try:
    +            task.result()
    +        except asyncio.CancelledError:
    +            if not self._reader_quiescing:
    +                self.request_close()
    +        except BaseException as exc:
    +            self.endpoint.offer_best_effort(
    +                ServerClosed(f"session reader failed: {exc}")
    +            )
    +            self.endpoint.outbox.begin_close("session reader failed")
    +            self.request_close()
    +
    +    def _request_transport_close(self, reason: str) -> None:
    +        self._writer_allowed.set()
    +        if reason != "runtime closed":
    +            self.writer.close()
    +
    +    def _submit_next(self) -> None:
    +        if (
    +            self._closed
    +            or self._reader_quiescing
    +            or self._pending_command is not None
    +            or not self._frames
    +        ):
    +            return
    +        request = self._frames.popleft()
    +        task = asyncio.create_task(
    +            self.runtime.execute_for_session(self.session_id, request),
    +            name=f"miniredis:tcp-command:{self.session_id}",
    +        )
    +        self._pending_command = task
    +        task.add_done_callback(self._command_done)
    +
    +    def _command_done(self, task: asyncio.Task[None]) -> None:
    +        if self._pending_command is task:
    +            self._pending_command = None
    +        try:
    +            task.result()
    +        except asyncio.CancelledError:
    +            self.runtime.request_session_close(self.session_id)
    +        except BaseException as exc:
    +            self.endpoint.offer_best_effort(
    +                ServerClosed(f"session command failed: {exc}")
    +            )
    +            self.endpoint.outbox.begin_close("session command failed")
    +        self._submit_next()
    +
    +    async def _read_loop(self) -> None:
    +        protocol_error: RespProtocolError | None = None
    +        saw_eof = False
    +        try:
    +            while not self._reader_quiescing:
    +                data = await self.reader.read(65536)
    +                if not data:
    +                    saw_eof = True
    +                    try:
    +                        self.decoder.finish()
    +                    except RespProtocolError as exc:
    +                        protocol_error = exc
    +                    break
    +                try:
    +                    frames = self.decoder.feed(data)
    +                    for frame in frames:
    +                        self._frames.append(frame_to_request(frame))
    +                    if len(self._frames) > self._max_buffered_frames:
    +                        raise RespProtocolError("too many buffered command frames")
    +                except RespProtocolError as exc:
    +                    protocol_error = exc
    +                    break
    +                self._submit_next()
    +        except asyncio.CancelledError:
    +            if not self._reader_quiescing:
    +                raise
    +        finally:
    +            self._reader_quiesced.set()
    +
    +        if self._reader_quiescing:
    +            return
    +        if protocol_error is not None:
    +            self.endpoint.offer_best_effort(
    +                ServerClosed(f"protocol error: {protocol_error}")
    +            )
    +            self.endpoint.outbox.begin_close("protocol error")
    +            await self._drain_protocol_error_best_effort()
    +        if saw_eof or protocol_error is not None:
    +            await self.runtime.close_session(self.session_id)
    +            if self._pending_command is not None:
    +                await asyncio.gather(
    +                    self._pending_command,
    +                    return_exceptions=True,
    +                )
    +            await self._finish_transport()
    +
    +    async def _write_loop(self) -> None:
    +        try:
    +            while True:
    +                await self._writer_allowed.wait()
    +                item = await self.endpoint.receive()
    +                self.writer.write(encode_outbound(item))
    +                await self.writer.drain()
    +        except OutboxClosed:
    +            self.runtime.request_session_close(self.session_id)
    +        except (ConnectionError, BrokenPipeError):
    +            self.runtime.request_session_close(self.session_id)
    +
    +    async def _drain_protocol_error_best_effort(self) -> None:
    +        task = self._writer_task
    +        if task is None or task is asyncio.current_task() or task.done():
    +            return
    +        try:
    +            async with asyncio.timeout(
    +                self.runtime.config.outbox_drain_grace_ms / 1000
    +            ):
    +                await asyncio.shield(task)
    +        except TimeoutError:
    +            return
    +
    +    def request_reader_quiesce(self) -> None:
    +        if self._reader_quiescing:
    +            return
    +        self._reader_quiescing = True
    +        if self._reader_task is not None and not self._reader_task.done():
    +            self._reader_task.cancel()
    +
    +    async def wait_reader_quiesced(self) -> None:
    +        await self._reader_quiesced.wait()
    +
    +    async def close(self) -> None:
    +        self.request_close()
    +        assert self._close_task is not None
    +        await asyncio.shield(self._close_task)
    +
    +    def request_close(self) -> None:
    +        if self._close_task is None:
    +            self._close_task = asyncio.create_task(
    +                self._close_once(),
    +                name=f"miniredis:tcp-close:{self.session_id}",
    +            )
    +            self._close_task.add_done_callback(self._close_done)
    +
    +    @staticmethod
    +    def _close_done(task: asyncio.Task[None]) -> None:
    +        try:
    +            task.result()
    +        except BaseException:
    +            return
    +
    +    async def _close_once(self) -> None:
    +        if self._closed:
    +            return
    +        self._closed = True
    +        self.request_reader_quiesce()
    +        await self.wait_reader_quiesced()
    +        await self._settle_reader()
    +        await self.runtime.close_session(self.session_id)
    +        if self._pending_command is not None:
    +            await asyncio.gather(
    +                self._pending_command,
    +                return_exceptions=True,
    +            )
    +        self.endpoint.outbox.abort("session closed")
    +        await self._finish_transport()
    +
    +    async def finish_runtime_close(self) -> None:
    +        self._closed = True
    +        self._writer_allowed.set()
    +        self.endpoint.outbox.abort("runtime closed")
    +        await self._finish_transport()
    +        await self._settle_reader()
    +        if self._pending_command is not None:
    +            await asyncio.gather(
    +                self._pending_command,
    +                return_exceptions=True,
    +            )
    +
    +    async def _settle_reader(self) -> None:
    +        if (
    +            self._reader_task is not None
    +            and self._reader_task is not asyncio.current_task()
    +        ):
    +            await asyncio.gather(
    +                self._reader_task,
    +                return_exceptions=True,
    +            )
    +
    +    async def _finish_transport(self) -> None:
    +        if self._transport_finishing:
    +            await self._transport_finished.wait()
    +            return
    +        self._transport_finishing = True
    +        self._writer_allowed.set()
    +        try:
    +            self.writer.close()
    +            if (
    +                self._writer_task is not None
    +                and self._writer_task is not asyncio.current_task()
    +            ):
    +                await asyncio.gather(
    +                    self._writer_task,
    +                    return_exceptions=True,
    +                )
    +            try:
    +                await self.writer.wait_closed()
    +            except ConnectionError:
    +                pass
    +            self._on_closed(self)
    +        finally:
    +            self._transport_finished.set()
    +
    +    def debug_pause_writer(self) -> None:
    +        self._writer_allowed.clear()
    +
    +    def debug_resume_writer(self) -> None:
    +        self._writer_allowed.set()
    +
    +    @property
    +    def owned_task_count(self) -> int:
    +        return sum(
    +            task is not None and not task.done()
    +            for task in (
    +                self._reader_task,
    +                self._writer_task,
    +                self._pending_command,
    +                self._close_task,
    +            )
    +        )
    +
    +
    +class TcpServer:
    +    def __init__(
    +        self,
    +        runtime: MiniRedis,
    +        host: str,
    +        port: int,
    +        outbox_limit: int,
    +    ) -> None:
    +        self.runtime = runtime
    +        self.host = host
    +        self.port = port
    +        self.outbox_limit = outbox_limit
    +        self._server: asyncio.Server | None = None
    +        self._closing_server: asyncio.Server | None = None
    +        self._sessions: set[TcpSession] = set()
    +        self._tasks: set[asyncio.Task[None]] = set()
    +        self._close_task: asyncio.Task[None] | None = None
    +        self._closed = False
    +        self._new_writers_paused = False
    +
    +    async def start(self) -> None:
    +        if self._server is not None:
    +            return
    +        self._server = await asyncio.start_server(self._accept, self.host, self.port)
    +
    +    @property
    +    def address(self) -> tuple[str, int]:
    +        if self._server is None or not self._server.sockets:
    +            raise RuntimeError("TCP server is not started")
    +        host, port, *_ = self._server.sockets[0].getsockname()
    +        return str(host), int(port)
    +
    +    @property
    +    def closed(self) -> bool:
    +        return self._closed
    +
    +    async def _accept(
    +        self,
    +        reader: asyncio.StreamReader,
    +        writer: asyncio.StreamWriter,
    +    ) -> None:
    +        owner = asyncio.current_task()
    +        if owner is not None:
    +            self._tasks.add(owner)
    +        if self._closed:
    +            writer.close()
    +            await writer.wait_closed()
    +            if owner is not None:
    +                self._tasks.discard(owner)
    +            return
    +        session = TcpSession(
    +            self.runtime,
    +            reader,
    +            writer,
    +            self.outbox_limit,
    +            self.runtime.config.max_session_frames,
    +            self._session_finished,
    +            self._new_writers_paused,
    +        )
    +        self._sessions.add(session)
    +        try:
    +            await session.start()
    +        except BaseException:
    +            self._sessions.discard(session)
    +            writer.close()
    +            await writer.wait_closed()
    +            raise
    +        finally:
    +            if owner is not None:
    +                self._tasks.discard(owner)
    +
    +    def _session_finished(self, session: TcpSession) -> None:
    +        self._sessions.discard(session)
    +
    +    async def close(self) -> None:
    +        if self._close_task is None:
    +            self._close_task = asyncio.create_task(
    +                self._close_once(),
    +                name="miniredis:tcp-server-close",
    +            )
    +        await asyncio.shield(self._close_task)
    +
    +    async def _close_once(self) -> None:
    +        await self.quiesce()
    +        await asyncio.gather(
    +            *(session.close() for session in tuple(self._sessions)),
    +            return_exceptions=False,
    +        )
    +        await self._wait_listener_closed()
    +        if self._tasks:
    +            await asyncio.gather(*tuple(self._tasks), return_exceptions=True)
    +        self._sessions.clear()
    +        self._tasks.clear()
    +        self._closed = True
    +        self.runtime.unregister_tcp_server(self)
    +
    +    async def quiesce(self) -> None:
    +        if self._server is not None:
    +            self._server.close()
    +            self._closing_server = self._server
    +            self._server = None
    +        sessions = tuple(self._sessions)
    +        for session in sessions:
    +            session.request_reader_quiesce()
    +        await asyncio.gather(
    +            *(session.wait_reader_quiesced() for session in sessions),
    +            return_exceptions=True,
    +        )
    +
    +    async def finish_runtime_close(self) -> None:
    +        await asyncio.gather(
    +            *(session.finish_runtime_close() for session in tuple(self._sessions)),
    +            return_exceptions=False,
    +        )
    +        await self._wait_listener_closed()
    +        if self._tasks:
    +            await asyncio.gather(*tuple(self._tasks), return_exceptions=True)
    +        self._sessions.clear()
    +        self._tasks.clear()
    +        self._closed = True
    +        self.runtime.unregister_tcp_server(self)
    +
    +    async def _wait_listener_closed(self) -> None:
    +        server, self._closing_server = self._closing_server, None
    +        if server is not None:
    +            await server.wait_closed()
    +
    +    def debug_sessions(self) -> tuple[TcpSession, ...]:
    +        return tuple(
    +            sorted(
    +                self._sessions,
    +                key=lambda session: session.session_id,
    +            )
    +        )
    +
    +    def debug_pause_new_writers(self) -> None:
    +        self._new_writers_paused = True
    +
    +    def debug_resume_new_writers(self) -> None:
    +        self._new_writers_paused = False
    +
    +    @property
    +    def session_count(self) -> int:
    +        return len(self._sessions)
    +
    +    @property
    +    def owned_task_count(self) -> int:
    +        return (
    +            sum(not task.done() for task in self._tasks)
    +            + sum(session.owned_task_count for session in self._sessions)
    +            + int(self._close_task is not None and not self._close_task.done())
    +        )
    ```

**What it is and why it appears**

This module owns TCP server and per-connection lifecycle rather than database logic.

**Runtime role**

It incrementally reads RESP2, bounds decoded frames, submits one session command at a time, writes only from the outbox pump, and joins every task on close.

**Key code**

```python
if (
    self._closed
    or self._reader_quiescing
    or self._pending_command is not None
    or not self._frames
):
    return
request = self._frames.popleft()
```

**Statement understanding**

The reader may pipeline frames, but one pending-command slot preserves arrival order without occupying the global executor while a socket read waits.

#### Runtime-owned network sessions

Register TCP endpoints with the same executor, preserve request cancellation and outbox semantics, and close servers before releasing endpoints.

??? note "File diff: src/miniredis/config.py"
    ```diff
    diff --git a/src/miniredis/config.py b/src/miniredis/config.py
    index 999209a5a6ad80cb8c5f48ee2e984be0b8ac061e..98ad63d78a39025f944287c267c6916ee4bf7a03 100644
    --- a/src/miniredis/config.py
    +++ b/src/miniredis/config.py
    @@ -25,6 +25,7 @@ class MiniRedisConfig:
         snapshot_path: Path | None = None
         replica_queue_limit: int = 64
         replica_drain_grace_ms: int = 1000
    +    max_session_frames: int = 128

         def __post_init__(self) -> None:
             if self.max_pending_commands <= 0:
    @@ -47,3 +48,5 @@ class MiniRedisConfig:
                 raise ValueError("replica_queue_limit must be positive")
             if self.replica_drain_grace_ms < 0:
                 raise ValueError("replica_drain_grace_ms cannot be negative")
    +        if self.max_session_frames <= 0:
    +            raise ValueError("max_session_frames must be positive")
    ```

**What it is and why it appears**

Configuration adds a positive bound on decoded command frames retained per session.

**Runtime role**

It prevents a fast reader from accumulating unbounded work behind a slow or blocking command.

**Key code**

```python
if self.max_session_frames <= 0:
    raise ValueError("max_session_frames must be positive")
```

**Statement understanding**

Protocol byte limits and decoded-work limits protect different allocations; both are required.

??? note "File diff: src/miniredis/core/executor.py"
    ```diff
    diff --git a/src/miniredis/core/executor.py b/src/miniredis/core/executor.py
    index 24e5c4edd784e2020ec4dac137e244bae0b0c97a..8c0b2622ca8adda611fb12a49e15406458a25593 100644
    --- a/src/miniredis/core/executor.py
    +++ b/src/miniredis/core/executor.py
    @@ -47,7 +47,6 @@ from miniredis.core.outbound import (
         Replied,
         RuntimeClosed,
         RuntimeFailed,
    -    ServerClosed,
         SessionEndpoint,
         SubscriptionAck,
         TransportClosed,
    @@ -321,7 +320,7 @@ class CommandExecutor:
             if len(self._requests) >= self.max_pending_commands:
                 return Failure("BUSY", "command queue is full")

    -        token = RequestToken(next(self._request_tokens))
    +        token = self.new_request_token()
             future: asyncio.Future[RequestOutcome] = (
                 asyncio.get_running_loop().create_future()
             )
    @@ -337,6 +336,9 @@ class CommandExecutor:
             self._on_debug_change()
             return SubmittedRequest(token, future)

    +    def new_request_token(self) -> RequestToken:
    +        return RequestToken(next(self._request_tokens))
    +
         def _finish_request(
             self,
             token: RequestToken,
    @@ -540,8 +542,6 @@ class CommandExecutor:
             self._replica_sinks.clear()
             for token in tuple(self._requests):
                 self._finish_request(token, event.outcome)
    -        for endpoint in self._endpoints.values():
    -            endpoint.offer_best_effort(ServerClosed("runtime closed"))
             if not event.completion.done():
                 event.completion.set_result(None)
             self._shutdown_barrier_held = True
    ```

**What it is and why it appears**

The executor exposes token allocation and leaves transport-close messages to the runtime/session owner.

**Runtime role**

TCP parse/admission failures still receive ordered reply tokens, while shutdown avoids two owners writing the same terminal event.

**Key code**

```python
def new_request_token(self) -> RequestToken:
    return RequestToken(next(self._request_tokens))
```

**Statement understanding**

Even failures before normal executor submission participate in one monotonic outbound order.

??? note "File diff: src/miniredis/runtime.py"
    ```diff
    diff --git a/src/miniredis/runtime.py b/src/miniredis/runtime.py
    index 29b1d432b8fba6a6a7e6b5c7e50e702da06f8df9..dab3ad3adc40ea8861197322e15ef9453fd6d8b8 100644
    --- a/src/miniredis/runtime.py
    +++ b/src/miniredis/runtime.py
    @@ -23,6 +23,7 @@ from miniredis.core.commit import CommitBatch, StoredEntry
     from miniredis.core.database import Database
     from miniredis.core.executor import (
         ActiveExpireTick,
    +    AbandonRequest,
         BeginShutdown,
         CommandExecutor,
         CommitBarrier,
    @@ -31,6 +32,7 @@ from miniredis.core.executor import (
     )
     from miniredis.core.expiration import ActiveExpireProducer
     from miniredis.core.outbound import (
    +    ReplyMessage,
         RequestOutcome,
         RequestToken,
         RuntimeClosed,
    @@ -164,6 +166,7 @@ class MiniRedis:
             self._failure_reason: str | None = None
             self._shutdown_complete = False
             self._owned_replica_sinks: set[ReplicaSink] = set()
    +        self._tcp_servers: set[Any] = set()

         @classmethod
         def open(
    @@ -344,6 +347,71 @@ class MiniRedis:
                 endpoint.request_transport_close(reason)
             self.executor.post_control(SessionClosed(session_id))

    +    def new_session_id(self) -> int:
    +        return next(self._session_ids)
    +
    +    def register_session(self, endpoint: SessionEndpoint) -> None:
    +        self.executor.register_endpoint(endpoint)
    +
    +    def request_session_close(self, session_id: int) -> None:
    +        self.executor.post_control(SessionClosed(session_id))
    +
    +    async def close_session(self, session_id: int) -> None:
    +        endpoint = self.executor.endpoint(session_id)
    +        if endpoint is None:
    +            return
    +        completion = asyncio.get_running_loop().create_future()
    +        if not self.executor.post_control(SessionClosed(session_id, completion)):
    +            endpoint.outbox.abort("runtime closed")
    +            return
    +        await asyncio.shield(completion)
    +
    +    def session_became_slow(
    +        self,
    +        session_id: int,
    +        reason: str,
    +    ) -> None:
    +        self._session_became_slow(session_id, reason)
    +
    +    async def execute_for_session(
    +        self,
    +        session_id: int,
    +        request: CommandRequest,
    +    ) -> None:
    +        parsed = self.parse(request)
    +        endpoint = self.executor.endpoint(session_id)
    +        if endpoint is None:
    +            return
    +        if isinstance(parsed, Failure):
    +            token = self.executor.new_request_token()
    +            endpoint.offer(ReplyMessage(token, parsed))
    +            return
    +        submitted = self.executor.submit(session_id, parsed)
    +        if isinstance(submitted, Failure):
    +            token = self.executor.new_request_token()
    +            endpoint.offer(ReplyMessage(token, submitted))
    +            return
    +        try:
    +            await asyncio.shield(submitted.future)
    +        except asyncio.CancelledError:
    +            self.executor.post_control(AbandonRequest(submitted.token))
    +            raise
    +
    +    async def start_tcp(self, host: str, port: int) -> Any:
    +        from miniredis.adapters.tcp import TcpServer
    +
    +        if self.state is not RuntimeState.RUNNING:
    +            raise RuntimeError("runtime is not running")
    +        server = TcpServer(self, host, port, self.config.outbox_limit)
    +        await server.start()
    +        self._tcp_servers.add(server)
    +        self._control_producers.add(server)
    +        return server
    +
    +    def unregister_tcp_server(self, server: Any) -> None:
    +        self._tcp_servers.discard(server)
    +        self._control_producers.discard(server)
    +
         async def close(self) -> None:
             async with self._lifecycle_lock:
                 if self._shutdown_task is None:
    @@ -408,6 +476,11 @@ class MiniRedis:
             for endpoint in endpoints:
                 endpoint.outbox.abort("runtime closed")
                 endpoint.request_transport_close("runtime closed")
    +        await asyncio.gather(
    +            *(server.finish_runtime_close() for server in tuple(self._tcp_servers)),
    +            return_exceptions=False,
    +        )
    +        self._tcp_servers.clear()
             self.executor.release_endpoints()

             if self._snapshot_manager is not None:
    ```

**What it is and why it appears**

The runtime exposes session operations and owns every started TCP server.

**Runtime role**

It registers endpoints, submits/abandons session requests, isolates slow endpoints, starts servers only while running, and closes servers before releasing endpoints.

**Key code**

```python
await asyncio.gather(
    *(server.finish_runtime_close() for server in tuple(self._tcp_servers)),
    return_exceptions=False,
)
```

**Statement understanding**

Network producers and consumers finish while their executor endpoints still exist; endpoint release is the final ownership step.

#### Real-client test dependency

Add the redis-py interop dependency and lock it reproducibly without treating packaging metadata as a storage mechanism.

??? note "File diff: pyproject.toml"
    ```diff
    diff --git a/pyproject.toml b/pyproject.toml
    index c0d64b9b863a57c9f4053018f0ef842aab273075..5b06675d2886e7456b9578561e28a0256b03e2f5 100644
    --- a/pyproject.toml
    +++ b/pyproject.toml
    @@ -13,6 +13,7 @@ dependencies = []
     dev = [
         "pytest>=9,<10",
         "pytest-asyncio>=1.3,<2",
    +    "redis>=8,<9",
     ]

     [tool.hatch.build.targets.wheel]
    @@ -22,5 +23,8 @@ packages = ["src/miniredis"]
     asyncio_mode = "auto"
     asyncio_default_fixture_loop_scope = "function"
     asyncio_default_test_loop_scope = "function"
    -pythonpath = ["src", "."]
    +pythonpath = ["src"]
     testpaths = ["tests"]
    +markers = [
    +    "interop: redis-py RESP2 smoke with client metadata disabled",
    +]
    ```

**What it is and why it appears**

Project metadata adds redis-py as an interop test dependency.

**Runtime role**

It lets verification use an independently implemented RESP2 client; production MiniRedis remains protocol-library independent.

**Key code**

```toml
"redis>=8,<9",
```

**Statement understanding**

The dependency is test scaffolding for external compatibility evidence, not part of the TCP mechanism.

??? note "File diff: uv.lock"
    ```diff
    diff --git a/uv.lock b/uv.lock
    index 0650428f45c9f2a1905285a21710cb07872234f5..5a0c22f78821433335439811257c03d2ca7e5595 100644
    --- a/uv.lock
    +++ b/uv.lock
    @@ -29,6 +29,7 @@ source = { editable = "." }
     dev = [
         { name = "pytest" },
         { name = "pytest-asyncio" },
    +    { name = "redis" },
     ]

     [package.metadata]
    @@ -37,6 +38,7 @@ dev = [
     dev = [
         { name = "pytest", specifier = ">=9,<10" },
         { name = "pytest-asyncio", specifier = ">=1.3,<2" },
    +    { name = "redis", specifier = ">=8,<9" },
     ]

     [[package]]
    @@ -95,6 +97,15 @@ wheels = [
         { url = "https://files.pythonhosted.org/packages/03/e2/08a497ef684b88559c9cc5f4ad53a37e7b99e727094a86d6ea32536d5d3c/pytest_asyncio-1.4.0-py3-none-any.whl", hash = "sha256:933ca923a23075a87fb7070c0ec272a6848489824d887c85c812670932835aa1", size = 16930, upload-time = "2026-05-26T09:56:02.576Z" },
     ]

    +[[package]]
    +name = "redis"
    +version = "8.0.1"
    +source = { registry = "https://pypi.org/simple" }
    +sdist = { url = "https://files.pythonhosted.org/packages/cc/c3/928b290c2c0ca99ab96eea5b4ff8f30be8112b075301a7d3ba214a3c8c12/redis-8.0.1.tar.gz", hash = "sha256:afc5a7a2f5a084f5b1880dec548dd45be17db7e43c82a30d84f952aefb05cfb0", size = 5114170, upload-time = "2026-06-23T14:52:37.728Z" }
    +wheels = [
    +    { url = "https://files.pythonhosted.org/packages/fd/0a/c2345ebf1ebe70840ce3f6c6ee612f8fa749cfbd1b03069c53bf0c62aaad/redis-8.0.1-py3-none-any.whl", hash = "sha256:47daa35a058c23468d6437f17a8c76882cb316b838ef763036af99b96cedd743", size = 502406, upload-time = "2026-06-23T14:52:36.137Z" },
    +]
    +
     [[package]]
     name = "typing-extensions"
     version = "4.16.0"
    ```

**What it is and why it appears**

The lockfile freezes the resolved interop dependency graph.

**Runtime role**

It makes the real-client check reproducible across learner environments.

**Key code**

```toml
name = "redis"
```

**Statement understanding**

This file records dependency resolution; it does not explain runtime behavior and is intentionally grouped with test scaffolding.

### Verification evidence

Run all four focused test modules in `tests.txt`, then cumulatively build Stages 1–20 and compare the owned tree with commit `5419f99`.

### Durable takeaways

- Per-session command order and cross-session concurrency coexist.
- Exactly one writer owns each transport.
- EOF is a session-close event that must release domain waiters.
- Real-client and direct/TCP parity tests prove different boundaries.

### Explain it in your own words

How can BLPOP remain ordered within its connection without preventing RPUSH on another connection, and which object owns the eventual BLPOP reply?

### Textbook

This stage applies the reactor/pump pattern and structured ownership to an async server. Its bounded queues create explicit backpressure domains, while adapter parity is a refinement check: the transport implementation preserves the same abstract machine.

[Compare this stage on GitHub](https://github.com/system-in-miniature/mini-redis/compare/c088652...5419f99)

After finishing, run `python -m journey.tools.build_journey check 20` to verify the learner workspace.

[Complete reference patch / 完整参考补丁](https://github.com/system-in-miniature/mini-redis/blob/main/journey/stages/20-tcp-parity/stage.patch)
