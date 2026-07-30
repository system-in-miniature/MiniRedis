> **Language**: English | [简体中文](../zh/tutorial/10-protocol.md)

# Protocol Layer: Direct Calls, RESP2, and TCP

## Learning objectives

By the end of this chapter, you will be able to:

- separate command semantics from Direct and RESP2/TCP transport concerns;
- trace fragmented bytes through `RespDecoder`, `CommandRequest`, the shared
  executor, and reply encoding;
- explain the decoder's binary-safety and resource limits;
- configure redis-py for the supported RESP2 subset; and
- identify what interoperability evidence does and does not prove.

## One semantic core, two adapters

MiniRedis has two ways to submit commands:

1. a Python `DirectClient` accepts `CommandRequest`; and
2. a TCP session decodes RESP2 bytes into the same `CommandRequest`.

Neither adapter implements `SET`, transactions, expiry, persistence, or list
blocking. Both call `src/miniredis/runtime.py`,
`MiniRedis.submit_request`, which parses the request into the closed command
model and submits it to the same `CommandExecutor`.

This is the project's **Direct-first** boundary. The Direct adapter is not a
test double for a separate network implementation. It is the smallest adapter
over the real semantic core. TCP adds framing, buffering, sessions, and wire
encoding, but it must not acquire a second definition of command behavior.

The resulting flow is:

```text
Direct:
CommandRequest -> runtime parser -> executor -> domain Reply

RESP2/TCP:
bytes -> RespDecoder -> frame_to_request -> runtime parser
      -> executor -> domain Reply/Outbound -> encode_outbound -> bytes
```

`tests/adapters/test_direct_resp_parity.py::test_selected_sequence_has_state_and_reply_parity`
executes the same string, hash, list, set, sorted-set, type-error, and deletion
sequence through both routes. It compares encoded replies, logical state, and
commit sequence. That test is stronger than checking that TCP returns `PONG`:
it checks that the adapter did not drift from the core for a representative
sequence.

## RESP2 is a stream, not a packet format

TCP delivers an ordered byte stream. One `read` can contain half a command,
exactly one command, or several commands. Code that assumes one read equals
one request will fail under normal network behavior.

`src/miniredis/adapters/resp2.py`, `RespDecoder.feed`, appends incoming bytes to
an internal bytearray and repeatedly calls `_parse`. `_NeedMore` is internal
control flow: it means the current suffix could become a valid frame when more
bytes arrive. Complete frames are returned, and their consumed prefix is
removed from the buffer.

At end-of-stream, `RespDecoder.finish` calls `feed(b"")` and rejects any
remaining bytes as `RespProtocolError("truncated RESP frame")`. This separates
“incomplete for now” from “incomplete forever.”

RESP2 frames use a leading type byte:

| Prefix | MiniRedis frame | Example |
|---|---|---|
| `+` | `RespSimple` | `+OK\r\n` |
| `-` | `RespError` | `-ERR message\r\n` |
| `:` | `RespInteger` | `:2\r\n` |
| `$` | `RespBulk` | `$3\r\nabc\r\n` |
| `*` | `RespArray` | `*1\r\n$4\r\nPING\r\n` |

Commands must be non-empty arrays of non-null bulk strings.
`src/miniredis/adapters/resp2.py`, `frame_to_request`, keeps the first bulk
value as the command name and the rest as byte arguments. It does not decode
UTF-8, so keys and values remain binary-safe. Numeric interpretation happens
later in the command parser for commands that require it.

## Bounded parsing is part of the protocol contract

A streaming decoder holds untrusted client input. An attacker could announce a
gigantic bulk value, nest arrays deeply, or send an endless incomplete frame.
`RespLimits` bounds:

- total buffered bytes (`max_buffer`, default 1 MiB);
- one bulk value (`max_bulk`, default 1 MiB);
- array elements (`max_array`, default 1024); and
- nesting depth (`max_depth`, default 8).

`RespDecoder._parse` validates strict ASCII decimal lengths, null markers,
terminating CRLF, and known type bytes. Known malformed data fails
immediately. A syntactically plausible but incomplete frame waits for more
data until `finish`.

The decoder accepts RESP frame types needed for replies and nested arrays, but
`frame_to_request` deliberately narrows command input to bulk-string arrays.
This is an example of parsing in layers: the frame grammar is broader than the
server's command-request grammar.

## TCP session ownership and ordered replies

`src/miniredis/adapters/tcp.py`, `TcpServer.start`, calls
`asyncio.start_server`. Each accepted connection becomes `TcpSession` with one
reader task, one writer task, one `SessionEndpoint`, a decoder, and bounded
pending-frame state.

`TcpSession._read_loop` reads up to 65,536 bytes, feeds the decoder, converts
frames into requests, and calls `_submit_available`. It can submit several
coalesced commands before the first completes, subject to
`MiniRedisConfig.max_session_frames` and executor admission.

Pipelining is not a transaction. Requests may already be queued together, but
the executor processes each normally and other sessions can interleave at
mailbox boundaries. The session's endpoint and outbox preserve reply order.
`TcpSession._write_loop` is the only writer: it receives ordered `Outbound`
items, calls `encode_outbound`, writes, and drains.

This single-writer rule also covers Pub/Sub pushes and protocol errors.
Multiple tasks do not concurrently write arbitrary bytes to the same
`StreamWriter`.

On a protocol error, the reader offers a `ServerClosed` message through the
outbox, begins outbox close, permits a bounded best-effort drain, then closes
the session. On server shutdown, `TcpServer.quiesce` stops accepting, asks
readers to quiesce, closes sessions, and finally awaits listener closure. These
ownership rules are more than cleanup polish: leaked readers, writers, or
sessions could retain request state after the runtime claims to be closed.

## Mapping domain replies to RESP2

`src/miniredis/adapters/resp2.py`, `_reply_frame`, maps the closed domain reply
types:

- `Ok` -> simple string;
- `Bytes` -> bulk string, including null bulk;
- `Number` -> integer;
- `Items` -> array;
- `NullArray` -> null array; and
- `Failure` -> error line containing code and message.

`encode_outbound` extends the mapping to subscription acknowledgments, Pub/Sub
messages, and server-close messages. The core does not know that
`Bytes(b"v")` is `$1\r\nv\r\n`; the adapter does not know how `GET` found
`b"v"`.

This separation makes semantic changes testable through Direct before any
wire concerns. It also makes adapter parity explicit: a new domain reply type
cannot silently pass through TCP, because the closed match will raise until a
wire mapping is added and tested.

## redis-py interoperability

The repository's supported interoperability profile is intentionally narrow:
RESP2 and the implemented command subset. Current redis-py versions may use
RESP3 features or send client metadata commands by default, so the test in
`tests/interop/test_redis_py_resp2.py` configures:

```python
client = redis.asyncio.Redis(
    host=host,
    port=port,
    protocol=2,
    decode_responses=False,
    driver_info=None,
)
```

`protocol=2` selects the implemented wire version.
`decode_responses=False` preserves bytes and matches MiniRedis's binary-safe
surface. `driver_info=None` suppresses unsupported driver metadata behavior.

The smoke test verifies `PING`, `SET`, `INCR`, `HSET`, and `HGET`. Passing it
proves that redis-py can exchange this supported subset over an actual local
TCP connection. It does not prove RESP3 support, the complete Redis command
surface, TLS, ACLs, cluster redirection, production throughput, or compatibility
with every redis-py convenience API. See the
[RESP2/TCP adapter and command rows](../behavior-matrix.md).

## Compared with real Redis

Redis's networking and protocol machinery spans files such as `src/networking.c`,
the command table and command implementations, and protocol-specific code that
has evolved across versions. Production Redis supports RESP2 and RESP3,
thousands of command variants and options, authentication, connection
management, client tracking, replication protocol, cluster messages, TLS
builds, and highly optimized I/O paths.

MiniRedis keeps:

- incremental length-delimited parsing;
- binary-safe bulk strings;
- multiple frames per TCP read and frames split across reads;
- ordered pipelined replies;
- bounded input and output ownership; and
- a client-library interoperability checkpoint.

It omits RESP3, inline commands, authentication, TLS, cluster and replication
wire protocols, broad command compatibility, and production performance
optimizations. The point is not “RESP is simple, therefore Redis is simple.”
The point is that a small adapter can preserve a semantic core when its
framing, ordering, bounds, and lifecycle contracts are explicit.

## Hands-on lab 1: inspect fragmentation without TCP

Save as `/tmp/miniredis_resp2.py`:

```python
import asyncio

from miniredis import CommandRequest, MiniRedis
from miniredis.adapters.resp2 import (
    RespDecoder,
    encode_outbound,
    frame_to_request,
)


async def main():
    decoder = RespDecoder()
    print("after fragment 1:",
          decoder.feed(b"*2\r\n$3\r\nGE"))
    frames = decoder.feed(b"T\r\n$1\r\nk\r\n")
    print("after fragment 2:", frames)

    async with MiniRedis.open() as runtime:
        client = runtime.direct_client()
        await client.execute(CommandRequest(b"SET", (b"k", b"v")))
        reply = await client.execute(frame_to_request(frames[0]))
        print("domain reply:", reply)
        print("RESP2 bytes:", encode_outbound(reply))


asyncio.run(main())
```

Run:

```bash
uv run python /tmp/miniredis_resp2.py
```

Measured output:

```text
after fragment 1: ()
after fragment 2: (RespArray(items=(RespBulk(data=b'GET'), RespBulk(data=b'k'))),)
domain reply: Bytes(value=b'v')
RESP2 bytes: b'$1\r\nv\r\n'
```

The first feed has no complete frame, so it returns an empty tuple rather than
an error. The second feed completes exactly one request. `frame_to_request`
bridges to the same core used by Direct, and only the final step knows the
RESP2 byte representation.

## Hands-on lab 2: redis-py over local TCP

Run the repository interop test:

```bash
uv run pytest -q tests/interop/test_redis_py_resp2.py
```

Expected output in an environment that permits loopback TCP bind:

```text
.                                                                        [100%]
1 passed
```

**Needs runtime verification:** the documentation authoring sandbox denied
`bind(("127.0.0.1", 0))`, so this TCP command could not be completed there.
The attempted run reached `MiniRedis.start_tcp` and failed with:

```text
OSError: could not bind on any address out of [('127.0.0.1', 0)]
```

This is an environment restriction, not passing interoperability evidence.
Run the command on a normal local development host before treating the
redis-py path as verified for that host.

You can also run protocol tests that do not bind a socket:

```bash
uv run pytest -q tests/adapters/test_resp2_decode.py \
  tests/adapters/test_resp2_encode.py \
  tests/adapters/test_resp2_mapping.py
```

Expected repository result:

```text
23 passed
```

## Exercises

### 1. Understanding: incomplete versus invalid

Why does `decoder.feed(b"*2\r\n$3\r\nGE")` wait, while an unknown type byte
fails immediately?

??? note "Reference answer"

    The first byte sequence is a valid prefix of a RESP array containing a bulk
    string; more bytes can complete it, so `_NeedMore` retains the suffix. An
    unknown type byte cannot become valid by appending data, so `_parse` raises
    `RespProtocolError` immediately. At EOF, `finish` turns any retained valid
    prefix into a truncated-frame error.

### 2. Understanding: separate pipeline from transaction

What ordering does a TCP pipeline guarantee, and what transaction properties
does it not provide?

??? note "Reference answer"

    The session preserves result-slot/reply order for its submitted frames. The
    requests remain ordinary executor commands: another session may interleave,
    one failure does not roll back others, and the pipeline does not collapse
    writes into one `CommitBatch`. `MULTI`/`EXEC` is the separate atomic
    grouping mechanism.

### 3. Hands-on: add a fragmented binary command test

Task boundary: add one test to `tests/adapters/test_resp2_decode.py`. Feed a
bulk value containing `b"\xff\x00\r\n"` across at least three chunks. Do not
change `src/`.

Acceptance:

```bash
uv run pytest -q tests/adapters/test_resp2_decode.py
```

Assert that early feeds return no frame and the final `RespBulk.data` equals
the original bytes exactly.

??? note "Reference answer"

    Construct a correct `$4\r\n` bulk frame and split both its body and final
    CRLF. Call `feed` for each chunk and compare the final tuple with
    `RespBulk(b"\xff\x00\r\n")`. The expected diff is one test only; never
    decode the payload as text.

### 4. Hands-on: extend redis-py smoke coverage

Task boundary: in a temporary exercise branch, add one supported list command
round-trip to `tests/interop/test_redis_py_resp2.py`. Do not add a MiniRedis
command and do not change `src/`.

Acceptance: on a host that permits loopback bind,

```bash
uv run pytest -q tests/interop/test_redis_py_resp2.py
```

must pass with `protocol=2`, `decode_responses=False`, and `driver_info=None`.

??? note "Reference answer"

    Add `await client.rpush(b"jobs", b"a", b"b") == 2` followed by
    `await client.lrange(b"jobs", 0, -1) == [b"a", b"b"]`. Keep the existing
    client options. This tests only commands already documented in the
    behavior matrix and requires no production diff.

## Summary

MiniRedis ends where it began: one semantic core with explicit boundaries.
Direct requests and RESP2/TCP requests share parsing, planning, serialization,
durability, and replies; the network adapter owns only streaming frames,
bounded buffering, session lifecycle, and wire mapping. redis-py
interoperability is valuable evidence for the supported RESP2 subset, but it
is not evidence for every Redis protocol or operational feature. “Simplify
the protocol, preserve the semantics” is the series method: isolate the
mechanism under study, state the omitted production surface honestly, and tie
every claim to executable behavior.
