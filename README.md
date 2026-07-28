# MiniRedis

MiniRedis is a compact Redis-inspired reference project for learning typed
in-memory data structures, serialized command atomicity, expiration, eviction,
blocking operations, ordered adapter pipelines, Pub/Sub, persistence, and
asynchronous replication loss. It is not a production-compatible Redis
replacement.

## Why Direct-first

The primary API accepts binary-safe `CommandRequest` values. Direct calls and
RESP2/TCP calls meet at the same parser, typed command model, and serialized
executor; sockets and RESP frames do not own command semantics.

```text
DirectClient ───────────────┐
                            ├─> CommandRequest -> parser -> CommandExecutor
TCP -> RESP2 decoder ───────┘                         |
                                                       v
                         prepare -> AOF barrier -> apply CommitBatch
                                              -> ReplicaSink -> reply/outbox
```

## Supported commands

| Area | Commands |
|---|---|
| General | `PING`, `ECHO`, `DEL`, `EXISTS`, `TYPE`, `MULTI`, `EXEC`, `DISCARD`, `WATCH`, `UNWATCH` |
| String | `GET`, `MGET`, `SET [NX\|XX] [EX seconds\|PX ms]`, `MSET`, `INCR`, `DECR`, `INCRBY`, `COMPAREDEL`, `CHECKDECR` |
| Hash | `HSET`, `HGET`, `HDEL`, `HGETALL`, `HINCRBY` |
| List | `LPUSH`, `RPUSH`, `LPOP`, `RPOP`, `LRANGE`, `BLPOP`, `BRPOP` |
| Set | `SADD`, `SREM`, `SISMEMBER`, `SMEMBERS`, `SINTER` |
| Sorted Set | `ZADD`, `ZREM`, `ZSCORE`, `ZRANK`, `ZRANGE`, `ZRANGEBYSCORE` |
| Expiry | `EXPIRE`, `TTL`, `PTTL`, `PERSIST` |
| Pub/Sub | `SUBSCRIBE`, `UNSUBSCRIBE`, `PUBLISH` |

Keys, fields, members, values, and channels are `bytes`.

`DirectPipeline` and coalesced RESP2 frames submit independent commands in
order without waiting for each preceding result. Pipeline batches adapter
submission only. It does not provide atomic execution, rollback, or
cross-client isolation.

Transactions queue typed commands per connection. Queue-time errors make
`EXEC` fail with `EXECABORT`; runtime command errors retain their result slots
and do not roll back other successful commands. All successful mutations from
one `EXEC` cross AOF, replication, and crash recovery as one `CommitBatch`.
`WATCH` compares a persistent per-key revision, so create-delete cycles are
still observable.

## Quick start: Direct API

```python
import asyncio

from miniredis import CommandRequest, MiniRedis


async def main():
    async with MiniRedis.open() as redis:
        client = redis.direct_client()
        print(await client.execute(CommandRequest(b"SET", (b"k", b"1"))))
        print(await client.execute(CommandRequest(b"INCR", (b"k",))))

        pipeline = redis.direct_pipeline()
        pipeline.queue(CommandRequest(b"MSET", (b"a", b"1", b"b", b"2")))
        pipeline.queue(CommandRequest(b"MGET", (b"a", b"b")))
        print(await pipeline.execute())


asyncio.run(main())
```

## Optional RESP2 server and redis-py

```python
import asyncio

from miniredis import MiniRedis


async def main():
    async with MiniRedis.open() as redis:
        server = await redis.start_tcp("127.0.0.1", 0)
        print(server.address)
        await asyncio.Event().wait()


asyncio.run(main())
```

redis-py interoperability is a development smoke, forced to RESP2 with client
metadata disabled:

```bash
uv run pytest tests/interop/test_redis_py_resp2.py -q
```

## Deterministic reliability experiments

```bash
uv run pytest tests/reliability/test_commit_barrier.py -q
uv run pytest tests/reliability/test_restart.py -q
uv run pytest tests/reliability/test_lost_acked_write.py -q
uv run pytest tests/reliability/test_final_acceptance.py -q
```

These tests use injected clocks, schedulers, persistence failures, and replica
gates. The acknowledged-write-loss test deliberately pauses replica apply,
acknowledges a primary write, simulates primary crash without replica drain,
and promotes the lagging replica.

## Deterministic eviction metadata

`allkeys-lru` orders victims by exact access tick. `allkeys-lfu` projects each
frequency by halving it once per injected-time decay window, then orders by
effective frequency, access tick, and binary key. Successful client reads and
writes materialize one touch; eviction planning only reads projected values.
LFU/LRU metadata is operational state and resets to neutral values on restart
and full replica sync.

## Compatibility simplifications

- Five values use Python containers, not Redis internal encodings.
- Memory is a deterministic logical budget, not allocator/RSS accounting.
- LRU is exact and LFU is deterministic with binary-key tie-breaks, rather
  than Redis's sampled LRU and probabilistic LFU counter.
- AOF and snapshots are custom versioned formats, not Redis AOF or RDB.
- Replication is one in-process `ReplicaSink`, not a network protocol.
- RESP2/TCP is a bounded correctness adapter with ordered pipelined submission,
  not a throughput target.

See [docs/behavior-matrix.md](docs/behavior-matrix.md) for exact evidence.

## Non-goals

MiniRedis does not implement RESP3, inline protocol, Lua or a general script
VM, Streams, ACL, multiple databases, Modules, AOF rewrite, network
replication, PSYNC, backlog, heartbeat, ACK quorum, election, Sentinel,
Cluster, authentication, TLS, or production performance parity.

## Test and SLOC commands

```bash
uv sync --dev
uv run pytest -q
uv run python -m compileall -q src tests
uv run python tools/count_sloc.py
```

SLOC reports production Python, test Python, and Markdown documentation
separately. It is reported, never accepted or rejected by a size range.

## Course separation

This repository is the finished reference project. Course material is separate
and has not yet been generated; there is no `course/` directory or fixed
chapter count here.
