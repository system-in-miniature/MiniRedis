# MiniRedis Reference

MiniRedis is a Direct-first Python reference implementation of selected Redis
core semantics. A `DirectClient` parses binary-safe `CommandRequest` values
into a closed typed command union; one event-loop-owned executor prepares and
applies immutable full-key commits in mailbox order.

Phase 1 implements String, Hash, List, Set, and Sorted Set values. Its command
surface is:

- general: `PING`, `ECHO`, `DEL`, `EXISTS`, `TYPE`;
- String: `GET`, `SET [NX|XX] [EX seconds|PX ms]`, `INCR`, `INCRBY`;
- Hash: `HSET`, `HGET`, `HDEL`, `HGETALL`, `HINCRBY`;
- List: `LPUSH`, `RPUSH`, `LPOP`, `RPOP`, `LRANGE`;
- Set: `SADD`, `SREM`, `SISMEMBER`, `SMEMBERS`, `SINTER`;
- Sorted Set: `ZADD`, `ZREM`, `ZSCORE`, `ZRANK`, `ZRANGE`,
  `ZRANGEBYSCORE`;
- expiry: `EXPIRE`, `TTL`, `PTTL`, `PERSIST`.

Every accepted command is atomic, but Phase 1 does not implement transactions.
TTL deadlines are absolute wall-clock milliseconds with lazy and bounded active
cleanup. Memory limits use deterministic logical byte accounting and exact LRU;
they do not model Python allocator size or Redis's sampled eviction.
`SMEMBERS`, `SINTER`, and `HGETALL` result order is unspecified even though the
implementation emits deterministic test-friendly order.

Phase 1 does not claim `BLPOP`, Pub/Sub, persistence, replication, RESP, or TCP
support. Those boundaries are added only by their later implementation phases.

## Async semantics

MiniRedis implements BLPOP with executor-owned FIFO waiter indexes and
deterministic injected timers. Push plus all resulting blocked pops is one
CommitBatch; cancellation, timeout, disconnect, and shutdown are ordered
control messages.

Pub/Sub is exact-channel, ephemeral, bounded, and at-most-once. A full endpoint
is closed without blocking other clients. Runtime shutdown first quiesces
control producers, then executes one barrier and drains accepted output for a
bounded grace period. These mechanisms do not provide reliable queues,
delivery acknowledgment, replay, pattern subscriptions, or a general event
bus.

Run the complete suite from this directory:

```bash
uv sync --dev
uv run pytest -q
```
