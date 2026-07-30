# MiniRedis Tutorial

> English quick start · [中文版](zh/index.md)

MiniRedis is a direct-first, binary-safe model of Redis command execution,
expiration, eviction, transactions, persistence, Pub/Sub, and resumable
replication. A typed command is planned without side effects, crosses one
durability boundary, and is then applied and propagated as an immutable commit
batch.

中文简述：MiniRedis 用可检查的执行计划与提交批次展示 Redis 的命令、过期、
淘汰、事务、持久化和复制机制；它是教学参考实现，不是完整 Redis 替代品。

## Install

```bash
git clone https://github.com/system-in-miniature/mini-redis.git
cd MiniRedis
uv sync
```

Python 3.12+ and [uv](https://docs.astral.sh/uv/) are required.

## First experiment

Run the deterministic LFU example:

```bash
uv run python examples/lfu_eviction.py
```

It creates equally small `hot` and `cold` keys, reads `hot` four times, then
adds a larger key. The final public `GET` observations show that `cold` is
missing while `hot` and `new` remain.

Continue with the [architecture guide](architecture.md) and its
[Redis mapping](redis-mapping.md).

For the full command set, adapters, compatibility boundary, and verification
commands, read the
[repository README](https://github.com/system-in-miniature/mini-redis/blob/main/README.md).
