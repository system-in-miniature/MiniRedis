# MiniRedis Tutorial

> **Language:** English | [简体中文](../zh/tutorial/index.md)

MiniRedis is an executable teaching kernel for the core mechanisms behind
Redis: typed commands, serialized execution, data structures, expiration,
eviction, persistence, replication, transactions, blocking operations, and
RESP2. Follow the chapters in order. Each chapter connects source code to a
measured experiment and explicitly states where MiniRedis differs from Redis.

MiniRedis 是一个可执行的 Redis 核心机制教学内核。本教程按依赖顺序讲解类型化命令、
串行执行、数据结构、过期、淘汰、持久化、复制、事务、阻塞操作与 RESP2；每章都把
源码、实测实验和与真实 Redis 的差异放在一起。

## Chapters

| # | Chapter | 中文章名 | Main mechanism |
|---:|---|---|---|
| 01 | [Meet MiniRedis](01-getting-started.md) | 认识 MiniRedis | Runtime lifecycle, Direct API, and the book map |
| 02 | [The Life of a Command](02-life-of-a-command.md) | 一条命令的一生 | Parse, plan, commit, apply, propagate, reply |
| 03 | [Data Types and the Command Surface](03-data-types.md) | 数据类型与命令面 | Five value models, per-type planners, `WRONGTYPE` |
| 04 | [Expiration](04-expiration.md) | 过期 | Lazy visibility, bounded active cleanup, replica safety |
| 05 | [Memory and Eviction](05-eviction.md) | 内存与淘汰 | Logical maxmemory, exact LRU, deterministic LFU |
| 06 | [Persistence I: AOF](06-persistence-aof.md) | 持久化 I：AOF | Append, fsync policies, recovery, tail repair |
| 07 | [Persistence II: Rewrite and Snapshots](07-rewrite-snapshot.md) | 持久化 II：重写与快照 | Base plus delta, atomic replace, staged recovery |
| 08 | [Replication](08-replication.md) | 复制 | Identity, offsets, backlog, partial/full synchronization |
| 09 | [Transactions and Blocking](09-transactions-blocking.md) | 事务与阻塞 | `MULTI`/`EXEC`, `WATCH`, one batch, blocking waiters |
| 10 | [Protocol Layer](10-protocol.md) | 协议层 | Direct adapter, RESP2/TCP, redis-py interoperability |

## How to use this tutorial

Run every command from the repository root after `uv sync --dev`. Read source
anchors as `relative/path.py`, `function_or_class`; links to the
[behavior matrix](../behavior-matrix.md) define compatibility boundaries.
Exercises that change code are intentionally presented as folded reference
diffs—the tutorial itself does not modify `src/`.

The finished project and its tests are the evidence base. If an experiment
differs from the printed measured output, first check the current branch,
configuration, and Python environment, then use the cited function and focused
pytest node to investigate.
