> **Language**: [English](README.md) | 简体中文

# MiniRedis

[![CI](https://github.com/system-in-miniature/mini-redis/actions/workflows/ci.yml/badge.svg)](https://github.com/system-in-miniature/mini-redis/actions/workflows/ci.yml) [![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE) ![Python 3.12+](https://img.shields.io/badge/Python-3.12%2B-blue.svg)

MiniRedis 是一个紧凑的、受 Redis 启发的参考项目，用于学习类型化内存数据结构、串行化命令原子性、过期、淘汰、阻塞操作、有序适配器流水线、发布/订阅（Pub/Sub）、持久化以及异步复制中的数据丢失。它不是可用于生产环境、与 Redis 兼容的替代品。

## 学习 MiniRedis

**[进入在线学习站点 →](https://system-in-miniature.github.io/mini-redis/zh/)**

MiniRedis 不只是供人阅读的实现，也是一套可以真正走完的学习模型。你可以按自己的目标选择路径：

| 学习模式 | 你会获得什么 | 从这里开始 |
|---|---|---|
| 机制教程 | 按 Redis 机制、运行链路与所有权边界理解完成后的系统。 | [开始教程](https://system-in-miniature.github.io/mini-redis/zh/tutorial/) |
| 自主重建 | 通过 30 个浏览器原生 Stage 重建 MiniRedis，逐步理解测试契约、机制分组、关键语句与累计证据。 | [从 Stage 01 开始](https://system-in-miniature.github.io/mini-redis/zh/journey/) |
| Agent 带教 | 让 Codex 准备或续接指定 Stage，并互动带你完成实现。 | [查看使用教程](https://system-in-miniature.github.io/mini-redis/zh/agent-guided/) |

三种模式使用同一套实现与机制边界。测试负责把错误动机和完成证据变成可执行契约，但不会强制每一课采用测试优先叙事。

## 为什么采用 Direct-first

主 API 接受二进制安全的 `CommandRequest` 值。Direct 调用和 RESP2/TCP 调用汇合到同一个解析器、类型化命令模型和串行化执行器；套接字和 RESP 帧并不拥有命令语义。

```text
DirectClient ───────────────┐
                            ├─> CommandRequest -> parser -> CommandExecutor
TCP -> RESP2 decoder ───────┘                         |
                                                       v
                         prepare -> AOF barrier -> apply CommitBatch
                                              -> ReplicaSink -> reply/outbox
```

## 支持的命令

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

键、字段、成员、值和频道均为 `bytes`。

`DirectPipeline` 和合并的 RESP2 帧会按顺序提交彼此独立的命令，而不会等待每个前序结果。流水线（pipeline）只对适配器提交进行批处理，不提供原子执行、回滚或跨客户端隔离。

事务为每个连接排队类型化命令。入队时错误会使 `EXEC` 以 `EXECABORT` 失败；运行时命令错误会保留其结果槽位，并且不会回滚其他成功的命令。一次 `EXEC` 中的所有成功变更会作为一个 `CommitBatch` 共同经过 AOF、复制和崩溃恢复。`WATCH` 比较持久的逐键修订号，因此即使键经历“创建后删除”的循环也仍然可观测。

## 快速开始：Direct API

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

## 可选的 RESP2 服务器与 redis-py

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

redis-py 互操作性是一项开发冒烟测试，强制使用 RESP2，并禁用客户端元数据：

```bash
uv run pytest tests/interop/test_redis_py_resp2.py -q
```

## 确定性可靠性实验

```bash
uv run pytest tests/reliability/test_commit_barrier.py -q
uv run pytest tests/reliability/test_restart.py -q
uv run pytest tests/reliability/test_lost_acked_write.py -q
uv run pytest tests/reliability/test_final_acceptance.py -q
```

这些测试使用注入的时钟、调度器、持久化故障和副本门控。已确认写入丢失测试会刻意暂停副本应用，确认主节点上的一次写入，在不排空副本的情况下模拟主节点崩溃，然后提升这个落后的副本。

## 逻辑复制续传

每段主节点（Primary）历史都有一个逻辑复制 ID 和一个有界的 `CommitBatch` 积压区（backlog）。`replication_backlog_batches` 控制保留的完整批次数量；它独立于每条链路的 `replica_queue_limit`。

`ReplicaSink.disconnect()` 只保留最后一个已完整应用的 `(replication_id, seq)` 游标。手动重新挂接时，如果 ID 匹配且所有缺失批次仍在覆盖范围内，就使用部分同步（partial sync）。追赶批次会连续应用，然后才应用挂接期间实时到达的批次。身份不匹配、游标指向未来或积压区存在缺口时，会回退到完整快照。

重启和提升会创建新的主节点复制 ID 与空积压区，即使可见数据和序列号相同，也会隔离先前的历史。复制仍是异步的：主节点可能在副本应用写入之前就确认该写入，因此手动提升落后副本仍可能丢失已经确认的写入。

## 在线 AOF 重写

配置 AOF 后，`await redis.rewrite_aof()` 会在线压缩日志。执行器在同一个有序邮箱轮次中捕获逻辑存活的 `SnapshotImage`，并将其注册到写入器。旧 AOF 在基础镜像写入期间仍是权威数据源；之后提交的记录进入一个有界增量区。最终化会追加该增量、对临时文件执行 fsync、原子替换路径、对其父目录执行 fsync，然后切换写入器文件描述符。

```python
from pathlib import Path

from miniredis import MiniRedis
from miniredis.config import MiniRedisConfig


async with MiniRedis.open(
    MiniRedisConfig(aof_path=Path("appendonly.mraof"))
) as redis:
    outcome = await redis.rewrite_aof()
    print(outcome)
```

`aof_rewrite_delta_limit_bytes` 限制并发重写内存。溢出或重命名前的其他故障只会中止重写，并让旧 AOF 保持可写。重命名后的故障是终止性的，因为持久路径的所有权变得不明确。优雅关闭会完成活跃的重写；模拟崩溃则会中止尚未最终化的重写，并删除其临时文件。

恢复既支持旧版的纯批次 AOF 文件，也支持带有一个前置状态基线（state base）的重写文件。当快照和 AOF 基线同时存在时，它会选择较新的完整检查点（时间相同时 AOF 基线优先），并且只重放其后连续的批次。

## 确定性淘汰元数据

`allkeys-lru` 按精确访问时钟刻度排列淘汰候选。`allkeys-lfu` 在每个注入时间衰减窗口中将每个频率减半一次，然后按有效频率、访问时钟刻度和二进制键排序。成功的客户端读写各物化一次触碰；淘汰规划只读取投影值。LFU/LRU 元数据属于运行状态，重启和副本完整同步后会重置为中性值。

## 兼容性简化

- 五种值使用 Python 容器，而非 Redis 内部编码。
- 内存是确定性的逻辑预算，而非分配器/RSS 计量。
- LRU 是精确的；LFU 是确定性的，并以二进制键打破平局，而不是采用 Redis 的采样 LRU 和概率式 LFU 计数器。
- AOF、AOF 状态基线和快照采用自定义版本化格式，而不是 Redis AOF 或 RDB。
- 复制是一个进程内 `ReplicaSink`，使用逻辑批次游标，而不是网络协议或 Redis PSYNC 线协议实现。
- RESP2/TCP 是带有有序流水线提交的有界正确性适配器，而非吞吐量目标。

有关精确证据，请参阅 [docs/behavior-matrix.md](docs/behavior-matrix.md)。
有关引导式源码导览和 Redis 概念映射，请参阅
[docs/architecture.md](docs/architecture.md)。

## 可运行示例

`examples/` 中的脚本使用文档所述的运行时和命令 API；它们不依赖仅供测试使用的时钟、门控或执行器钩子：

```bash
uv run python examples/aof_crash_recovery.py
uv run python examples/replication_resync.py
uv run python examples/lfu_eviction.py
```

- `aof_crash_recovery.py` 使用 `AofPolicy.ALWAYS` 写入、模拟崩溃、从同一 AOF 重启，并验证已确认的值。
- `replication_resync.py` 对比可使用部分重新同步的短暂断连，以及游标已落后于有界积压区、因此需要完整快照的情况。
- `lfu_eviction.py` 将一个键变为热键、超过逻辑最大内存预算，并且只通过 `GET` 响应观察确定性的 LFU 淘汰对象。

## 非目标

MiniRedis 不实现 RESP3、内联协议、Lua 或通用脚本虚拟机、Streams、ACL、多数据库、Modules、网络复制、PSYNC 线协议兼容性、心跳、ACK 法定人数/`WAIT`、选举、Sentinel、Cluster、身份验证、TLS 或生产性能对等。

## 测试与 SLOC 命令

```bash
uv sync --dev
uv run pytest -q
uv run python -m compileall -q src tests
uv run python tools/count_sloc.py
```

SLOC 分别报告生产 Python、测试 Python 和 Markdown 文档。它只负责报告，绝不会根据规模范围判定接受或拒绝。

## 商标声明

MiniRedis 是独立的教学项目，与 Redis Ltd. 无隶属、背书或赞助关系。"Redis" 商标归其所有者所有。
