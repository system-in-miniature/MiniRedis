> **Language**: [English](../architecture.md) | 简体中文

# MiniRedis 架构指南

理解 MiniRedis 最容易的方式，是把它看作 Redis 机制的一个小型、显式模型，而不是源码兼容的重写。它让重要边界清晰可见：解析命令，规划其响应和变更，以持久方式发布一个提交单元，然后应用并传播该单元。

## 建议阅读顺序

1. 从 `commands/request.py`、`commands/parser.py` 和
   `commands/model.py` 开始。它们展示一个二进制安全请求如何转化为封闭的类型化命令。
2. 阅读 `core/reply.py`、`core/values.py` 和 `core/commit.py`。它们构成执行两侧共同使用的词汇：响应、存活值和冻结的传播记录。
3. 阅读 `core/planning.py` 和一个小型类型规划器，例如
   `core/hash_planner.py`，然后把 `core/planner.py` 作为其他规划器的路由索引。
4. 对照阅读 `core/database.py` 和 `core/executor.py`。执行器串行化决策；数据库应用由此产生的 `CommitBatch`。
5. 逐个加入横切机制：
   `core/expiration.py`、`core/eviction.py`、`core/blocking.py`、
   `core/transactions.py` 和 `core/pubsub.py`。
6. 沿着一次写入阅读 `persistence/aof.py`，然后阅读
   `persistence/snapshot.py`、`persistence/recovery.py` 和
   `persistence/codec.py`。
7. 最后阅读 `replication/backlog.py`、`replication/sink.py` 和
   `runtime.py`。它们组装可续传复制，并负责启动/关闭。
8. 到这时再阅读 Direct 和 RESP2/TCP 适配器。它们有意不拥有命令语义。

阅读时请同时打开 [`behavior-matrix.md`](behavior-matrix.md)，以查阅可观测的兼容性边界和测试证据。

## 一条命令从 API 到传播

```text
CommandRequest
  -> parser -> typed Command
  -> mailbox -> CommandExecutor
  -> CommandPlanner -> ExecutionPlan(reply, operations)
  -> PreparedCommit -> AOF durability barrier
  -> CommitBatch(seq, operations)
  -> Database.apply_batch
  -> replication backlog -> ReplicaSink
  -> reply / Pub/Sub outbox
```

规划（planning）没有副作用。响应可能在写入持久化之前已经确定，但执行器只有在提交屏障（commit barrier）和数据库应用完成后才会发布该成功响应。`EXEC` 可以在一个分叉副本上评估多个排队命令，但最多只会发出一个最终 `CommitBatch`，因此 AOF、复制和恢复把该事务视为一个传播单元。

## 与真实 Redis 的映射

下列标签描述教学关系：

- **等价（Equivalent）**：保留该机制重要的可观测契约，尽管实现语言和数据结构不同。
- **有意简化（Intentional simplification）**：保留课程要点，同时舍弃规模、线协议兼容性或生产优化。
- **语义相反（Semantically opposite）**：有意或当前表现为相反方向，不得将其推断为 Redis 的行为。

| MiniRedis module or term | Redis source / concept | Level | What to carry across |
|---|---|---|---|
| `commands/parser.py` + typed command model | command table, argument validation, and command implementations under Redis `src/` | Intentional simplification | MiniRedis freezes a small command grammar up front instead of reproducing Redis's full command metadata and option surface. |
| per-type planners + `CommandPlanner` | command implementations such as `t_string.c`, `t_hash.c`, `t_list.c`, `t_set.c`, `t_zset.c` | Intentional simplification | A planner is the conceptual command implementation, but it returns data instead of mutating the keyspace directly. |
| executor mailbox | Redis event loop's serialized command execution | Equivalent | One owner chooses a total order for commands and control events. MiniRedis uses an asyncio queue rather than Redis's event loop internals. |
| `ExecutionPlan` / `PreparedCommit` | command result plus the mutations Redis is about to make | Intentional simplification | The split makes the pre-commit decision inspectable; Redis generally mutates in place inside the command implementation. |
| `CommitBatch` | one atomic propagation unit sent to AOF and replicas | Equivalent | A transaction's successful writes stay grouped and ordered across durability, replication, and recovery. |
| `Database.apply_batch` | Redis keyspace mutation in `db.c` and command implementations | Intentional simplification | MiniRedis copies the table and recomputes logical usage for invariant clarity; Redis normally updates dictionaries and accounting in place. |
| transaction fork | `multi.c` queueing and `EXEC` execution | Intentional simplification | MiniRedis deep-copies the database to calculate a final batch; Redis executes queued commands against the live database. |
| `WATCH` inside `MULTI` marks the transaction dirty | Redis rejects `WATCH` inside `MULTI` without dirtying the transaction | Semantically opposite | In MiniRedis, the later `EXEC` aborts with `EXECABORT`; do not treat that as Redis behavior. |
| `ActiveExpireProducer` + expiry planning | `activeExpireCycle`, expiration logic historically associated with `expire.c` and `server.c` | Intentional simplification | Both combine lazy visibility with bounded active sampling; MiniRedis uses an injected scheduler and deterministic cursor-sized work. |
| replica expiry suppression | Redis replicas logically hide expired keys but wait for the primary's propagated deletion | Equivalent | A replica never creates its own expiry-delete commit, preventing its sequence from diverging from the primary. |
| `core/eviction.py` | `evict.c` maxmemory enforcement | Intentional simplification | The policy is enforced before committing the triggering write, but MiniRedis uses exact deterministic ordering and logical bytes instead of sampling and allocator memory. |
| `core/pubsub.py` + outbox | `pubsub.c` and per-client output buffers | Intentional simplification | Fan-out is bounded and at-most-once, with exact channels only and no pattern subscriptions. |
| AOF writer and rewrite | `aof.c`, append-only propagation, and background AOF rewrite | Intentional simplification | Base-plus-delta and atomic replacement preserve the rewrite lesson; the file format and execution model are custom. |
| snapshot and staged recovery | `rdb.c`, RDB loading, and AOF replay | Intentional simplification | A stable checkpoint is installed before contiguous later batches; MiniRedis's snapshot is not RDB compatible. |
| replication backlog | replication backlog in `replication.c` | Intentional simplification | Both retain bounded history for resume. MiniRedis retains whole logical batches rather than bytes in the PSYNC wire stream. |
| `ReplicaSink` | a replica link / client state in `replication.c` | Intentional simplification | It models async delivery, disconnect, cursor retention, catch-up, and failure, but stays in process and has no ACK or network protocol. |
| partial/full attachment | PSYNC partial resynchronization and full synchronization | Equivalent | Matching history identity plus complete backlog coverage permits catch-up; otherwise the replica receives a full snapshot. |
| `runtime.py` | Redis server lifecycle and subsystem ownership | Intentional simplification | Runtime owns startup, worker failure, draining, and zero-owner shutdown without reproducing Redis process management. |
| RESP2/TCP adapter | networking layer, RESP parsing, and client reply buffers | Intentional simplification | It proves protocol interoperability and ordered pipelines, not production throughput or RESP3 compatibility. |

## MiniRedis 术语

| Term | Meaning in this repository |
|---|---|
| **Direct-first** | Direct Python calls and RESP2/TCP requests share the same parser and executor; the network adapter does not define semantics. |
| **planner** | A pure command implementation that reads a database view and returns an `ExecutionPlan`. |
| **execution plan** | The immediate reply, proposed commit operations, access touches, and commit trigger for one command. |
| **prepared commit** | A non-empty, sequence-free set of operations ready to cross the durability barrier. |
| **commit batch** | The immutable, sequence-numbered unit applied locally, appended to AOF, retained in the backlog, and offered to a replica. |
| **commit barrier** | The durability boundary that must accept a batch before the executor applies it and releases success. |
| **mailbox turn** | One serialized executor step. Capturing related state in the same turn prevents commands from interleaving with that decision. |
| **sink** | The primary-side asynchronous link that installs or resumes a replica and applies offered batches in order. |
| **replication cursor** | `(replication_id, applied_seq)`, identifying both a history and the last fully applied batch. |
| **history fence** | A new replication ID after restart or promotion, preventing a cursor from an old primary history from being resumed accidentally. |
| **state base** | A complete logical database image at an AOF rewrite checkpoint, followed by later batch records. |
| **rewrite delta** | Commits that arrive after the rewrite base snapshot; they are appended before the new AOF becomes authoritative. |
| **logical memory** | A deterministic byte estimate used for teaching eviction, not Python allocator usage or RSS. |
| **logical expiry** | An expired key behaves as absent even if its physical entry remains until lazy or active deletion. |
| **owner** | A runtime component responsible for closing a task, timer, session, outbox, or replica link during shutdown. |

## 模型刻意付出更多成本的地方

`Database.apply_batch` 会暂存整个键表的浅拷贝，并重新计算总逻辑用量，因此一次写入的复杂度按键数量计为 O(N)。事务执行还会使用 `Database.fork()`，它深拷贝所有值，因此 `EXEC` 在应用最终批次之前的复杂度为 O(N + transaction work)。这些选择使失败的规划不会产生变更，也让不变量易于检查。真实 Redis 通常会原地修改其字典和内存计数器，因此普通单键更新的平均复杂度为 O(1)（不计命令自身的数据结构工作）。

这些复杂度差异是教学脚手架，而非性能论断。
