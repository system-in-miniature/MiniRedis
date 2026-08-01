# Stage 30 · Public parity and reading map / 公共一致性与阅读地图

<!-- journey: chapter=11 tests_added=0 -->

## English

### Goal

Close the rebuild by documenting every public/runtime module boundary and providing executable public-API scenarios for durability, LFU eviction, and replica resynchronization without adding another hidden mechanism.

### Deliverable files

- `examples/aof_crash_recovery.py`
- `examples/lfu_eviction.py`
- `examples/replication_resync.py`
- `src/miniredis/__init__.py`
- `src/miniredis/adapters/direct.py`
- `src/miniredis/adapters/resp2.py`
- `src/miniredis/adapters/tcp.py`
- `src/miniredis/clock.py`
- `src/miniredis/commands/model.py`
- `src/miniredis/commands/parser.py`
- `src/miniredis/commands/request.py`
- `src/miniredis/config.py`
- `src/miniredis/core/blocking.py`
- `src/miniredis/core/commit.py`
- `src/miniredis/core/database.py`
- `src/miniredis/core/eviction.py`
- `src/miniredis/core/expiration.py`
- `src/miniredis/core/frequency.py`
- `src/miniredis/core/hash_planner.py`
- `src/miniredis/core/list_planner.py`
- `src/miniredis/core/mailbox.py`
- `src/miniredis/core/outbound.py`
- `src/miniredis/core/planner.py`
- `src/miniredis/core/planning.py`
- `src/miniredis/core/pubsub.py`
- `src/miniredis/core/reply.py`
- `src/miniredis/core/set_planner.py`
- `src/miniredis/core/transactions.py`
- `src/miniredis/core/ttl_planner.py`
- `src/miniredis/core/values.py`
- `src/miniredis/core/zset_planner.py`
- `src/miniredis/persistence/aof.py`
- `src/miniredis/persistence/codec.py`
- `src/miniredis/persistence/recovery.py`
- `src/miniredis/persistence/snapshot.py`
- `src/miniredis/replication/backlog.py`

### The problem at this point

All mechanisms exist, but a completed learning model also needs a stable map from public behavior to internal ownership. Without module contracts and runnable scenarios, learners can pass isolated tests yet remain unsure which component owns ordering, durability, expiry, transactions, or resynchronization—and which differences from Redis are deliberate simplifications rather than accidental gaps.

### Failure preview

A documentation-only final stage can become empty commentary disconnected from executable behavior. An example that imports test helpers or debug internals does not prove the public surface. A module description that claims byte-level PSYNC, allocator memory accounting, or Redis's exact in-place algorithms would overstate parity. Conversely, listing files individually without grouping hides the few architectural boundaries that matter.

### Test contract

No test file changes in this stage. The focused final-acceptance suite is deliberately reused as the locked whole-system evidence: it activates commands, blocking waiters, TCP, Pub/Sub, AOF, snapshots, replication, statistics, and shutdown, then proves durable artifacts and zero remaining owners. The new examples add public demonstrations; they do not replace that contract.

### Basic concepts

Public parity means the documented API and executable examples exercise the same production paths already locked by the Journey. Reading-map parity means every module states its responsibility and relevant Redis correspondence without claiming identical internals. Deliberate MiniRedis trade-offs include logical rather than allocator memory, exact rather than sampled eviction, staged-copy atomicity, whole-batch rather than byte-ring backlog, and an in-process replica transport.

### Why this mechanism is necessary

A polished miniature should be understandable as a system, not only as a pile of passing mechanisms. Module-level contracts let learners navigate from a behavior to its owner, while public-only examples prove the facade is sufficient for meaningful experiments. Explicit trade-offs prevent “small” from being mistaken for “identical implementation.”

### Runtime mental model

The final system has four boundaries: adapters turn Direct/TCP inputs into typed requests and ordered outbounds; the core single-writer plans and applies atomic batches; persistence publishes and recovers durable checkpoints/logs; replication transfers the same batches under epoch/cursor fencing. The examples enter only through `MiniRedis`, `MiniRedisConfig`, `CommandRequest`, and the documented replica sink, then observe replies and status.

### Mechanism blocks

<!-- journey-file: examples/aof_crash_recovery.py -->
<!-- journey-file: examples/lfu_eviction.py -->
<!-- journey-file: examples/replication_resync.py -->
#### Executable public scenarios

The three examples demonstrate acknowledged AOF recovery after simulated crash, exact allkeys-LFU victim behavior, and short-disconnect partial resume versus backlog-gap full fallback. They use production lifecycle and public replies rather than test fixtures.

<!-- journey-file: src/miniredis/__init__.py -->
<!-- journey-file: src/miniredis/adapters/direct.py -->
<!-- journey-file: src/miniredis/adapters/resp2.py -->
<!-- journey-file: src/miniredis/adapters/tcp.py -->
<!-- journey-file: src/miniredis/clock.py -->
<!-- journey-file: src/miniredis/commands/model.py -->
<!-- journey-file: src/miniredis/commands/parser.py -->
<!-- journey-file: src/miniredis/commands/request.py -->
<!-- journey-file: src/miniredis/config.py -->
#### Public, adapter, and command boundary

These modules document the Direct-first facade, TCP session pumps, binary-safe RESP2 codec, injectable time, immutable request/command language, strict parser, and validated runtime policy inputs. Grouping them shows that transport syntax ends before semantic planning begins.

<!-- journey-file: src/miniredis/core/blocking.py -->
<!-- journey-file: src/miniredis/core/commit.py -->
<!-- journey-file: src/miniredis/core/database.py -->
<!-- journey-file: src/miniredis/core/eviction.py -->
<!-- journey-file: src/miniredis/core/expiration.py -->
<!-- journey-file: src/miniredis/core/frequency.py -->
<!-- journey-file: src/miniredis/core/hash_planner.py -->
<!-- journey-file: src/miniredis/core/list_planner.py -->
<!-- journey-file: src/miniredis/core/mailbox.py -->
<!-- journey-file: src/miniredis/core/outbound.py -->
<!-- journey-file: src/miniredis/core/planner.py -->
<!-- journey-file: src/miniredis/core/planning.py -->
<!-- journey-file: src/miniredis/core/pubsub.py -->
<!-- journey-file: src/miniredis/core/reply.py -->
<!-- journey-file: src/miniredis/core/set_planner.py -->
<!-- journey-file: src/miniredis/core/transactions.py -->
<!-- journey-file: src/miniredis/core/ttl_planner.py -->
<!-- journey-file: src/miniredis/core/values.py -->
<!-- journey-file: src/miniredis/core/zset_planner.py -->
#### Core state-machine reading map

These module contracts identify the immutable propagation unit, staged database owner, pure planners, waiter and Pub/Sub registries, mailbox/outbox ordering, transaction workspace, expiry/LRU/LFU policies, typed replies/values, and per-data-type planning. Key comments name deliberate differences: O(N) staged copies and exact deterministic eviction instead of Redis's in-place and sampled production algorithms.

<!-- journey-file: src/miniredis/persistence/aof.py -->
<!-- journey-file: src/miniredis/persistence/codec.py -->
<!-- journey-file: src/miniredis/persistence/recovery.py -->
<!-- journey-file: src/miniredis/persistence/snapshot.py -->
<!-- journey-file: src/miniredis/replication/backlog.py -->
#### Durability and replication reading map

These modules document framed logical records, AOF base-plus-delta rewrite, atomic snapshot publication, strict recovery composition, and bounded whole-batch replication backlog. Comments connect them to Redis AOF/PSYNC ideas while explicitly retaining MiniRedis's custom record format and batch-level in-process model.

### Verification evidence

Run the focused final-acceptance suite, execute all three examples, cumulatively build Stages 1–30, and require byte parity for every Journey-owned source/example/test path with endpoint `8151fae`.

### Durable takeaways

- The final stage maps existing mechanisms; it does not invent a new one.
- Examples use only public production paths.
- Redis correspondence and MiniRedis trade-offs are both explicit.
- Full parity means tests, examples, source tree, and documented ownership agree.

### Explain it in your own words

Trace one SET with AOF and replication from public request to recovery, naming the owner at each boundary and one deliberate way MiniRedis differs from Redis.

### Textbook

This is an architectural closeout: executable examples serve as usage-level proofs, module contracts form a responsibility map, and explicit abstraction gaps define the miniature's model boundary.

## 中文

### 目标

通过记录每个 Public/Runtime Module Boundary，并提供 Durability、LFU Eviction、Replica Resynchronization 的可执行 Public-API 场景完成重建，而不再加入隐藏机制。

### 交付文件

- `examples/aof_crash_recovery.py`
- `examples/lfu_eviction.py`
- `examples/replication_resync.py`
- `src/miniredis/__init__.py`
- `src/miniredis/adapters/direct.py`
- `src/miniredis/adapters/resp2.py`
- `src/miniredis/adapters/tcp.py`
- `src/miniredis/clock.py`
- `src/miniredis/commands/model.py`
- `src/miniredis/commands/parser.py`
- `src/miniredis/commands/request.py`
- `src/miniredis/config.py`
- `src/miniredis/core/blocking.py`
- `src/miniredis/core/commit.py`
- `src/miniredis/core/database.py`
- `src/miniredis/core/eviction.py`
- `src/miniredis/core/expiration.py`
- `src/miniredis/core/frequency.py`
- `src/miniredis/core/hash_planner.py`
- `src/miniredis/core/list_planner.py`
- `src/miniredis/core/mailbox.py`
- `src/miniredis/core/outbound.py`
- `src/miniredis/core/planner.py`
- `src/miniredis/core/planning.py`
- `src/miniredis/core/pubsub.py`
- `src/miniredis/core/reply.py`
- `src/miniredis/core/set_planner.py`
- `src/miniredis/core/transactions.py`
- `src/miniredis/core/ttl_planner.py`
- `src/miniredis/core/values.py`
- `src/miniredis/core/zset_planner.py`
- `src/miniredis/persistence/aof.py`
- `src/miniredis/persistence/codec.py`
- `src/miniredis/persistence/recovery.py`
- `src/miniredis/persistence/snapshot.py`
- `src/miniredis/replication/backlog.py`

### 当前遇到的问题

所有机制已经存在，但完整 Learning Model 还需要从 Public Behavior 到 Internal Ownership 的稳定地图。缺少 Module Contract 与 Runnable Scenario 时，学习者即使通过孤立 Test，仍不清楚 Ordering、Durability、Expiry、Transaction、Resynchronization 分别由谁持有，也无法区分 MiniRedis 与 Redis 的差异是有意简化还是意外缺口。

### 先看会坏在哪里

Documentation-only Final Stage 可能变成与 Executable Behavior 脱节的空评论。导入 Test Helper 或 Debug Internal 的 Example 不能证明 Public Surface。声称 Byte-level PSYNC、Allocator Memory Accounting 或 Redis Exact In-place Algorithm 会夸大 Parity。反过来，逐个平铺文件又会隐藏少数真正重要的 Architectural Boundary。

### 测试契约

本阶段没有修改 Test File。聚焦 Final-acceptance Suite 被有意复用为锁定的 Whole-system Evidence：同时激活 Command、Blocking Waiter、TCP、Pub/Sub、AOF、Snapshot、Replication、Stats 与 Shutdown，再证明 Durable Artifact 与 Zero Remaining Owner。新 Example 提供 Public Demonstration，但不替代该契约。

### 基本概念

Public Parity 表示 Documented API 与 Executable Example 经过前面 Journey 已锁定的同一 Production Path。Reading-map Parity 表示每个 Module 都说明 Responsibility 与对应 Redis Concept，却不声称内部完全相同。MiniRedis 的有意取舍包括 Logical 而非 Allocator Memory、Exact 而非 Sampled Eviction、Staged-copy Atomicity、Whole-batch 而非 Byte-ring Backlog，以及 In-process Replica Transport。

### 为什么需要这个机制

Polished Miniature 应该作为系统可理解，而不只是通过测试的机制堆。Module-level Contract 让学习者从 Behavior 找到 Owner，Public-only Example 证明 Facade 足以完成有意义实验。显式 Trade-off 防止“小型实现”被误解为“内部完全相同”。

### 运行时心智模型

最终系统有四个边界：Adapter 把 Direct/TCP Input 变成 Typed Request 与 Ordered Outbound；Core Single-writer 规划并应用 Atomic Batch；Persistence 发布并恢复 Durable Checkpoint/Log；Replication 在 Epoch/Cursor Fencing 下传输同一 Batch。Example 只经过 `MiniRedis`、`MiniRedisConfig`、`CommandRequest` 与 Documented Replica Sink，再观察 Reply/Status。

### 机制板块

<!-- journey-file: examples/aof_crash_recovery.py -->
<!-- journey-file: examples/lfu_eviction.py -->
<!-- journey-file: examples/replication_resync.py -->
#### 可执行公共场景

三个 Example 分别演示 Simulated Crash 后 Acknowledged AOF Recovery、Exact allkeys-LFU Victim，以及 Short-disconnect Partial Resume 与 Backlog-gap Full Fallback。它们使用 Production Lifecycle 与 Public Reply，不依赖 Test Fixture。

<!-- journey-file: src/miniredis/__init__.py -->
<!-- journey-file: src/miniredis/adapters/direct.py -->
<!-- journey-file: src/miniredis/adapters/resp2.py -->
<!-- journey-file: src/miniredis/adapters/tcp.py -->
<!-- journey-file: src/miniredis/clock.py -->
<!-- journey-file: src/miniredis/commands/model.py -->
<!-- journey-file: src/miniredis/commands/parser.py -->
<!-- journey-file: src/miniredis/commands/request.py -->
<!-- journey-file: src/miniredis/config.py -->
#### 公共接口、Adapter 与 Command Boundary

这些 Module 记录 Direct-first Facade、TCP Session Pump、Binary-safe RESP2 Codec、Injectable Time、Immutable Request/Command Language、Strict Parser 与 Validated Runtime Policy Input。合并理解能看出 Transport Syntax 在 Semantic Planning 前结束。

<!-- journey-file: src/miniredis/core/blocking.py -->
<!-- journey-file: src/miniredis/core/commit.py -->
<!-- journey-file: src/miniredis/core/database.py -->
<!-- journey-file: src/miniredis/core/eviction.py -->
<!-- journey-file: src/miniredis/core/expiration.py -->
<!-- journey-file: src/miniredis/core/frequency.py -->
<!-- journey-file: src/miniredis/core/hash_planner.py -->
<!-- journey-file: src/miniredis/core/list_planner.py -->
<!-- journey-file: src/miniredis/core/mailbox.py -->
<!-- journey-file: src/miniredis/core/outbound.py -->
<!-- journey-file: src/miniredis/core/planner.py -->
<!-- journey-file: src/miniredis/core/planning.py -->
<!-- journey-file: src/miniredis/core/pubsub.py -->
<!-- journey-file: src/miniredis/core/reply.py -->
<!-- journey-file: src/miniredis/core/set_planner.py -->
<!-- journey-file: src/miniredis/core/transactions.py -->
<!-- journey-file: src/miniredis/core/ttl_planner.py -->
<!-- journey-file: src/miniredis/core/values.py -->
<!-- journey-file: src/miniredis/core/zset_planner.py -->
#### 核心状态机阅读地图

这些 Module Contract 标出 Immutable Propagation Unit、Staged Database Owner、Pure Planner、Waiter/PubSub Registry、Mailbox/Outbox Ordering、Transaction Workspace、Expiry/LRU/LFU Policy、Typed Reply/Value 与 Per-data-type Planning。关键 Comment 说明有意差异：O(N) Staged Copy 与 Exact Deterministic Eviction，而不是 Redis 的 In-place 与 Sampled Production Algorithm。

<!-- journey-file: src/miniredis/persistence/aof.py -->
<!-- journey-file: src/miniredis/persistence/codec.py -->
<!-- journey-file: src/miniredis/persistence/recovery.py -->
<!-- journey-file: src/miniredis/persistence/snapshot.py -->
<!-- journey-file: src/miniredis/replication/backlog.py -->
#### Durability 与 Replication 阅读地图

这些 Module 记录 Framed Logical Record、AOF Base-plus-delta Rewrite、Atomic Snapshot Publication、Strict Recovery Composition 与 Bounded Whole-batch Replication Backlog。Comment 把它们连接到 Redis AOF/PSYNC Concept，同时明确保留 MiniRedis Custom Record Format 与 Batch-level In-process Model。

### 验证证据

运行聚焦 Final-acceptance Suite，执行全部三个 Example，累计构建 Stage 1–30，并要求每个 Journey-owned Source/Example/Test Path 与端点 `8151fae` 字节一致。

### 需要真正记住的内容

- Final Stage 映射既有机制，不发明新机制。
- Example 只使用 Public Production Path。
- Redis Correspondence 与 MiniRedis Trade-off 都显式说明。
- Full Parity 表示 Test、Example、Source Tree 与 Documented Ownership 一致。

### 用自己的话讲清楚

从 Public Request 到 Recovery 追踪一次带 AOF 与 Replication 的 SET，指出每个 Boundary 的 Owner，以及 MiniRedis 与 Redis 的一个有意差异。

### 教材

这是 Architectural Closeout：Executable Example 充当 Usage-level Proof，Module Contract 形成 Responsibility Map，显式 Abstraction Gap 定义 Miniature Model Boundary。
