# Stage 23 · Transactions and atomic functions / 事务与原子函数

<!-- journey: chapter=9 tests_added=7 -->

## English

### Goal

Queue commands per session, validate WATCH revisions, speculatively execute EXEC on a fork, and publish all successful mutations as one durable commit while preserving one reply slot per queued command.

### Deliverable files

- `src/miniredis/adapters/resp2.py`
- `src/miniredis/commands/model.py`
- `src/miniredis/commands/parser.py`
- `src/miniredis/core/blocking.py`
- `src/miniredis/core/database.py`
- `src/miniredis/core/executor.py`
- `src/miniredis/core/planning.py`
- `src/miniredis/core/reply.py`
- `src/miniredis/core/transactions.py`
- `tests/adapters/test_resp2_mapping.py`
- `tests/contract/test_atomic_functions.py`
- `tests/mechanisms/test_transactions.py`
- `tests/mechanisms/test_watch.py`
- `tests/reliability/test_transaction_commit.py`
- `tests/unit/commands/test_parser.py`
- `tests/unit/core/test_domain_types.py`

### The problem at this point

A pipeline admits several independent commands; MULTI/EXEC instead promises that queued state transitions become one published commit. Runtime errors still need result slots, parse/admission errors must dirty the transaction before EXEC, WATCH must detect even create-then-delete cycles, and queued pushes must not reserve the same blocked waiter twice.

### Failure preview

Executing queued commands directly on the live database exposes partial state before durability. Comparing only current values misses a key changed and restored or created then deleted. Stopping at the first runtime error loses later result slots. Applying each queued write as its own AOF batch breaks atomic recovery. Reusing a waiter during speculative pushes can deliver two items to one blocked request.

### Test contract

<!-- journey-file: tests/mechanisms/test_transactions.py -->
#### `tests/mechanisms/test_transactions.py`

Locks MULTI queueing, DISCARD, dirty transaction abort, disallowed blocking commands, ordered runtime-error slots, empty EXEC, one commit, and one waiter reservation. The key evidence is `debug_commit_seq == before + 1` after several queued writes; failure means EXEC leaked intermediate transitions.

<!-- journey-file: tests/mechanisms/test_watch.py -->
#### `tests/mechanisms/test_watch.py`

Locks UNWATCH cleanup, WATCH restrictions, session-close cleanup, and detection of create-then-delete through revisions. `EXEC == NullArray()` means optimistic validation failed without applying the queue.

<!-- journey-file: tests/contract/test_atomic_functions.py -->
#### `tests/contract/test_atomic_functions.py`

Locks COMPAREDEL and CHECKDECR across missing/wrong-type/mismatch/insufficient cases, TTL preservation, single-winner concurrency, and transaction queueing. Failed preconditions must not allocate a commit.

<!-- journey-file: tests/reliability/test_transaction_commit.py -->
#### `tests/reliability/test_transaction_commit.py`

Locks two queued writes as one AOF batch and one recovered sequence. Failure means live atomicity and durable atomicity disagree.

<!-- journey-file: tests/unit/core/test_domain_types.py -->
#### `tests/unit/core/test_domain_types.py`

Locks monotonic per-key revisions across deletion and deep database fork independence, including access and logical-usage metadata.

<!-- journey-file: tests/unit/commands/test_parser.py -->
#### `tests/unit/commands/test_parser.py`

Locks exact typed transaction/atomic commands and rejects invalid arity or non-positive CHECKDECR amounts before executor state changes.

<!-- journey-file: tests/adapters/test_resp2_mapping.py -->
#### `tests/adapters/test_resp2_mapping.py`

Locks `NullArray()` as RESP2 `*-1`, distinguishing WATCH abort from an empty successful EXEC array.

### Basic concepts

Transaction state is per session: active, dirty, queued commands, and watched key revisions. WATCH is optimistic concurrency control. A database fork is a private speculative state. EXEC plans each command against that evolving fork, collects replies and operations, then sends one combined prepared commit through the existing durability barrier. Null array means optimistic abort; empty array means successful no-op.

### Why this mechanism is necessary

The single executor already provides a serialization point, but durability and runtime-error semantics require more than running a Python loop on live state. Speculation prevents partial publication, revisions fence ABA-like value histories, and one combined commit preserves the same atomic boundary in memory, AOF, recovery, and replication.

### Runtime mental model

MULTI creates session state. Later allowed commands are parsed normally but queued and return QUEUED. Parse errors or disallowed commands mark the state dirty. EXEC first checks dirty and WATCH revisions, forks the database, plans queued commands in order, applies successful operations only to the fork, reserves wakeups once, and gathers result slots. If operations exist, one durability barrier publishes their combined tuple; only then are touches, waiter replies, and EXEC results settled.

### Mechanism blocks

<!-- journey-file: src/miniredis/commands/model.py -->
#### `src/miniredis/commands/model.py`

Defines immutable transaction controls plus COMPAREDEL/CHECKDECR and places each in the exhaustive dataset-mutation trait partition.

<!-- journey-file: src/miniredis/commands/parser.py -->
#### `src/miniredis/commands/parser.py`

Freezes keys/expected values/positive amounts and rejects malformed controls before they enter transaction state.

<!-- journey-file: src/miniredis/core/reply.py -->
#### `src/miniredis/core/reply.py`

Adds `NullArray` as a semantic reply distinct from `Items(())`.

<!-- journey-file: src/miniredis/adapters/resp2.py -->
#### `src/miniredis/adapters/resp2.py`

Maps that semantic abort reply to RESP2 null array `*-1\r\n`, preserving the domain/wire separation.

<!-- journey-file: src/miniredis/core/transactions.py -->
#### `src/miniredis/core/transactions.py`

Holds compact per-session transaction state and the private execution workspace: fork, combined operations, ordered replies, touches, wakeups, and reserved waiter IDs.

<!-- journey-file: src/miniredis/core/database.py -->
#### `src/miniredis/core/database.py`

Maintains mutation revisions independently of current key presence and creates a deep fork with runtime metadata.

```python
staged_revision_clock += 1
staged_key_revisions[operation.key] = staged_revision_clock
```

Every attempted operation advances history, so create-delete cannot look unchanged merely because the key is absent again.

<!-- journey-file: src/miniredis/core/executor.py -->
#### `src/miniredis/core/executor.py`

Owns transaction routing, dirty/watch validation, speculative ordered planning, one prepared commit, and final session cleanup.

```python
workspace = TransactionWorkspace(self.database.fork())
```

The live database remains untouched while queued commands observe prior speculative writes and produce their individual replies.

<!-- journey-file: src/miniredis/core/blocking.py -->
#### `src/miniredis/core/blocking.py`

Accepts a reserved-waiter set so multiple queued pushes cannot claim the same blocked request while planning on a fork.

<!-- journey-file: src/miniredis/core/planning.py -->
#### `src/miniredis/core/planning.py`

Implements COMPAREDEL and CHECKDECR as ordinary pure plans, retaining TTL and producing no operation when a precondition fails.

### Verification evidence

Run all seven focused modules in `tests.txt`, cumulatively build Stages 1–23, and require owned-tree parity with `b195a43`.

### Durable takeaways

- Pipeline ordering is not transaction atomicity.
- EXEC speculates on a fork and publishes one combined commit.
- WATCH compares mutation revisions, not just current values.
- Runtime errors occupy result slots; queue-time errors abort EXEC.

### Explain it in your own words

Why can EXEC continue after a queued WRONGTYPE result but must abort after a parse error? Why does WATCH require a revision clock even when a key ends absent?

### Textbook

MULTI/EXEC is optimistic transactional execution inside a serialized state machine. The fork is a private write set, the revision map supplies validation versions, and the durability barrier is the atomic commit point.

## 中文

### 目标

按 Session 排队 Command，校验 WATCH Revision，在 Fork 上推测执行 EXEC，并把所有成功 Mutation 发布为一个 Durable Commit，同时为每条 Queued Command 保留一个 Reply Slot。

### 交付文件

- `src/miniredis/adapters/resp2.py`
- `src/miniredis/commands/model.py`
- `src/miniredis/commands/parser.py`
- `src/miniredis/core/blocking.py`
- `src/miniredis/core/database.py`
- `src/miniredis/core/executor.py`
- `src/miniredis/core/planning.py`
- `src/miniredis/core/reply.py`
- `src/miniredis/core/transactions.py`
- `tests/adapters/test_resp2_mapping.py`
- `tests/contract/test_atomic_functions.py`
- `tests/mechanisms/test_transactions.py`
- `tests/mechanisms/test_watch.py`
- `tests/reliability/test_transaction_commit.py`
- `tests/unit/commands/test_parser.py`
- `tests/unit/core/test_domain_types.py`

### 当前遇到的问题

Pipeline 接纳多条独立 Command；MULTI/EXEC 则承诺 Queued State Transition 成为一个 Published Commit。Runtime Error 仍需要结果槽，Parse/Admission Error 必须在 EXEC 前把 Transaction 标 Dirty，WATCH 必须检测 Create-then-delete，Queued Push 也不能重复 Reserve 同一个 Blocked Waiter。

### 先看会坏在哪里

直接在 Live Database 上执行 Queue 会在 Durability 前暴露部分状态。只比较当前 Value 会漏掉 Key 被修改后恢复或创建后删除。遇到第一个 Runtime Error 就停止会丢失后续结果槽。把每个 Queued Write 写成独立 AOF Batch 会破坏原子恢复。推测 Push 时重用 Waiter 可能向一个 Blocked Request 交付两个 Item。

### 测试契约

<!-- journey-file: tests/mechanisms/test_transactions.py -->
#### `tests/mechanisms/test_transactions.py`

锁定 MULTI Queueing、DISCARD、Dirty Abort、禁用 Blocking Command、有序 Runtime-error Slot、Empty EXEC、一次 Commit 与一次 Waiter Reservation。多条排队写入后 `debug_commit_seq == before + 1` 是关键证据；失败说明 EXEC 泄漏中间 Transition。

<!-- journey-file: tests/mechanisms/test_watch.py -->
#### `tests/mechanisms/test_watch.py`

锁定 UNWATCH Cleanup、WATCH 限制、Session-close Cleanup，以及通过 Revision 检测 Create-then-delete。`EXEC == NullArray()` 表示 Optimistic Validation 失败且 Queue 未应用。

<!-- journey-file: tests/contract/test_atomic_functions.py -->
#### `tests/contract/test_atomic_functions.py`

锁定 COMPAREDEL/CHECKDECR 的 Missing/Wrong-type/Mismatch/Insufficient、TTL 保留、并发 Single-winner 与 Transaction Queueing。Precondition 失败不能分配 Commit。

<!-- journey-file: tests/reliability/test_transaction_commit.py -->
#### `tests/reliability/test_transaction_commit.py`

锁定两条排队写入成为一个 AOF Batch 与一个恢复 Sequence。失败说明 Live Atomicity 与 Durable Atomicity 不一致。

<!-- journey-file: tests/unit/core/test_domain_types.py -->
#### `tests/unit/core/test_domain_types.py`

锁定删除后仍单调的 Per-key Revision 与 Deep Database Fork 独立性，包括 Access 与 Logical-usage Metadata。

<!-- journey-file: tests/unit/commands/test_parser.py -->
#### `tests/unit/commands/test_parser.py`

锁定精确 Typed Transaction/Atomic Command，并在 Executor State 改变前拒绝非法 Arity 或非正 CHECKDECR Amount。

<!-- journey-file: tests/adapters/test_resp2_mapping.py -->
#### `tests/adapters/test_resp2_mapping.py`

锁定 `NullArray()` 对应 RESP2 `*-1`，从而区分 WATCH Abort 与 Empty Successful EXEC Array。

### 基本概念

Transaction State 按 Session 持有：Active、Dirty、Queued Commands、Watched Key Revisions。WATCH 是 Optimistic Concurrency Control。Database Fork 是私有推测状态。EXEC 在不断演化的 Fork 上规划每个 Command，收集 Reply/Operation，再把合并的 Prepared Commit 送入既有 Durability Barrier。Null Array 表示 Optimistic Abort；Empty Array 表示成功 No-op。

### 为什么需要这个机制

Single Executor 已提供序列化点，但 Durability 与 Runtime-error 语义要求的不只是对 Live State 跑 Python Loop。Speculation 防止部分发布，Revision 防住类似 ABA 的 Value History，一个 Combined Commit 则在 Memory、AOF、Recovery 与 Replication 中保持同一原子边界。

### 运行时心智模型

MULTI 创建 Session State。后续允许的 Command 正常 Parse 但只 Queue 并返回 QUEUED。Parse Error 或禁用 Command 标记 Dirty。EXEC 先检查 Dirty/WATCH Revision，再 Fork Database，按序 Plan Queued Command，只在 Fork 上应用成功 Operation，唯一 Reserve Wakeup，并收集 Result Slot。如果存在 Operation，一个 Durability Barrier 发布其合并 Tuple；之后才收敛 Touch、Waiter Reply 与 EXEC Result。

### 机制板块

<!-- journey-file: src/miniredis/commands/model.py -->
#### `src/miniredis/commands/model.py`

定义 Immutable Transaction Control 与 COMPAREDEL/CHECKDECR，并把每个类型加入完备 Dataset-mutation Trait 分区。

<!-- journey-file: src/miniredis/commands/parser.py -->
#### `src/miniredis/commands/parser.py`

冻结 Keys/Expected Value/Positive Amount，在它们进入 Transaction State 前拒绝格式错误 Control。

<!-- journey-file: src/miniredis/core/reply.py -->
#### `src/miniredis/core/reply.py`

加入与 `Items(())` 语义不同的 `NullArray`。

<!-- journey-file: src/miniredis/adapters/resp2.py -->
#### `src/miniredis/adapters/resp2.py`

把语义 Abort Reply 映射为 RESP2 Null Array `*-1\r\n`，保持 Domain/Wire 分离。

<!-- journey-file: src/miniredis/core/transactions.py -->
#### `src/miniredis/core/transactions.py`

持有紧凑 Per-session Transaction State 与私有 Execution Workspace：Fork、Combined Operations、Ordered Replies、Touches、Wakeups、Reserved Waiter IDs。

<!-- journey-file: src/miniredis/core/database.py -->
#### `src/miniredis/core/database.py`

独立于当前 Key Presence 地维护 Mutation Revision，并创建带 Runtime Metadata 的 Deep Fork。

```python
staged_revision_clock += 1
staged_key_revisions[operation.key] = staged_revision_clock
```

每个 Operation Attempt 都推进 History，因此 Create-delete 不会仅因 Key 再次缺失而看似未改变。

<!-- journey-file: src/miniredis/core/executor.py -->
#### `src/miniredis/core/executor.py`

持有 Transaction Routing、Dirty/Watch Validation、有序推测 Planning、一次 Prepared Commit 与最终 Session Cleanup。

```python
workspace = TransactionWorkspace(self.database.fork())
```

Queued Command 观察前面的 Speculative Write 并产生独立 Reply 时，Live Database 仍未改变。

<!-- journey-file: src/miniredis/core/blocking.py -->
#### `src/miniredis/core/blocking.py`

接受 Reserved-waiter Set，使多个 Queued Push 在 Fork 上 Planning 时不能 Claim 同一个 Blocked Request。

<!-- journey-file: src/miniredis/core/planning.py -->
#### `src/miniredis/core/planning.py`

把 COMPAREDEL/CHECKDECR 实现为普通 Pure Plan，保留 TTL，并在 Precondition 失败时不产生 Operation。

### 验证证据

运行 `tests.txt` 中七个聚焦模块，累计构建 Stage 1–23，并要求 Owned-tree 与 `b195a43` 一致。

### 需要真正记住的内容

- Pipeline Ordering 不是 Transaction Atomicity。
- EXEC 在 Fork 上推测并发布一个 Combined Commit。
- WATCH 比较 Mutation Revision，而不只是当前 Value。
- Runtime Error 占结果槽；Queue-time Error 让 EXEC Abort。

### 用自己的话讲清楚

为什么 EXEC 遇到 Queued WRONGTYPE 后可以继续，却必须在 Parse Error 后 Abort？即使 Key 最终缺失，WATCH 为什么仍需要 Revision Clock？

### 教材

MULTI/EXEC 是 Serialized State Machine 内的 Optimistic Transactional Execution。Fork 是 Private Write Set，Revision Map 提供 Validation Version，Durability Barrier 则是 Atomic Commit Point。
