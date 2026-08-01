# Stage 21 · Bulk strings and directional blocking pop / 批量 String 与方向性阻塞 Pop

<!-- journey: chapter=3 tests_added=5 -->

## English

### Goal

Add MGET, MSET, DECR, and BRPOP without weakening atomic commits, ordered results, blocking-waiter direction, or whole-runtime ownership evidence.

### Deliverable files

- `pyproject.toml`
- `src/miniredis/adapters/direct.py`
- `src/miniredis/commands/model.py`
- `src/miniredis/commands/parser.py`
- `src/miniredis/core/blocking.py`
- `src/miniredis/core/executor.py`
- `src/miniredis/core/planner.py`
- `src/miniredis/core/planning.py`
- `src/miniredis/persistence/aof.py`
- `src/miniredis/replication/sink.py`
- `src/miniredis/runtime.py`
- `tests/contract/test_strings.py`
- `tests/mechanisms/test_blpop.py`
- `tests/reliability/test_final_acceptance.py`
- `tests/unit/commands/test_command_traits.py`
- `tests/unit/commands/test_parser.py`

### The problem at this point

Multi-key commands are not loops around single-key client calls: MSET needs one commit and deterministic duplicate-key semantics, while MGET must preserve argument order and treat non-strings as null. BRPOP is not a separate waiter machine; its right-pop choice must survive both immediate scan and later wake-up.

### Failure preview

A naïve MSET can expose partial state or allocate several commit sequences. A dict-only MGET can lose duplicates and order. A blocked BRPOP can wake using BLPOP direction. At full-system scale, successful behavior can still hide tasks, sessions, waiters, durability jobs, or replica links after close.

### Test contract

<!-- journey-file: tests/contract/test_strings.py -->
#### `tests/contract/test_strings.py`

Locks ordered/null MGET, one-commit MSET with last duplicate winning, TTL replacement, and DECR integer/TTL reuse. The decisive evidence is `runtime.debug_commit_seq == before + 1`; failure means a bulk command was decomposed into externally visible transitions.

<!-- journey-file: tests/mechanisms/test_blpop.py -->
#### `tests/mechanisms/test_blpop.py`

Locks BRPOP's first-ready-key rule and right-side choice both immediately and after blocking. A failure means direction was not frozen into waiter ownership.

<!-- journey-file: tests/reliability/test_final_acceptance.py -->
#### `tests/reliability/test_final_acceptance.py`

Activates TCP, BLPOP, AOF, snapshot, Pub/Sub, and replication together, then closes twice and asserts every owner field and named MiniRedis task reaches zero. A failure identifies a lifecycle owner that never settled.

<!-- journey-file: tests/unit/commands/test_parser.py -->
#### `tests/unit/commands/test_parser.py`

Locks exact typed parsing and invalid arity for MGET, MSET, and DECR. Odd MSET arguments must be rejected before planning.

<!-- journey-file: tests/unit/commands/test_command_traits.py -->
#### `tests/unit/commands/test_command_traits.py`

Keeps the exhaustive read/write trait partition valid after `BlockingPop`, `MultiGet`, and `MultiSet` enter the command union.

### Basic concepts

MGET is an ordered observation over keys; MSET is one normalized state transition. Duplicate MSET keys use last-value-wins before commit construction. A `BlockingPop` freezes keys, deadline, and direction. Acceptance ownership counts are terminal invariants, not performance metrics.

### Why this mechanism is necessary

Bulk APIs exist to express one semantic operation, not save client syntax. Planning them together preserves atomicity and one commit sequence. Carrying pop direction in the typed command and waiter prevents the immediate and deferred paths from drifting. Whole-runtime acceptance catches leaks that isolated feature tests cannot see.

### Runtime mental model

The parser creates `MultiGet`, `MultiSet`, `Increment(-1)`, or `BlockingPop(left=...)`. Planning walks MGET keys in input order, normalizes MSET pairs before one operation tuple, and chooses the correct list end. If no item exists, the waiter stores that same direction until a push produces wake-up operations.

### Mechanism blocks

<!-- journey-file: src/miniredis/commands/model.py -->
#### `src/miniredis/commands/model.py`

Defines immutable bulk commands and replaces `BlPop` with direction-bearing `BlockingPop`; exhaustive traits classify MGET read-only and MSET mutating.

<!-- journey-file: src/miniredis/commands/parser.py -->
#### `src/miniredis/commands/parser.py`

Validates arity/pairs and maps BLPOP/BRPOP into one command with `left=name == b"BLPOP"`.

<!-- journey-file: src/miniredis/core/planning.py -->
#### `src/miniredis/core/planning.py`

Preserves MGET order and duplicates, treats missing/non-string values as null, and collapses MSET duplicates before returning one `ExecutionPlan`.

<!-- journey-file: src/miniredis/core/planner.py -->
#### `src/miniredis/core/planner.py`

Uses `popleft()` or `pop()` from the frozen direction during the immediate blocking-pop scan.

<!-- journey-file: src/miniredis/core/blocking.py -->
#### `src/miniredis/core/blocking.py`

Stores `left` in `BlockingWaiter`, so deferred wake-up removes from the same side chosen at admission.

<!-- journey-file: src/miniredis/core/executor.py -->
#### `src/miniredis/core/executor.py`

Registers a waiter only after the direction-aware immediate plan returns no item, forwarding keys, deadline, and direction as one ownership record.

<!-- journey-file: src/miniredis/adapters/direct.py -->
<!-- journey-file: src/miniredis/persistence/aof.py -->
<!-- journey-file: src/miniredis/replication/sink.py -->
<!-- journey-file: src/miniredis/runtime.py -->
#### Whole-runtime ownership counters

Direct, AOF, replica sink, and runtime surfaces expose their owned task/link/session counts. They do not change command semantics; they let final acceptance prove shutdown convergence across components.

<!-- journey-file: pyproject.toml -->
#### Grouped acceptance scaffold

This metadata adjusts the acceptance-test boundary. It is grouped separately because it does not explain the runtime mechanism introduced by MGET/MSET/DECR/BRPOP.

### Verification evidence

Run all five focused modules in `tests.txt`, cumulatively build Stages 1–21, and require owned-tree parity with `40d00de`.

### Durable takeaways

- MSET is one normalized commit, not repeated SET.
- MGET preserves input position, including duplicates and nulls.
- Blocking direction belongs to waiter state.
- Full acceptance must prove zero owners after close.

### Explain it in your own words

Why does MSET normalize duplicate keys before creating commit operations, and why must BRPOP store direction after its initial scan fails?

### Textbook

Bulk commands demonstrate transaction granularity inside a single-writer state machine. Direction-bearing waiters show continuation state: deferred execution must retain every semantic choice required to resume correctly.

## 中文

### 目标

加入 MGET、MSET、DECR 与 BRPOP，同时不削弱原子 Commit、有序结果、Blocking Waiter 方向与完整 Runtime 所有权证据。

### 交付文件

- `pyproject.toml`
- `src/miniredis/adapters/direct.py`
- `src/miniredis/commands/model.py`
- `src/miniredis/commands/parser.py`
- `src/miniredis/core/blocking.py`
- `src/miniredis/core/executor.py`
- `src/miniredis/core/planner.py`
- `src/miniredis/core/planning.py`
- `src/miniredis/persistence/aof.py`
- `src/miniredis/replication/sink.py`
- `src/miniredis/runtime.py`
- `tests/contract/test_strings.py`
- `tests/mechanisms/test_blpop.py`
- `tests/reliability/test_final_acceptance.py`
- `tests/unit/commands/test_command_traits.py`
- `tests/unit/commands/test_parser.py`

### 当前遇到的问题

Multi-key Command 不是 Client 端循环调用 Single-key Command：MSET 需要一个 Commit 和确定的重复 Key 语义，MGET 必须保留参数顺序并把非 String 当作 Null。BRPOP 也不应复制一套 Waiter Machine；它的右侧选择必须贯穿立即扫描与以后唤醒。

### 先看会坏在哪里

朴素 MSET 会暴露部分状态或分配多个 Commit Sequence。只用 Dict 的 MGET 会丢失重复项和顺序。Blocked BRPOP 可能按 BLPOP 方向唤醒。全系统行为即使成功，Close 后也可能隐藏 Task、Session、Waiter、Durability Job 或 Replica Link。

### 测试契约

<!-- journey-file: tests/contract/test_strings.py -->
#### `tests/contract/test_strings.py`

锁定有序/Null MGET、一次 Commit 且 Last-duplicate-wins 的 MSET、TTL 替换与 DECR 复用。关键证据是 `runtime.debug_commit_seq == before + 1`；失败说明 Bulk Command 被拆成可观察的多次 Transition。

<!-- journey-file: tests/mechanisms/test_blpop.py -->
#### `tests/mechanisms/test_blpop.py`

锁定 BRPOP 的 First-ready-key 与右侧选择在立即和 Blocked 路径一致。失败说明 Direction 没有冻结进 Waiter Ownership。

<!-- journey-file: tests/reliability/test_final_acceptance.py -->
#### `tests/reliability/test_final_acceptance.py`

同时激活 TCP、BLPOP、AOF、Snapshot、Pub/Sub 与 Replication，重复 Close 后断言每个 Owner Field 和命名 MiniRedis Task 都归零。失败会指出没有收敛的生命周期 Owner。

<!-- journey-file: tests/unit/commands/test_parser.py -->
#### `tests/unit/commands/test_parser.py`

锁定 MGET、MSET、DECR 的精确类型解析与非法 Arity；奇数 MSET 参数必须在 Planning 前拒绝。

<!-- journey-file: tests/unit/commands/test_command_traits.py -->
#### `tests/unit/commands/test_command_traits.py`

在 `BlockingPop`、`MultiGet`、`MultiSet` 进入 Command Union 后，维持完备 Read/Write Trait 分区。

### 基本概念

MGET 是对 Key 的有序观察；MSET 是一次归一化状态转移。重复 MSET Key 在构造 Commit 前使用 Last-value-wins。`BlockingPop` 冻结 Keys、Deadline 与 Direction。Acceptance Ownership Count 是终态不变量，不是性能指标。

### 为什么需要这个机制

Bulk API 表达的是一个语义操作，不只是节省 Client Syntax。整体 Planning 保持原子性与一个 Commit Sequence。把 Pop Direction 放进 Typed Command 与 Waiter 能防止立即和延迟路径漂移。完整 Runtime Acceptance 能捕获孤立 Feature Test 看不到的泄漏。

### 运行时心智模型

Parser 创建 `MultiGet`、`MultiSet`、`Increment(-1)` 或 `BlockingPop(left=...)`。Planning 按输入顺序遍历 MGET Key，在一次 Operation Tuple 前归一化 MSET Pair，并选择正确 List 端。如果没有 Item，Waiter 保存同一 Direction，直到 Push 产生 Wake-up Operation。

### 机制板块

<!-- journey-file: src/miniredis/commands/model.py -->
#### `src/miniredis/commands/model.py`

定义 Immutable Bulk Command，用含方向的 `BlockingPop` 替代 `BlPop`；完备 Trait 把 MGET 分类为只读、MSET 分类为写入。

<!-- journey-file: src/miniredis/commands/parser.py -->
#### `src/miniredis/commands/parser.py`

校验 Arity/Pair，并用 `left=name == b"BLPOP"` 把 BLPOP/BRPOP 映射到一个 Command。

<!-- journey-file: src/miniredis/core/planning.py -->
#### `src/miniredis/core/planning.py`

保留 MGET 顺序与重复项，把 Missing/Non-string 当 Null，并在返回一个 `ExecutionPlan` 前折叠 MSET 重复项。

<!-- journey-file: src/miniredis/core/planner.py -->
#### `src/miniredis/core/planner.py`

在立即 Blocking-pop Scan 中按冻结方向选择 `popleft()` 或 `pop()`。

<!-- journey-file: src/miniredis/core/blocking.py -->
#### `src/miniredis/core/blocking.py`

在 `BlockingWaiter` 中保存 `left`，使延迟 Wake-up 从 Admission 时选择的同一侧移除。

<!-- journey-file: src/miniredis/core/executor.py -->
#### `src/miniredis/core/executor.py`

只有 Direction-aware Immediate Plan 找不到 Item 后才注册 Waiter，并把 Keys、Deadline、Direction 作为一个 Ownership Record 传入。

<!-- journey-file: src/miniredis/adapters/direct.py -->
<!-- journey-file: src/miniredis/persistence/aof.py -->
<!-- journey-file: src/miniredis/replication/sink.py -->
<!-- journey-file: src/miniredis/runtime.py -->
#### 完整 Runtime 所有权计数

Direct、AOF、Replica Sink 与 Runtime 暴露 Owned Task/Link/Session Count。它们不改变命令语义，只让最终 Acceptance 证明跨组件 Shutdown 收敛。

<!-- journey-file: pyproject.toml -->
#### 合并的验收脚手架

这份 Metadata 调整 Acceptance-test Boundary。它被单独合并理解，因为不解释 MGET/MSET/DECR/BRPOP 引入的 Runtime Mechanism。

### 验证证据

运行 `tests.txt` 中五个聚焦模块，累计构建 Stage 1–21，并要求 Owned-tree 与 `40d00de` 一致。

### 需要真正记住的内容

- MSET 是一次归一化 Commit，不是重复 SET。
- MGET 保留输入位置，包括重复项与 Null。
- Blocking Direction 属于 Waiter State。
- Full Acceptance 必须证明 Close 后 Owner 归零。

### 用自己的话讲清楚

MSET 为什么在创建 Commit Operation 前归一化重复 Key？BRPOP 初次扫描失败后为什么必须保存 Direction？

### 教材

Bulk Command 展示 Single-writer State Machine 内的 Transaction Granularity。Direction-bearing Waiter 则展示 Continuation State：延迟执行必须保留正确恢复所需的每个语义选择。
