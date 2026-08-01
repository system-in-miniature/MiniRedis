# Stage 01 · Domain state and commit vocabulary / 领域状态与提交词汇

<!-- journey: chapter=2 tests_added=13 -->

## English

### Goal

Define the binary-safe values, immutable commit vocabulary, replies, and database boundary that every later command shares.

### Deliverable files

- `pyproject.toml`
- `src/miniredis/__init__.py`
- `src/miniredis/adapters/__init__.py`
- `src/miniredis/commands/__init__.py`
- `src/miniredis/commands/request.py`
- `src/miniredis/core/__init__.py`
- `src/miniredis/core/commit.py`
- `src/miniredis/core/database.py`
- `src/miniredis/core/reply.py`
- `src/miniredis/core/values.py`
- `src/miniredis/persistence/__init__.py`
- `src/miniredis/replication/__init__.py`
- `tests/__init__.py`
- `tests/helpers/__init__.py`
- `tests/unit/core/test_domain_types.py`
- `uv.lock`

### The problem at this point

An in-memory server cannot safely start from a dictionary of arbitrary Python objects. Later persistence and replication need stable values that cannot change behind their backs, while command execution needs mutable containers and transport adapters need replies that do not know about RESP2.

### Failure preview

The first contract mutates live Hash, List, Set, and Sorted Set containers after freezing them. If the stored forms share those containers, an already-created commit changes without receiving a new sequence. Another case submits a mixed batch with an unsupported operation and requires the database to remain completely unchanged.

### Test contract

<!-- journey-file: tests/__init__.py -->
#### `tests/__init__.py`

Marks the cumulative test tree as importable support; it introduces no behavior on its own.

<!-- journey-file: tests/helpers/__init__.py -->
#### `tests/helpers/__init__.py`

Reserves one shared helper package so later deterministic clocks and runtime fixtures do not leak into production modules.

<!-- journey-file: tests/unit/core/test_domain_types.py -->
#### `tests/unit/core/test_domain_types.py`

##### What this test locks

It locks binary safety, deep freeze/thaw isolation, exact logical sizes, contiguous commit sequences, immutable reply shapes, and non-mutating invariant failure.

##### How it constructs the counterexample

It retains aliases to mutable containers, freezes them, mutates the aliases, and compares the stored values. It also applies invalid and out-of-order batches around a captured database snapshot.

##### Key test statement

```python
assert freeze_value(live) == stored
```

##### What a failure means

A failure means an atomic or durable unit can be changed indirectly, or a rejected batch can leak partial state before the executor even exists.

### Basic concepts

MiniRedis distinguishes live `RedisValue` containers from immutable `StoredValue` records. A `CommitBatch` is an ordered tuple of `PutEntry` and `DeleteKey` operations. A `Reply` is a transport-neutral result. `Database.apply_batch` is the only state transition in this Stage.

Binary safe means keys and values remain `bytes`; no UTF-8 decoding is required to store or compare them. Atomic application means either every operation in one batch becomes visible or the original database remains intact.

### Why this mechanism is necessary

Without a frozen state vocabulary, AOF, snapshots, and replicas would observe mutable Python aliases rather than one historical fact. Without a closed reply vocabulary, Direct and RESP2 adapters would create separate semantics. Without sequence validation, recovery could silently accept a missing or reordered commit.

### Runtime mental model

A future planner will produce immutable operations. The database first stages copies of its tables, validates every operation and logical-size invariant, then swaps the staged state into place and advances exactly one commit sequence. No command or socket is needed to understand that transition.

### Mechanism blocks

<!-- journey-file: src/miniredis/commands/request.py -->
#### `src/miniredis/commands/request.py`

##### What it is and why it appears

`CommandRequest` is the smallest transport-neutral input: a binary command name and an immutable tuple of binary arguments.

##### Runtime role

Direct clients and the later RESP2 adapter will construct the same value before parsing.

##### Key code

```python
class CommandRequest:
    name: bytes
    args: tuple[bytes, ...] = ()
```

##### Statement understanding

Keeping the request binary and transport-neutral prevents the network adapter from owning command meaning.

<!-- journey-file: src/miniredis/core/values.py -->
#### `src/miniredis/core/values.py`

##### What it is and why it appears

These five live value types expose the Python containers planners will copy and modify.

##### Runtime role

The database stores one `RedisValue` per key while command-specific planners enforce type compatibility.

##### Key code

```python
RedisValue: TypeAlias = StringValue | HashValue | ListValue | SetValue | ZSetValue
```

##### Statement understanding

The closed union makes every supported live shape explicit; adding a new value type must update freezing, sizing, persistence, and planners.

<!-- journey-file: src/miniredis/core/reply.py -->
#### `src/miniredis/core/reply.py`

##### What it is and why it appears

Replies describe semantic outcomes before any adapter chooses wire bytes.

##### Runtime role

Direct callers receive these values directly; RESP2 later maps the same values to frames.

##### Key code

```python
Reply: TypeAlias = Ok | Bytes | Number | Items | Failure
```

##### Statement understanding

An error is data in the reply union, not an exception that can accidentally bypass ordered request completion.

<!-- journey-file: src/miniredis/core/commit.py -->
#### `src/miniredis/core/commit.py`

##### What it is and why it appears

Immutable stored values and commit operations are the vocabulary that later crosses AOF, recovery, and replication.

##### Runtime role

One positive sequence number orders one non-empty tuple of operations as a single atomic fact.

##### Key code

```python
class CommitBatch:
    seq: int
    operations: tuple[CommitOperation, ...]
```

##### Statement understanding

The batch, rather than an individual key mutation, is the propagation unit; splitting it later would break transaction and waiter atomicity.

<!-- journey-file: src/miniredis/core/database.py -->
#### `src/miniredis/core/database.py`

##### What it is and why it appears

The database owns live entries, sequence order, access metadata, and logical memory accounting.

##### Runtime role

`apply_batch` stages changes, thaws immutable values into fresh containers, verifies invariants, and only then replaces live state.

##### Key code

```python
next_seq = self.commit_seq + 1
if batch.seq != next_seq:
    raise ValueError(f"expected commit seq {next_seq}, got {batch.seq}")
```

##### Statement understanding

Sequence validation happens before publication, so a gap cannot be normalized into a plausible but incomplete history.

#### Package and test scaffold

The remaining files install the package, create package namespaces, and pin the test environment. They are necessary to run the Stage but add no MiniRedis mechanism.

<!-- journey-file: pyproject.toml -->
#### `pyproject.toml`

Project and pytest configuration.

<!-- journey-file: src/miniredis/__init__.py -->
#### `src/miniredis/__init__.py`

Initial public package marker.

<!-- journey-file: src/miniredis/adapters/__init__.py -->
#### `src/miniredis/adapters/__init__.py`

Adapter package marker.

<!-- journey-file: src/miniredis/commands/__init__.py -->
#### `src/miniredis/commands/__init__.py`

Command package marker.

<!-- journey-file: src/miniredis/core/__init__.py -->
#### `src/miniredis/core/__init__.py`

Core package marker.

<!-- journey-file: src/miniredis/persistence/__init__.py -->
#### `src/miniredis/persistence/__init__.py`

Persistence package marker for later Stages.

<!-- journey-file: src/miniredis/replication/__init__.py -->
#### `src/miniredis/replication/__init__.py`

Replication package marker for later Stages.

<!-- journey-file: uv.lock -->
#### `uv.lock`

Reproducible development dependency resolution.

### Verification evidence

Run `uv run pytest -q $(cat journey/stages/01-domain-state/tests.txt)`. It proves the value, reply, commit, and atomic database contracts; it does not yet prove command execution or concurrency.

### Durable takeaways

Live values and historical values are different representations. One immutable batch is the ordered state-transition unit. Replies are independent of transports, and rejected database transitions publish nothing.

### Explain it in your own words

MiniRedis begins by defining what may exist and how one change becomes a stable fact. Mutable containers stay inside the database, immutable stored values cross boundaries, and a sequence-checked batch becomes visible only after the whole staged state is valid.

### Textbook

[Chapter 2](https://github.com/system-in-miniature/mini-redis/blob/main/docs/tutorial/02-command-life.md)

## 中文

### 目标

定义后续所有命令共用的二进制安全值、不可变提交词汇、回复与数据库边界。

### 交付文件

- `pyproject.toml`
- `src/miniredis/__init__.py`
- `src/miniredis/adapters/__init__.py`
- `src/miniredis/commands/__init__.py`
- `src/miniredis/commands/request.py`
- `src/miniredis/core/__init__.py`
- `src/miniredis/core/commit.py`
- `src/miniredis/core/database.py`
- `src/miniredis/core/reply.py`
- `src/miniredis/core/values.py`
- `src/miniredis/persistence/__init__.py`
- `src/miniredis/replication/__init__.py`
- `tests/__init__.py`
- `tests/helpers/__init__.py`
- `tests/unit/core/test_domain_types.py`
- `uv.lock`

### 当前遇到的问题

内存服务器不能从一个装任意 Python 对象的字典直接开始。后续持久化与复制需要不会被背后修改的稳定值，命令执行需要可变容器，而传输适配器需要不了解 RESP2 的回复。

### 先看会坏在哪里

第一组契约在冻结 Hash、List、Set 与 Sorted Set 后继续修改原容器。如果 Stored 形式共享容器，已经建立的提交会在没有新序列号时变化。另一条用例在混合 Batch 中插入不支持的操作，并要求数据库完整保持原状。

### 测试契约

<!-- journey-file: tests/__init__.py -->
#### `tests/__init__.py`

把累计测试树标记为可导入支撑，本身不引入行为。

<!-- journey-file: tests/helpers/__init__.py -->
#### `tests/helpers/__init__.py`

预留共享测试帮助包，使后续确定性时钟和 Runtime Fixture 不进入生产模块。

<!-- journey-file: tests/unit/core/test_domain_types.py -->
#### `tests/unit/core/test_domain_types.py`

##### 测试锁定什么

锁定二进制安全、深冻结/解冻隔离、精确逻辑大小、连续提交序列、不可变回复形状与失败不变更。

##### 如何构造反例

测试保留可变容器别名，冻结后修改别名，再比较 Stored 值；同时围绕数据库快照应用非法与乱序 Batch。

##### 关键测试语句

```python
assert freeze_value(live) == stored
```

##### 失败意味着什么

失败表示一个原子或持久单元能被间接改写，或被拒绝的 Batch 在执行器出现以前就泄漏部分状态。

### 基本概念

MiniRedis 区分可变的运行时 `RedisValue` 容器与不可变 `StoredValue` 记录。`CommitBatch` 是有序的 `PutEntry`/`DeleteKey` 元组，`Reply` 是传输无关结果，`Database.apply_batch` 是本 Stage 唯一状态迁移。

二进制安全表示 Key 与 Value 保持 `bytes`，存储与比较不要求 UTF-8 解码。原子应用表示一个 Batch 要么全部可见，要么数据库保持原样。

### 为什么需要这个机制

没有冻结状态词汇，AOF、Snapshot 与 Replica 看到的会是可变 Python 别名，而不是历史事实。没有封闭回复词汇，Direct 与 RESP2 会发展出两套语义。没有序列校验，恢复可能静默接受缺失或乱序提交。

### 运行时心智模型

未来 Planner 产生不可变操作。数据库先复制并暂存表，校验所有操作与逻辑大小不变量，再一次替换实时状态并只推进一个提交序列。理解这条迁移不需要命令或 Socket。

### 机制板块

<!-- journey-file: src/miniredis/commands/request.py -->
#### `src/miniredis/commands/request.py`

##### 是什么，为什么现在需要

`CommandRequest` 是最小传输无关输入：二进制命令名与不可变二进制参数元组。

##### 在运行时做什么

Direct Client 与后续 RESP2 适配器都会在解析前构造同一个值。

##### 关键代码

```python
class CommandRequest:
    name: bytes
    args: tuple[bytes, ...] = ()
```

##### 关键语句理解

保持请求为二进制且与传输无关，网络适配器就不能拥有命令含义。

<!-- journey-file: src/miniredis/core/values.py -->
#### `src/miniredis/core/values.py`

##### 是什么，为什么现在需要

五种运行时值类型暴露 Planner 将复制与修改的 Python 容器。

##### 在运行时做什么

数据库每个 Key 保存一个 `RedisValue`，命令专属 Planner 检查类型兼容性。

##### 关键代码

```python
RedisValue: TypeAlias = StringValue | HashValue | ListValue | SetValue | ZSetValue
```

##### 关键语句理解

封闭 Union 让支持的实时形状全部显式；加入新值类型时必须同步更新冻结、计量、持久化与 Planner。

<!-- journey-file: src/miniredis/core/reply.py -->
#### `src/miniredis/core/reply.py`

##### 是什么，为什么现在需要

Reply 在适配器选择 Wire Bytes 以前描述语义结果。

##### 在运行时做什么

Direct 调用方直接收到这些值；RESP2 稍后把同一值映射成 Frame。

##### 关键代码

```python
Reply: TypeAlias = Ok | Bytes | Number | Items | Failure
```

##### 关键语句理解

错误是回复 Union 中的数据，不是可能绕开有序请求完成的异常。

<!-- journey-file: src/miniredis/core/commit.py -->
#### `src/miniredis/core/commit.py`

##### 是什么，为什么现在需要

不可变 Stored 值与提交操作是后续跨越 AOF、恢复与复制的词汇。

##### 在运行时做什么

一个正序列号把一个非空操作元组排序成单一原子事实。

##### 关键代码

```python
class CommitBatch:
    seq: int
    operations: tuple[CommitOperation, ...]
```

##### 关键语句理解

传播单元是 Batch 而不是单 Key 变更；后续拆开它会破坏事务和 Waiter 原子性。

<!-- journey-file: src/miniredis/core/database.py -->
#### `src/miniredis/core/database.py`

##### 是什么，为什么现在需要

数据库拥有实时 Entry、序列顺序、访问元数据与逻辑内存计量。

##### 在运行时做什么

`apply_batch` 暂存变更，把不可变值解冻为新容器，校验不变量，最后替换实时状态。

##### 关键代码

```python
next_seq = self.commit_seq + 1
if batch.seq != next_seq:
    raise ValueError(f"expected commit seq {next_seq}, got {batch.seq}")
```

##### 关键语句理解

序列校验发生在发布前，因此缺口不能被归一化成看似合理但不完整的历史。

#### 包与测试脚手架

其余文件安装包、建立包命名空间并锁定测试环境。它们是运行 Stage 的必要支撑，但不增加 MiniRedis 机制。

<!-- journey-file: pyproject.toml -->
#### `pyproject.toml`

项目与 pytest 配置。

<!-- journey-file: src/miniredis/__init__.py -->
#### `src/miniredis/__init__.py`

初始公开包标记。

<!-- journey-file: src/miniredis/adapters/__init__.py -->
#### `src/miniredis/adapters/__init__.py`

Adapter 包标记。

<!-- journey-file: src/miniredis/commands/__init__.py -->
#### `src/miniredis/commands/__init__.py`

Command 包标记。

<!-- journey-file: src/miniredis/core/__init__.py -->
#### `src/miniredis/core/__init__.py`

Core 包标记。

<!-- journey-file: src/miniredis/persistence/__init__.py -->
#### `src/miniredis/persistence/__init__.py`

为后续 Stage 预留的 Persistence 包标记。

<!-- journey-file: src/miniredis/replication/__init__.py -->
#### `src/miniredis/replication/__init__.py`

为后续 Stage 预留的 Replication 包标记。

<!-- journey-file: uv.lock -->
#### `uv.lock`

可复现的开发依赖解析。

### 验证证据

运行 `uv run pytest -q $(cat journey/stages/01-domain-state/tests.txt)`。它证明值、回复、提交与原子数据库契约，但尚未证明命令执行或并发。

### 需要真正记住的内容

运行时值与历史值是不同表示。一个不可变 Batch 是有序状态迁移单元。Reply 与传输独立，被拒绝的数据库迁移不发布任何内容。

### 用自己的话讲清楚

MiniRedis 先定义什么可以存在，以及一次变更如何成为稳定事实。可变容器留在数据库内，不可变 Stored 值跨越边界，序列校验 Batch 只有在完整暂存状态合法后才可见。

### 教材

[第 2 章](https://github.com/system-in-miniature/mini-redis/blob/main/docs/zh/tutorial/02-command-life.md)
