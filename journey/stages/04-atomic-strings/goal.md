# Stage 04 · Atomic String planning / 原子 String 规划

<!-- journey: chapter=3 tests_added=6 -->

## English

### Goal

Plan String commands as side-effect-free replies plus one serialized commit.

### Deliverable files

- `src/miniredis/core/executor.py`
- `src/miniredis/core/expiration.py`
- `src/miniredis/core/planner.py`
- `src/miniredis/core/planning.py`
- `tests/concurrency/test_atomic_incr.py`
- `tests/contract/test_strings.py`

### The problem at this point

The executor can order requests but only answers `PING` and `ECHO`. A String mutation needs to inspect old state, reject wrong types or overflow without allocating a commit, and let one hundred concurrent `INCR` calls each observe a distinct serialized predecessor.

### Failure preview

The contract stores the non-canonical integer `01`, attempts `INCR`, and requires both the value and commit sequence to remain unchanged. Another case starts one hundred concurrent increments and requires the final value and sequence to account for every accepted mutation exactly once.

### Test contract

<!-- journey-file: tests/concurrency/test_atomic_incr.py -->
#### `tests/concurrency/test_atomic_incr.py`

##### What this test locks

It locks read-plan-apply as one executor turn under concurrent callers.

##### How it constructs the counterexample

One hundred tasks submit `INCR` without external locking, then the test checks the final value and unique numeric replies.

##### Key test statement

```python
assert await client.execute(CommandRequest(b"GET", (b"counter",))) == Bytes(b"100")
```

##### What a failure means

A lost or duplicated increment means callers read stale state outside the serialized owner.

<!-- journey-file: tests/contract/test_strings.py -->
#### `tests/contract/test_strings.py`

##### What this test locks

It locks `SET` conditions, missing values, signed 64-bit arithmetic, overflow, type replacement, ordered multi-key replies, and no-commit error/no-op paths.

##### How it constructs the counterexample

It captures `debug_commit_seq` before invalid integers, overflow, and failed `NX`, then verifies both sequence and stored bytes are unchanged.

##### Key test statement

```python
assert runtime.debug_commit_seq == before
```

##### What a failure means

A semantic error has leaked an operation or allocated a false historical commit.

### Basic concepts

An `ExecutionPlan` contains a reply, immutable operations, optional touched keys, and a trigger. Planning reads state but does not publish it. A no-op or failure can return a reply with no operations; only a non-empty successful plan becomes a sequenced `CommitBatch`.

### Why this mechanism is necessary

Separating planning from application makes error atomicity visible and keeps the executor as the sole sequence allocator. It also gives later AOF and replication one stable batch rather than a command-specific mutation procedure.

### Runtime mental model

Inside one executor turn, the planner looks up the key, proposes expiry cleanup and a new stored String if valid, and returns a reply. The executor allocates `commit_seq + 1`, applies the whole batch, touches successful reads, and only then completes the request.

### Mechanism blocks

<!-- journey-file: src/miniredis/core/expiration.py -->
#### `src/miniredis/core/expiration.py`

##### What it is and why it appears

The initial expiry helpers classify an entry against the executor's sampled time and build an explicit delete operation.

##### Runtime role

String lookup can treat elapsed data as absent without mutating during planning.

##### Key code

```python
def is_expired(entry: Entry, now_ms: int) -> bool:
    return entry.expire_at_ms is not None and entry.expire_at_ms <= now_ms
```

##### Statement understanding

Logical absence and physical cleanup are separated; a later commit decides whether the proposed delete publishes.

<!-- journey-file: src/miniredis/core/planning.py -->
#### `src/miniredis/core/planning.py`

##### What it is and why it appears

This module owns shared lookup/building rules and String command semantics.

##### Runtime role

It returns precise replies plus frozen `PutEntry`/`DeleteKey` operations while preserving old expiry when required.

##### Key code

```python
new_value = old_value + amount
if not INT64_MIN <= new_value <= INT64_MAX:
    return _integer_failure()
```

##### Statement understanding

Overflow returns a plan without operations; checking after applying would corrupt both value and history.

<!-- journey-file: src/miniredis/core/planner.py -->
#### `src/miniredis/core/planner.py`

##### What it is and why it appears

`CommandPlanner` is the stable routing facade between typed commands and per-family pure planners.

##### Runtime role

The executor invokes one method without learning String-specific branches.

##### Key code

```python
return plan_general_and_strings(command, database, now_ms)
```

##### Statement understanding

Command-family growth stays behind the planner boundary rather than expanding executor ownership.

<!-- journey-file: src/miniredis/core/executor.py -->
#### `src/miniredis/core/executor.py`

##### What it is and why it appears

The executor now turns non-empty plans into ordered commit batches and applies them exactly once.

##### Runtime role

It samples time, plans against current state, allocates the next sequence, applies, then completes the reply.

##### Key code

```python
batch = prepared.to_batch(self.database.commit_seq + 1)
self.database.apply_batch(batch, track_access=True)
```

##### Statement understanding

Sequence allocation occurs beside application under the same owner, so concurrent `INCR` calls cannot share a predecessor.

### Verification evidence

Run `uv run pytest -q $(cat journey/stages/04-atomic-strings/tests.txt)`. It proves String semantics; also run `tests/concurrency/test_atomic_incr.py` to observe serialized concurrent increments.

### Durable takeaways

Planning is pure, errors and no-ops have no commit, and only the executor allocates and applies the next batch. Concurrency is resolved by ownership, not by command-specific locks.

### Explain it in your own words

A String command first becomes a proposal. If the proposal is valid and mutating, the executor turns it into the next immutable batch and applies it before replying; otherwise it returns a semantic result without inventing history.

### Textbook

[Chapter 3](https://github.com/system-in-miniature/mini-redis/blob/main/docs/tutorial/03-data-types.md)

## 中文

### 目标

把 String 命令规划成无副作用的 Reply 加一次串行提交。

### 交付文件

- `src/miniredis/core/executor.py`
- `src/miniredis/core/expiration.py`
- `src/miniredis/core/planner.py`
- `src/miniredis/core/planning.py`
- `tests/concurrency/test_atomic_incr.py`
- `tests/contract/test_strings.py`

### 当前遇到的问题

Executor 能排序请求，却只能回答 `PING`/`ECHO`。String 变更需要检查旧状态，在 Wrong Type 或 Overflow 时不分配 Commit，还要让一百个并发 `INCR` 各自观察不同的串行前驱。

### 先看会坏在哪里

契约保存非规范整数 `01`，尝试 `INCR`，要求 Value 与 Commit Sequence 都不变。另一条用例启动一百个并发 Increment，要求最终值与序列对每个已接受变更恰好计数一次。

### 测试契约

<!-- journey-file: tests/concurrency/test_atomic_incr.py -->
#### `tests/concurrency/test_atomic_incr.py`

##### 测试锁定什么

锁定并发调用方下 Read-Plan-Apply 仍是一个 Executor Turn。

##### 如何构造反例

一百个 Task 在没有外部锁时提交 `INCR`，再检查最终值与互不重复的数字 Reply。

##### 关键测试语句

```python
assert await client.execute(CommandRequest(b"GET", (b"counter",))) == Bytes(b"100")
```

##### 失败意味着什么

丢失或重复 Increment 表示调用方在串行 Owner 外读取了过期状态。

<!-- journey-file: tests/contract/test_strings.py -->
#### `tests/contract/test_strings.py`

##### 测试锁定什么

锁定 `SET` 条件、Missing Value、有符号 64 位运算、Overflow、类型替换、有序多 Key Reply 与 Error/No-op 不提交。

##### 如何构造反例

在非法整数、Overflow 与失败 `NX` 前捕获 `debug_commit_seq`，再确认序列与存储 Bytes 都未变化。

##### 关键测试语句

```python
assert runtime.debug_commit_seq == before
```

##### 失败意味着什么

语义错误泄漏了 Operation，或分配了不存在的历史 Commit。

### 基本概念

`ExecutionPlan` 包含 Reply、不可变 Operation、可选 Touch Key 与 Trigger。规划读取状态但不发布。No-op 或 Failure 可以返回无操作 Reply；只有非空成功 Plan 才成为带序列 `CommitBatch`。

### 为什么需要这个机制

分离规划与应用让错误原子性可见，并让 Executor 保持唯一序列分配者。后续 AOF 与复制也得到稳定 Batch，而不是命令专属变更过程。

### 运行时心智模型

在一个 Executor Turn 内，Planner 查找 Key，提出 Expiry Cleanup 和合法时的新 Stored String，再返回 Reply。Executor 分配 `commit_seq + 1`，应用整个 Batch，Touch 成功读取，最后完成请求。

### 机制板块

<!-- journey-file: src/miniredis/core/expiration.py -->
#### `src/miniredis/core/expiration.py`

##### 是什么，为什么现在需要

初始 Expiry Helper 按 Executor 采样时间分类 Entry，并建立显式 Delete Operation。

##### 在运行时做什么

String Lookup 可以把已过期数据视为不存在，而不在规划中直接修改。

##### 关键代码

```python
def is_expired(entry: Entry, now_ms: int) -> bool:
    return entry.expire_at_ms is not None and entry.expire_at_ms <= now_ms
```

##### 关键语句理解

逻辑不可见与物理清理分离；后续 Commit 决定提出的 Delete 是否发布。

<!-- journey-file: src/miniredis/core/planning.py -->
#### `src/miniredis/core/planning.py`

##### 是什么，为什么现在需要

该模块拥有共享 Lookup/Building 规则与 String 命令语义。

##### 在运行时做什么

它返回精确 Reply 加冻结 `PutEntry`/`DeleteKey`，并在需要时保留旧 Expiry。

##### 关键代码

```python
new_value = old_value + amount
if not INT64_MIN <= new_value <= INT64_MAX:
    return _integer_failure()
```

##### 关键语句理解

Overflow 返回无操作 Plan；如果应用后才检查，会同时破坏值与历史。

<!-- journey-file: src/miniredis/core/planner.py -->
#### `src/miniredis/core/planner.py`

##### 是什么，为什么现在需要

`CommandPlanner` 是类型化命令与各命令族纯 Planner 间的稳定路由门面。

##### 在运行时做什么

Executor 调用一个方法，不学习 String 专属分支。

##### 关键代码

```python
return plan_general_and_strings(command, database, now_ms)
```

##### 关键语句理解

命令族增长留在 Planner 边界后，不扩大 Executor 所有权。

<!-- journey-file: src/miniredis/core/executor.py -->
#### `src/miniredis/core/executor.py`

##### 是什么，为什么现在需要

Executor 现在把非空 Plan 变成有序 Commit Batch，并恰好应用一次。

##### 在运行时做什么

它采样时间、基于当前状态规划、分配下一序列、应用，再完成 Reply。

##### 关键代码

```python
batch = prepared.to_batch(self.database.commit_seq + 1)
self.database.apply_batch(batch, track_access=True)
```

##### 关键语句理解

序列分配与应用在同一 Owner 下相邻发生，因此并发 `INCR` 不能共享前驱。

### 验证证据

运行 `uv run pytest -q $(cat journey/stages/04-atomic-strings/tests.txt)`；再运行 `tests/concurrency/test_atomic_incr.py` 观察并发 Increment 串行化。

### 需要真正记住的内容

规划是纯的；Error 与 No-op 没有 Commit；只有 Executor 分配并应用下一 Batch。并发通过所有权解决，不靠命令专属锁。

### 用自己的话讲清楚

String 命令先成为 Proposal。合法且变更时，Executor 把它变成下一不可变 Batch，在回复前应用；否则返回语义结果而不伪造历史。

### 教材

[第 3 章](https://github.com/system-in-miniature/mini-redis/blob/main/docs/zh/tutorial/03-data-types.md)
