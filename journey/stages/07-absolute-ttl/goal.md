# Stage 07 · Absolute TTL and bounded expiry / 绝对 TTL 与有界过期

<!-- journey: chapter=4 tests_added=4 -->

## English

### Goal

Make expiration a deterministic state transition with absolute deadlines, lazy invisibility, and bounded active cleanup.

### Deliverable files

- `pyproject.toml`
- `src/miniredis/core/executor.py`
- `src/miniredis/core/planner.py`
- `src/miniredis/core/ttl_planner.py`
- `src/miniredis/runtime.py`
- `tests/contract/test_ttl.py`
- `tests/helpers/time.py`

### The problem at this point

Values can now be updated atomically, but time does not yet affect visibility. Relative countdowns would drift during pause or restart, while deleting directly from a read path would bypass the serialized commit owner.

### Failure preview

An expired key must already be logically absent even if its physical entry remains. A later command error must not accidentally commit the pending lazy delete, and in-place mutations must preserve the original deadline instead of extending the key's life.

### Test contract

<!-- journey-file: tests/contract/test_ttl.py -->
#### `tests/contract/test_ttl.py`

##### What this test locks

It locks lazy invisibility, TTL rounding, PERSIST, bounded active cleanup, deadline preservation for every value family, and error atomicity.

##### How it constructs the counterexample

It advances injected time exactly to a deadline, separates logical reads from physical counts, and combines an expired operand with a later WRONGTYPE operand.

##### Key test statement

```python
assert runtime.debug_commit_seq == before
```

##### What a failure means

Time changed state outside the commit protocol, an error leaked a proposed expiry delete, or a mutation silently reset an absolute deadline.

<!-- journey-file: tests/helpers/time.py -->
#### `tests/helpers/time.py`

##### What this test locks

The helper makes elapsed time an explicit input rather than a sleep or wall-clock assumption.

##### How it constructs the counterexample

Each test selects an exact millisecond and advances it synchronously, so boundary equality is reproducible.

##### Key test statement

```python
def advance(self, milliseconds: int) -> None:
    self.value += milliseconds
```

##### What a failure means

TTL behavior can no longer be distinguished from scheduler timing or a flaky real-time delay.

### Basic concepts

MiniRedis stores `expire_at_ms`, an absolute deadline. Lazy expiry makes an elapsed entry invisible during lookup and proposes `DeleteKey(EXPIRED)`; active expiry independently samples physical TTL entries so cold keys are eventually reclaimed. Logical absence and physical reclamation are therefore separate moments.

### Why this mechanism is necessary

Absolute time survives pauses and later persistence without recomputing a countdown. Routing both lazy and active deletes through `CommitBatch` preserves ordering, durability hooks, and future replication semantics. A bounded sample prevents one maintenance tick from monopolizing the executor.

### Runtime mental model

The injected Clock supplies `now_ms`. A command planner compares it with an entry deadline and returns a reply plus proposed operations. The executor either commits the complete successful plan or discards it on failure. Active ticks enter the same mailbox, select at most N TTL keys, and commit expired deletes as one maintenance batch.

### Mechanism blocks

<!-- journey-file: src/miniredis/core/ttl_planner.py -->
#### `src/miniredis/core/ttl_planner.py`

##### What it is and why it appears

This command-family planner owns EXPIRE, TTL/PTTL, and PERSIST without teaching the executor command semantics.

##### Runtime role

It resolves lazy expiry first, stores `now_ms + duration`, and represents immediate expiry or persistence changes as ordinary operations.

##### Key code

```python
put = make_put(
    key,
    previous.value,
    previous,
    now_ms + seconds * 1_000,
)
```

##### Statement understanding

The stored value is an absolute deadline. In-place updates can copy it unchanged, while TTL can compute remaining time from one clock reading.

<!-- journey-file: src/miniredis/core/executor.py -->
#### `src/miniredis/core/executor.py`

##### What it is and why it appears

The single executor gains a control message for bounded active expiry rather than a second task mutating the database directly.

##### Runtime role

It rotates through sorted TTL keys, proposes expired deletes, appends one `ACTIVE_EXPIRE` batch, and applies it in mailbox order.

##### Key code

```python
candidate_keys = ordered_keys[: self.active_expire_sample_size]
self._active_expire_cursor = candidate_keys[-1]
```

##### Statement understanding

The slice is the per-tick work bound; the cursor prevents every tick from repeatedly examining only the same prefix.

<!-- journey-file: src/miniredis/core/planner.py -->
#### `src/miniredis/core/planner.py`

##### What it is and why it appears

The stable planner facade adds the TTL command family after the existing value planners.

##### Runtime role

It routes a typed TTL command to exactly one semantic owner and preserves the existing unknown-command fallback.

##### Key code

```python
if plan is None:
    plan = plan_ttl(command, database, now_ms)
```

##### Statement understanding

`None` still means “not my command family”; an `ExecutionPlan` with no operations can still be a complete TTL result.

<!-- journey-file: src/miniredis/runtime.py -->
#### `src/miniredis/runtime.py`

##### What it is and why it appears

The public runtime construction path now accepts a Clock and exposes narrow expiry diagnostics for contracts.

##### Runtime role

It passes the configured sample bound into the executor and delegates active cleanup without exposing direct database mutation.

##### Key code

```python
return cls.open(
    config,
    clock=clock,
    commit_barrier=commit_barrier,
    **options,
)
```

##### Statement understanding

`open_with_dependencies` and `open` converge on one construction path, so production and deterministic tests cannot drift in wiring.

<!-- journey-file: pyproject.toml -->
#### `pyproject.toml`

##### What it is and why it appears

This test-only path setting lets contract modules import their shared deterministic clock.

##### Runtime role

It affects pytest discovery, not MiniRedis expiration semantics or package behavior.

##### Key code

```toml
pythonpath = ["src", "."]
```

##### Statement understanding

The project root is added only so `tests.helpers.time` is a stable test dependency.

### Verification evidence

Run `uv run pytest -q $(cat journey/stages/07-absolute-ttl/tests.txt)`. It proves the TTL contract through the public Direct client and executor, including deterministic boundary time and bounded physical cleanup.

### Durable takeaways

Store absolute deadlines; separate logical invisibility from physical reclamation; propose expiry deletes; discard every proposed operation when a command fails; preserve deadlines across in-place mutations.

### Explain it in your own words

Expiration does not create a second writer. Clock time changes what lookup proposes, but only the executor publishes deletion. Lazy reads provide immediate logical absence, while bounded active ticks eventually reclaim untouched physical entries through the same ordered commit path.

### Textbook

[Chapter 4](https://github.com/system-in-miniature/mini-redis/blob/main/docs/tutorial/04-expiration.md)

## 中文

### 目标

用绝对截止时间、惰性不可见与有界主动清理，把过期变成可确定的状态迁移。

### 交付文件

- `pyproject.toml`
- `src/miniredis/core/executor.py`
- `src/miniredis/core/planner.py`
- `src/miniredis/core/ttl_planner.py`
- `src/miniredis/runtime.py`
- `tests/contract/test_ttl.py`
- `tests/helpers/time.py`

### 当前遇到的问题

值已能原子更新，但时间还不会改变可见性。相对倒计时会在暂停或重启时漂移；如果读路径直接删数据，又会绕过串行提交所有者。

### 先看会坏在哪里

过期 Key 即使还有物理 Entry，逻辑上也必须已经不可见。后续命令若失败，不得顺手提交待处理的惰性删除；原地变更也不得把旧截止时间向后延。

### 测试契约

<!-- journey-file: tests/contract/test_ttl.py -->
#### `tests/contract/test_ttl.py`

##### 测试锁定什么

它锁定惰性不可见、TTL 取整、PERSIST、有界主动清理、所有值族的截止时间保留与错误原子性。

##### 如何构造反例

测试把注入时间精确推进到截止点，分别观察逻辑读取与物理计数，并把过期操作数与后续 WRONGTYPE 操作数组合。

##### 关键测试语句

```python
assert runtime.debug_commit_seq == before
```

##### 失败意味着什么

时间在提交协议外改变了状态，错误泄漏了拟议过期删除，或变更偷偷重置了绝对截止时间。

<!-- journey-file: tests/helpers/time.py -->
#### `tests/helpers/time.py`

##### 测试锁定什么

这个 Helper 把流逝时间变成显式输入，而不是 Sleep 或墙上时钟假设。

##### 如何构造反例

每个测试选定精确毫秒并同步推进，因此边界等值可重现。

##### 关键测试语句

```python
def advance(self, milliseconds: int) -> None:
    self.value += milliseconds
```

##### 失败意味着什么

TTL 行为将无法与调度时机或不稳定的真实延迟区分。

### 基本概念

MiniRedis 存储 `expire_at_ms` 这个绝对截止时间。惰性过期在 Lookup 时让已到期 Entry 不可见，并提出 `DeleteKey(EXPIRED)`；主动过期独立采样物理 TTL Entry，使冷 Key 最终被回收。因此，逻辑消失与物理回收是两个时刻。

### 为什么需要这个机制

绝对时间能经过暂停与后续持久化，而无需重算倒计时。惰性和主动删除都走 `CommitBatch`，才能保留排序、耐久性 Hook 与未来复制语义。有界采样则防止一次维护 Tick 长时间占住 Executor。

### 运行时心智模型

注入的 Clock 提供 `now_ms`。Command Planner 用它与 Entry 截止时间比较，返回 Reply 与拟议操作。Executor 要么提交完整成功 Plan，要么在失败时丢弃它。Active Tick 也进入同一 Mailbox，最多选 N 个 TTL Key，再把过期删除作为一个维护 Batch 提交。

### 机制板块

<!-- journey-file: src/miniredis/core/ttl_planner.py -->
#### `src/miniredis/core/ttl_planner.py`

##### 是什么，为什么现在需要

这个命令族 Planner 拥有 EXPIRE、TTL/PTTL 与 PERSIST，不需要把命令语义教给 Executor。

##### 在运行时做什么

它先解析惰性过期，存储 `now_ms + duration`，再把立即过期或持久化变化表示为普通操作。

##### 关键代码

```python
put = make_put(
    key,
    previous.value,
    previous,
    now_ms + seconds * 1_000,
)
```

##### 关键语句理解

存储的是绝对截止时间；原地变更可原样复制，TTL 也可用一次时钟读取计算剩余时间。

<!-- journey-file: src/miniredis/core/executor.py -->
#### `src/miniredis/core/executor.py`

##### 是什么，为什么现在需要

单一 Executor 增加有界主动过期 Control Message，而不是让第二个 Task 直接修改 Database。

##### 在运行时做什么

它循环遍历排序 TTL Key，提出过期删除，追加一个 `ACTIVE_EXPIRE` Batch，再按 Mailbox 顺序应用。

##### 关键代码

```python
candidate_keys = ordered_keys[: self.active_expire_sample_size]
self._active_expire_cursor = candidate_keys[-1]
```

##### 关键语句理解

切片是每 Tick 工作量上界；Cursor 防止每次都只检查同一前缀。

<!-- journey-file: src/miniredis/core/planner.py -->
#### `src/miniredis/core/planner.py`

##### 是什么，为什么现在需要

稳定 Planner 门面在既有值 Planner 之后增加 TTL 命令族。

##### 在运行时做什么

它把一个类型化 TTL 命令路由到唯一语义所有者，并保留既有未知命令 Fallback。

##### 关键代码

```python
if plan is None:
    plan = plan_ttl(command, database, now_ms)
```

##### 关键语句理解

`None` 仍表示“不属于我的命令族”；不带操作的 `ExecutionPlan` 仍可以是完整 TTL 结果。

<!-- journey-file: src/miniredis/runtime.py -->
#### `src/miniredis/runtime.py`

##### 是什么，为什么现在需要

公开 Runtime 构造路径现在可接收 Clock，并暴露窄化过期诊断供契约观察。

##### 在运行时做什么

它把配置的采样上限传入 Executor，并委托主动清理，而不暴露数据库直接变更。

##### 关键代码

```python
return cls.open(
    config,
    clock=clock,
    commit_barrier=commit_barrier,
    **options,
)
```

##### 关键语句理解

`open_with_dependencies` 与 `open` 收敛到同一构造路径，生产环境与确定性测试不会在接线上漂移。

<!-- journey-file: pyproject.toml -->
#### `pyproject.toml`

##### 是什么，为什么现在需要

这个仅测试路径设置让 Contract Module 可导入共享确定 Clock。

##### 在运行时做什么

它影响 Pytest Discovery，不影响 MiniRedis 过期语义或 Package 行为。

##### 关键代码

```toml
pythonpath = ["src", "."]
```

##### 关键语句理解

项目根目录仅用来让 `tests.helpers.time` 成为稳定测试依赖。

### 验证证据

运行 `uv run pytest -q $(cat journey/stages/07-absolute-ttl/tests.txt)`。它经公开 Direct Client 与 Executor 证明 TTL 契约，包括确定的边界时间与有界物理清理。

### 需要真正记住的内容

存储绝对截止时间；分开逻辑不可见与物理回收；把过期删除当作 Proposal；命令失败时丢弃全部拟议操作；原地变更保留截止时间。

### 用自己的话讲清楚

过期不会创建第二个 Writer。Clock 时间改变 Lookup 的 Proposal，但只有 Executor 发布删除。惰性读取立即提供逻辑不可见，有界 Active Tick 则经同一有序提交路径最终回收未触碰的物理 Entry。

### 教材

[第 4 章](https://github.com/system-in-miniature/mini-redis/blob/main/docs/zh/tutorial/04-expiration.md)
