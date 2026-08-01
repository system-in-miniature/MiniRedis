# Stage 06 · Set and Sorted Set projections / Set 与 Sorted Set 投影

<!-- journey: chapter=3 tests_added=6 -->

## English

### Goal

Add uniqueness and score-order semantics with deterministic public projections.

### Deliverable files

- `src/miniredis/core/planner.py`
- `src/miniredis/core/set_planner.py`
- `src/miniredis/core/zset_planner.py`
- `tests/contract/test_sets.py`
- `tests/contract/test_sorted_sets.py`

### The problem at this point

Python sets have no stable iteration order, and a Sorted Set must define ties as well as score order. Multi-item parse failures must also reject the whole request before an early valid pair can mutate anything.

### Failure preview

One contract intersects a missing Set with a later String and still requires `WRONGTYPE`; stopping after the missing key would hide an invalid operand. Another submits a valid ZADD pair followed by `nan` and requires no member or commit to appear.

### Test contract

<!-- journey-file: tests/contract/test_sets.py -->
#### `tests/contract/test_sets.py`

##### What this test locks

It locks uniqueness counts, deterministic members, full-operand type checks, intersection, and last-member removal.

##### How it constructs the counterexample

It places a missing key before a wrong-type key so an incorrect early-empty optimization returns the wrong semantic result.

##### Key test statement

```python
assert reply.code == "WRONGTYPE"
```

##### What a failure means

Optimization changed validation semantics or unordered storage leaked into a public reply.

<!-- journey-file: tests/contract/test_sorted_sets.py -->
#### `tests/contract/test_sorted_sets.py`

##### What this test locks

It locks score/member ordering, binary tie-breaks, exclusive bounds, ranks, score formatting, whole-request validation, and empty-key removal.

##### How it constructs the counterexample

Equal scores arrive in reverse binary order, and a later NaN follows an earlier valid pair.

##### Key test statement

```python
assert runtime.debug_commit_seq == before
```

##### What a failure means

Score validation was incremental, or result order depends on dictionary insertion rather than the public ordering rule.

### Basic concepts

Set storage owns uniqueness but not presentation order. MiniRedis sorts members by bytes when materializing replies and stored state. Sorted Set storage maps member to score; its total order is `(score, member_bytes)`, which makes equal-score behavior deterministic.

### Why this mechanism is necessary

Deterministic projection separates mathematical collection semantics from Python container iteration. Whole-request parsing and copy-on-plan ensure a later invalid operand cannot leave earlier partial state.

### Runtime mental model

Typed commands already contain validated members and scores. A family planner copies the live collection, calculates counts and ordering, proposes a frozen replacement or deletion, and returns deterministic `Items`/`Number`/`Bytes` replies.

### Mechanism blocks

<!-- journey-file: src/miniredis/core/set_planner.py -->
#### `src/miniredis/core/set_planner.py`

##### What it is and why it appears

Set planning owns uniqueness-changing operations and deterministic read projections.

##### Runtime role

It uses live sets for membership math and binary sorting only when returning public items.

##### Key code

```python
Items(tuple(Bytes(member) for member in sorted(entry.value.items))),
```

##### Statement understanding

Sorting is a projection rule, not a claim that the live Set is an ordered container.

<!-- journey-file: src/miniredis/core/zset_planner.py -->
#### `src/miniredis/core/zset_planner.py`

##### What it is and why it appears

Sorted Set planning defines one total member order and score-bound filtering.

##### Runtime role

It copies the member-score map, applies typed pairs, and projects ranges and ranks through `_ordered`.

##### Key code

```python
return sorted(scores.items(), key=lambda item: (item[1], item[0]))
```

##### Statement understanding

Member bytes are the stable tie-break when scores compare equal, so results do not inherit insertion order.

<!-- journey-file: src/miniredis/core/planner.py -->
#### `src/miniredis/core/planner.py`

##### What it is and why it appears

The planner facade adds Set and Sorted Set handlers after earlier families.

##### Runtime role

It keeps one executor-facing planning call while preserving per-family ownership.

##### Key code

```python
if plan is None:
    plan = plan_zset(command, database, now_ms)
```

##### Statement understanding

Routing order does not alter semantics because each typed command has exactly one owning family.

### Verification evidence

Run `uv run pytest -q $(cat journey/stages/06-sets-and-sorted-sets/tests.txt)`. It proves deterministic projections and all-or-nothing validation for both families.

### Durable takeaways

Container order and public order are separate. Equal scores require an explicit tie-break. A later invalid argument prevents every earlier proposed mutation.

### Explain it in your own words

Sets use Python containers for efficient mathematical operations but sort at the boundary. Sorted Sets define a complete `(score, bytes)` order. Both planners return one deterministic proposal rather than mutating as they parse.

### Textbook

[Chapter 3](https://github.com/system-in-miniature/mini-redis/blob/main/docs/tutorial/03-data-types.md)

## 中文

### 目标

加入唯一性与 Score 顺序语义，并提供确定性公开投影。

### 交付文件

- `src/miniredis/core/planner.py`
- `src/miniredis/core/set_planner.py`
- `src/miniredis/core/zset_planner.py`
- `tests/contract/test_sets.py`
- `tests/contract/test_sorted_sets.py`

### 当前遇到的问题

Python Set 没有稳定迭代顺序，Sorted Set 除了 Score 顺序还必须定义 Tie。多项请求的后续解析失败也必须拒绝整个请求，不能让前面的合法 Pair 已经变更状态。

### 先看会坏在哪里

一条契约把 Missing Set 与后面的 String 做 Intersection，仍要求 `WRONGTYPE`；遇到 Missing 就停止会隐藏非法 Operand。另一条先提交合法 ZADD Pair，再提交 `nan`，要求没有 Member 或 Commit 出现。

### 测试契约

<!-- journey-file: tests/contract/test_sets.py -->
#### `tests/contract/test_sets.py`

##### 测试锁定什么

锁定唯一性计数、确定性 Member、完整 Operand 类型检查、Intersection 与最后 Member 删除。

##### 如何构造反例

把 Missing Key 放在 Wrong Type Key 前，暴露错误的 Early-empty Optimization。

##### 关键测试语句

```python
assert reply.code == "WRONGTYPE"
```

##### 失败意味着什么

优化改变了校验语义，或无序存储泄漏进公开 Reply。

<!-- journey-file: tests/contract/test_sorted_sets.py -->
#### `tests/contract/test_sorted_sets.py`

##### 测试锁定什么

锁定 Score/Member 顺序、二进制 Tie-break、Exclusive Bound、Rank、Score 格式、完整请求校验与空 Key 删除。

##### 如何构造反例

相同 Score 按反向二进制顺序到达，后续 NaN 跟在前一个合法 Pair 之后。

##### 关键测试语句

```python
assert runtime.debug_commit_seq == before
```

##### 失败意味着什么

Score 校验是增量的，或结果顺序依赖 Dict 插入顺序而不是公开规则。

### 基本概念

Set 存储拥有唯一性，但不拥有展示顺序。MiniRedis 在物化 Reply 与 Stored State 时按 Bytes 排序。Sorted Set 把 Member 映射到 Score，其总序是 `(score, member_bytes)`，使相同 Score 行为确定。

### 为什么需要这个机制

确定性投影把数学集合语义与 Python 容器迭代分离。完整请求解析和 Copy-on-plan 保证后续非法 Operand 不会留下前面的部分状态。

### 运行时心智模型

类型化命令已包含校验过的 Member 与 Score。命令族 Planner 复制实时集合，计算计数和顺序，提出冻结 Replacement 或 Delete，并返回确定性 `Items`/`Number`/`Bytes` Reply。

### 机制板块

<!-- journey-file: src/miniredis/core/set_planner.py -->
#### `src/miniredis/core/set_planner.py`

##### 是什么，为什么现在需要

Set Planner 拥有改变唯一性的操作与确定性读取投影。

##### 在运行时做什么

用实时 Set 做 Membership 运算，只在返回公开 Item 时二进制排序。

##### 关键代码

```python
Items(tuple(Bytes(member) for member in sorted(entry.value.items))),
```

##### 关键语句理解

排序是投影规则，不表示实时 Set 是有序容器。

<!-- journey-file: src/miniredis/core/zset_planner.py -->
#### `src/miniredis/core/zset_planner.py`

##### 是什么，为什么现在需要

Sorted Set Planner 定义唯一总序与 Score Bound 过滤。

##### 在运行时做什么

复制 Member-Score Map，应用类型化 Pair，并通过 `_ordered` 投影 Range 与 Rank。

##### 关键代码

```python
return sorted(scores.items(), key=lambda item: (item[1], item[0]))
```

##### 关键语句理解

Score 相等时用 Member Bytes 稳定 Tie-break，结果不继承插入顺序。

<!-- journey-file: src/miniredis/core/planner.py -->
#### `src/miniredis/core/planner.py`

##### 是什么，为什么现在需要

Planner 门面在早期命令族后增加 Set 与 Sorted Set Handler。

##### 在运行时做什么

保留一个 Executor-facing Planning Call，同时维持命令族所有权。

##### 关键代码

```python
if plan is None:
    plan = plan_zset(command, database, now_ms)
```

##### 关键语句理解

路由顺序不改变语义，因为每个类型化命令只有一个拥有者命令族。

### 验证证据

运行 `uv run pytest -q $(cat journey/stages/06-sets-and-sorted-sets/tests.txt)`，证明两个命令族的确定性投影与全有全无校验。

### 需要真正记住的内容

容器顺序与公开顺序分离；相同 Score 需要显式 Tie-break；后续非法参数阻止所有前序 Proposal。

### 用自己的话讲清楚

Set 用 Python 容器做集合运算，在边界排序。Sorted Set 定义完整 `(score, bytes)` 顺序。两者都返回一个确定性 Proposal，而不是边解析边变更。

### 教材

[第 3 章](https://github.com/system-in-miniature/mini-redis/blob/main/docs/zh/tutorial/03-data-types.md)
