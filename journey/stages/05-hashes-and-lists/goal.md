# Stage 05 · Hash and List planning / Hash 与 List 规划

<!-- journey: chapter=3 tests_added=7 -->

## English

### Goal

Extend pure planning to field maps and direction-sensitive ordered sequences.

### Deliverable files

- `src/miniredis/core/hash_planner.py`
- `src/miniredis/core/list_planner.py`
- `src/miniredis/core/planner.py`
- `tests/contract/test_hashes.py`
- `tests/contract/test_lists.py`

### The problem at this point

String replacement has one scalar value. Hash commands must distinguish new fields from overwritten fields and delete an empty key; List commands must preserve left/right direction and Redis's inclusive negative range rules. Neither should mutate the live container while deciding its reply.

### Failure preview

The Hash contract stores `01`, attempts `HINCRBY`, and requires no commit or field change. The List contract asks for reversed and far-negative ranges and performs the final pop, proving boundary calculations cannot accidentally retain an empty key or slice with Python's different conventions.

### Test contract

<!-- journey-file: tests/contract/test_hashes.py -->
#### `tests/contract/test_hashes.py`

##### What this test locks

It locks duplicate-field counting, overwrite, integer error atomicity, alternating `HGETALL`, wrong-type behavior, no-op touches, and last-field key removal.

##### How it constructs the counterexample

It combines duplicate fields and invalid stored integers while checking both replies and commit sequence.

##### Key test statement

```python
assert runtime.debug_commit_seq == before
```

##### What a failure means

The planner mutated while validating, counted arguments instead of new fields, or represented an empty Hash as a live key.

<!-- journey-file: tests/contract/test_lists.py -->
#### `tests/contract/test_lists.py`

##### What this test locks

It locks LPUSH/RPUSH order, LPOP/RPOP direction, inclusive negative ranges, wrong-type safety, missing pops, and last-element removal.

##### How it constructs the counterexample

It pushes the same values from both ends and probes ranges such as `-99..99`, `2..1`, and `-1..-1`.

##### Key test statement

```python
assert await c.execute(CommandRequest(b"TYPE", (b"q",))) == Bytes(b"none")
```

##### What a failure means

Direction, boundary normalization, or empty-container deletion differs from the public List contract.

### Basic concepts

Both planners use copy-on-plan: clone the current container, calculate the reply and final frozen value, then return an operation. Wrong type returns `WRONGTYPE` without operations. Removing the last member of a collection produces `DeleteKey`, not an empty stored container.

### Why this mechanism is necessary

Mutable Python dictionaries and deques are convenient live representations but unsafe planning workspaces. Copying keeps validation side-effect free and preserves the same executor commit protocol used by Strings.

### Runtime mental model

The router selects a command-family planner. That planner performs logical lookup, copies the container, applies field or directional rules, freezes a replacement or proposes deletion, and returns a reply. The executor remains unaware of collection details.

### Mechanism blocks

<!-- journey-file: src/miniredis/core/hash_planner.py -->
#### `src/miniredis/core/hash_planner.py`

##### What it is and why it appears

Hash planning owns field-level counts, integer updates, sorted reply materialization, and empty-key removal.

##### Runtime role

It copies `items`, changes the copy, and produces one replacement or delete operation.

##### Key code

```python
items = {} if previous is None else dict(previous.value.items)
```

##### Statement understanding

The copy prevents duplicate-field validation or failed integer conversion from editing the live Hash.

<!-- journey-file: src/miniredis/core/list_planner.py -->
#### `src/miniredis/core/list_planner.py`

##### What it is and why it appears

List planning defines directional deque changes and converts Redis inclusive ranges into Python half-open slices.

##### Runtime role

It copies the deque, changes one end, and deletes the key when no items remain.

##### Key code

```python
return start, stop + 1
```

##### Statement understanding

The `+1` is the semantic bridge from an inclusive public stop index to an exclusive Python slice end.

<!-- journey-file: src/miniredis/core/planner.py -->
#### `src/miniredis/core/planner.py`

##### What it is and why it appears

The router now tries general/String, Hash, then List planners in a stable chain.

##### Runtime role

It returns the first plan that owns the typed command.

##### Key code

```python
if plan is None:
    plan = plan_hash(command, database, now_ms)
```

##### Statement understanding

`None` means “not my command family,” while a `Failure` inside an `ExecutionPlan` is an owned semantic result.

### Verification evidence

Run `uv run pytest -q $(cat journey/stages/05-hashes-and-lists/tests.txt)`. It proves collection-specific replies and mutation boundaries through the shared executor.

### Durable takeaways

Copy before planning, distinguish “unhandled” from “handled failure,” translate public index conventions explicitly, and remove keys when their final collection member disappears.

### Explain it in your own words

Hash and List add different data rules without adding new ownership rules. Each planner works on a copy, returns a frozen final operation, and leaves the executor to publish it in the same ordered commit path.

### Textbook

[Chapter 3](https://github.com/system-in-miniature/mini-redis/blob/main/docs/tutorial/03-data-types.md)

## 中文

### 目标

把纯规划扩展到 Field Map 与方向敏感的有序序列。

### 交付文件

- `src/miniredis/core/hash_planner.py`
- `src/miniredis/core/list_planner.py`
- `src/miniredis/core/planner.py`
- `tests/contract/test_hashes.py`
- `tests/contract/test_lists.py`

### 当前遇到的问题

String 替换只有一个标量。Hash 命令必须区分新增与覆盖 Field，并删除空 Key；List 命令必须保留左右方向与 Redis 闭区间负索引规则。两者都不能在决定 Reply 时修改实时容器。

### 先看会坏在哪里

Hash 契约保存 `01` 后尝试 `HINCRBY`，要求 Commit 与 Field 都不变。List 契约请求反向和远负 Range，并执行最后一次 Pop，证明边界计算不能错误保留空 Key，也不能直接套用 Python 的不同切片约定。

### 测试契约

<!-- journey-file: tests/contract/test_hashes.py -->
#### `tests/contract/test_hashes.py`

##### 测试锁定什么

锁定重复 Field 计数、覆盖、整数错误原子性、交替 `HGETALL`、Wrong Type、No-op Touch 与最后 Field 删除 Key。

##### 如何构造反例

组合重复 Field 与非法存储整数，同时检查 Reply 和 Commit Sequence。

##### 关键测试语句

```python
assert runtime.debug_commit_seq == before
```

##### 失败意味着什么

Planner 在校验时已变更、按参数而非新 Field 计数，或把空 Hash 表示为实时 Key。

<!-- journey-file: tests/contract/test_lists.py -->
#### `tests/contract/test_lists.py`

##### 测试锁定什么

锁定 LPUSH/RPUSH 顺序、LPOP/RPOP 方向、闭区间负索引、Wrong Type 安全、Missing Pop 与最后元素删除。

##### 如何构造反例

从两端 Push 相同值，并探测 `-99..99`、`2..1` 与 `-1..-1` 等 Range。

##### 关键测试语句

```python
assert await c.execute(CommandRequest(b"TYPE", (b"q",))) == Bytes(b"none")
```

##### 失败意味着什么

方向、边界归一化或空容器删除偏离公开 List 契约。

### 基本概念

两个 Planner 都使用 Copy-on-plan：复制当前容器，计算 Reply 与最终冻结值，再返回 Operation。Wrong Type 返回无操作 `WRONGTYPE`。删除集合最后一个成员产生 `DeleteKey`，而不是保存空容器。

### 为什么需要这个机制

可变 Python Dict 与 Deque 适合作为实时表示，却不是安全的规划工作区。复制让校验无副作用，并保留 String 使用的同一 Executor Commit 协议。

### 运行时心智模型

Router 选择命令族 Planner。Planner 做逻辑 Lookup、复制容器、应用 Field 或方向规则、冻结 Replacement 或提出 Delete，再返回 Reply。Executor 不学习集合细节。

### 机制板块

<!-- journey-file: src/miniredis/core/hash_planner.py -->
#### `src/miniredis/core/hash_planner.py`

##### 是什么，为什么现在需要

Hash Planner 拥有 Field 计数、整数更新、有序 Reply 物化与空 Key 删除。

##### 在运行时做什么

复制 `items`，修改副本，再产生一个 Replacement 或 Delete Operation。

##### 关键代码

```python
items = {} if previous is None else dict(previous.value.items)
```

##### 关键语句理解

复制阻止重复 Field 校验或整数转换失败修改实时 Hash。

<!-- journey-file: src/miniredis/core/list_planner.py -->
#### `src/miniredis/core/list_planner.py`

##### 是什么，为什么现在需要

List Planner 定义方向性 Deque 变更，并把 Redis 闭区间转换为 Python 半开切片。

##### 在运行时做什么

复制 Deque，改变一端，并在无 Item 时删除 Key。

##### 关键代码

```python
return start, stop + 1
```

##### 关键语句理解

`+1` 是公开闭 Stop Index 到 Python Exclusive Slice End 的语义桥梁。

<!-- journey-file: src/miniredis/core/planner.py -->
#### `src/miniredis/core/planner.py`

##### 是什么，为什么现在需要

Router 现在按稳定顺序尝试 General/String、Hash、List Planner。

##### 在运行时做什么

返回第一个拥有该类型命令的 Plan。

##### 关键代码

```python
if plan is None:
    plan = plan_hash(command, database, now_ms)
```

##### 关键语句理解

`None` 表示“不属于本命令族”，`ExecutionPlan` 内的 `Failure` 则是已拥有的语义结果。

### 验证证据

运行 `uv run pytest -q $(cat journey/stages/05-hashes-and-lists/tests.txt)`，通过共享 Executor 证明集合专属 Reply 与变更边界。

### 需要真正记住的内容

规划前复制；区分“未处理”与“已处理失败”；显式翻译公开索引约定；最后一个集合成员消失时删除 Key。

### 用自己的话讲清楚

Hash 与 List 增加不同数据规则，却不增加新所有权规则。各 Planner 在副本上工作，返回冻结最终操作，再由 Executor 沿同一有序提交路径发布。

### 教材

[第 3 章](https://github.com/system-in-miniature/mini-redis/blob/main/docs/zh/tutorial/03-data-types.md)
