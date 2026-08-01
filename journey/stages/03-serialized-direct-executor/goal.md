# Stage 03 · Serialized Direct executor / 串行 Direct 执行器

<!-- journey: chapter=2 tests_added=13 -->

## English

### Goal

Create a bounded Direct-first runtime whose single executor owns command and lifecycle order.

### Deliverable files

- `src/miniredis/__init__.py`
- `src/miniredis/adapters/direct.py`
- `src/miniredis/clock.py`
- `src/miniredis/config.py`
- `src/miniredis/core/executor.py`
- `src/miniredis/core/mailbox.py`
- `src/miniredis/core/planner.py`
- `src/miniredis/runtime.py`
- `tests/concurrency/test_direct_executor.py`

### The problem at this point

Typed commands still have no owner. If every caller reads and writes the database directly, concurrent requests can allocate the same next sequence, shutdown can race accepted work, and a full command queue can prevent the control message needed to close it.

### Failure preview

The contract pauses an executor with capacity one, submits a first request, observes a second request receive `BUSY`, and then posts control work that must still resume and close the runtime. Failure-cleanup cases also inject planner and barrier exceptions and require every accepted caller to reach one terminal outcome.

### Test contract

<!-- journey-file: tests/concurrency/test_direct_executor.py -->
#### `tests/concurrency/test_direct_executor.py`

##### What this test locks

It locks bounded user admission, independent control admission, one owned worker, cancellation-safe start/close, terminal failure cleanup, binary Direct calls, and idempotent lifecycle operations.

##### How it constructs the counterexample

Gates pause the executor at specific ownership boundaries while multiple callers, shutdown, cancellation, or an injected failure race for the next mailbox turn.

##### Key test statement

```python
assert await client.execute(CommandRequest(b"PING")) == Failure(
    "BUSY", "command queue is full"
)
```

##### What a failure means

A failure means accepted work can be orphaned, control work can deadlock behind user pressure, or more than one component can decide runtime order.

### Basic concepts

Serialization means state-affecting events are processed one at a time in a total mailbox order. Admission is separate from execution: rejecting excess user work does not consume a turn. Control messages use the same ordered owner but a separate unbounded-by-user admission path.

An owned task is created, supervised, and terminalized by the runtime. Caller cancellation must not silently cancel shared startup, shutdown, or already-accepted state work.

### Why this mechanism is necessary

Atomic planners are insufficient if multiple tasks can apply them concurrently. One owner makes sequence allocation and state transition indivisible. Bounded admission prevents memory growth; separate control admission guarantees overload cannot prevent cleanup.

### Runtime mental model

The Direct client parses a request and asks the executor to admit it. A user event enters the mailbox with a runtime-unique token. The one worker plans and completes it, while control events close sessions or the runtime in the same total order. The runtime assembles and supervises this owner.

### Mechanism blocks

<!-- journey-file: src/miniredis/core/mailbox.py -->
#### `src/miniredis/core/mailbox.py`

##### What it is and why it appears

`EventLoopMailbox` is a single-loop queue with bounded user slots and independent control admission.

##### Runtime role

It preserves event order, exposes user pressure, and always leaves a path for shutdown until control admission closes.

##### Key code

```python
def admit_user(self, item: T) -> bool:
    if not self._user_open or self._pending_users >= self._max_pending_users:
        return False
```

##### Statement understanding

Capacity rejection occurs before enqueue, so a `BUSY` request never becomes accepted work that later needs terminalization.

<!-- journey-file: src/miniredis/core/planner.py -->
#### `src/miniredis/core/planner.py`

##### What it is and why it appears

The initial planner handles side-effect-free `PING` and `ECHO` as `ExecutionPlan` values.

##### Runtime role

It demonstrates the future boundary: planning returns a reply and operations rather than mutating the database.

##### Key code

```python
return ExecutionPlan(Ok(b"PONG"))
```

##### Statement understanding

A plan with no operations is a semantic result but not a commit; the executor can reply without advancing state.

<!-- journey-file: src/miniredis/core/executor.py -->
#### `src/miniredis/core/executor.py`

##### What it is and why it appears

The executor owns accepted tokens, the mailbox worker, planning, control events, and terminal failure cleanup.

##### Runtime role

It turns one mailbox event into one outcome and prevents later events from observing a partially handled turn.

##### Key code

```python
event = await self.mailbox.take()
await self._handle_event(event)
```

##### Statement understanding

Only this loop chooses the next state event; asyncio callers may run concurrently but cannot bypass mailbox order.

<!-- journey-file: src/miniredis/adapters/direct.py -->
#### `src/miniredis/adapters/direct.py`

##### What it is and why it appears

The Direct adapter is the first public client and contains no data-structure semantics.

##### Runtime role

It submits binary requests, awaits executor-owned outcomes, and maps inactive lifecycle states to stable failures.

##### Key code

```python
submitted = self._runtime.submit_request(
    session_id=self.session_id,
    request=request,
)
```

##### Statement understanding

The adapter delegates both parsing and ordering; a future socket adapter can meet it at the same request boundary.

<!-- journey-file: src/miniredis/clock.py -->
#### `src/miniredis/clock.py`

##### What it is and why it appears

The `Clock` protocol makes time an injected observation before TTL is introduced.

##### Runtime role

The executor samples one `now_ms` for a planning turn instead of letting commands read wall time independently.

##### Key code

```python
class Clock(Protocol):
    def now_ms(self) -> int: ...
```

##### Statement understanding

Explicit time enables deterministic expiry tests and prevents one command from observing multiple inconsistent instants.

<!-- journey-file: src/miniredis/config.py -->
#### `src/miniredis/config.py`

##### What it is and why it appears

Frozen configuration collects bounded runtime choices and validates them at construction.

##### Runtime role

The runtime and executor receive one immutable set of limits.

##### Key code

```python
if self.max_pending_commands <= 0:
    raise ValueError("max_pending_commands must be positive")
```

##### Statement understanding

An invalid bound is rejected before tasks start, so runtime code never needs a zero-capacity special state.

<!-- journey-file: src/miniredis/runtime.py -->
#### `src/miniredis/runtime.py`

##### What it is and why it appears

`MiniRedis` assembles database, parser, planner, executor, clock, clients, and lifecycle state.

##### Runtime role

It starts one worker, admits clients only while running, and shields owned close work from cancelling callers.

##### Key code

```python
await asyncio.shield(self._close_task)
```

##### Statement understanding

Cancelling one waiter does not cancel shared cleanup; runtime ownership outlives the caller awaiting it.

#### Public API wiring

<!-- journey-file: src/miniredis/__init__.py -->
#### `src/miniredis/__init__.py`

Exports `CommandRequest`, `MiniRedisConfig`, `MiniRedis`, and `RuntimeState`; it adds no independent execution rule.

### Verification evidence

Run `uv run pytest -q $(cat journey/stages/03-serialized-direct-executor/tests.txt)`. It proves ownership and lifecycle under controlled races; no data mutation exists yet.

### Durable takeaways

Admission and execution are different. User capacity never blocks control cleanup. One supervised worker owns state order, and accepted requests always receive one terminal outcome.

### Explain it in your own words

MiniRedis allows callers to be concurrent but makes state ownership single-threaded in the logical sense. Requests queue through bounded admission, control events keep a guaranteed route, and one runtime-owned executor decides the only observable order.

### Textbook

[Chapter 2](https://github.com/system-in-miniature/mini-redis/blob/main/docs/tutorial/02-command-life.md)

## 中文

### 目标

建立一个有界的 Direct-first Runtime，由单一 Executor 拥有命令与生命周期顺序。

### 交付文件

- `src/miniredis/__init__.py`
- `src/miniredis/adapters/direct.py`
- `src/miniredis/clock.py`
- `src/miniredis/config.py`
- `src/miniredis/core/executor.py`
- `src/miniredis/core/mailbox.py`
- `src/miniredis/core/planner.py`
- `src/miniredis/runtime.py`
- `tests/concurrency/test_direct_executor.py`

### 当前遇到的问题

类型化命令仍没有所有者。如果每个调用方直接读写 Database，并发请求可能分配同一个下一序列，Shutdown 会与已接受工作竞态，满命令队列还可能阻止关闭所需的 Control Message。

### 先看会坏在哪里

契约暂停容量为一的 Executor，提交第一个请求，观察第二个得到 `BUSY`，再投递仍必须能够恢复和关闭 Runtime 的控制工作。失败清理用例还注入 Planner/Barrier 异常，并要求每个已接受调用方抵达唯一终态。

### 测试契约

<!-- journey-file: tests/concurrency/test_direct_executor.py -->
#### `tests/concurrency/test_direct_executor.py`

##### 测试锁定什么

锁定有界用户准入、独立控制准入、单一 Owned Worker、取消安全的 Start/Close、终态失败清理、二进制 Direct 调用与幂等生命周期操作。

##### 如何构造反例

Gate 在具体所有权边界暂停 Executor，让多个调用方、Shutdown、Cancellation 或注入失败竞逐下一 Mailbox Turn。

##### 关键测试语句

```python
assert await client.execute(CommandRequest(b"PING")) == Failure(
    "BUSY", "command queue is full"
)
```

##### 失败意味着什么

失败表示已接受工作可能失去归宿，控制工作可能被用户压力死锁，或不止一个组件能够决定 Runtime 顺序。

### 基本概念

串行化表示影响状态的事件按一个 Mailbox 全序逐个处理。准入不同于执行：拒绝多余用户工作不会消耗 Turn。控制消息使用同一个有序 Owner，但拥有独立于用户容量的准入路径。

Owned Task 由 Runtime 创建、监督并终态化。调用方取消不能静默取消共享 Startup、Shutdown 或已经接受的状态工作。

### 为什么需要这个机制

如果多个 Task 可以并发应用，原子 Planner 仍然不够。单一 Owner 让序列分配与状态迁移不可分。有界准入阻止内存增长，独立控制准入保证过载不能阻止清理。

### 运行时心智模型

Direct Client 解析请求并请求 Executor 准入。用户事件带 Runtime 唯一 Token 进入 Mailbox。唯一 Worker 规划并完成它；Control Event 在同一全序中关闭 Session 或 Runtime。Runtime 负责组装与监督这个 Owner。

### 机制板块

<!-- journey-file: src/miniredis/core/mailbox.py -->
#### `src/miniredis/core/mailbox.py`

##### 是什么，为什么现在需要

`EventLoopMailbox` 是带有界用户槽与独立控制准入的单 Event Loop 队列。

##### 在运行时做什么

它保留事件顺序、暴露用户压力，并在控制准入关闭前始终保留 Shutdown 路径。

##### 关键代码

```python
def admit_user(self, item: T) -> bool:
    if not self._user_open or self._pending_users >= self._max_pending_users:
        return False
```

##### 关键语句理解

容量拒绝发生在入队前，因此 `BUSY` 请求从未成为需要后续终态化的已接受工作。

<!-- journey-file: src/miniredis/core/planner.py -->
#### `src/miniredis/core/planner.py`

##### 是什么，为什么现在需要

初始 Planner 把无副作用 `PING`/`ECHO` 处理成 `ExecutionPlan`。

##### 在运行时做什么

它展示后续边界：规划返回 Reply 与操作，而不是修改 Database。

##### 关键代码

```python
return ExecutionPlan(Ok(b"PONG"))
```

##### 关键语句理解

没有操作的 Plan 是语义结果但不是 Commit；Executor 可以回复而不推进状态。

<!-- journey-file: src/miniredis/core/executor.py -->
#### `src/miniredis/core/executor.py`

##### 是什么，为什么现在需要

Executor 拥有已接受 Token、Mailbox Worker、规划、Control Event 与终态失败清理。

##### 在运行时做什么

它把一个 Mailbox Event 变成一个 Outcome，阻止后续事件看到处理一半的 Turn。

##### 关键代码

```python
event = await self.mailbox.take()
await self._handle_event(event)
```

##### 关键语句理解

只有这个 Loop 选择下一个状态事件；asyncio 调用方可以并发，但不能绕过 Mailbox 顺序。

<!-- journey-file: src/miniredis/adapters/direct.py -->
#### `src/miniredis/adapters/direct.py`

##### 是什么，为什么现在需要

Direct Adapter 是第一个公开 Client，不包含数据结构语义。

##### 在运行时做什么

它提交二进制请求，等待 Executor-owned Outcome，并把非活动生命周期映射为稳定 Failure。

##### 关键代码

```python
submitted = self._runtime.submit_request(
    session_id=self.session_id,
    request=request,
)
```

##### 关键语句理解

Adapter 同时委托解析与排序；未来 Socket Adapter 可以在同一请求边界与它汇合。

<!-- journey-file: src/miniredis/clock.py -->
#### `src/miniredis/clock.py`

##### 是什么，为什么现在需要

`Clock` Protocol 在 TTL 出现前先让时间成为注入观察。

##### 在运行时做什么

Executor 为一个 Planning Turn 采样一次 `now_ms`，而不是让命令各自读取墙钟。

##### 关键代码

```python
class Clock(Protocol):
    def now_ms(self) -> int: ...
```

##### 关键语句理解

显式时间支持确定性 Expiry 测试，也防止一个命令观察多个不一致时刻。

<!-- journey-file: src/miniredis/config.py -->
#### `src/miniredis/config.py`

##### 是什么，为什么现在需要

冻结 Config 收集有界 Runtime 选择，并在构造时校验。

##### 在运行时做什么

Runtime 与 Executor 接收同一组不可变 Limit。

##### 关键代码

```python
if self.max_pending_commands <= 0:
    raise ValueError("max_pending_commands must be positive")
```

##### 关键语句理解

非法 Bound 在 Task 启动前被拒绝，Runtime 代码无需处理零容量特殊状态。

<!-- journey-file: src/miniredis/runtime.py -->
#### `src/miniredis/runtime.py`

##### 是什么，为什么现在需要

`MiniRedis` 组装 Database、Parser、Planner、Executor、Clock、Client 与生命周期状态。

##### 在运行时做什么

它启动一个 Worker，只在 Running 时准入 Client，并用 Shield 保护 Owned Close Work 不被等待者取消。

##### 关键代码

```python
await asyncio.shield(self._close_task)
```

##### 关键语句理解

取消一个 Waiter 不会取消共享清理；Runtime 所有权比等待它的调用方更长。

#### 公开 API 接线

<!-- journey-file: src/miniredis/__init__.py -->
#### `src/miniredis/__init__.py`

导出 `CommandRequest`、`MiniRedisConfig`、`MiniRedis` 与 `RuntimeState`，不增加独立执行规则。

### 验证证据

运行 `uv run pytest -q $(cat journey/stages/03-serialized-direct-executor/tests.txt)`。它在受控竞态下证明所有权与生命周期；此时尚无数据变更。

### 需要真正记住的内容

准入与执行不同；用户容量不能阻塞控制清理；一个受监督 Worker 拥有状态顺序；已接受请求总会得到一个终态。

### 用自己的话讲清楚

MiniRedis 允许调用方并发，但在逻辑上让状态所有权单线程化。请求经过有界准入排队，控制事件保留保证路径，一个 Runtime-owned Executor 决定唯一可观察顺序。

### 教材

[第 2 章](https://github.com/system-in-miniature/mini-redis/blob/main/docs/zh/tutorial/02-command-life.md)
