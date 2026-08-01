# Stage 11 · Blocking pop race ownership / 阻塞 Pop 竞态所有权

<!-- journey: chapter=9 tests_added=12 -->

## English

### Goal

Implement BLPOP so push, timeout, cancellation, and session close have one mailbox-ordered winner.

### Deliverable files

- `src/miniredis/adapters/direct.py`
- `src/miniredis/clock.py`
- `src/miniredis/commands/model.py`
- `src/miniredis/commands/parser.py`
- `src/miniredis/core/blocking.py`
- `src/miniredis/core/executor.py`
- `src/miniredis/core/planner.py`
- `src/miniredis/runtime.py`
- `tests/concurrency/test_blpop_races.py`
- `tests/helpers/time.py`
- `tests/mechanisms/test_blpop.py`
- `tests/mechanisms/test_blpop_push_batch.py`

### The problem at this point

A command can now remain owned after its immediate execution turn. The same waiter may be targeted by a list push, timer callback, caller cancellation, or session close. If those paths mutate separate Futures or indexes, one item can be consumed twice or a stale waiter can survive forever.

### Failure preview

Timeout-before-push must leave the later item; push-before-timeout must consume it once and make the timer stale. One multi-item push waking two FIFO waiters must still allocate only one commit, and every terminal path must remove all waiter indexes and timer ownership.

### Test contract

<!-- journey-file: tests/mechanisms/test_blpop.py -->
#### `tests/mechanisms/test_blpop.py`

##### What this test locks

It locks strict finite timeout parsing, first-ready-key order, type-check stopping, and one waiter indexed under every requested key.

##### How it constructs the counterexample

It mixes a ready list with a later wrong type, reverses them, and cancels one infinite waiter registered under two keys.

##### Key test statement

```python
assert runtime.debug_waiter_index_counts == (0, 0, 0)
```

##### What a failure means

Parser syntax drifted, scan order changed semantics, or terminal cleanup left identity, key, or session indexes behind.

<!-- journey-file: tests/concurrency/test_blpop_races.py -->
#### `tests/concurrency/test_blpop_races.py`

##### What this test locks

It locks explicit timer firing, timeout/push ordering, cancellation, session close, and stale-event harmlessness.

##### How it constructs the counterexample

It advances a fake clock without firing callbacks, then chooses whether timeout, push, cancel, or close enters the mailbox first.

##### Key test statement

```python
scheduler.fire_due()
assert await task == Bytes(None)
```

##### What a failure means

Clock reads performed hidden scheduling, or a losing race path could still transition an already-terminal waiter.

<!-- journey-file: tests/mechanisms/test_blpop_push_batch.py -->
#### `tests/mechanisms/test_blpop_push_batch.py`

##### What this test locks

It locks FIFO waiter assignment, complete LPUSH/RPUSH order, and one atomic batch for storage plus wakeups.

##### How it constructs the counterexample

Two clients block before one two-item push; the contract then inspects replies, commit sequence, and the final storage operation.

##### Key test statement

```python
assert runtime.debug_commit_seq == before + 1
```

##### What a failure means

Wakeups became separate commits, observed a partial push, or consumed an item more than once.

<!-- journey-file: tests/helpers/time.py -->
#### `tests/helpers/time.py`

##### What this test locks

The manual scheduler separates deadline registration, clock advancement, callback firing, and cancellation.

##### How it constructs the counterexample

It orders equal-deadline handles with a sequence and fires only non-cancelled callbacks whose deadline is due.

##### Key test statement

```python
while self._calls and self._calls[0].deadline_ms <= self.clock.now_ms():
```

##### What a failure means

Concurrency tests can no longer state which timer event entered the mailbox first.

### Basic concepts

A waiter has identity, generation, owning token/session, ordered keys, optional deadline, and one state transition. Multiple indexes accelerate lookup but do not create multiple owners. Timer callbacks only post control messages; the executor decides whether their generation is still active.

### Why this mechanism is necessary

Blocking extends request lifetime across turns, so single-writer ownership must extend with it. Mailbox-ordering all terminal events converts races into deterministic event order and lets list storage change and waiter wakeups share one commit decision.

### Runtime mental model

BLPOP first performs an immediate ordered scan. If nothing is ready, the executor registers one waiter and optional timer. A push plans its full list result, reserves FIFO waiters, adjusts the one storage operation, commits once, then transitions and replies to reserved waiters. Timeout, cancel, and close use the same generation-checked transition gate.

### Mechanism blocks

<!-- journey-file: src/miniredis/commands/model.py -->
#### `src/miniredis/commands/model.py`

##### What it is and why it appears

The typed command freezes BLPOP key order and timeout milliseconds.

##### Runtime role

Downstream code receives validated immutable intent and never reparses transport bytes.

##### Key code

```python
class BlPop:
    keys: tuple[bytes, ...]
    timeout_ms: int
```

##### Statement understanding

Key tuple order is observable because BLPOP chooses the first ready key.

<!-- journey-file: src/miniredis/commands/parser.py -->
#### `src/miniredis/commands/parser.py`

##### What it is and why it appears

The strict parser accepts Redis-style finite decimal timeouts and rejects huge or nonnumeric forms.

##### Runtime role

It converts seconds to ceiling-rounded milliseconds before any waiter or timer exists.

##### Key code

```python
milliseconds = int(timeout_ms.to_integral_value(rounding=ROUND_CEILING))
return BlPop(tuple(args[:-1]), milliseconds)
```

##### Statement understanding

Ceiling avoids firing earlier than the requested positive fractional timeout.

<!-- journey-file: src/miniredis/clock.py -->
#### `src/miniredis/clock.py`

##### What it is and why it appears

Timer scheduling is separated from the Clock value source.

##### Runtime role

Production uses event-loop callbacks; tests inject a manual scheduler against the same deadline contract.

##### Key code

```python
delay = max(0, deadline_ms - self._clock.now_ms()) / 1000
return asyncio.get_running_loop().call_later(delay, callback)
```

##### Statement understanding

The scheduler derives delay at registration, while the callback still enters state ownership only through mailbox control.

<!-- journey-file: src/miniredis/core/blocking.py -->
#### `src/miniredis/core/blocking.py`

##### What it is and why it appears

This module owns waiter indexes, state transitions, and deterministic reservation of pushed list items.

##### Runtime role

It finds FIFO eligible waiters, removes all indexes on one transition, cancels timers, and returns wakeup proposals.

##### Key code

```python
if (
    waiter is None
    or waiter.generation != generation
    or waiter.state is not WaiterState.ACTIVE
):
    return None
```

##### Statement understanding

Identity plus generation makes late timeout/cancel events harmless after another event already won.

<!-- journey-file: src/miniredis/core/planner.py -->
#### `src/miniredis/core/planner.py`

##### What it is and why it appears

The planner owns the immediate, nonblocking BLPOP scan.

##### Runtime role

It checks keys in order, stops at the first ready list, and proposes exactly one updated or deleted list entry.

##### Key code

```python
for key in command.keys:
    entry, expired = lookup(database, key, now_ms)
```

##### Statement understanding

Ordered lookup is semantic: a ready earlier key ends scanning before a later wrong type.

<!-- journey-file: src/miniredis/core/executor.py -->
#### `src/miniredis/core/executor.py`

##### What it is and why it appears

The executor becomes the sole arbiter of waiter registration, wakeup, timeout, abandonment, and close.

##### Runtime role

It registers timers as control producers, folds push wakeups into the storage plan, commits once, then terminalizes winning waiters.

##### Key code

```python
waiter = self.waiters.register(
    request.token,
    request.session_id,
    request.command.keys,
    deadline,
)
```

##### Statement understanding

The original request remains owned while blocked; a separate waiter Future is unnecessary and would split completion ownership.

<!-- journey-file: src/miniredis/adapters/direct.py -->
#### `src/miniredis/adapters/direct.py`

##### What it is and why it appears

The Direct boundary maps session loss for BLPOP to the public nil result.

##### Runtime role

It otherwise preserves the typed terminal-outcome handling introduced for all requests.

##### Key code

```python
case TransportClosed() if isinstance(parsed, BlPop):
    return Bytes(None)
```

##### Statement understanding

Transport lifecycle is translated at the adapter boundary; the executor still reports a transport outcome, not protocol bytes.

<!-- journey-file: src/miniredis/runtime.py -->
#### `src/miniredis/runtime.py`

##### What it is and why it appears

The runtime injects a TimerScheduler and exposes narrow waiter lifecycle evidence.

##### Runtime role

It reports waiter/timer counts and waits on debug notifications instead of sleeps.

##### Key code

```python
async def debug_wait_for_waiters(self, count: int) -> None:
    await self._debug_wait(lambda: self.executor.waiters.active_count == count)
```

##### Statement understanding

The contract observes registry ownership directly, making race setup deterministic.

### Verification evidence

Run `uv run pytest -q $(cat journey/stages/11-blocking-pop-races/tests.txt)`. It covers parser boundaries, immediate scans, waiter indexing, push/timeout order, cancellation, session close, and one-batch FIFO wakeups.

### Durable takeaways

One blocked request remains runtime-owned; index but do not duplicate ownership; make timer callbacks control messages; use generation checks; reserve wakeups against the complete push; commit storage once before replying.

### Explain it in your own words

BLPOP is not a Future waiting beside the executor. It is an accepted request whose waiter metadata stays inside executor ownership. Push, timeout, cancel, and close become ordered messages, and the first valid transition wins while every stale event becomes a no-op.

### Textbook

[Chapter 9](https://github.com/system-in-miniature/mini-redis/blob/main/docs/tutorial/09-blocking-pubsub-transactions.md)

## 中文

### 目标

实现 BLPOP，使 Push、Timeout、取消与 Session 关闭仅有一个按 Mailbox 顺序产生的胜者。

### 交付文件

- `src/miniredis/adapters/direct.py`
- `src/miniredis/clock.py`
- `src/miniredis/commands/model.py`
- `src/miniredis/commands/parser.py`
- `src/miniredis/core/blocking.py`
- `src/miniredis/core/executor.py`
- `src/miniredis/core/planner.py`
- `src/miniredis/runtime.py`
- `tests/concurrency/test_blpop_races.py`
- `tests/helpers/time.py`
- `tests/mechanisms/test_blpop.py`
- `tests/mechanisms/test_blpop_push_batch.py`

### 当前遇到的问题

命令现在可在立即执行 Turn 之后仍被所有。同一 Waiter 可被 List Push、Timer Callback、调用方取消或 Session 关闭同时目标。如果这些路径修改不同 Future 或索引，一个 Item 可被消费两次，或过期 Waiter 永久残留。

### 先看会坏在哪里

Timeout 先于 Push 时，后来的 Item 必须保留；Push 先于 Timeout 时，Item 只消费一次，Timer 变成过期事件。一次多 Item Push 唤醒两个 FIFO Waiter 仍只能分配一个 Commit，每个终态路径都要移除全部 Waiter 索引与 Timer 所有权。

### 测试契约

<!-- journey-file: tests/mechanisms/test_blpop.py -->
#### `tests/mechanisms/test_blpop.py`

##### 测试锁定什么

它锁定严格有限 Timeout Parse、第一就绪 Key 顺序、类型检查停止与一个 Waiter 在所有请求 Key 下的索引。

##### 如何构造反例

它混合已就绪 List 与后续 Wrong Type，再反转它们，并取消一个注册在两个 Key 下的无限 Waiter。

##### 关键测试语句

```python
assert runtime.debug_waiter_index_counts == (0, 0, 0)
```

##### 失败意味着什么

Parser 语法漂移、Scan 顺序改变语义，或终态清理留下身份、Key 或 Session 索引。

<!-- journey-file: tests/concurrency/test_blpop_races.py -->
#### `tests/concurrency/test_blpop_races.py`

##### 测试锁定什么

它锁定显式 Timer 触发、Timeout/Push 顺序、取消、Session 关闭与过期事件无害性。

##### 如何构造反例

它推进 Fake Clock 但不触发 Callback，然后选择 Timeout、Push、Cancel 或 Close 中谁先进 Mailbox。

##### 关键测试语句

```python
scheduler.fire_due()
assert await task == Bytes(None)
```

##### 失败意味着什么

Clock Read 执行了隐式调度，或失败竞态路径仍可迁移已终态 Waiter。

<!-- journey-file: tests/mechanisms/test_blpop_push_batch.py -->
#### `tests/mechanisms/test_blpop_push_batch.py`

##### 测试锁定什么

它锁定 FIFO Waiter 分配、完整 LPUSH/RPUSH 顺序与 Storage + Wakeup 的单一原子 Batch。

##### 如何构造反例

两个 Client 在一次两 Item Push 前阻塞，契约随后检查 Reply、Commit Sequence 与最终 Storage Operation。

##### 关键测试语句

```python
assert runtime.debug_commit_seq == before + 1
```

##### 失败意味着什么

Wakeup 变成独立 Commit，观察到部分 Push，或同一 Item 被消费多次。

<!-- journey-file: tests/helpers/time.py -->
#### `tests/helpers/time.py`

##### 测试锁定什么

Manual Scheduler 分开 Deadline 注册、Clock 推进、Callback 触发与取消。

##### 如何构造反例

它用 Sequence 排序相同 Deadline Handle，并只触发已到期且未取消的 Callback。

##### 关键测试语句

```python
while self._calls and self._calls[0].deadline_ms <= self.clock.now_ms():
```

##### 失败意味着什么

并发测试将无法声明哪个 Timer 事件先进 Mailbox。

### 基本概念

Waiter 拥有身份、Generation、所属 Token/Session、有序 Key、可选 Deadline 与唯一状态迁移。多索引加速 Lookup，但不创建多个所有者。Timer Callback 只发 Control Message；Executor 决定其 Generation 是否仍 Active。

### 为什么需要这个机制

Blocking 把 Request Lifetime 延长到多个 Turn，所以 Single-writer 所有权也必须延伸。把全部终态事件按 Mailbox 排序，可把竞态变成确定事件顺序，并让 List Storage 变化与 Waiter Wakeup 共享一个 Commit 决策。

### 运行时心智模型

BLPOP 先做立即有序 Scan。无就绪项时，Executor 注册一个 Waiter 与可选 Timer。Push 规划完整 List 结果，预留 FIFO Waiter，调整唯一 Storage Operation，只提交一次，再迁移并 Reply 预留 Waiter。Timeout、Cancel 与 Close 走同一 Generation-checked Gate。

### 机制板块

<!-- journey-file: src/miniredis/commands/model.py -->
#### `src/miniredis/commands/model.py`

##### 是什么，为什么现在需要

类型化命令冻结 BLPOP Key 顺序与 Timeout 毫秒。

##### 在运行时做什么

下游代码接收已校验不可变意图，不再解析 Transport Bytes。

##### 关键代码

```python
class BlPop:
    keys: tuple[bytes, ...]
    timeout_ms: int
```

##### 关键语句理解

Key Tuple 顺序可观察，因为 BLPOP 选第一就绪 Key。

<!-- journey-file: src/miniredis/commands/parser.py -->
#### `src/miniredis/commands/parser.py`

##### 是什么，为什么现在需要

严格 Parser 接收 Redis-style 有限小数 Timeout，拒绝过大或非数字形式。

##### 在运行时做什么

在 Waiter 或 Timer 存在前，它把秒向上取整为毫秒。

##### 关键代码

```python
milliseconds = int(timeout_ms.to_integral_value(rounding=ROUND_CEILING))
return BlPop(tuple(args[:-1]), milliseconds)
```

##### 关键语句理解

向上取整避免比请求的正小数 Timeout 更早触发。

<!-- journey-file: src/miniredis/clock.py -->
#### `src/miniredis/clock.py`

##### 是什么，为什么现在需要

Timer 调度与 Clock 值源分离。

##### 在运行时做什么

生产环境用 Event-loop Callback；测试针对同一 Deadline 契约注入 Manual Scheduler。

##### 关键代码

```python
delay = max(0, deadline_ms - self._clock.now_ms()) / 1000
return asyncio.get_running_loop().call_later(delay, callback)
```

##### 关键语句理解

Scheduler 在注册时导出 Delay，Callback 仍只经 Mailbox Control 进入状态所有权。

<!-- journey-file: src/miniredis/core/blocking.py -->
#### `src/miniredis/core/blocking.py`

##### 是什么，为什么现在需要

该模块拥有 Waiter 索引、状态迁移与 Push List Item 的确定预留。

##### 在运行时做什么

它查找 FIFO 合格 Waiter，在一次迁移中移除全部索引、取消 Timer，并返回 Wakeup Proposal。

##### 关键代码

```python
if (
    waiter is None
    or waiter.generation != generation
    or waiter.state is not WaiterState.ACTIVE
):
    return None
```

##### 关键语句理解

Identity + Generation 使另一事件已获胜后到达的 Timeout/Cancel 无害。

<!-- journey-file: src/miniredis/core/planner.py -->
#### `src/miniredis/core/planner.py`

##### 是什么，为什么现在需要

Planner 拥有立即、非阻塞 BLPOP Scan。

##### 在运行时做什么

它按序检查 Key，在第一就绪 List 停止，并只提出一个更新或删除的 List Entry。

##### 关键代码

```python
for key in command.keys:
    entry, expired = lookup(database, key, now_ms)
```

##### 关键语句理解

有序 Lookup 属于语义：更早就绪 Key 会在后续 Wrong Type 前结束 Scan。

<!-- journey-file: src/miniredis/core/executor.py -->
#### `src/miniredis/core/executor.py`

##### 是什么，为什么现在需要

Executor 成为 Waiter 注册、Wakeup、Timeout、Abandon 与 Close 的唯一裁决者。

##### 在运行时做什么

它把 Timer 注册为 Control Producer，把 Push Wakeup 折叠进 Storage Plan，提交一次，再终结获胜 Waiter。

##### 关键代码

```python
waiter = self.waiters.register(
    request.token,
    request.session_id,
    request.command.keys,
    deadline,
)
```

##### 关键语句理解

原请求在阻塞时仍被所有；无需单独 Waiter Future，否则会拆分完成所有权。

<!-- journey-file: src/miniredis/adapters/direct.py -->
#### `src/miniredis/adapters/direct.py`

##### 是什么，为什么现在需要

Direct 边界把 BLPOP 的 Session 丢失映射为公开 Nil 结果。

##### 在运行时做什么

它在其他方面保留所有请求的类型化终态处理。

##### 关键代码

```python
case TransportClosed() if isinstance(parsed, BlPop):
    return Bytes(None)
```

##### 关键语句理解

Transport Lifecycle 在 Adapter 边界翻译；Executor 仍报告 Transport Outcome，而非 Protocol Bytes。

<!-- journey-file: src/miniredis/runtime.py -->
#### `src/miniredis/runtime.py`

##### 是什么，为什么现在需要

Runtime 注入 TimerScheduler，并暴露窄化 Waiter Lifecycle 证据。

##### 在运行时做什么

它报告 Waiter/Timer 计数，并通过 Debug Notification 而非 Sleep 等待。

##### 关键代码

```python
async def debug_wait_for_waiters(self, count: int) -> None:
    await self._debug_wait(lambda: self.executor.waiters.active_count == count)
```

##### 关键语句理解

契约直接观察 Registry 所有权，使竞态设置可确定。

### 验证证据

运行 `uv run pytest -q $(cat journey/stages/11-blocking-pop-races/tests.txt)`。它覆盖 Parser 边界、立即 Scan、Waiter 索引、Push/Timeout 顺序、取消、Session Close 与单 Batch FIFO Wakeup。

### 需要真正记住的内容

一个阻塞请求仍由 Runtime 拥有；索引但不复制所有权；把 Timer Callback 变成 Control Message；用 Generation 校验；针对完整 Push 预留 Wakeup；Reply 前只提交一次 Storage。

### 用自己的话讲清楚

BLPOP 不是在 Executor 旁等待的 Future。它是一个已接受请求，Waiter 元数据留在 Executor 所有权内。Push、Timeout、Cancel 与 Close 变成有序消息，第一个有效迁移获胜，所有过期事件成为 No-op。

### 教材

[第 9 章](https://github.com/system-in-miniature/mini-redis/blob/main/docs/zh/tutorial/09-blocking-pubsub-transactions.md)
