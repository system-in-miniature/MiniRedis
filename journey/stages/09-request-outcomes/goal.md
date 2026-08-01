# Stage 09 · Request ownership and terminal outcomes / 请求所有权与终态结果

<!-- journey: chapter=2 tests_added=2 -->

## English

### Goal

Give every accepted request a runtime-owned lifetime from admission to one typed terminal outcome.

### Deliverable files

- `src/miniredis/adapters/direct.py`
- `src/miniredis/core/executor.py`
- `src/miniredis/core/mailbox.py`
- `src/miniredis/core/outbound.py`
- `src/miniredis/runtime.py`
- `tests/concurrency/test_request_ownership.py`

### The problem at this point

The executor serializes commits, but a caller can cancel while its request is queued. If cancellation owns the shared Future, the runtime loses the only completion channel even though the command may still commit. Shutdown and transport loss also need distinguishable outcomes rather than one generic exception.

### Failure preview

Canceling a caller while the executor is paused must not cancel the accepted request or reuse its token. After resume, the mutation still commits, every owned Future reaches exactly one terminal state, and runtime statistics return to zero.

### Test contract

<!-- journey-file: tests/concurrency/test_request_ownership.py -->
#### `tests/concurrency/test_request_ownership.py`

##### What this test locks

It locks runtime-unique monotonic tokens, cancellation shielding, post-cancel commit behavior, and complete request cleanup.

##### How it constructs the counterexample

It pauses the executor, waits until INCR is admitted, cancels only the caller task, resumes the mailbox, and observes the resulting value plus ownership counters.

##### Key test statement

```python
assert stats.accepted_requests == 0
assert stats.pending_futures == 0
```

##### What a failure means

Caller lifetime leaked into runtime ownership, an accepted request became orphaned, or a token/Future was completed more or less than once.

### Basic concepts

Admission transfers request ownership to MiniRedis. A `RequestToken` is correlation identity; the executor-owned Future is the completion slot; `RequestOutcome` is a closed terminal vocabulary. Caller cancellation requests abandonment but cannot directly cancel the owned slot.

### Why this mechanism is necessary

Commit and client-await lifetimes are different. Keeping them separate makes cancellation, shutdown, transport closure, and internal failure explicit while preserving one ordered owner for state changes. It also prevents invisible pending Futures from accumulating.

### Runtime mental model

The adapter parses and submits a request, then shields its Future. The executor records token, request, and Future before mailbox admission. Cancellation posts `AbandonRequest` into the control lane. Whichever ordered event owns completion first removes the request and sets exactly one typed outcome; terminal cleanup finishes every remaining token.

### Mechanism blocks

<!-- journey-file: src/miniredis/adapters/direct.py -->
#### `src/miniredis/adapters/direct.py`

##### What it is and why it appears

The Direct adapter becomes a lifetime boundary rather than the owner of parsing and completion state.

##### Runtime role

It shields the runtime Future, posts abandonment after caller cancellation, and maps typed terminal outcomes to Direct-client results.

##### Key code

```python
except asyncio.CancelledError:
    self._runtime.executor.post_control(AbandonRequest(submitted.token))
    raise
```

##### Statement understanding

The caller still receives cancellation immediately, but cleanup becomes an ordered executor event instead of mutating shared ownership from outside.

<!-- journey-file: src/miniredis/core/executor.py -->
#### `src/miniredis/core/executor.py`

##### What it is and why it appears

The executor now owns the complete accepted-request registry and its monotonic correlation sequence.

##### Runtime role

It records before admission, dispatches commands and controls in mailbox order, and removes each request through one `_finish_request` gate.

##### Key code

```python
message = self._requests.pop(token, None)
if message is None:
    return False
```

##### Statement understanding

Pop is the single ownership transfer to terminal state. A later competing event observes absence and cannot complete the Future again.

<!-- journey-file: src/miniredis/core/outbound.py -->
#### `src/miniredis/core/outbound.py`

##### What it is and why it appears

This module defines request correlation and the closed set of terminal outcomes independently of a transport adapter.

##### Runtime role

Adapters pattern-match `Replied`, `Abandoned`, `TransportClosed`, `RuntimeClosed`, or `RuntimeFailed` without inferring cause from exceptions.

##### Key code

```python
RequestOutcome: TypeAlias = (
    Replied | Abandoned | TransportClosed | RuntimeClosed | RuntimeFailed
)
```

##### Statement understanding

The union makes every terminal cause explicit and forces new lifecycle cases to be handled at consumers.

<!-- journey-file: src/miniredis/core/mailbox.py -->
#### `src/miniredis/core/mailbox.py`

##### What it is and why it appears

The mailbox distinguishes bounded user admission from internal control admission.

##### Runtime role

User commands consume capacity; abandonment and shutdown controls can still enter after user admission closes.

##### Key code

```python
def post_control(self, item: T) -> bool:
    if not self._control_open:
        return False
```

##### Statement understanding

Closing user pressure is not the same as closing lifecycle coordination. The control lane stays available until terminal cleanup is safe.

<!-- journey-file: src/miniredis/runtime.py -->
#### `src/miniredis/runtime.py`

##### What it is and why it appears

The runtime centralizes parse ownership and exposes predicate-based lifecycle diagnostics.

##### Runtime role

It constructs the executor's debug notification boundary and waits for queued or idle states without scheduler sleeps.

##### Key code

```python
async def debug_wait_until_idle(self) -> None:
    await self._debug_wait(lambda: self.executor.idle)
```

##### Statement understanding

Tests wait for an owned state transition, not an estimated amount of wall time, so concurrency evidence is causal.

### Verification evidence

Run `uv run pytest -q $(cat journey/stages/09-request-outcomes/tests.txt)`. It proves token uniqueness and cancellation cleanup around the real executor mailbox and public Direct adapter.

### Durable takeaways

Admission transfers ownership; shield runtime Futures; represent terminal causes as data; complete through one pop gate; keep control admission separate; observe concurrency with predicates rather than sleeps.

### Explain it in your own words

A canceled waiter and an accepted command are not the same lifetime. MiniRedis owns the command after admission, so the adapter can stop waiting while the executor still deterministically commits or abandons it and closes its Future exactly once.

### Textbook

[Chapter 2](https://github.com/system-in-miniature/mini-redis/blob/main/docs/tutorial/02-architecture.md)

## 中文

### 目标

让每个已接受请求从准入到唯一类型化终态结果，都由 Runtime 拥有。

### 交付文件

- `src/miniredis/adapters/direct.py`
- `src/miniredis/core/executor.py`
- `src/miniredis/core/mailbox.py`
- `src/miniredis/core/outbound.py`
- `src/miniredis/runtime.py`
- `tests/concurrency/test_request_ownership.py`

### 当前遇到的问题

Executor 已串行化 Commit，但调用方可在请求排队时取消。如果取消方拥有共享 Future，即使命令仍会提交，Runtime 也会失去唯一完成通道。关闭与 Transport 丢失也需要可区分结果，而非通用异常。

### 先看会坏在哪里

Executor 暂停时取消调用方，不得取消已接受请求，也不得复用 Token。恢复后变更仍会提交，每个 Owned Future 到达且仅到达一个终态，Runtime 统计最终回到零。

### 测试契约

<!-- journey-file: tests/concurrency/test_request_ownership.py -->
#### `tests/concurrency/test_request_ownership.py`

##### 测试锁定什么

它锁定 Runtime 唯一单调 Token、取消 Shield、取消后 Commit 行为与完整请求清理。

##### 如何构造反例

它暂停 Executor，等 INCR 准入后只取消调用方 Task，再恢复 Mailbox，观察值与所有权计数。

##### 关键测试语句

```python
assert stats.accepted_requests == 0
assert stats.pending_futures == 0
```

##### 失败意味着什么

调用方生命周期泄漏进 Runtime 所有权，已接受请求成为孤儿，或 Token/Future 完成了非一次。

### 基本概念

准入会把请求所有权转给 MiniRedis。`RequestToken` 是关联身份，Executor-owned Future 是完成槽，`RequestOutcome` 是封闭终态词汇。调用方取消只请求 Abandon，不能直接取消 Owned Slot。

### 为什么需要这个机制

Commit 与 Client Await 是不同生命周期。分开它们，才能在保留单一有序状态变更所有者的同时，明确表示取消、关闭、Transport 丢失与内部失败，也防止不可见 Pending Future 累积。

### 运行时心智模型

Adapter 解析并提交请求，再 Shield Future。Executor 在 Mailbox 准入前记录 Token、Request 与 Future。取消会把 `AbandonRequest` 投入 Control Lane。无论哪个有序事件先拥有完成，都会移除请求并设置且仅设置一个类型化结果；终态清理则收束全部剩余 Token。

### 机制板块

<!-- journey-file: src/miniredis/adapters/direct.py -->
#### `src/miniredis/adapters/direct.py`

##### 是什么，为什么现在需要

Direct Adapter 成为生命周期边界，而不再拥有 Parse 和完成状态。

##### 在运行时做什么

它 Shield Runtime Future，在调用方取消后发送 Abandon，再把类型化终态结果映射为 Direct Client 结果。

##### 关键代码

```python
except asyncio.CancelledError:
    self._runtime.executor.post_control(AbandonRequest(submitted.token))
    raise
```

##### 关键语句理解

调用方仍立即获得取消，但清理变成有序 Executor 事件，而非从外部修改共享所有权。

<!-- journey-file: src/miniredis/core/executor.py -->
#### `src/miniredis/core/executor.py`

##### 是什么，为什么现在需要

Executor 现在拥有完整已接受请求 Registry 与单调关联序列。

##### 在运行时做什么

它在准入前记录，按 Mailbox 顺序 Dispatch 命令与 Control，并通过唯一 `_finish_request` Gate 移除每个请求。

##### 关键代码

```python
message = self._requests.pop(token, None)
if message is None:
    return False
```

##### 关键语句理解

Pop 是到终态的唯一所有权转移；后到的竞争事件只会看到缺失，无法再次完成 Future。

<!-- journey-file: src/miniredis/core/outbound.py -->
#### `src/miniredis/core/outbound.py`

##### 是什么，为什么现在需要

该模块独立于 Transport Adapter 定义请求关联与封闭终态结果集。

##### 在运行时做什么

Adapter 模式匹配 `Replied`、`Abandoned`、`TransportClosed`、`RuntimeClosed` 或 `RuntimeFailed`，不从异常猜测原因。

##### 关键代码

```python
RequestOutcome: TypeAlias = (
    Replied | Abandoned | TransportClosed | RuntimeClosed | RuntimeFailed
)
```

##### 关键语句理解

Union 使每个终态原因显式化，并迫使 Consumer 处理新生命周期情况。

<!-- journey-file: src/miniredis/core/mailbox.py -->
#### `src/miniredis/core/mailbox.py`

##### 是什么，为什么现在需要

Mailbox 区分有界 User 准入与内部 Control 准入。

##### 在运行时做什么

User 命令占用容量；Abandon 与 Shutdown Control 在 User 准入关闭后仍可进入。

##### 关键代码

```python
def post_control(self, item: T) -> bool:
    if not self._control_open:
        return False
```

##### 关键语句理解

关闭 User 压力不等于关闭生命周期协调；Control Lane 要保持到终态清理安全完成。

<!-- journey-file: src/miniredis/runtime.py -->
#### `src/miniredis/runtime.py`

##### 是什么，为什么现在需要

Runtime 集中 Parse 所有权，并暴露基于 Predicate 的生命周期诊断。

##### 在运行时做什么

它构造 Executor Debug Notification 边界，并不用 Scheduler Sleep 等待 Queued 或 Idle 状态。

##### 关键代码

```python
async def debug_wait_until_idle(self) -> None:
    await self._debug_wait(lambda: self.executor.idle)
```

##### 关键语句理解

测试等待 Owned State Transition，而非估计的墙上时间，因此并发证据具有因果性。

### 验证证据

运行 `uv run pytest -q $(cat journey/stages/09-request-outcomes/tests.txt)`。它围绕真实 Executor Mailbox 与公开 Direct Adapter，证明 Token 唯一性与取消清理。

### 需要真正记住的内容

准入转移所有权；Shield Runtime Future；用数据表示终态原因；通过唯一 Pop Gate 完成；分开 Control 准入；用 Predicate 而非 Sleep 观察并发。

### 用自己的话讲清楚

被取消的 Waiter 与已接受命令不是同一生命周期。准入后 MiniRedis 拥有命令，所以 Adapter 可停止等待，Executor 仍会确定地提交或 Abandon，并且只关闭 Future 一次。

### 教材

[第 2 章](https://github.com/system-in-miniature/mini-redis/blob/main/docs/zh/tutorial/02-architecture.md)
