# Stage 12 · Pub/Sub and supervised shutdown / Pub/Sub 与受监督关闭

<!-- journey: chapter=9 tests_added=13 -->

## English

### Goal

Add binary Pub/Sub and close every asynchronous owner through one explicit shutdown barrier.

### Deliverable files

- `src/miniredis/adapters/direct.py`
- `src/miniredis/commands/model.py`
- `src/miniredis/commands/parser.py`
- `src/miniredis/config.py`
- `src/miniredis/core/executor.py`
- `src/miniredis/core/expiration.py`
- `src/miniredis/core/pubsub.py`
- `src/miniredis/runtime.py`
- `tests/concurrency/test_async_invariants.py`
- `tests/concurrency/test_shutdown.py`
- `tests/concurrency/test_slow_endpoint.py`
- `tests/mechanisms/test_pubsub.py`

### The problem at this point

Sessions have outboxes and BLPOP has long-lived waiters, but no push protocol yet uses them. Runtime close also has several producers and owners to coordinate: user admission, timer callbacks, executor controls, waiters, subscriptions, endpoint output, owned tasks, and failure fallback.

### Failure preview

A slow subscriber must be removed without delaying a fast subscriber or publisher. A canceled close caller must not cancel cleanup, shutdown control must bypass a full user queue, infinite waiters must terminalize on failure, and final statistics must prove every asynchronous owner is gone.

### Test contract

<!-- journey-file: tests/mechanisms/test_pubsub.py -->
#### `tests/mechanisms/test_pubsub.py`

##### What this test locks

It locks exact binary channels, repeated acknowledgements, subscribed-mode restrictions, publish counts without commits, PING push replies, and unsubscribe-all order.

##### How it constructs the counterexample

It distinguishes `b"a"` from `b"a\x00"`, repeats a subscription, then exercises normal commands and empty UNSUBSCRIBE while subscribed.

##### Key test statement

```python
assert runtime.debug_commit_seq == before
```

##### What a failure means

Pub/Sub mutated the database, normalized binary identity, or mixed request replies with ordered push output.

<!-- journey-file: tests/concurrency/test_shutdown.py -->
#### `tests/concurrency/test_shutdown.py`

##### What this test locks

It locks shielded idempotent close, producer quiescence, shutdown control admission, failure terminalization, and supervisor fallback after abrupt worker stop.

##### How it constructs the counterexample

It cancels a close waiter, fills and pauses the user mailbox, injects runtime failure, and directly cancels the executor worker.

##### Key test statement

```python
assert runtime.debug_stats().owned_tasks == 0
```

##### What a failure means

Shutdown depended on its caller, a control barrier could be starved by users, or a worker failure orphaned runtime-owned resources.

<!-- journey-file: tests/concurrency/test_async_invariants.py -->
#### `tests/concurrency/test_async_invariants.py`

##### What this test locks

It locks stale cancellation/close safety, pre-barrier command completion, post-barrier rejection, and complete cleanup of requests, waiters, subscriptions, sessions, timers, and tasks.

##### How it constructs the counterexample

It races push with cancel and close, then creates a subscriber, waiter, and maintenance timer before closing.

##### Key test statement

```python
assert stats.pending_futures == 0
assert stats.waiters == 0
assert stats.subscriptions == 0
assert stats.sessions == 0
```

##### What a failure means

Some asynchronous registry has a different terminal boundary or cleanup order from the runtime lifecycle.

<!-- journey-file: tests/concurrency/test_slow_endpoint.py -->
#### `tests/concurrency/test_slow_endpoint.py`

##### What this test locks

It locks slow-subscriber isolation and subscription cleanup without blocking a fast endpoint.

##### How it constructs the counterexample

One subscriber leaves a capacity-one acknowledgement unread while another drains; one publish overflows only the slow session.

##### Key test statement

```python
assert await publisher.execute(CommandRequest(b"PING")) == Ok(b"PONG")
```

##### What a failure means

Per-session pressure escaped into the global executor or the closed subscriber remained in delivery ownership.

### Basic concepts

Pub/Sub is ephemeral session output, not database state: PUBLISH does not allocate a commit. A bidirectional registry owns channel membership and session cleanup. Shutdown is a barrier sequence: stop new users, quiesce control producers, terminalize executor-owned state, drain output within a bound, abort leftovers, join tasks, and prove registries empty.

### Why this mechanism is necessary

Push delivery and shutdown share the same ownership graph. Without one ordered barrier, a timer or publisher can enqueue after cleanup, a slow endpoint can hold global progress, or a failed worker can leave infinite waiters unresolved. Supervision makes every producer and terminal path explicit.

### Runtime mental model

Subscribe/unsubscribe/publish commands enter the executor like other requests but update only session registries and outboxes. Close first rejects new commands and quiesces scheduled producers. `BeginShutdown` then passes through the control lane to finish waiters, requests, subscriptions, and notices. The runtime briefly drains endpoints, aborts the rest, joins the executor, and clears task ownership.

### Mechanism blocks

<!-- journey-file: src/miniredis/commands/model.py -->
#### `src/miniredis/commands/model.py`

##### What it is and why it appears

Typed Subscribe, Unsubscribe, and Publish values preserve exact channel bytes.

##### Runtime role

They let the executor own session-mode semantics without transport parsing branches.

##### Key code

```python
class Publish:
    channel: bytes
    payload: bytes
```

##### Statement understanding

Channel identity remains byte-exact; no text normalization or prefix interpretation occurs.

<!-- journey-file: src/miniredis/commands/parser.py -->
#### `src/miniredis/commands/parser.py`

##### What it is and why it appears

The parser gives each Pub/Sub command an explicit arity contract.

##### Runtime role

It permits empty UNSUBSCRIBE as “all”, requires at least one SUBSCRIBE channel, and freezes PUBLISH channel/payload.

##### Key code

```python
case b"UNSUBSCRIBE":
    return Unsubscribe(tuple(args))
```

##### Statement understanding

Empty arguments are meaningful domain intent here, not a generic arity error.

<!-- journey-file: src/miniredis/core/pubsub.py -->
#### `src/miniredis/core/pubsub.py`

##### What it is and why it appears

The registry owns bidirectional channel/session membership.

##### Runtime role

It preserves subscription order per session, finds publish targets, and removes every channel of a closed session without scanning unrelated sessions.

##### Key code

```python
self._channels: dict[bytes, dict[int, None]] = defaultdict(dict)
self._sessions: dict[int, dict[bytes, None]] = defaultdict(dict)
```

##### Statement understanding

Dual indexes are duplicated lookup structure under one owner, not duplicated subscription truth.

<!-- journey-file: src/miniredis/core/executor.py -->
#### `src/miniredis/core/executor.py`

##### What it is and why it appears

The executor integrates subscribed-mode commands and the terminal shutdown control.

##### Runtime role

It offers ordered acknowledgements/messages, removes slow sessions, and closes waiters, requests, subscriptions, and endpoints at one barrier.

##### Key code

```python
self.pubsub.clear()
for token in tuple(self._requests):
    self._finish_request(token, event.outcome)
for endpoint in self._endpoints.values():
    endpoint.offer_best_effort(ServerClosed("runtime closed"))
```

##### Statement understanding

Terminalization occurs while the executor still owns every registry, before its control lane closes permanently.

<!-- journey-file: src/miniredis/config.py -->
#### `src/miniredis/config.py`

##### What it is and why it appears

Configuration adds bounded outbox-drain grace and active-expiry interval.

##### Runtime role

It keeps shutdown latency and maintenance cadence explicit and validated.

##### Key code

```python
if self.active_expire_interval_ms <= 0:
    raise ValueError("active_expire_interval_ms must be positive")
```

##### Statement understanding

An active producer needs positive cadence; graceful drain may deliberately be zero for immediate teardown.

<!-- journey-file: src/miniredis/core/expiration.py -->
#### `src/miniredis/core/expiration.py`

##### What it is and why it appears

Active expiry gains a lifecycle-owned periodic control producer.

##### Runtime role

It schedules one next tick while running and cancels the outstanding handle during quiescence.

##### Key code

```python
async def quiesce(self) -> None:
    self._running = False
    if self._handle is not None:
        self._handle.cancel()
```

##### Statement understanding

Quiescence removes the source before shutdown closes control admission, preventing post-barrier ticks.

<!-- journey-file: src/miniredis/runtime.py -->
#### `src/miniredis/runtime.py`

##### What it is and why it appears

The runtime becomes the supervisor for producers, executor, endpoints, shutdown task, and failure fallback.

##### Runtime role

It shields one idempotent shutdown task, quiesces producers, posts the barrier, bounds outbox drain, joins ownership, and exposes final evidence.

##### Key code

```python
self.executor.mailbox.close_user_admission()
await asyncio.gather(
    *(
        producer.quiesce()  # type: ignore[attr-defined]
        for producer in tuple(self._control_producers)
    )
)
```

##### Statement understanding

New user work stops before producers quiesce; control admission remains open long enough to deliver the shutdown barrier.

<!-- journey-file: src/miniredis/adapters/direct.py -->
#### `src/miniredis/adapters/direct.py`

##### What it is and why it appears

Direct-client close becomes idempotent, shielded, and executor-owned.

##### Runtime role

It posts `SessionClosed` with completion evidence and maps runtime-versus-session terminal outcomes to public behavior.

##### Key code

```python
await asyncio.shield(self._close_task)
```

##### Statement understanding

Canceling one close caller cannot cancel the session cleanup task it initiated.

### Verification evidence

Run `uv run pytest -q $(cat journey/stages/12-pubsub-and-shutdown/tests.txt)`. It covers Pub/Sub semantics, slow-session isolation, shutdown admission, worker failure, race staleness, and final zero-resource invariants.

### Durable takeaways

Pub/Sub is session output, not database commit state; index membership both ways under one owner; isolate slow endpoints; stop producers before closing controls; shield idempotent cleanup; terminalize every registry; verify zero owned resources.

### Explain it in your own words

The same ownership design that orders commands also orders pushes and shutdown. Pub/Sub changes session registries and outboxes inside the executor. Closing first stops new sources, then sends one barrier through that owner, drains bounded output, and proves no Future, waiter, subscription, session, timer, or task survived.

### Textbook

[Chapter 9](https://github.com/system-in-miniature/mini-redis/blob/main/docs/tutorial/09-blocking-pubsub-transactions.md)

## 中文

### 目标

增加二进制 Pub/Sub，并通过一个显式 Shutdown Barrier 关闭每个异步所有者。

### 交付文件

- `src/miniredis/adapters/direct.py`
- `src/miniredis/commands/model.py`
- `src/miniredis/commands/parser.py`
- `src/miniredis/config.py`
- `src/miniredis/core/executor.py`
- `src/miniredis/core/expiration.py`
- `src/miniredis/core/pubsub.py`
- `src/miniredis/runtime.py`
- `tests/concurrency/test_async_invariants.py`
- `tests/concurrency/test_shutdown.py`
- `tests/concurrency/test_slow_endpoint.py`
- `tests/mechanisms/test_pubsub.py`

### 当前遇到的问题

Session 已有 Outbox，BLPOP 已有长生命 Waiter，但还没有 Push Protocol 使用它们。Runtime Close 也需协调多个 Producer 与 Owner：User 准入、Timer Callback、Executor Control、Waiter、Subscription、Endpoint 输出、Owned Task 与 Failure Fallback。

### 先看会坏在哪里

慢 Subscriber 必须被移除，且不延迟快 Subscriber 或 Publisher。取消 Close 调用方不得取消清理，Shutdown Control 必须绕过已满 User Queue，无限 Waiter 必须在 Failure 时终结，最终统计必须证明每个异步所有者都已消失。

### 测试契约

<!-- journey-file: tests/mechanisms/test_pubsub.py -->
#### `tests/mechanisms/test_pubsub.py`

##### 测试锁定什么

它锁定精确二进制 Channel、重复 Ack、Subscribed-mode 限制、无 Commit Publish Count、PING Push Reply 与 Unsubscribe-all 顺序。

##### 如何构造反例

它区分 `b"a"` 与 `b"a\x00"`，重复 Subscription，再在 Subscribed 状态下执行普通命令与空 UNSUBSCRIBE。

##### 关键测试语句

```python
assert runtime.debug_commit_seq == before
```

##### 失败意味着什么

Pub/Sub 修改了 Database、归一化了二进制身份，或把 Request Reply 与有序 Push 输出混合。

<!-- journey-file: tests/concurrency/test_shutdown.py -->
#### `tests/concurrency/test_shutdown.py`

##### 测试锁定什么

它锁定 Shielded 幂等 Close、Producer Quiescence、Shutdown Control 准入、Failure 终结与突然 Worker Stop 后的 Supervisor Fallback。

##### 如何构造反例

它取消 Close Waiter，填满并暂停 User Mailbox，注入 Runtime Failure，并直接取消 Executor Worker。

##### 关键测试语句

```python
assert runtime.debug_stats().owned_tasks == 0
```

##### 失败意味着什么

Shutdown 依赖其调用方，Control Barrier 可被 User 饿饿，或 Worker Failure 留下 Runtime-owned 孤儿资源。

<!-- journey-file: tests/concurrency/test_async_invariants.py -->
#### `tests/concurrency/test_async_invariants.py`

##### 测试锁定什么

它锁定过期 Cancel/Close 无害、Barrier 前命令完成、Barrier 后拒绝，以及 Request、Waiter、Subscription、Session、Timer 与 Task 完整清理。

##### 如何构造反例

它让 Push 与 Cancel/Close 竞争，再在 Close 前创建 Subscriber、Waiter 与 Maintenance Timer。

##### 关键测试语句

```python
assert stats.pending_futures == 0
assert stats.waiters == 0
assert stats.subscriptions == 0
assert stats.sessions == 0
```

##### 失败意味着什么

某个异步 Registry 与 Runtime Lifecycle 使用了不同终态边界或清理顺序。

<!-- journey-file: tests/concurrency/test_slow_endpoint.py -->
#### `tests/concurrency/test_slow_endpoint.py`

##### 测试锁定什么

它锁定慢 Subscriber 隔离与 Subscription 清理，不阻塞快 Endpoint。

##### 如何构造反例

一个 Subscriber 不读容量为一的 Ack，另一个正常 Drain；一次 Publish 只溢出慢 Session。

##### 关键测试语句

```python
assert await publisher.execute(CommandRequest(b"PING")) == Ok(b"PONG")
```

##### 失败意味着什么

Per-session 压力逃到全局 Executor，或已关闭 Subscriber 仍留在 Delivery 所有权中。

### 基本概念

Pub/Sub 是短暂 Session Output，不是 Database State：PUBLISH 不分配 Commit。双向 Registry 拥有 Channel Membership 与 Session Cleanup。Shutdown 是 Barrier Sequence：停止新 User、静止 Control Producer、终结 Executor-owned State、在上限内 Drain Output、Abort 剩余项、Join Task，并证明 Registry 为空。

### 为什么需要这个机制

Push Delivery 与 Shutdown 共享同一所有权图。没有一个有序 Barrier，Timer 或 Publisher 可在 Cleanup 后入队，慢 Endpoint 可占住全局进度，失败 Worker 可留下未解决无限 Waiter。Supervision 使每个 Producer 与终态路径显式。

### 运行时心智模型

Subscribe/Unsubscribe/Publish 命令像其他请求一样进 Executor，但只更新 Session Registry 与 Outbox。Close 先拒绝新命令并静止 Scheduled Producer。`BeginShutdown` 再经 Control Lane 收束 Waiter、Request、Subscription 与 Notice。Runtime 短暂 Drain Endpoint，Abort 其余项，Join Executor，并清空 Task 所有权。

### 机制板块

<!-- journey-file: src/miniredis/commands/model.py -->
#### `src/miniredis/commands/model.py`

##### 是什么，为什么现在需要

类型化 Subscribe、Unsubscribe 与 Publish 值保留精确 Channel Bytes。

##### 在运行时做什么

它们让 Executor 拥有 Session-mode 语义，不需 Transport Parse Branch。

##### 关键代码

```python
class Publish:
    channel: bytes
    payload: bytes
```

##### 关键语句理解

Channel 身份保持 Bytes-exact，不做文本归一化或前缀解释。

<!-- journey-file: src/miniredis/commands/parser.py -->
#### `src/miniredis/commands/parser.py`

##### 是什么，为什么现在需要

Parser 为每个 Pub/Sub 命令提供显式 Arity 契约。

##### 在运行时做什么

它允许空 UNSUBSCRIBE 表示 All，要求 SUBSCRIBE 至少一个 Channel，并冻结 PUBLISH Channel/Payload。

##### 关键代码

```python
case b"UNSUBSCRIBE":
    return Unsubscribe(tuple(args))
```

##### 关键语句理解

空参数在此是有意义的 Domain Intent，不是通用 Arity Error。

<!-- journey-file: src/miniredis/core/pubsub.py -->
#### `src/miniredis/core/pubsub.py`

##### 是什么，为什么现在需要

Registry 拥有双向 Channel/Session Membership。

##### 在运行时做什么

它保留 Per-session Subscription 顺序，查找 Publish Target，并在不扫描无关 Session 的情况下移除已关闭 Session 的所有 Channel。

##### 关键代码

```python
self._channels: dict[bytes, dict[int, None]] = defaultdict(dict)
self._sessions: dict[int, dict[bytes, None]] = defaultdict(dict)
```

##### 关键语句理解

双索引是同一 Owner 下的重复 Lookup Structure，不是重复 Subscription Truth。

<!-- journey-file: src/miniredis/core/executor.py -->
#### `src/miniredis/core/executor.py`

##### 是什么，为什么现在需要

Executor 集成 Subscribed-mode 命令与终态 Shutdown Control。

##### 在运行时做什么

它 Offer 有序 Ack/Message，移除慢 Session，并在一个 Barrier 关闭 Waiter、Request、Subscription 与 Endpoint。

##### 关键代码

```python
self.pubsub.clear()
for token in tuple(self._requests):
    self._finish_request(token, event.outcome)
for endpoint in self._endpoints.values():
    endpoint.offer_best_effort(ServerClosed("runtime closed"))
```

##### 关键语句理解

终结发生在 Executor 仍拥有每个 Registry 时，且早于 Control Lane 永久关闭。

<!-- journey-file: src/miniredis/config.py -->
#### `src/miniredis/config.py`

##### 是什么，为什么现在需要

Config 增加有界 Outbox Drain Grace 与 Active-expiry Interval。

##### 在运行时做什么

它使 Shutdown Latency 与 Maintenance Cadence 显式且可校验。

##### 关键代码

```python
if self.active_expire_interval_ms <= 0:
    raise ValueError("active_expire_interval_ms must be positive")
```

##### 关键语句理解

Active Producer 需要正 Cadence；Graceful Drain 可故意为零以立即 Teardown。

<!-- journey-file: src/miniredis/core/expiration.py -->
#### `src/miniredis/core/expiration.py`

##### 是什么，为什么现在需要

Active Expiry 获得 Lifecycle-owned Periodic Control Producer。

##### 在运行时做什么

运行时它只调度下一 Tick，Quiescence 时取消 Outstanding Handle。

##### 关键代码

```python
async def quiesce(self) -> None:
    self._running = False
    if self._handle is not None:
        self._handle.cancel()
```

##### 关键语句理解

Quiescence 在 Shutdown 关 Control Admission 前移除 Source，防止 Barrier 后 Tick。

<!-- journey-file: src/miniredis/runtime.py -->
#### `src/miniredis/runtime.py`

##### 是什么，为什么现在需要

Runtime 成为 Producer、Executor、Endpoint、Shutdown Task 与 Failure Fallback 的 Supervisor。

##### 在运行时做什么

它 Shield 一个幂等 Shutdown Task，静止 Producer，发 Barrier，设界 Drain Outbox，Join 所有权，并暴露最终证据。

##### 关键代码

```python
self.executor.mailbox.close_user_admission()
await asyncio.gather(
    *(
        producer.quiesce()  # type: ignore[attr-defined]
        for producer in tuple(self._control_producers)
    )
)
```

##### 关键语句理解

新 User Work 在 Producer Quiesce 前停止；Control Admission 保持足够长以交付 Shutdown Barrier。

<!-- journey-file: src/miniredis/adapters/direct.py -->
#### `src/miniredis/adapters/direct.py`

##### 是什么，为什么现在需要

Direct-client Close 变成幂等、Shielded 且 Executor-owned。

##### 在运行时做什么

它发送带 Completion Evidence 的 `SessionClosed`，并把 Runtime 与 Session 终态映射为公开行为。

##### 关键代码

```python
await asyncio.shield(self._close_task)
```

##### 关键语句理解

取消一个 Close Caller 不能取消其启动的 Session Cleanup Task。

### 验证证据

运行 `uv run pytest -q $(cat journey/stages/12-pubsub-and-shutdown/tests.txt)`。它覆盖 Pub/Sub 语义、慢 Session 隔离、Shutdown 准入、Worker Failure、竞态过期性与最终零资源不变量。

### 需要真正记住的内容

Pub/Sub 是 Session Output 而非 Database Commit State；在一个 Owner 下双向索引 Membership；隔离慢 Endpoint；关 Control 前停 Producer；Shield 幂等 Cleanup；终结每个 Registry；验证零 Owned Resource。

### 用自己的话讲清楚

排序命令的同一所有权设计也排序 Push 与 Shutdown。Pub/Sub 在 Executor 内改 Session Registry 与 Outbox。Close 先停新 Source，再把一个 Barrier 发进该 Owner，有界 Drain Output，并证明没有 Future、Waiter、Subscription、Session、Timer 或 Task 存活。

### 教材

[第 9 章](https://github.com/system-in-miniature/mini-redis/blob/main/docs/zh/tutorial/09-blocking-pubsub-transactions.md)
