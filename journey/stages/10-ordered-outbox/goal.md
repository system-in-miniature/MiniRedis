# Stage 10 · Ordered outbox and slow sessions / 有序 Outbox 与慢 Session

<!-- journey: chapter=2 tests_added=3 -->

## English

### Goal

Give every session one bounded ordered output channel with explicit graceful-close and overflow behavior.

### Deliverable files

- `src/miniredis/adapters/direct.py`
- `src/miniredis/config.py`
- `src/miniredis/core/executor.py`
- `src/miniredis/core/outbound.py`
- `src/miniredis/runtime.py`
- `tests/unit/core/test_outbound.py`

### The problem at this point

Request Futures can return direct replies, but later Pub/Sub and TCP work need unsolicited output in the same per-session order. An unbounded queue lets one slow consumer exhaust memory; a sentinel-based close can displace accepted output or require reserved capacity.

### Failure preview

Graceful close must drain two accepted messages before reporting closure. Overflow must discard pending output, request transport close once, and never let a best-effort notice displace already accepted data.

### Test contract

<!-- journey-file: tests/unit/core/test_outbound.py -->
#### `tests/unit/core/test_outbound.py`

##### What this test locks

It locks FIFO drain, sentinel-free close, destructive overflow, single overflow notification, and non-displacing best-effort output.

##### How it constructs the counterexample

It fills tiny capacities of one or two, closes before receiving, then attempts one additional normal or best-effort offer.

##### Key test statement

```python
assert outbox.pending_count == 0
assert overflows == 1
```

##### What a failure means

Accepted order was lost, slow-session cleanup repeated, or lifecycle output consumed capacity promised to user-visible messages.

### Basic concepts

An outbox is the sole ordered stream from runtime to one session. `begin_close` is graceful: stop accepting but drain buffered items. `abort` is destructive: discard pending items and wake receivers. Overflow is a transport failure, not backpressure on the global executor.

### Why this mechanism is necessary

Replies and future unsolicited messages must share transport ordering. Bounding each session isolates slow consumers, while explicit close state avoids magic values in the output type and gives shutdown a precise drain contract.

### Runtime mental model

The runtime allocates a monotonic session ID and registers one `SessionEndpoint`. The executor owns that registry and offers output only through the endpoint. A receiver pops FIFO items. If capacity is exhausted, the outbox aborts, requests transport closure once, and posts `SessionClosed` so all accepted requests for that session terminate as `TransportClosed`.

### Mechanism blocks

<!-- journey-file: src/miniredis/adapters/direct.py -->
#### `src/miniredis/adapters/direct.py`

##### What it is and why it appears

The Direct client now carries a registered endpoint instead of only a numeric session ID.

##### Runtime role

Normal execute still awaits request outcome, while `receive` consumes the same endpoint stream future push-style output will use.

##### Key code

```python
async def receive(self) -> Outbound:
    return await self.endpoint.receive()
```

##### Statement understanding

The adapter does not create a second queue or reorder output; it delegates to the session-owned stream.

<!-- journey-file: src/miniredis/core/outbound.py -->
#### `src/miniredis/core/outbound.py`

##### What it is and why it appears

This module adds typed outbound messages, a close-aware bounded queue, and the endpoint wrapper that owns transport callbacks.

##### Runtime role

It preserves FIFO order, distinguishes graceful close from abort, and turns the first overflow into exactly one slow-session signal.

##### Key code

```python
if len(self._items) == self._capacity:
    self.abort("outbox full")
```

##### Statement understanding

Overflow invalidates the session stream; dropping only the newest item would leave the transport with an unknowable partial history.

<!-- journey-file: src/miniredis/core/executor.py -->
#### `src/miniredis/core/executor.py`

##### What it is and why it appears

The single writer also owns session registration and the mapping from accepted requests to endpoints.

##### Runtime role

It offers replies when configured, removes closed endpoints, and resolves every still-owned request for that session.

##### Key code

```python
for token, request in tuple(self._requests.items()):
    if request.session_id == session_id:
        self._finish_request(token, TransportClosed())
```

##### Statement understanding

Session closure has finite ownership scope: all and only requests correlated with that session receive the transport terminal outcome.

<!-- journey-file: src/miniredis/config.py -->
#### `src/miniredis/config.py`

##### What it is and why it appears

Configuration gains one positive per-session output capacity.

##### Runtime role

Every endpoint receives the same validated default unless a deployment chooses a different bound.

##### Key code

```python
if self.outbox_limit <= 0:
    raise ValueError("outbox_limit must be positive")
```

##### Statement understanding

Zero capacity cannot preserve the promise that an accepted output can be queued, so it is rejected at construction.

<!-- journey-file: src/miniredis/runtime.py -->
#### `src/miniredis/runtime.py`

##### What it is and why it appears

The runtime becomes the session factory and routes slow-session signals back to executor control.

##### Runtime role

It allocates IDs, builds endpoints with configured capacity, registers them, and reports session counts in lifecycle evidence.

##### Key code

```python
self.executor.register_endpoint(endpoint)
return DirectClient(self, endpoint)
```

##### Statement understanding

Registration happens before the client escapes, so no request can reference a session unknown to the executor.

### Verification evidence

Run `uv run pytest -q $(cat journey/stages/10-ordered-outbox/tests.txt)`. The focused set combines queue-unit contracts with the existing slow-endpoint concurrency evidence.

### Durable takeaways

One session owns one ordered stream; bound it; separate graceful drain from destructive abort; never reserve a magic sentinel slot; close a slow transport once; finish every correlated request explicitly.

### Explain it in your own words

The outbox is not just a queue. It is the ordering and lifecycle contract for a session: accepted messages leave in order, graceful closure drains them, and overflow invalidates only that slow session without blocking the global executor.

### Textbook

[Chapter 2](https://github.com/system-in-miniature/mini-redis/blob/main/docs/tutorial/02-architecture.md)

## 中文

### 目标

为每个 Session 提供一个有界有序输出通道，并明确优雅关闭与溢出行为。

### 交付文件

- `src/miniredis/adapters/direct.py`
- `src/miniredis/config.py`
- `src/miniredis/core/executor.py`
- `src/miniredis/core/outbound.py`
- `src/miniredis/runtime.py`
- `tests/unit/core/test_outbound.py`

### 当前遇到的问题

Request Future 可返回直接 Reply，但后续 Pub/Sub 与 TCP 需要同一 Per-session 顺序中的非请求输出。无界队列会让一个慢 Consumer 耗尽内存；基于 Sentinel 的关闭可能挤掉已接受输出，或需预留容量。

### 先看会坏在哪里

优雅关闭必须在报告关闭前排空两条已接受消息。溢出必须丢弃待发输出、只请求一次 Transport 关闭，并且不让 Best-effort Notice 挤掉已接受数据。

### 测试契约

<!-- journey-file: tests/unit/core/test_outbound.py -->
#### `tests/unit/core/test_outbound.py`

##### 测试锁定什么

它锁定 FIFO 排空、无 Sentinel 关闭、破坏性溢出、单次溢出通知与不挤占的 Best-effort 输出。

##### 如何构造反例

它填满容量为一或二的小队列，在 Receive 前关闭，再尝试额外的普通或 Best-effort Offer。

##### 关键测试语句

```python
assert outbox.pending_count == 0
assert overflows == 1
```

##### 失败意味着什么

已接受顺序丢失，慢 Session 清理重复，或 Lifecycle 输出占用了承诺给用户可见消息的容量。

### 基本概念

Outbox 是 Runtime 到单个 Session 的唯一有序流。`begin_close` 是优雅关闭：停止接收，但排空已缓冲项。`abort` 是破坏性关闭：丢弃待发项并唤醒 Receiver。溢出是 Transport 失败，不是对全局 Executor 施加背压。

### 为什么需要这个机制

Reply 与未来非请求消息必须共享 Transport 顺序。为每个 Session 设界可隔离慢 Consumer；显式 Close State 则避免 Outbound Type 内的魔法值，并给 Shutdown 一个精确 Drain 契约。

### 运行时心智模型

Runtime 分配单调 Session ID 并注册一个 `SessionEndpoint`。Executor 拥有 Registry，只通过 Endpoint Offer 输出。Receiver 按 FIFO Pop。容量耗尽时，Outbox Abort、只请求一次 Transport 关闭，并发送 `SessionClosed`，使该 Session 所有已接受请求以 `TransportClosed` 收束。

### 机制板块

<!-- journey-file: src/miniredis/adapters/direct.py -->
#### `src/miniredis/adapters/direct.py`

##### 是什么，为什么现在需要

Direct Client 现在携带已注册 Endpoint，而不只是数字 Session ID。

##### 在运行时做什么

普通 Execute 仍等 Request Outcome，`receive` 则消费未来 Push-style 输出也会使用的同一 Endpoint Stream。

##### 关键代码

```python
async def receive(self) -> Outbound:
    return await self.endpoint.receive()
```

##### 关键语句理解

Adapter 不创建第二个 Queue，也不重排输出，只委托给 Session-owned Stream。

<!-- journey-file: src/miniredis/core/outbound.py -->
#### `src/miniredis/core/outbound.py`

##### 是什么，为什么现在需要

该模块增加类型化 Outbound Message、Close-aware 有界队列与拥有 Transport Callback 的 Endpoint Wrapper。

##### 在运行时做什么

它保留 FIFO 顺序，区分优雅 Close 与 Abort，并把第一次溢出变成且仅变成一个慢 Session Signal。

##### 关键代码

```python
if len(self._items) == self._capacity:
    self.abort("outbox full")
```

##### 关键语句理解

溢出使 Session Stream 失效；如果只丢最新项，Transport 会留下不可解释的部分历史。

<!-- journey-file: src/miniredis/core/executor.py -->
#### `src/miniredis/core/executor.py`

##### 是什么，为什么现在需要

单 Writer 也拥有 Session 注册与已接受请求到 Endpoint 的映射。

##### 在运行时做什么

它按配置 Offer Reply，移除已关闭 Endpoint，并收束该 Session 仍所有的每个请求。

##### 关键代码

```python
for token, request in tuple(self._requests.items()):
    if request.session_id == session_id:
        self._finish_request(token, TransportClosed())
```

##### 关键语句理解

Session 关闭有有限所有权范围：全部且仅该 Session 关联请求获得 Transport 终态。

<!-- journey-file: src/miniredis/config.py -->
#### `src/miniredis/config.py`

##### 是什么，为什么现在需要

Config 增加一个正的 Per-session 输出容量。

##### 在运行时做什么

除非部署选择其他上限，每个 Endpoint 都获得同一已校验默认值。

##### 关键代码

```python
if self.outbox_limit <= 0:
    raise ValueError("outbox_limit must be positive")
```

##### 关键语句理解

零容量无法保留“已接受输出可入队”的承诺，因此在构造时拒绝。

<!-- journey-file: src/miniredis/runtime.py -->
#### `src/miniredis/runtime.py`

##### 是什么，为什么现在需要

Runtime 成为 Session Factory，并把慢 Session Signal 路由回 Executor Control。

##### 在运行时做什么

它分配 ID、用配置容量构建 Endpoint、注册它，再在生命周期证据中报告 Session 数。

##### 关键代码

```python
self.executor.register_endpoint(endpoint)
return DirectClient(self, endpoint)
```

##### 关键语句理解

注册在 Client 逃出前完成，所以不会有请求引用 Executor 未知的 Session。

### 验证证据

运行 `uv run pytest -q $(cat journey/stages/10-ordered-outbox/tests.txt)`。该焦点集组合 Queue Unit 契约与已有慢 Endpoint 并发证据。

### 需要真正记住的内容

一个 Session 拥有一条有序流；给它设界；区分优雅 Drain 与破坏性 Abort；不预留魔法 Sentinel 槽；只关闭慢 Transport 一次；显式收束每个关联请求。

### 用自己的话讲清楚

Outbox 不只是 Queue。它是 Session 的排序与生命周期契约：已接受消息按序离开，优雅关闭排空它们，溢出只使该慢 Session 失效，不阻塞全局 Executor。

### 教材

[第 2 章](https://github.com/system-in-miniature/mini-redis/blob/main/docs/zh/tutorial/02-architecture.md)
