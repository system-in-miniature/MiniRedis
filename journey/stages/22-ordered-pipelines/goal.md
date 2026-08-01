# Stage 22 · Ordered pipelines / 有序 Pipeline

<!-- journey: chapter=10 tests_added=2 -->

## English

### Goal

Admit several requests before earlier replies finish while preserving exact per-session reply order, bounded capacity, parse-error position, and cancellation ownership.

### Deliverable files

- `src/miniredis/__init__.py`
- `src/miniredis/adapters/direct.py`
- `src/miniredis/adapters/tcp.py`
- `src/miniredis/core/executor.py`
- `src/miniredis/core/outbound.py`
- `src/miniredis/runtime.py`
- `tests/adapters/test_direct_pipeline.py`
- `tests/adapters/test_tcp_async_semantics.py`

### The problem at this point

Stage 20 preserved connection order by allowing only one pending command. That is correct but not pipelining: a client cannot submit later work while the first command blocks. Simply launching every command task is also wrong because later replies can overtake an earlier BLPOP, parse failures can bypass mailbox order, and global queue pressure can leak an internal BUSY response onto the wire.

### Failure preview

A paused executor may see only the first frame instead of the whole pipeline. A quick PING after BLPOP may be written first. An invalid middle command may be emitted directly and jump ahead of accepted work. With `max_pending_commands=1`, later TCP frames may receive BUSY rather than waiting for admission. Closing a session can also leave admission or command tasks alive.

### Test contract

<!-- journey-file: tests/adapters/test_direct_pipeline.py -->
#### `tests/adapters/test_direct_pipeline.py`

##### What this test locks

It locks one result slot per queued request, non-atomic execution, parse failures in executor-token order, queue reset, and close behavior.

##### How it constructs the counterexample

It queues SET, an unknown or malformed command, and INCR; pauses the executor; then proves all three tokens are accepted before execution resumes.

##### Key test statement

```python
assert [token.value for token in runtime.debug_accepted_tokens] == [1, 2, 3]
```

##### What a failure means

The direct pipeline is serially awaiting, losing input positions, or routing parse failures outside normal request ownership.

<!-- journey-file: tests/adapters/test_tcp_async_semantics.py -->
#### `tests/adapters/test_tcp_async_semantics.py`

##### What this test locks

It locks eager decoded-frame admission, exact reply order behind blocking work, retry on temporary global capacity pressure, and existing TCP close/slow-client semantics.

##### How it constructs the counterexample

It pipelines SET/error/INCR with a paused executor, pipelines BLPOP before PING, and runs three commands through a one-request global capacity.

##### Key test statement

```python
await expect(reader, b"*2\r\n$1\r\nq\r\n$1\r\nx\r\n+PONG\r\n")
await expect(reader, b"+OK\r\n:2\r\n:3\r\n")
```

##### What a failure means

Completion order is being confused with request order, or temporary admission pressure has changed protocol-visible semantics.

### Basic concepts

Pipelining separates admission order from completion time. Each admitted request receives a monotonically ordered token. A session endpoint stores token order and completed outbound bundles; only the completed prefix may enter the physical outbox. A parse rejection is still a request outcome. BUSY from the shared executor is backpressure for TCP admission, not a command reply.

### Why this mechanism is necessary

Pipelining improves utilization only if multiple requests can be in flight. Token-ordered buffering preserves the connection's observable sequential semantics even when a blocking command completes later. Routing all outcomes through the same executor ownership also keeps shutdown, abandonment, Pub/Sub acknowledgements, and slow-client behavior coherent.

### Runtime mental model

The TCP reader decodes frames into a bounded deque and submits while both the per-session window and global executor capacity allow. Every accepted token is registered with the endpoint. Commands may finish in any time order, but their outbound bundles wait in `_completed_requests` until all earlier tokens complete. The sole writer drains that ordered outbox. DirectPipeline uses the same submit/resolve split without pretending the batch is atomic.

### Mechanism blocks

<!-- journey-file: src/miniredis/core/outbound.py -->
#### `src/miniredis/core/outbound.py`

##### What it is and why it appears

`SessionEndpoint` becomes the per-session reply reorder buffer.

##### Runtime role

It registers token order, stores complete outbound bundles, cancels tokens as empty bundles, and flushes only the oldest continuous completed prefix.

##### Key code

```python
while (
    self._request_order
    and self._request_order[0] in self._completed_requests
):
    token = self._request_order.popleft()
```

##### Statement understanding

A later completed request remains invisible until every earlier token has a terminal bundle, preserving wire order without serial execution.

<!-- journey-file: src/miniredis/core/executor.py -->
#### `src/miniredis/core/executor.py`

##### What it is and why it appears

The executor admits typed commands and parse rejections through one request path and coordinates endpoint registration/completion.

##### Runtime role

It assigns tokens, registers them before execution, packages multi-item Pub/Sub outcomes atomically, and signals waiters when global submission capacity returns.

##### Key code

```python
endpoint = self._endpoints.get(session_id)
if endpoint is not None:
    endpoint.register_request(token)
self._accepted_tokens.append(token)
self._accepted_changed.set()
self._on_debug_change()
return SubmittedRequest(token, future, command)
```

##### Statement understanding

Admission establishes both executor ownership and the reply-order slot before any outcome can be produced.

<!-- journey-file: src/miniredis/adapters/direct.py -->
#### `src/miniredis/adapters/direct.py`

##### What it is and why it appears

The direct client splits submission from resolution, and `DirectPipeline` batches that reusable primitive.

##### Runtime role

It submits every queued request first, then resolves futures in input order; closing discards unsent requests and closes the owned client.

##### Key code

```python
submitted = [self._client.submit(request) for request in requests]
return tuple([await self._client.resolve(item) for item in submitted])
```

##### Statement understanding

Result collection is ordered, but the commands are already admitted; the batch is pipelined, not transactional.

<!-- journey-file: src/miniredis/adapters/tcp.py -->
#### `src/miniredis/adapters/tcp.py`

##### What it is and why it appears

The TCP session replaces one pending command with a bounded set of command tasks plus one admission waiter.

##### Runtime role

It submits available frames, waits rather than replying on temporary BUSY, resumes admission after capacity changes, and joins every task during close.

##### Key code

```python
if submitted.code == "BUSY":
    self._ensure_admission_waiter()
    return
```

##### Statement understanding

Global capacity pressure delays socket consumption but does not fabricate a Redis command failure.

<!-- journey-file: src/miniredis/runtime.py -->
#### `src/miniredis/runtime.py`

##### What it is and why it appears

The runtime centralizes request submission and waiting for both adapters.

##### Runtime role

It converts parse failures into executor rejections, exposes capacity waiting, and abandons accepted tokens if a session task is cancelled.

##### Key code

```python
if isinstance(parsed, Failure):
    return self.executor.submit_rejection(session_id, parsed)
```

##### Statement understanding

Invalid syntax keeps its exact pipeline position because it enters the same mailbox and token sequence as valid commands.

<!-- journey-file: src/miniredis/__init__.py -->
#### `src/miniredis/__init__.py`

##### What it is and why it appears

The package exports `DirectPipeline` as a documented learner-facing type.

##### Runtime role

It changes only the public import surface; creation still flows through `MiniRedis.direct_pipeline()`.

##### Key code

```python
from miniredis.adapters.direct import DirectPipeline
```

##### Statement understanding

The export makes the new adapter capability explicit without duplicating its ownership factory.

### Verification evidence

Run both focused adapter modules from `tests.txt`, cumulatively build Stages 1–22, and require owned-tree parity with `0016059`.

### Durable takeaways

- Pipeline admission order and completion time are different axes.
- Reply bundles flush only from the completed token prefix.
- Parse failure occupies a normal ordered request slot.
- Direct pipelines are ordered but not atomic.

### Explain it in your own words

Why may PING execute while an earlier BLPOP is still waiting, yet its reply cannot appear first? Why is executor BUSY retried for TCP rather than sent to the client?

### Textbook

The endpoint is a reorder buffer similar to those used when concurrent work must retire in program order. Tokens act as sequence numbers; completion is speculative, while publication is ordered retirement.

## 中文

### 目标

在更早 Reply 尚未完成时接纳多个 Request，同时保持精确的 Per-session Reply Order、有界容量、Parse-error 位置与 Cancellation Ownership。

### 交付文件

- `src/miniredis/__init__.py`
- `src/miniredis/adapters/direct.py`
- `src/miniredis/adapters/tcp.py`
- `src/miniredis/core/executor.py`
- `src/miniredis/core/outbound.py`
- `src/miniredis/runtime.py`
- `tests/adapters/test_direct_pipeline.py`
- `tests/adapters/test_tcp_async_semantics.py`

### 当前遇到的问题

Stage 20 通过只允许一个 Pending Command 保持 Connection Order。这虽然正确，却不是真正 Pipelining：第一条命令阻塞时 Client 无法提交后续工作。直接启动所有 Command Task 也不正确，因为后完成的 Reply 可能越过先前 BLPOP，Parse Failure 可能绕过 Mailbox Order，全局 Queue 压力还可能把内部 BUSY 暴露到 Wire。

### 先看会坏在哪里

Executor 暂停时可能只看到第一个 Frame，而不是完整 Pipeline。BLPOP 后的快速 PING 可能先写出。中间非法 Command 可能直接 Emit 并跳过已接纳工作。`max_pending_commands=1` 时，后续 TCP Frame 可能收到 BUSY 而非等待 Admission。Session Close 也可能遗留 Admission 或 Command Task。

### 测试契约

<!-- journey-file: tests/adapters/test_direct_pipeline.py -->
#### `tests/adapters/test_direct_pipeline.py`

##### 锁定什么

锁定每个 Queued Request 一个结果槽、非原子执行、Parse Failure 进入 Executor-token Order、Queue Reset 与 Close 行为。

##### 如何构造反例

排入 SET、未知或格式错误命令、INCR；暂停 Executor；证明恢复执行前三个 Token 已全部接纳。

##### 关键测试语句

```python
assert [token.value for token in runtime.debug_accepted_tokens] == [1, 2, 3]
```

##### 失败意味着什么

Direct Pipeline 在串行等待、丢失输入位置，或把 Parse Failure 路由到正常请求所有权之外。

<!-- journey-file: tests/adapters/test_tcp_async_semantics.py -->
#### `tests/adapters/test_tcp_async_semantics.py`

##### 锁定什么

锁定 Eager Decoded-frame Admission、Blocking Work 后的精确 Reply Order、临时全局容量压力重试，以及既有 TCP Close/Slow-client 语义。

##### 如何构造反例

在暂停 Executor 时 Pipeline SET/Error/INCR，在 PING 前 Pipeline BLPOP，并让三个 Command 经过容量为 1 的全局 Queue。

##### 关键测试语句

```python
await expect(reader, b"*2\r\n$1\r\nq\r\n$1\r\nx\r\n+PONG\r\n")
await expect(reader, b"+OK\r\n:2\r\n:3\r\n")
```

##### 失败意味着什么

Completion Order 被误当成 Request Order，或临时 Admission Pressure 改变了协议可见语义。

### 基本概念

Pipelining 分离 Admission Order 与 Completion Time。每个已接纳 Request 获得单调 Token。Session Endpoint 保存 Token Order 与已完成 Outbound Bundle；只有完成前缀能进入物理 Outbox。Parse Rejection 仍是 Request Outcome。共享 Executor 返回的 BUSY 是 TCP Admission 背压，而不是 Command Reply。

### 为什么需要这个机制

只有多个 Request 能同时 In-flight，Pipelining 才能提高利用率。Token-ordered Buffer 即使面对更晚完成的 Blocking Command，也保持 Connection 可观察的串行语义。让所有 Outcome 经过同一 Executor Ownership，还能统一 Shutdown、Abandonment、Pub/Sub Ack 与 Slow-client 行为。

### 运行时心智模型

TCP Reader 把 Frame 解码进有界 Deque，并在 Per-session Window 与全局 Executor Capacity 都允许时提交。每个 Accepted Token 都注册到 Endpoint。Command 可以按任意时间顺序完成，但 Outbound Bundle 要在 `_completed_requests` 中等待所有更早 Token 完成。唯一 Writer Drain 有序 Outbox。DirectPipeline 使用同一 Submit/Resolve 分离，但不假装 Batch 原子。

### 机制板块

<!-- journey-file: src/miniredis/core/outbound.py -->
#### `src/miniredis/core/outbound.py`

##### 是什么，为什么出现

`SessionEndpoint` 成为 Per-session Reply Reorder Buffer。

##### 运行时角色

注册 Token Order，保存完整 Outbound Bundle，把取消 Token 作为空 Bundle，并只 Flush 最老连续完成前缀。

##### 关键代码

```python
while (
    self._request_order
    and self._request_order[0] in self._completed_requests
):
    token = self._request_order.popleft()
```

##### 关键语句理解

更晚完成的 Request 在每个更早 Token 获得终态 Bundle 前不可见，因此无需串行执行也能保持 Wire Order。

<!-- journey-file: src/miniredis/core/executor.py -->
#### `src/miniredis/core/executor.py`

##### 是什么，为什么出现

Executor 通过一条 Request 路径接纳 Typed Command 与 Parse Rejection，并协调 Endpoint Registration/Completion。

##### 运行时角色

分配 Token、执行前注册、原子打包多项 Pub/Sub Outcome，并在全局 Submission Capacity 恢复时通知 Waiter。

##### 关键代码

```python
endpoint = self._endpoints.get(session_id)
if endpoint is not None:
    endpoint.register_request(token)
self._accepted_tokens.append(token)
self._accepted_changed.set()
self._on_debug_change()
return SubmittedRequest(token, future, command)
```

##### 关键语句理解

Admission 在任何 Outcome 产生前，同时建立 Executor Ownership 与 Reply-order Slot。

<!-- journey-file: src/miniredis/adapters/direct.py -->
#### `src/miniredis/adapters/direct.py`

##### 是什么，为什么出现

Direct Client 分离 Submission 与 Resolution，`DirectPipeline` 批量复用这一原语。

##### 运行时角色

先提交全部 Queued Request，再按输入顺序 Resolve Future；Close 丢弃未发送 Request 并关闭 Owned Client。

##### 关键代码

```python
submitted = [self._client.submit(request) for request in requests]
return tuple([await self._client.resolve(item) for item in submitted])
```

##### 关键语句理解

结果收集有序，但 Command 已经全部接纳；这个 Batch 是 Pipelined，而不是 Transactional。

<!-- journey-file: src/miniredis/adapters/tcp.py -->
#### `src/miniredis/adapters/tcp.py`

##### 是什么，为什么出现

TCP Session 用有界 Command Task Set 与一个 Admission Waiter 替代单 Pending Command。

##### 运行时角色

提交可用 Frame，遇到临时 BUSY 时等待而不回复，容量变化后恢复 Admission，并在 Close 时 Join 每个 Task。

##### 关键代码

```python
if submitted.code == "BUSY":
    self._ensure_admission_waiter()
    return
```

##### 关键语句理解

全局 Capacity Pressure 延迟 Socket 消费，但不会虚构 Redis Command Failure。

<!-- journey-file: src/miniredis/runtime.py -->
#### `src/miniredis/runtime.py`

##### 是什么，为什么出现

Runtime 为两个 Adapter 集中 Request Submission 与等待。

##### 运行时角色

把 Parse Failure 转成 Executor Rejection，暴露 Capacity Waiting，并在 Session Task 取消时 Abandon Accepted Token。

##### 关键代码

```python
if isinstance(parsed, Failure):
    return self.executor.submit_rejection(session_id, parsed)
```

##### 关键语句理解

非法 Syntax 进入与合法 Command 相同的 Mailbox/Token Sequence，因此保留精确 Pipeline 位置。

<!-- journey-file: src/miniredis/__init__.py -->
#### `src/miniredis/__init__.py`

##### 是什么，为什么出现

Package 把 `DirectPipeline` 暴露为有文档的学习者接口。

##### 运行时角色

只改变 Public Import Surface；创建仍经过 `MiniRedis.direct_pipeline()`。

##### 关键代码

```python
from miniredis.adapters.direct import DirectPipeline
```

##### 关键语句理解

Export 明确新 Adapter 能力，却不复制其 Ownership Factory。

### 验证证据

运行 `tests.txt` 中两个聚焦 Adapter 模块，累计构建 Stage 1–22，并要求 Owned-tree 与 `0016059` 一致。

### 需要真正记住的内容

- Pipeline Admission Order 与 Completion Time 是不同维度。
- Reply Bundle 只从已完成 Token Prefix Flush。
- Parse Failure 占据正常有序 Request Slot。
- Direct Pipeline 有序但不原子。

### 用自己的话讲清楚

为什么 PING 可以在更早 BLPOP 等待时执行，但 Reply 不能先出现？为什么 Executor BUSY 对 TCP 要重试而不是发给 Client？

### 教材

Endpoint 是 Reorder Buffer，类似并发工作必须按 Program Order Retire 的系统。Token 是 Sequence Number；Completion 可以乱序，而 Publication 必须有序退休。
