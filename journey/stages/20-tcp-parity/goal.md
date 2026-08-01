# Stage 20 · TCP runtime parity / TCP Runtime 一致性

<!-- journey: chapter=10 tests_added=4 -->

## English

### Goal

Expose the same serialized MiniRedis semantics through real TCP sessions, with per-connection ordering, bounded buffering, slow-client isolation, and fully owned shutdown.

### Deliverable files

- `pyproject.toml`
- `src/miniredis/adapters/tcp.py`
- `src/miniredis/config.py`
- `src/miniredis/core/executor.py`
- `src/miniredis/runtime.py`
- `tests/adapters/test_direct_resp_parity.py`
- `tests/adapters/test_tcp_async_semantics.py`
- `tests/adapters/test_tcp_smoke.py`
- `tests/interop/test_redis_py_resp2.py`
- `uv.lock`

### The problem at this point

A codec is not a server. Each socket introduces concurrent reader, writer, command, and close work. Commands on one connection must remain ordered without letting a blocking command stop another connection; all replies and Pub/Sub events must share one ordered outbox; EOF, protocol error, slow writers, server close, and runtime close must converge on one session outcome.

### Failure preview

Submitting every decoded frame concurrently can reorder pipelined commands. Awaiting a blocking command in the read loop can stop unrelated frames or connections. Writing replies directly from producers can reorder Pub/Sub and replies. EOF can leave BLPOP waiters alive, while close races can leak reader/writer tasks or spend the drain grace period twice.

### Test contract

<!-- journey-file: tests/adapters/test_tcp_smoke.py -->
#### `tests/adapters/test_tcp_smoke.py`

##### What this test locks

It locks fragmentation, multiple commands, protocol-error delivery, EOF truncation, idempotent server close, and admission only on a running runtime.

##### How it constructs the counterexample

It splits PING across writes, coalesces SET/GET, sends invalid/truncated frames, and closes live sessions and servers repeatedly.

##### Key test statement

```python
assert await reader.readline() == b"-CLOSED protocol error: truncated RESP frame\r\n"
```

##### What a failure means

Socket lifecycle or protocol errors bypass the ordered session boundary.

<!-- journey-file: tests/adapters/test_direct_resp_parity.py -->
#### `tests/adapters/test_direct_resp_parity.py`

##### What this test locks

It locks reply bytes, logical state, and commit sequence parity between direct and TCP adapters.

##### How it constructs the counterexample

It runs one mixed command sequence through two runtimes and compares each encoded reply plus final state.

##### Key test statement

```python
assert tcp_runtime.debug_logical_items() == direct_runtime.debug_logical_items()
```

##### What a failure means

The network adapter has invented semantics instead of transporting the same executor behavior.

<!-- journey-file: tests/adapters/test_tcp_async_semantics.py -->
#### `tests/adapters/test_tcp_async_semantics.py`

##### What this test locks

It locks cross-connection progress, EOF waiter cleanup, Pub/Sub ordering, slow-subscriber isolation, and one bounded runtime-close drain.

##### How it constructs the counterexample

It blocks BLPOP, disconnects clients, pauses one writer with a tiny outbox, and gates transport drain during runtime shutdown.

##### Key test statement

```python
assert server.owned_task_count == 0
assert await reader.read() == b"+PONG\r\n"
```

##### What a failure means

Async session ownership leaks, ordering diverges, or one slow transport controls healthy clients/runtime shutdown.

<!-- journey-file: tests/interop/test_redis_py_resp2.py -->
#### `tests/interop/test_redis_py_resp2.py`

##### What this test locks

It locks basic compatibility with an independent Redis client implementation.

##### How it constructs the counterexample

redis-py connects in RESP2 binary mode and performs string and hash operations.

##### Key test statement

```python
assert await client.incr(b"k") == 2
assert await client.hget(b"h", b"f") == b"v"
```

##### What a failure means

In-process codec tests missed a real client handshake or wire-behavior mismatch.

### Basic concepts

A TCP session owns one decoder/read pump, one ordered command-at-a-time submission chain, one outbox/write pump, and one idempotent close task. Pipelining means reading more frames before earlier replies finish; it does not permit per-session execution reorder. Slow-client isolation means outbox overflow closes that endpoint without blocking executor progress.

### Why this mechanism is necessary

The network boundary multiplies lifecycle races but must not fork database semantics. Reusing the executor endpoint/outbox contract preserves ordering and backpressure behavior across direct and TCP access. Explicit session ownership makes EOF and shutdown release blocked commands, subscriptions, transports, and tasks together.

### Runtime mental model

The server accepts a socket and registers a `SessionEndpoint`. The reader decodes into a bounded frame deque and starts at most one command task. Executor outcomes enter the endpoint outbox. The writer alone serializes outbound values and drains the transport. Any terminal path quiesces the reader, closes the executor session, aborts/drains the outbox as appropriate, joins tasks, closes the socket, and unregisters the session.

### Mechanism blocks

<!-- journey-file: src/miniredis/adapters/tcp.py -->
#### `src/miniredis/adapters/tcp.py`

##### What it is and why it appears

This module owns TCP server and per-connection lifecycle rather than database logic.

##### Runtime role

It incrementally reads RESP2, bounds decoded frames, submits one session command at a time, writes only from the outbox pump, and joins every task on close.

##### Key code

```python
if (
    self._closed
    or self._reader_quiescing
    or self._pending_command is not None
    or not self._frames
):
    return
request = self._frames.popleft()
```

##### Statement understanding

The reader may pipeline frames, but one pending-command slot preserves arrival order without occupying the global executor while a socket read waits.

<!-- journey-file: src/miniredis/config.py -->
#### `src/miniredis/config.py`

##### What it is and why it appears

Configuration adds a positive bound on decoded command frames retained per session.

##### Runtime role

It prevents a fast reader from accumulating unbounded work behind a slow or blocking command.

##### Key code

```python
if self.max_session_frames <= 0:
    raise ValueError("max_session_frames must be positive")
```

##### Statement understanding

Protocol byte limits and decoded-work limits protect different allocations; both are required.

<!-- journey-file: src/miniredis/core/executor.py -->
#### `src/miniredis/core/executor.py`

##### What it is and why it appears

The executor exposes token allocation and leaves transport-close messages to the runtime/session owner.

##### Runtime role

TCP parse/admission failures still receive ordered reply tokens, while shutdown avoids two owners writing the same terminal event.

##### Key code

```python
def new_request_token(self) -> RequestToken:
    return RequestToken(next(self._request_tokens))
```

##### Statement understanding

Even failures before normal executor submission participate in one monotonic outbound order.

<!-- journey-file: src/miniredis/runtime.py -->
#### `src/miniredis/runtime.py`

##### What it is and why it appears

The runtime exposes session operations and owns every started TCP server.

##### Runtime role

It registers endpoints, submits/abandons session requests, isolates slow endpoints, starts servers only while running, and closes servers before releasing endpoints.

##### Key code

```python
await asyncio.gather(
    *(server.finish_runtime_close() for server in tuple(self._tcp_servers)),
    return_exceptions=False,
)
```

##### Statement understanding

Network producers and consumers finish while their executor endpoints still exist; endpoint release is the final ownership step.

<!-- journey-file: pyproject.toml -->
#### `pyproject.toml`

##### What it is and why it appears

Project metadata adds redis-py as an interop test dependency.

##### Runtime role

It lets verification use an independently implemented RESP2 client; production MiniRedis remains protocol-library independent.

##### Key code

```toml
"redis>=8,<9",
```

##### Statement understanding

The dependency is test scaffolding for external compatibility evidence, not part of the TCP mechanism.

<!-- journey-file: uv.lock -->
#### `uv.lock`

##### What it is and why it appears

The lockfile freezes the resolved interop dependency graph.

##### Runtime role

It makes the real-client check reproducible across learner environments.

##### Key code

```toml
name = "redis"
```

##### Statement understanding

This file records dependency resolution; it does not explain runtime behavior and is intentionally grouped with test scaffolding.

### Verification evidence

Run all four focused test modules in `tests.txt`, then cumulatively build Stages 1–20 and compare the owned tree with commit `5419f99`.

### Durable takeaways

- Per-session command order and cross-session concurrency coexist.
- Exactly one writer owns each transport.
- EOF is a session-close event that must release domain waiters.
- Real-client and direct/TCP parity tests prove different boundaries.

### Explain it in your own words

How can BLPOP remain ordered within its connection without preventing RPUSH on another connection, and which object owns the eventual BLPOP reply?

### Textbook

This stage applies the reactor/pump pattern and structured ownership to an async server. Its bounded queues create explicit backpressure domains, while adapter parity is a refinement check: the transport implementation preserves the same abstract machine.

## 中文

### 目标

通过真实 TCP Session 暴露同一套序列化 MiniRedis 语义，同时保持每连接顺序、有界 Buffer、慢 Client 隔离与完整 Owned Shutdown。

### 交付文件

- `pyproject.toml`
- `src/miniredis/adapters/tcp.py`
- `src/miniredis/config.py`
- `src/miniredis/core/executor.py`
- `src/miniredis/runtime.py`
- `tests/adapters/test_direct_resp_parity.py`
- `tests/adapters/test_tcp_async_semantics.py`
- `tests/adapters/test_tcp_smoke.py`
- `tests/interop/test_redis_py_resp2.py`
- `uv.lock`

### 当前遇到的问题

Codec 还不是 Server。每个 Socket 都引入并发 Reader、Writer、Command 与 Close 工作。同一连接的 Command 必须保持顺序，但 Blocking Command 不能阻止另一连接；所有 Reply 与 Pub/Sub Event 必须共享一个有序 Outbox；EOF、Protocol Error、Slow Writer、Server Close 与 Runtime Close 必须汇合为同一个 Session 结局。

### 先看会坏在哪里

并发提交所有已解码 Frame 会重排 Pipeline Command。在 Read Loop 中等待 Blocking Command 会卡住其他 Frame 或 Connection。Producer 直接写 Reply 会重排 Pub/Sub 与 Reply。EOF 可能遗留 BLPOP Waiter，而 Close Race 可能泄漏 Reader/Writer Task 或重复消耗 Drain Grace。

### 测试契约

<!-- journey-file: tests/adapters/test_tcp_smoke.py -->
#### `tests/adapters/test_tcp_smoke.py`

##### 锁定什么

锁定 Fragmentation、多 Command、Protocol-error Delivery、EOF Truncation、幂等 Server Close，以及只在 Running Runtime 上接纳。

##### 如何构造反例

跨 Write 切开 PING，合并 SET/GET，发送非法或截断 Frame，并重复关闭存活 Session 与 Server。

##### 关键测试语句

```python
assert await reader.readline() == b"-CLOSED protocol error: truncated RESP frame\r\n"
```

##### 失败意味着什么

Socket Lifecycle 或 Protocol Error 绕过有序 Session Boundary。

<!-- journey-file: tests/adapters/test_direct_resp_parity.py -->
#### `tests/adapters/test_direct_resp_parity.py`

##### 锁定什么

锁定 Direct 与 TCP Adapter 的 Reply Bytes、Logical State 与 Commit Sequence 一致。

##### 如何构造反例

让两个 Runtime 执行同一混合 Command Sequence，比较每个 Encoded Reply 与最终状态。

##### 关键测试语句

```python
assert tcp_runtime.debug_logical_items() == direct_runtime.debug_logical_items()
```

##### 失败意味着什么

Network Adapter 发明了新语义，而不是运输同一 Executor 行为。

<!-- journey-file: tests/adapters/test_tcp_async_semantics.py -->
#### `tests/adapters/test_tcp_async_semantics.py`

##### 锁定什么

锁定跨连接推进、EOF Waiter 清理、Pub/Sub 顺序、慢 Subscriber 隔离，以及一次有界 Runtime-close Drain。

##### 如何构造反例

阻塞 BLPOP、断开 Client、用极小 Outbox 暂停一个 Writer，并在 Runtime Shutdown 时 Gate Transport Drain。

##### 关键测试语句

```python
assert server.owned_task_count == 0
assert await reader.read() == b"+PONG\r\n"
```

##### 失败意味着什么

Async Session 所有权泄漏、顺序分叉，或一个慢 Transport 控制健康 Client/Runtime Shutdown。

<!-- journey-file: tests/interop/test_redis_py_resp2.py -->
#### `tests/interop/test_redis_py_resp2.py`

##### 锁定什么

锁定与独立 Redis Client 实现的基本兼容性。

##### 如何构造反例

redis-py 以 RESP2 Binary Mode 连接并执行 String 与 Hash 操作。

##### 关键测试语句

```python
assert await client.incr(b"k") == 2
assert await client.hget(b"h", b"f") == b"v"
```

##### 失败意味着什么

进程内 Codec Test 漏掉真实 Client Handshake 或 Wire-behavior 不一致。

### 基本概念

一个 TCP Session 持有一个 Decoder/Read Pump、一条有序的单 Command Submission Chain、一个 Outbox/Write Pump 与一个幂等 Close Task。Pipelining 表示前一 Reply 完成前可继续读取 Frame，并不允许 Session 内执行重排。Slow-client Isolation 表示 Outbox Overflow 关闭该 Endpoint，而不阻塞 Executor。

### 为什么需要这个机制

网络边界增加大量 Lifecycle Race，却不能分叉 Database Semantics。复用 Executor Endpoint/Outbox 契约能跨 Direct 与 TCP 保持顺序和背压行为。显式 Session Ownership 让 EOF 与 Shutdown 一起释放 Blocked Command、Subscription、Transport 与 Task。

### 运行时心智模型

Server 接受 Socket 并注册 `SessionEndpoint`。Reader 解码到有界 Frame Deque，最多启动一个 Command Task。Executor Outcome 进入 Endpoint Outbox。只有 Writer 序列化 Outbound Value 并 Drain Transport。任何终态路径都 Quiesce Reader、关闭 Executor Session、按需 Abort/Drain Outbox、Join Task、关闭 Socket 并 Unregister Session。

### 机制板块

<!-- journey-file: src/miniredis/adapters/tcp.py -->
#### `src/miniredis/adapters/tcp.py`

##### 是什么，为什么出现

本模块持有 TCP Server 与每连接生命周期，而不持有 Database Logic。

##### 运行时角色

增量读取 RESP2、限制已解码 Frame、每次提交一个 Session Command、只从 Outbox Pump 写出，并在关闭时 Join 全部 Task。

##### 关键代码

```python
if (
    self._closed
    or self._reader_quiescing
    or self._pending_command is not None
    or not self._frames
):
    return
request = self._frames.popleft()
```

##### 关键语句理解

Reader 可以 Pipeline Frame，但单 Pending-command Slot 保持到达顺序，也不会让 Socket Read 占用全局 Executor。

<!-- journey-file: src/miniredis/config.py -->
#### `src/miniredis/config.py`

##### 是什么，为什么出现

配置增加每个 Session 保留的已解码 Command Frame 正数上限。

##### 运行时角色

防止 Fast Reader 在 Slow/Blocking Command 后积累无界工作。

##### 关键代码

```python
if self.max_session_frames <= 0:
    raise ValueError("max_session_frames must be positive")
```

##### 关键语句理解

Protocol Byte Limit 与 Decoded-work Limit 保护不同内存分配，两者都需要。

<!-- journey-file: src/miniredis/core/executor.py -->
#### `src/miniredis/core/executor.py`

##### 是什么，为什么出现

Executor 暴露 Token Allocation，并把 Transport-close Message 留给 Runtime/Session Owner。

##### 运行时角色

TCP Parse/Admission Failure 仍获得有序 Reply Token，Shutdown 则避免两个 Owner 写同一 Terminal Event。

##### 关键代码

```python
def new_request_token(self) -> RequestToken:
    return RequestToken(next(self._request_tokens))
```

##### 关键语句理解

即使 Failure 发生在正常 Executor Submission 前，也参与同一个单调 Outbound Order。

<!-- journey-file: src/miniredis/runtime.py -->
#### `src/miniredis/runtime.py`

##### 是什么，为什么出现

Runtime 暴露 Session Operation 并持有每个已启动 TCP Server。

##### 运行时角色

注册 Endpoint，提交/放弃 Session Request，隔离 Slow Endpoint，只在 Running 时启动 Server，并在释放 Endpoint 前关闭 Server。

##### 关键代码

```python
await asyncio.gather(
    *(server.finish_runtime_close() for server in tuple(self._tcp_servers)),
    return_exceptions=False,
)
```

##### 关键语句理解

Network Producer/Consumer 在 Executor Endpoint 仍存在时完成；Endpoint Release 是最后的所有权步骤。

<!-- journey-file: pyproject.toml -->
#### `pyproject.toml`

##### 是什么，为什么出现

Project Metadata 加入 redis-py 互操作测试依赖。

##### 运行时角色

让验证使用独立实现的 RESP2 Client；Production MiniRedis 不依赖 Protocol Library。

##### 关键代码

```toml
"redis>=8,<9",
```

##### 关键语句理解

该依赖只是外部兼容性证据的 Test Scaffold，不属于 TCP 机制。

<!-- journey-file: uv.lock -->
#### `uv.lock`

##### 是什么，为什么出现

Lockfile 冻结已解析的互操作依赖图。

##### 运行时角色

让真实 Client Check 在学习环境中可复现。

##### 关键代码

```toml
name = "redis"
```

##### 关键语句理解

本文件记录 Dependency Resolution，不解释 Runtime Behavior，因此刻意与 Test Scaffold 合并理解。

### 验证证据

运行 `tests.txt` 中四个聚焦测试模块，再累计构建 Stage 1–20，并把 Owned Tree 与提交 `5419f99` 比较。

### 需要真正记住的内容

- Per-session Command Order 与 Cross-session Concurrency 可以共存。
- 每个 Transport 恰好由一个 Writer 持有。
- EOF 是必须释放 Domain Waiter 的 Session-close Event。
- Real-client 与 Direct/TCP Parity Test 证明不同边界。

### 用自己的话讲清楚

BLPOP 如何在自身连接保持有序，同时不阻止另一连接的 RPUSH？最终 BLPOP Reply 由哪个对象持有？

### 教材

本阶段把 Reactor/Pump Pattern 与 Structured Ownership 用于 Async Server。有界 Queue 建立显式 Backpressure Domain，而 Adapter Parity 是一种 Refinement Check：Transport Implementation 保持同一个 Abstract Machine。
