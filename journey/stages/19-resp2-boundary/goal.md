# Stage 19 · RESP2 protocol boundary / RESP2 协议边界

<!-- journey: chapter=10 tests_added=3 -->

## English

### Goal

Translate a fragmented binary RESP2 byte stream into domain requests and translate every domain outbound value back to exact wire bytes.

### Deliverable files

- `src/miniredis/adapters/resp2.py`
- `tests/adapters/test_resp2_decode.py`
- `tests/adapters/test_resp2_encode.py`
- `tests/adapters/test_resp2_mapping.py`

### The problem at this point

The runtime understands typed requests and replies, but a socket supplies arbitrary byte chunks rather than whole commands. One read may contain half a frame or several frames. Protocol parsing must preserve binary bulk values, bound memory, distinguish incomplete input from invalid input, and keep wire rules outside domain execution.

### Failure preview

A decoder that assumes one read per command rejects fragmentation or merges coalesced commands. Text decoding corrupts arbitrary key/value bytes. Treating EOF as ordinary incompleteness accepts truncated frames, while missing size limits permits an unauthenticated connection to grow memory without bound.

### Test contract

<!-- journey-file: tests/adapters/test_resp2_decode.py -->
#### `tests/adapters/test_resp2_decode.py`

##### What this test locks

It locks incremental fragmentation/coalescing, binary bulk values, EOF validation, and buffer/bulk limits.

##### How it constructs the counterexample

It splits GET in the middle, feeds two PING frames together, supplies non-UTF-8 bytes, and ends malformed or truncated streams.

##### Key test statement

```python
assert decoder.feed(b"*2\r\n$3\r\nGE") == ()
assert decoder.feed(b"T\r\n$1\r\nk\r\n") == expected
```

##### What a failure means

Parser state depends on transport read boundaries or fails to reject unsafe wire input.

<!-- journey-file: tests/adapters/test_resp2_encode.py -->
#### `tests/adapters/test_resp2_encode.py`

##### What this test locks

It locks exact bytes for simple strings, errors, integers, binary/null bulk values, and normal/null arrays.

##### How it constructs the counterexample

It table-drives every RESP2 frame variant, including embedded NUL and null sentinels.

##### Key test statement

```python
assert encode_frame(RespBulk(b"a\x00b")) == b"$3\r\na\x00b\r\n"
```

##### What a failure means

Wire encoding is ambiguous, lossy, or not byte-for-byte RESP2.

<!-- journey-file: tests/adapters/test_resp2_mapping.py -->
#### `tests/adapters/test_resp2_mapping.py`

##### What this test locks

It locks the narrow mapping between command arrays and `CommandRequest`, plus exhaustive encoding of frozen outbound domain values.

##### How it constructs the counterexample

It rejects integers, null/empty arrays, null command names and non-bulk arguments, then encodes replies and Pub/Sub events.

##### Key test statement

```python
assert frame_to_request(frame) == CommandRequest(b"SET", (b"k", b"\xff"))
```

##### What a failure means

Protocol structure leaked into command execution or a valid runtime outbound value has no wire representation.

### Basic concepts

RESP2 is a framed byte protocol whose type prefix determines how the following bytes are parsed. Incremental decoding retains only incomplete suffix state between calls. Bulk strings are length-delimited bytes, not text. Mapping is a separate step: only a non-empty array of non-null bulk strings is a MiniRedis command.

### Why this mechanism is necessary

Separating decode, domain mapping, and encode keeps transport chunking and syntax errors out of the executor. Binary preservation maintains Redis-compatible key/value semantics, while explicit limits and EOF finalization make the network boundary safe to expose.

### Runtime mental model

Bytes enter a decoder buffer. Complete frames are removed and returned; an incomplete suffix stays buffered. A frame mapper validates the command shape and creates `CommandRequest`. On the way out, one exhaustive encoder maps domain replies and asynchronous outbound events to RESP2 bytes.

### Mechanism blocks

<!-- journey-file: src/miniredis/adapters/resp2.py -->
#### `src/miniredis/adapters/resp2.py`

##### What it is and why it appears

This module is the complete syntax and mapping boundary between RESP2 bytes and MiniRedis domain values.

##### Runtime role

It buffers bounded fragments, parses recursive arrays and scalar frames, validates command shape, and serializes outbound values without text conversion.

##### Key code

```python
frames.append(frame)
del self._buffer[:consumed]
```

##### Statement understanding

Only bytes belonging to complete frames are consumed; the remaining suffix is precisely the state needed by the next network read.

### Verification evidence

Run the three adapter test modules from `tests.txt`, then cumulatively build Stages 1–19 and compare the owned tree with commit `c088652`.

### Durable takeaways

- TCP read boundaries are not protocol frame boundaries.
- Bulk strings remain bytes end to end.
- EOF turns an incomplete suffix into a protocol error.
- Domain mapping is distinct from wire parsing.

### Explain it in your own words

Why must `feed()` tolerate an incomplete suffix while `finish()` rejects exactly that same suffix?

### Textbook

This is a streaming parser with explicit resource bounds and a protocol/domain anti-corruption layer. Its state machine is small, but it establishes the trust boundary for every later network interaction.

## 中文

### 目标

把任意分片的二进制 RESP2 Byte Stream 转成领域 Request，并把每种领域 Outbound Value 转回精确 Wire Bytes。

### 交付文件

- `src/miniredis/adapters/resp2.py`
- `tests/adapters/test_resp2_decode.py`
- `tests/adapters/test_resp2_encode.py`
- `tests/adapters/test_resp2_mapping.py`

### 当前遇到的问题

Runtime 已理解类型化 Request 与 Reply，但 Socket 提供的是任意 Byte Chunk，而不是完整 Command。一次 Read 可能只有半个 Frame，也可能包含多个 Frame。协议解析必须保留二进制 Bulk Value、限制内存、区分未完成与非法输入，并让 Wire Rule 留在领域执行之外。

### 先看会坏在哪里

假设一次 Read 对应一个 Command 的 Decoder 会拒绝 Fragmentation 或合并 Coalesced Command。文本解码会破坏任意 Key/Value Bytes。把 EOF 当普通未完成会接受截断 Frame，缺少尺寸限制则允许未认证连接无限增大内存。

### 测试契约

<!-- journey-file: tests/adapters/test_resp2_decode.py -->
#### `tests/adapters/test_resp2_decode.py`

##### 锁定什么

锁定增量 Fragmentation/Coalescing、二进制 Bulk、EOF 校验以及 Buffer/Bulk Limit。

##### 如何构造反例

从中间切开 GET，把两个 PING 一次送入，提供非 UTF-8 Bytes，并结束非法或截断 Stream。

##### 关键测试语句

```python
assert decoder.feed(b"*2\r\n$3\r\nGE") == ()
assert decoder.feed(b"T\r\n$1\r\nk\r\n") == expected
```

##### 失败意味着什么

Parser State 依赖 Transport Read 边界，或没有拒绝危险 Wire Input。

<!-- journey-file: tests/adapters/test_resp2_encode.py -->
#### `tests/adapters/test_resp2_encode.py`

##### 锁定什么

锁定 Simple String、Error、Integer、Binary/Null Bulk 与 Normal/Null Array 的精确 Bytes。

##### 如何构造反例

用表驱动覆盖每种 RESP2 Frame，包括内嵌 NUL 与 Null Sentinel。

##### 关键测试语句

```python
assert encode_frame(RespBulk(b"a\x00b")) == b"$3\r\na\x00b\r\n"
```

##### 失败意味着什么

Wire Encoding 含糊、有损或不是逐字节 RESP2。

<!-- journey-file: tests/adapters/test_resp2_mapping.py -->
#### `tests/adapters/test_resp2_mapping.py`

##### 锁定什么

锁定 Command Array 与 `CommandRequest` 的窄映射，以及 Frozen Outbound Domain Value 的完备编码。

##### 如何构造反例

拒绝 Integer、Null/Empty Array、Null Command Name 与非 Bulk Arg，再编码 Reply 与 Pub/Sub Event。

##### 关键测试语句

```python
assert frame_to_request(frame) == CommandRequest(b"SET", (b"k", b"\xff"))
```

##### 失败意味着什么

协议结构泄漏进 Command Execution，或合法 Runtime Outbound Value 没有 Wire 表示。

### 基本概念

RESP2 是 Framed Byte Protocol，Type Prefix 决定后续 Bytes 的解析方式。Incremental Decode 在调用间只保留未完成 Suffix。Bulk String 是长度分隔 Bytes，不是文本。Mapping 是独立步骤：只有由非 Null Bulk String 组成的非空 Array 才是 MiniRedis Command。

### 为什么需要这个机制

分离 Decode、Domain Mapping 与 Encode，能让 Transport Chunking 和 Syntax Error 留在 Executor 外。二进制保真维持 Redis Key/Value 语义，显式 Limit 与 EOF Finalization 则让网络边界可以安全暴露。

### 运行时心智模型

Bytes 进入 Decoder Buffer。完整 Frame 被移除并返回，未完成 Suffix 留在 Buffer。Frame Mapper 校验 Command Shape 并创建 `CommandRequest`。返回方向由一个完备 Encoder 把 Domain Reply 与异步 Outbound Event 映射成 RESP2 Bytes。

### 机制板块

<!-- journey-file: src/miniredis/adapters/resp2.py -->
#### `src/miniredis/adapters/resp2.py`

##### 是什么，为什么出现

本模块是 RESP2 Bytes 与 MiniRedis Domain Value 之间完整的 Syntax/Mapping Boundary。

##### 运行时角色

缓存有界 Fragment，解析递归 Array 与 Scalar Frame，校验 Command Shape，并在不做文本转换的情况下序列化 Outbound Value。

##### 关键代码

```python
frames.append(frame)
del self._buffer[:consumed]
```

##### 关键语句理解

只消费属于完整 Frame 的 Bytes；剩余 Suffix 正是下一次 Network Read 所需的全部状态。

### 验证证据

运行 `tests.txt` 中三个 Adapter 测试模块，再累计构建 Stage 1–19，并把 Owned Tree 与提交 `c088652` 比较。

### 需要真正记住的内容

- TCP Read Boundary 不是 Protocol Frame Boundary。
- Bulk String 端到端保持 Bytes。
- EOF 会把未完成 Suffix 变成 Protocol Error。
- Domain Mapping 与 Wire Parsing 是两件事。

### 用自己的话讲清楚

为什么 `feed()` 必须容忍未完成 Suffix，而 `finish()` 必须拒绝完全相同的 Suffix？

### 教材

这是带显式资源边界的 Streaming Parser，也是 Protocol/Domain Anti-corruption Layer。它的状态机很小，却建立了后续每次网络交互的 Trust Boundary。
