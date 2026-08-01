# Stage 19 · RESP2 协议边界

### 目标

把任意分片的二进制 RESP2 Byte Stream 转成领域 Request，并把每种领域 Outbound Value 转回精确 Wire Bytes。

??? note "交付文件"
    - `src/miniredis/adapters/resp2.py`
    - `tests/adapters/test_resp2_decode.py`
    - `tests/adapters/test_resp2_encode.py`
    - `tests/adapters/test_resp2_mapping.py`

### 当前遇到的问题

Runtime 已理解类型化 Request 与 Reply，但 Socket 提供的是任意 Byte Chunk，而不是完整 Command。一次 Read 可能只有半个 Frame，也可能包含多个 Frame。协议解析必须保留二进制 Bulk Value、限制内存、区分未完成与非法输入，并让 Wire Rule 留在领域执行之外。

### 测试契约

#### 先看会坏在哪里

假设一次 Read 对应一个 Command 的 Decoder 会拒绝 Fragmentation 或合并 Coalesced Command。文本解码会破坏任意 Key/Value Bytes。把 EOF 当普通未完成会接受截断 Frame，缺少尺寸限制则允许未认证连接无限增大内存。

??? note "文件差异：tests/adapters/test_resp2_decode.py"
    ```diff
    diff --git a/tests/adapters/test_resp2_decode.py b/tests/adapters/test_resp2_decode.py
    new file mode 100644
    index 0000000000000000000000000000000000000000..9652c7e73676188f7a9780d704752760d0812d33
    --- /dev/null
    +++ b/tests/adapters/test_resp2_decode.py
    @@ -0,0 +1,52 @@
    +import pytest
    +
    +from miniredis.adapters.resp2 import (
    +    RespArray,
    +    RespBulk,
    +    RespDecoder,
    +    RespLimits,
    +    RespProtocolError,
    +)
    +
    +
    +def test_fragmented_command_emits_only_when_complete():
    +    decoder = RespDecoder()
    +    assert decoder.feed(b"*2\r\n$3\r\nGE") == ()
    +    assert decoder.feed(b"T\r\n$1\r\nk\r\n") == (
    +        RespArray((RespBulk(b"GET"), RespBulk(b"k"))),
    +    )
    +
    +
    +def test_coalesced_commands_remain_separate():
    +    decoder = RespDecoder()
    +    assert decoder.feed(b"*1\r\n$4\r\nPING\r\n*1\r\n$4\r\nPING\r\n") == (
    +        RespArray((RespBulk(b"PING"),)),
    +        RespArray((RespBulk(b"PING"),)),
    +    )
    +
    +
    +def test_binary_bulk_is_not_utf8_decoded():
    +    decoder = RespDecoder()
    +    assert decoder.feed(b"$3\r\n\xff\x00x\r\n") == (RespBulk(b"\xff\x00x"),)
    +
    +
    +@pytest.mark.parametrize(
    +    "wire",
    +    [
    +        b"+bad\n",
    +        b"$x\r\n",
    +        b"$3\r\nab\r\n",
    +        b"*2\r\n$1\r\na\r\n",
    +    ],
    +)
    +def test_invalid_or_incomplete_at_eof_is_rejected(wire):
    +    decoder = RespDecoder()
    +    with pytest.raises(RespProtocolError):
    +        decoder.feed(wire)
    +        decoder.finish()
    +
    +
    +def test_bulk_and_buffer_limits_are_enforced():
    +    decoder = RespDecoder(RespLimits(max_buffer=16, max_bulk=2))
    +    with pytest.raises(RespProtocolError):
    +        decoder.feed(b"$3\r\nabc\r\n")
    ```

**锁定什么**

锁定增量 Fragmentation/Coalescing、二进制 Bulk、EOF 校验以及 Buffer/Bulk Limit。

**如何构造反例**

从中间切开 GET，把两个 PING 一次送入，提供非 UTF-8 Bytes，并结束非法或截断 Stream。

**关键测试语句**

```python
assert decoder.feed(b"*2\r\n$3\r\nGE") == ()
assert decoder.feed(b"T\r\n$1\r\nk\r\n") == expected
```

**失败意味着什么**

Parser State 依赖 Transport Read 边界，或没有拒绝危险 Wire Input。

??? note "文件差异：tests/adapters/test_resp2_encode.py"
    ```diff
    diff --git a/tests/adapters/test_resp2_encode.py b/tests/adapters/test_resp2_encode.py
    new file mode 100644
    index 0000000000000000000000000000000000000000..fda9c796cbab0f087c87788224dd338b546c64f7
    --- /dev/null
    +++ b/tests/adapters/test_resp2_encode.py
    @@ -0,0 +1,29 @@
    +import pytest
    +
    +from miniredis.adapters.resp2 import (
    +    RespArray,
    +    RespBulk,
    +    RespError,
    +    RespInteger,
    +    RespSimple,
    +    encode_frame,
    +)
    +
    +
    +@pytest.mark.parametrize(
    +    ("frame", "expected"),
    +    [
    +        (RespSimple(b"OK"), b"+OK\r\n"),
    +        (RespError(b"ERR bad"), b"-ERR bad\r\n"),
    +        (RespInteger(42), b":42\r\n"),
    +        (RespBulk(b"a\x00b"), b"$3\r\na\x00b\r\n"),
    +        (RespBulk(None), b"$-1\r\n"),
    +        (
    +            RespArray((RespBulk(b"GET"), RespBulk(b"k"))),
    +            b"*2\r\n$3\r\nGET\r\n$1\r\nk\r\n",
    +        ),
    +        (RespArray(None), b"*-1\r\n"),
    +    ],
    +)
    +def test_encode_frame(frame, expected):
    +    assert encode_frame(frame) == expected
    ```

**锁定什么**

锁定 Simple String、Error、Integer、Binary/Null Bulk 与 Normal/Null Array 的精确 Bytes。

**如何构造反例**

用表驱动覆盖每种 RESP2 Frame，包括内嵌 NUL 与 Null Sentinel。

**关键测试语句**

```python
assert encode_frame(RespBulk(b"a\x00b")) == b"$3\r\na\x00b\r\n"
```

**失败意味着什么**

Wire Encoding 含糊、有损或不是逐字节 RESP2。

??? note "文件差异：tests/adapters/test_resp2_mapping.py"
    ```diff
    diff --git a/tests/adapters/test_resp2_mapping.py b/tests/adapters/test_resp2_mapping.py
    new file mode 100644
    index 0000000000000000000000000000000000000000..9a70f989e42cd376cf3eb0cfee42b49cb8544776
    --- /dev/null
    +++ b/tests/adapters/test_resp2_mapping.py
    @@ -0,0 +1,72 @@
    +import pytest
    +
    +from miniredis.adapters.resp2 import (
    +    RespArray,
    +    RespBulk,
    +    RespInteger,
    +    RespProtocolError,
    +    encode_outbound,
    +    frame_to_request,
    +)
    +from miniredis.commands.request import CommandRequest
    +from miniredis.core.outbound import (
    +    PubSubMessage,
    +    PubSubPong,
    +    ReplyMessage,
    +    RequestToken,
    +    ServerClosed,
    +    SubscriptionAck,
    +)
    +from miniredis.core.reply import Bytes, Failure, Items, Number, Ok
    +
    +
    +def test_command_array_maps_without_text_decoding():
    +    frame = RespArray((RespBulk(b"SET"), RespBulk(b"k"), RespBulk(b"\xff")))
    +    assert frame_to_request(frame) == CommandRequest(b"SET", (b"k", b"\xff"))
    +
    +
    +@pytest.mark.parametrize(
    +    "frame",
    +    [
    +        RespInteger(1),
    +        RespArray(None),
    +        RespArray(()),
    +        RespArray((RespBulk(None),)),
    +        RespArray((RespInteger(1),)),
    +    ],
    +)
    +def test_non_command_frames_are_protocol_errors(frame):
    +    with pytest.raises(RespProtocolError):
    +        frame_to_request(frame)
    +
    +
    +def test_domain_replies_encode_as_resp2():
    +    assert encode_outbound(Ok()) == b"+OK\r\n"
    +    assert encode_outbound(Bytes(None)) == b"$-1\r\n"
    +    assert encode_outbound(Bytes(b"x")) == b"$1\r\nx\r\n"
    +    assert encode_outbound(Number(2)) == b":2\r\n"
    +    assert encode_outbound(Items((Bytes(b"a"), Number(1)))) == (
    +        b"*2\r\n$1\r\na\r\n:1\r\n"
    +    )
    +    assert encode_outbound(Failure("WRONGTYPE", "bad")) == b"-WRONGTYPE bad\r\n"
    +
    +
    +def test_every_frozen_outbound_value_encodes_as_resp2():
    +    token = RequestToken(7)
    +    assert encode_outbound(ReplyMessage(token, Number(3))) == b":3\r\n"
    +    assert (
    +        encode_outbound(SubscriptionAck("subscribe", b"c", 1))
    +        == b"*3\r\n$9\r\nsubscribe\r\n$1\r\nc\r\n:1\r\n"
    +    )
    +    assert (
    +        encode_outbound(SubscriptionAck("unsubscribe", None, 0))
    +        == b"*3\r\n$11\r\nunsubscribe\r\n$-1\r\n:0\r\n"
    +    )
    +    assert (
    +        encode_outbound(PubSubMessage(b"c", b"m"))
    +        == b"*3\r\n$7\r\nmessage\r\n$1\r\nc\r\n$1\r\nm\r\n"
    +    )
    +    assert encode_outbound(PubSubPong(b"x")) == b"*2\r\n$4\r\npong\r\n$1\r\nx\r\n"
    +    assert (
    +        encode_outbound(ServerClosed("runtime closed")) == b"-CLOSED runtime closed\r\n"
    +    )
    ```

**锁定什么**

锁定 Command Array 与 `CommandRequest` 的窄映射，以及 Frozen Outbound Domain Value 的完备编码。

**如何构造反例**

拒绝 Integer、Null/Empty Array、Null Command Name 与非 Bulk Arg，再编码 Reply 与 Pub/Sub Event。

**关键测试语句**

```python
assert frame_to_request(frame) == CommandRequest(b"SET", (b"k", b"\xff"))
```

**失败意味着什么**

协议结构泄漏进 Command Execution，或合法 Runtime Outbound Value 没有 Wire 表示。

### 基本概念

RESP2 是 Framed Byte Protocol，Type Prefix 决定后续 Bytes 的解析方式。Incremental Decode 在调用间只保留未完成 Suffix。Bulk String 是长度分隔 Bytes，不是文本。Mapping 是独立步骤：只有由非 Null Bulk String 组成的非空 Array 才是 MiniRedis Command。

### 为什么需要这个机制

分离 Decode、Domain Mapping 与 Encode，能让 Transport Chunking 和 Syntax Error 留在 Executor 外。二进制保真维持 Redis Key/Value 语义，显式 Limit 与 EOF Finalization 则让网络边界可以安全暴露。

### 运行时心智模型

Bytes 进入 Decoder Buffer。完整 Frame 被移除并返回，未完成 Suffix 留在 Buffer。Frame Mapper 校验 Command Shape 并创建 `CommandRequest`。返回方向由一个完备 Encoder 把 Domain Reply 与异步 Outbound Event 映射成 RESP2 Bytes。

### 机制板块

#### 二进制安全的 RESP2 边界

增量解码有界 Wire Frame，保持 Bulk Value 为 Bytes，把命令 Array 映射成领域 Request，并显式编码每种 Outbound Value。

??? note "文件差异：src/miniredis/adapters/resp2.py"
    ```diff
    diff --git a/src/miniredis/adapters/resp2.py b/src/miniredis/adapters/resp2.py
    new file mode 100644
    index 0000000000000000000000000000000000000000..a609b3208acca3fa14690be45ebfff88365bb13c
    --- /dev/null
    +++ b/src/miniredis/adapters/resp2.py
    @@ -0,0 +1,239 @@
    +from __future__ import annotations
    +
    +from dataclasses import dataclass
    +from typing import TypeAlias
    +
    +from miniredis.commands.request import CommandRequest
    +from miniredis.core.outbound import (
    +    Outbound,
    +    PubSubMessage,
    +    PubSubPong,
    +    ReplyMessage,
    +    ServerClosed,
    +    SubscriptionAck,
    +)
    +from miniredis.core.reply import Bytes, Failure, Items, Number, Ok, Reply
    +
    +
    +class RespProtocolError(ValueError):
    +    pass
    +
    +
    +@dataclass(frozen=True, slots=True)
    +class RespSimple:
    +    data: bytes
    +
    +
    +@dataclass(frozen=True, slots=True)
    +class RespError:
    +    data: bytes
    +
    +
    +@dataclass(frozen=True, slots=True)
    +class RespInteger:
    +    value: int
    +
    +
    +@dataclass(frozen=True, slots=True)
    +class RespBulk:
    +    data: bytes | None
    +
    +
    +@dataclass(frozen=True, slots=True)
    +class RespArray:
    +    items: tuple[RespFrame, ...] | None
    +
    +
    +RespFrame: TypeAlias = RespSimple | RespError | RespInteger | RespBulk | RespArray
    +
    +
    +@dataclass(frozen=True, slots=True)
    +class RespLimits:
    +    max_buffer: int = 1 << 20
    +    max_bulk: int = 1 << 20
    +    max_array: int = 1024
    +    max_depth: int = 8
    +
    +
    +class _NeedMore(Exception):
    +    pass
    +
    +
    +class RespDecoder:
    +    def __init__(self, limits: RespLimits | None = None) -> None:
    +        self._limits = limits or RespLimits()
    +        self._buffer = bytearray()
    +
    +    def feed(self, data: bytes) -> tuple[RespFrame, ...]:
    +        self._buffer.extend(data)
    +        if len(self._buffer) > self._limits.max_buffer:
    +            raise RespProtocolError("RESP buffer limit exceeded")
    +        frames: list[RespFrame] = []
    +        offset = 0
    +        while offset < len(self._buffer):
    +            try:
    +                frame, next_offset = self._parse(offset, 0)
    +            except _NeedMore:
    +                break
    +            frames.append(frame)
    +            offset = next_offset
    +        if offset:
    +            del self._buffer[:offset]
    +        return tuple(frames)
    +
    +    def finish(self) -> tuple[RespFrame, ...]:
    +        frames = self.feed(b"")
    +        if self._buffer:
    +            raise RespProtocolError("truncated RESP frame")
    +        return frames
    +
    +    def _read_line(self, offset: int) -> tuple[bytes, int]:
    +        end = self._buffer.find(b"\r\n", offset)
    +        if end < 0:
    +            raise _NeedMore
    +        return bytes(self._buffer[offset:end]), end + 2
    +
    +    @staticmethod
    +    def _decimal(data: bytes, label: str) -> int:
    +        try:
    +            text = data.decode("ascii")
    +            if not text or text in {"+", "-"}:
    +                raise ValueError
    +            if text[0] == "-":
    +                digits = text[1:]
    +            else:
    +                digits = text
    +            if not digits.isdecimal():
    +                raise ValueError
    +            return int(text)
    +        except (UnicodeDecodeError, ValueError) as exc:
    +            raise RespProtocolError(f"invalid RESP {label}") from exc
    +
    +    def _parse(self, offset: int, depth: int) -> tuple[RespFrame, int]:
    +        if depth > self._limits.max_depth:
    +            raise RespProtocolError("RESP nesting limit exceeded")
    +        if offset >= len(self._buffer):
    +            raise _NeedMore
    +        prefix = self._buffer[offset]
    +        if prefix in (ord("+"), ord("-"), ord(":")):
    +            line, end = self._read_line(offset + 1)
    +            if prefix == ord("+"):
    +                return RespSimple(line), end
    +            if prefix == ord("-"):
    +                return RespError(line), end
    +            return RespInteger(self._decimal(line, "integer")), end
    +        if prefix == ord("$"):
    +            line, body = self._read_line(offset + 1)
    +            length = self._decimal(line, "bulk length")
    +            if length == -1:
    +                return RespBulk(None), body
    +            if length < 0 or length > self._limits.max_bulk:
    +                raise RespProtocolError("invalid RESP bulk length")
    +            end = body + length
    +            if end + 2 > len(self._buffer):
    +                raise _NeedMore
    +            if self._buffer[end : end + 2] != b"\r\n":
    +                raise RespProtocolError("invalid bulk terminator")
    +            return RespBulk(bytes(self._buffer[body:end])), end + 2
    +        if prefix == ord("*"):
    +            line, cursor = self._read_line(offset + 1)
    +            length = self._decimal(line, "array length")
    +            if length == -1:
    +                return RespArray(None), cursor
    +            if length < 0 or length > self._limits.max_array:
    +                raise RespProtocolError("invalid RESP array length")
    +            items: list[RespFrame] = []
    +            for _ in range(length):
    +                item, cursor = self._parse(cursor, depth + 1)
    +                items.append(item)
    +            return RespArray(tuple(items)), cursor
    +        raise RespProtocolError("unknown RESP type byte")
    +
    +
    +def _line(prefix: bytes, data: bytes) -> bytes:
    +    if b"\r" in data or b"\n" in data:
    +        raise ValueError("RESP line values cannot contain CR or LF")
    +    return prefix + data + b"\r\n"
    +
    +
    +def encode_frame(frame: RespFrame) -> bytes:
    +    match frame:
    +        case RespSimple(data):
    +            return _line(b"+", data)
    +        case RespError(data):
    +            return _line(b"-", data)
    +        case RespInteger(value):
    +            return b":" + str(value).encode("ascii") + b"\r\n"
    +        case RespBulk(None):
    +            return b"$-1\r\n"
    +        case RespBulk(data):
    +            return b"$" + str(len(data)).encode("ascii") + b"\r\n" + data + b"\r\n"
    +        case RespArray(None):
    +            return b"*-1\r\n"
    +        case RespArray(items):
    +            return (
    +                b"*"
    +                + str(len(items)).encode("ascii")
    +                + b"\r\n"
    +                + b"".join(encode_frame(item) for item in items)
    +            )
    +    raise TypeError(f"unsupported RESP frame: {type(frame)!r}")
    +
    +
    +def frame_to_request(frame: RespFrame) -> CommandRequest:
    +    if not isinstance(frame, RespArray) or not frame.items:
    +        raise RespProtocolError("command must be a non-empty array")
    +    parts: list[bytes] = []
    +    for item in frame.items:
    +        if not isinstance(item, RespBulk) or item.data is None:
    +            raise RespProtocolError("command arguments must be bulk strings")
    +        parts.append(item.data)
    +    return CommandRequest(parts[0], tuple(parts[1:]))
    +
    +
    +def _reply_frame(reply: Reply) -> RespFrame:
    +    match reply:
    +        case Ok(message):
    +            return RespSimple(message)
    +        case Bytes(value):
    +            return RespBulk(value)
    +        case Number(value):
    +            return RespInteger(value)
    +        case Items(values):
    +            return RespArray(tuple(_reply_frame(value) for value in values))
    +        case Failure(code, message):
    +            return RespError(f"{code} {message}".encode())
    +    raise TypeError(f"unsupported reply: {type(reply)!r}")
    +
    +
    +def encode_outbound(outbound: Reply | Outbound) -> bytes:
    +    if isinstance(outbound, ReplyMessage):
    +        return encode_frame(_reply_frame(outbound.reply))
    +    if isinstance(outbound, (Ok, Bytes, Number, Items, Failure)):
    +        return encode_frame(_reply_frame(outbound))
    +    if isinstance(outbound, SubscriptionAck):
    +        channel = RespBulk(outbound.channel)
    +        return encode_frame(
    +            RespArray(
    +                (
    +                    RespBulk(outbound.kind.encode("ascii")),
    +                    channel,
    +                    RespInteger(outbound.subscription_count),
    +                )
    +            )
    +        )
    +    if isinstance(outbound, PubSubMessage):
    +        return encode_frame(
    +            RespArray(
    +                (
    +                    RespBulk(b"message"),
    +                    RespBulk(outbound.channel),
    +                    RespBulk(outbound.payload),
    +                )
    +            )
    +        )
    +    if isinstance(outbound, PubSubPong):
    +        return encode_frame(RespArray((RespBulk(b"pong"), RespBulk(outbound.payload))))
    +    if isinstance(outbound, ServerClosed):
    +        return encode_frame(RespError(f"CLOSED {outbound.reason}".encode()))
    +    raise TypeError(f"unsupported outbound: {type(outbound)!r}")
    ```

**是什么，为什么出现**

本模块是 RESP2 Bytes 与 MiniRedis Domain Value 之间完整的 Syntax/Mapping Boundary。

**运行时角色**

缓存有界 Fragment，解析递归 Array 与 Scalar Frame，校验 Command Shape，并在不做文本转换的情况下序列化 Outbound Value。

**关键代码**

```python
    frames.append(frame)
    offset = next_offset
if offset:
    del self._buffer[:offset]
```

**关键语句理解**

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

[在 GitHub 查看阶段差异](https://github.com/system-in-miniature/mini-redis/compare/0fbaeee...c088652)

完成后可运行 `python -m journey.tools.build_journey check 19` 验收学习工作区。

[Complete reference patch / 完整参考补丁](https://github.com/system-in-miniature/mini-redis/blob/main/journey/stages/19-resp2-boundary/stage.patch)
