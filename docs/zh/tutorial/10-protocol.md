> **语言**: [English](../../tutorial/10-protocol.md) | 简体中文

# 协议层：Direct、RESP2 与 TCP

## 学习目标

学完本章后，你将能够：

- 把命令语义与 Direct、RESP2/TCP 传输职责分开；
- 追踪碎片字节如何经过 `RespDecoder`、`CommandRequest`、共享 executor 与 reply 编码；
- 解释 decoder 的二进制安全与资源上限；
- 为已支持的 RESP2 子集正确配置 redis-py；
- 判断互通证据证明了什么、没有证明什么。

## 一个语义核心，两个适配器

MiniRedis 有两条命令入口：

1. Python `DirectClient` 接收 `CommandRequest`；
2. TCP session 把 RESP2 字节解码为同一个 `CommandRequest`。

adapter 都不实现 `SET`、事务、过期、持久化或阻塞 list。二者都调用 `src/miniredis/runtime.py` 的 `MiniRedis.submit_request`，把 request 解析为封闭 command model，再提交给同一个 `CommandExecutor`。

这就是 **Direct-first** 边界。Direct 不是另一套网络实现的 test double，而是语义核心上最薄的 adapter；TCP 增加 framing、buffer、session 与 wire encoding，却不能拥有第二份命令语义。

```text
Direct:
CommandRequest -> runtime parser -> executor -> domain Reply

RESP2/TCP:
bytes -> RespDecoder -> frame_to_request -> runtime parser
      -> executor -> domain Reply/Outbound -> encode_outbound -> bytes
```

`tests/adapters/test_direct_resp_parity.py::test_selected_sequence_has_state_and_reply_parity` 让 string、hash、list、set、sorted set、类型错误和删除序列分别经过两条路径，并比较编码 reply、逻辑状态和 commit seq。这比仅检查 TCP 返回 `PONG` 更强：它验证代表性序列中 adapter 没有偏离 core。

## RESP2 是 stream，不是 packet

TCP 提供有序字节流。一次 `read` 可能只有半条命令、恰好一条或多条命令；假定“一次 read 等于一次 request”在正常网络条件下也会失败。

`src/miniredis/adapters/resp2.py` 的 `RespDecoder.feed` 把新字节追加到内部 bytearray，反复调用 `_parse`。内部 `_NeedMore` 表示当前后缀可在收到更多字节后变为合法 frame；完整 frame 被返回，已消费前缀从 buffer 删除。

流结束时，`RespDecoder.finish` 调用 `feed(b"")`，若还有字节则抛出 `RespProtocolError("truncated RESP frame")`。这区分了“暂时不完整”和“永远不完整”。

| 前缀 | MiniRedis frame | 示例 |
|---|---|---|
| `+` | `RespSimple` | `+OK\r\n` |
| `-` | `RespError` | `-ERR message\r\n` |
| `:` | `RespInteger` | `:2\r\n` |
| `$` | `RespBulk` | `$3\r\nabc\r\n` |
| `*` | `RespArray` | `*1\r\n$4\r\nPING\r\n` |

命令必须是非空、元素均为非 null bulk string 的数组。`src/miniredis/adapters/resp2.py` 的 `frame_to_request` 把第一项作为命令名，其余保留为 byte 参数；它不做 UTF-8 解码，因此 key/value 二进制安全。需要数值的命令稍后由 command parser 解释。

## 有界解析也是协议契约

stream decoder 持有不可信输入。攻击者可以声明巨大 bulk、深层数组或发送无穷不完整 frame。`RespLimits` 限制：

- 总 buffer（默认 1 MiB）；
- 单个 bulk（默认 1 MiB）；
- array 元素（默认 1024）；
- nesting depth（默认 8）。

`RespDecoder._parse` 验证严格 ASCII 十进制长度、null 标记、CRLF terminator 与已知 type byte。确定非法的数据立即失败；语法上可能完整的前缀则等到 `finish` 才被判定截断。

decoder 能表示 reply 所需的多种 RESP frame 与嵌套数组，但 `frame_to_request` 把 command input 收窄为 bulk-string array。frame grammar 比服务端 command-request grammar 更宽，这是分层 parsing 的例子。

## TCP session 所有权与有序 reply

`src/miniredis/adapters/tcp.py` 的 `TcpServer.start` 调用 `asyncio.start_server`。每个连接成为 `TcpSession`，拥有 reader task、writer task、`SessionEndpoint`、decoder 和有界 pending-frame 状态。

`TcpSession._read_loop` 每次最多读 65,536 字节，feed decoder，转换 frame，再调用 `_submit_available`。在 `MiniRedisConfig.max_session_frames` 与 executor admission 范围内，它可以在第一条完成前提交多条合并命令。

pipeline 不是事务。请求虽一起排队，仍由 executor 逐条正常处理，其他 session 可在 mailbox 边界插入。session endpoint 与 outbox 保持 reply 顺序。`TcpSession._write_loop` 是唯一 writer：接收有序 `Outbound`，调用 `encode_outbound`，write 并 drain。Pub/Sub push 与协议错误也走这条单 writer 路径，不会由多个 task 并发写同一个 `StreamWriter`。

发生协议错误时，reader 通过 outbox 提供 `ServerClosed`，开始 outbox close，给予有界 best-effort drain，再关闭 session。服务关闭时，`TcpServer.quiesce` 停止接收、让 reader quiesce、关闭 session，最后等待 listener 关闭。资源 owner 规则是正确性的一部分：泄漏 task/session 会在 runtime 自称 closed 后仍保留请求状态。

## Domain reply 到 RESP2 的映射

`src/miniredis/adapters/resp2.py` 的 `_reply_frame` 映射封闭 domain 类型：

- `Ok` -> simple string；
- `Bytes` -> bulk string（含 null bulk）；
- `Number` -> integer；
- `Items` -> array；
- `NullArray` -> null array；
- `Failure` -> 含 code 和 message 的 error line。

`encode_outbound` 再加入订阅确认、Pub/Sub message 与 server-close。core 不知道 `Bytes(b"v")` 是 `$1\r\nv\r\n`；adapter 不知道 `GET` 如何找到 `b"v"`。

这种分离让语义变更可先通过 Direct 测试，也让 parity 明确：新增 domain reply 类型如果没有 wire mapping，封闭 match 会报错，而不会悄悄穿过 TCP。

## redis-py 互通

仓库支持的互通 profile 刻意很窄：RESP2 加已实现命令子集。当前 redis-py 可能使用 RESP3 特性或默认发送 client metadata，因此 `tests/interop/test_redis_py_resp2.py` 配置：

```python
client = redis.asyncio.Redis(
    host=host,
    port=port,
    protocol=2,
    decode_responses=False,
    driver_info=None,
)
```

`protocol=2` 选择已实现 wire 版本；`decode_responses=False` 保留 byte；`driver_info=None` 禁用不支持的 driver metadata 行为。

smoke test 验证 `PING`、`SET`、`INCR`、`HSET`、`HGET`。通过说明 redis-py 能在真实本地 TCP 上交换这一子集；不证明 RESP3、完整 Redis 命令、TLS、ACL、cluster redirect、生产吞吐或每个 redis-py convenience API。边界见[行为矩阵 RESP2/TCP 与命令行](../behavior-matrix.md)。

## 与真实 Redis 对照

Redis 网络与协议机制跨越 `src/networking.c`、命令表、命令实现和随版本演化的协议代码。生产 Redis 支持 RESP2/RESP3、庞大命令 option、认证、连接管理、client tracking、复制协议、Cluster message、TLS 构建和优化 I/O。

MiniRedis 保留：

- 增量长度分隔 parsing；
- 二进制安全 bulk string；
- 一次 TCP read 中多 frame，以及跨 read 的单 frame；
- 有序 pipeline reply；
- 有界输入、输出 ownership；
- client library 互通检查点。

它省略 RESP3、inline command、认证、TLS、Cluster/复制 wire protocol、广泛命令兼容与生产性能优化。结论不是“RESP 简单，所以 Redis 简单”，而是：只要 framing、顺序、边界与生命周期明确，小 adapter 可以保留语义核心。

## 动手实验 1：不使用 TCP 观察 fragmentation

保存为 `/tmp/miniredis_resp2.py`：

```python
import asyncio

from miniredis import CommandRequest, MiniRedis
from miniredis.adapters.resp2 import (
    RespDecoder,
    encode_outbound,
    frame_to_request,
)


async def main():
    decoder = RespDecoder()
    print("after fragment 1:",
          decoder.feed(b"*2\r\n$3\r\nGE"))
    frames = decoder.feed(b"T\r\n$1\r\nk\r\n")
    print("after fragment 2:", frames)
    async with MiniRedis.open() as runtime:
        client = runtime.direct_client()
        await client.execute(CommandRequest(b"SET", (b"k", b"v")))
        reply = await client.execute(frame_to_request(frames[0]))
        print("domain reply:", reply)
        print("RESP2 bytes:", encode_outbound(reply))


asyncio.run(main())
```

运行：

```bash
uv run python /tmp/miniredis_resp2.py
```

实测输出：

```text
after fragment 1: ()
after fragment 2: (RespArray(items=(RespBulk(data=b'GET'), RespBulk(data=b'k'))),)
domain reply: Bytes(value=b'v')
RESP2 bytes: b'$1\r\nv\r\n'
```

第一次 feed 尚无完整 frame，因此返回空 tuple 而非错误；第二次正好补全一条 request。`frame_to_request` 接到与 Direct 共用的 core，只有最后一步知道 RESP2 字节形式。

## 动手实验 2：redis-py 经本地 TCP

运行：

```bash
uv run pytest -q tests/interop/test_redis_py_resp2.py
```

允许 loopback TCP bind 的环境中，预期：

```text
.                                                                        [100%]
1 passed
```

**需运行时验证：**教材编写沙箱拒绝 `bind(("127.0.0.1", 0))`，所以这里无法完成 TCP 命令。实测调用到 `MiniRedis.start_tcp` 后失败：

```text
OSError: could not bind on any address out of [('127.0.0.1', 0)]
```

这是环境限制，不是互通通过证据。必须在普通本地开发环境重跑后，才能把 redis-py 路径视为该环境已验证。

无需 bind 的协议测试：

```bash
uv run pytest -q tests/adapters/test_resp2_decode.py \
  tests/adapters/test_resp2_encode.py \
  tests/adapters/test_resp2_mapping.py
```

预期仓库结果：

```text
23 passed
```

## 练习

### 1. 理解题：不完整与非法

为什么 `decoder.feed(b"*2\r\n$3\r\nGE")` 等待，而未知 type byte 立即失败？

??? note "参考答案"

    前者是 RESP array/bulk 的合法前缀，追加字节可以补全，因此 `_NeedMore` 保留后缀；未知 type byte 不可能因追加变合法，`_parse` 立即抛 `RespProtocolError`。EOF 时 `finish` 才把保留前缀判为截断。

### 2. 理解题：区分 pipeline 与 transaction

TCP pipeline 保证什么顺序，又不提供哪些事务性质？

??? note "参考答案"

    它保持本 session frame 的结果/reply 顺序，但每项仍是普通 executor command：其他 session 可插入，失败不回滚，写入不会折叠成一个 `CommitBatch`。原子组合要使用 `MULTI`/`EXEC`。

### 3. 动手题：增加碎片化二进制测试

任务边界：只在 `tests/adapters/test_resp2_decode.py` 增加一个测试，把含 `b"\xff\x00\r\n"` 的 bulk 分至少三块 feed；不改 `src/`。

验收：

```bash
uv run pytest -q tests/adapters/test_resp2_decode.py
```

早期 feed 不返回 frame，最终 `RespBulk.data` 与原始 byte 完全一致。

??? note "参考答案"

    构造正确 `$4\r\n` frame，并拆开 body 与末尾 CRLF；逐块 `feed`，最终比较 `RespBulk(b"\xff\x00\r\n")`。预期 diff 只有一个测试，不得把 payload 解码为文本。

### 4. 动手题：扩展 redis-py smoke

任务边界：在临时练习分支中，只给 `tests/interop/test_redis_py_resp2.py` 增加一个已支持的 list round-trip；不新增 MiniRedis 命令，不改 `src/`。

验收：允许 loopback bind 的机器上，

```bash
uv run pytest -q tests/interop/test_redis_py_resp2.py
```

必须在 `protocol=2`、`decode_responses=False`、`driver_info=None` 下通过。

??? note "参考答案"

    增加 `await client.rpush(b"jobs", b"a", b"b") == 2`，再断言 `await client.lrange(b"jobs", 0, -1) == [b"a", b"b"]`。只测试行为矩阵已有命令，无生产代码 diff。

## 小结

MiniRedis 以同一语义核心收束全书。Direct 和 RESP2/TCP 共享 parsing、planning、串行执行、持久化与 reply；网络 adapter 只拥有 stream frame、有界 buffer、session 生命周期和 wire mapping。redis-py 互通是所支持 RESP2 子集的宝贵证据，却不是所有 Redis 协议与运维特性的证据。“简化协议、保留语义”就是本系列方法：隔离所学机制，诚实声明省略的生产表面，并让每条论断落到可执行行为。
