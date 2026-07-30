# 第 1 章：认识 MiniRedis

> **语言：** [English](../../tutorial/01-getting-started.md) | 简体中文

## 学习目标

学完本章后，你将能够：

- 说明 MiniRedis 保留了 Redis 的哪些机制，又刻意省略了什么；
- 创建、启动、使用并关闭一个进程内 MiniRedis 运行时；
- 通过 Direct API 发出二进制安全命令并读取领域回复；
- 找到运行时、命令、planner、提交、持久化、复制和协议各层；
- 使用行为矩阵区分“已实现契约”和“生产级声明”。

## 为什么要学习教学内核？

在生产数据库中，Redis 源码已经算非常易读，但“易读”是相对的。它必须同时处理可移植
性、内存分配器行为、事件循环集成、紧凑编码、向后兼容、可观测性、模块、集群，以及
多年积累的运维边界。这些问题都是真实的，却可能遮住初读者最想找的机制：一条命令
究竟怎样变成一次有序状态变更？

MiniRedis 不是 Redis 的 Python 移植版。它是一个小型可执行模型，把这条机制链显式
保留下来。项目 README 称它为 **Direct-first** 的 RESP2/TCP 教学运行时。
“Direct-first”意味着进程内 Python 客户端与网络 adapter 最终提交同一个
`CommandRequest`；网络路径没有另一套 `SET` 或 `GET` 实现。公共组装点是
`src/miniredis/runtime.py`，其中 `MiniRedis.open`、`MiniRedis.start`、
`MiniRedis.direct_client` 和 `MiniRedis.close` 负责生命周期。

这个模型保留了若干生产形态的边界：

1. 字节先进入一个与传输无关的请求；
2. 解析生成封闭的类型化命令；
3. 串行 executor 选定唯一全序；
4. 纯 planner 提出回复与不可变操作；
5. 提交先跨越持久性屏障，再在本地发布；
6. 同一个已提交 batch 进入复制与恢复历史。

这些结构足以学习原子性、过期、淘汰、持久化、复制、事务、阻塞操作和 RESP framing，
无需先掌握 Redis 的每一种优化。代价是明确的简化：值使用 Python 容器；内存是确定性
逻辑估算而非 RSS；复制是进程内 sink 而非网络 PSYNC；AOF 与快照使用自定义格式。
完整边界写在 [`docs/behavior-matrix.md`](../../behavior-matrix.md) 的
“Deliberate difference”列中。

这个区分很重要：一种机制可以忠实到足以教学，而实现仍不适合生产。MiniRedis 展示
持久性屏障如何为已确认写入排序；它不声称拥有 Redis 的吞吐量、崩溃测试历史或运维
兼容性。

## 最小而完整的一次对话

`MiniRedis.open` 构造运行时，但还不会接收命令。`MiniRedis.start` 执行恢复、创建可选
AOF worker、启动 executor、启动主动过期 producer，最后才开放 mailbox 的用户准入。
这些步骤位于 `src/miniredis/runtime.py` 的 `MiniRedis._start_owned`。异步上下文
管理器在进入时调用启动路径、退出时调用协同关闭路径，因此它是最安全的默认写法：

```python
async with MiniRedis.open() as server:
    client = server.direct_client()
    reply = await client.execute(CommandRequest(b"PING"))
    await client.close()
```

请求名与每个参数都是 `bytes`。`src/miniredis/commands/request.py` 中
`CommandRequest` 的定义只有 `name: bytes` 和 `args: tuple[bytes, ...]`。存储
键和值不要求 UTF-8 解码；文本只是在边缘层的一种可能解释。

`src/miniredis/adapters/direct.py` 的 `DirectClient.execute` 刻意保持很薄。
它调用 `DirectClient.submit`，在 `DirectClient.resolve` 中等待 executor 所拥有的
future，然后返回领域回复。回复是 `src/miniredis/core/reply.py` 中冻结的 data
class：`Ok`、`Bytes`、`Number`、`Items`、`NullArray` 和 `Failure`。因此
`Bytes(value=None)` 表示 nil bulk 值，而不是 Python 函数没有返回结果；
`Failure(code="WRONGTYPE", ...)` 是普通命令回复，不是抛出的 Python 异常。

即使在运行时上下文中，显式关闭 client 也有价值。direct client 拥有一个 session
endpoint。`DirectClient.close` 把 `SessionClosed` 投递到同一个 executor mailbox，
随后 `MiniRedis.close` 就能排空一个 client 所有权已经终结的运行时。这对应项目的
生命周期规则：已接收的请求、session、waiter、timer、sink 或 worker 始终有一个
负责使其终结的 owner。

## 运行时组装了什么

打开 `src/miniredis/runtime.py` 的 `MiniRedis.__init__`。构造器会创建：

- `Database`：实时 keyspace；
- `CommandPlanner`：把类型化命令路由到各类型 planner；
- `CommandExecutor`：语义状态变更的串行 owner；
- 可选的 `SnapshotManager`；
- 拥有后台 task、replica sink 与 TCP server 的集合；
- 启动期间安装的 `ActiveExpireProducer`。

运行时是 facade，而不是语义中心。模块 docstring 直接说明：语义执行留在串行
executor 中。这种分离避免 adapter 或生命周期策略悄悄改变命令行为。

`src/miniredis/config.py` 的 `MiniRedisConfig` 默认值让教学边界具体可见：
mailbox 最多接收 1,024 条待处理命令；主动过期每次检查 20 个候选；默认没有逻辑
内存上限；默认淘汰策略是 `noeviction`；只有提供路径才启用 AOF 与快照。这些是有界
运维选择，不是 Redis 默认值声明。

运行时提供 `debug_stats` 用于观察。其 `RuntimeStats` 结果包含 key 数、逻辑内存用量、
过期和淘汰计数、提交序号、复制 cursor 以及 owner 数等语义和生命周期指标。对教学
内核而言，debug surface 尤其重要：实验可以验证机制，而不用随意钻进私有字段。

## 一种实用的源码阅读方法

把仓库当作可执行规格，不要按字母顺序读模块。从一个公共请求开始，并始终追问三个
问题：现在谁拥有输入？什么不可变值跨过下一个边界？什么证据证明转换已经完成？

对于基础写入，从 `src/miniredis/runtime.py` 的 `MiniRedis.submit_request` 开始，
沿 `parse_command_request` 进入类型化模型，再跳到
`src/miniredis/core/executor.py` 的 `CommandExecutor._execute`。命令类型会告诉你
该打开哪个 planner。先读返回的 `ExecutionPlan`，再跟随 `_apply_plan` 与
`_commit_prepared`，最后检查 `Database.apply_batch` 和领域回复。这条路径足够小，
可以完整放进工作记忆，也会成为后续机制的主干。

测试是规格的另一半。行为矩阵每一行都给出聚焦 pytest 节点；先读断言，再只追踪解释
它所需的函数。例如 String 行指向原子命令测试，Lifecycle 行指向关闭验收。这能避免
两个常见错误：只凭类名推断行为，以及把“接口存在”误当作“端到端路径已验证”。

阅读时还要区分运维状态与语义状态。值及其 expiry 是语义状态，会进入 stored batch；
LRU tick、task 集合、pending future 和 outbox 占用是运维所有权元数据。有些运维状态
会因读操作改变却不产生提交，有些会在重启时重置。后续章节会明确指出这些边界。

证据顺序应是公共实验优先于 debug hook：回复说明应用能观察到什么，debug 计数解释
为何如此，只有在研究传播结构时才记录 batch。这样既诚实又可迁移，也避免教程因为
方便而把内部字段误塑造成公共 API。使用 debug surface 时，应明确称其为观测工具，
并与客户端可见结果配对。

## 全书地图

后续章节按依赖关系展开，而不是罗列互不相关的功能：

- 第 2 章追踪命令从 parser 到 reply 的全程，并解释 plan/commit；
- 第 3 章展开五种值模型及其命令 planner；
- 第 4 章学习懒惰过期和有界主动过期；
- 第 5 章加入逻辑 maxmemory、精确 LRU 与确定性 LFU；
- 第 6、7 章跟随已提交 batch 进入 AOF、rewrite、快照与恢复；
- 第 8 章加入复制 ID、offset、backlog 与重同步；
- 第 9 章学习串行 owner 上的事务和阻塞 waiter；
- 第 10 章回到边缘层：RESP2/TCP 与 redis-py 互通。

这个顺序很重要。持久化和复制消费的不是任意字典修改，而是 `CommitBatch`。熟悉该传播
单元后，事务更容易理解。RESP2 放在最后，是因为它只是已通过 Direct API 学会的语义
之上的 adapter。

## 与真实 Redis 对照

真实 Redis 中，对应概念分布在 server 生命周期、命令表、网络层、数据库字典和事件
循环中。Redis 命令常实现在 `t_string.c` 等文件，keyspace 机制位于 `db.c`，事件
循环负责串行化命令执行。MiniRedis 把这些关系变成 Python 模块边界。

架构映射把 `src/miniredis/runtime.py` 标为 Redis server 生命周期的刻意简化，也把
RESP2/TCP adapter 标为有界正确性 adapter 而非吞吐目标。参见
[`docs/architecture.md`](../../architecture.md) 的 “Mapping to real Redis”。
行为矩阵才是兼容性契约：例如 String 行列出支持子集，Lifecycle 行引用零 owner
关闭测试。

不要推断未列出的功能。MiniRedis 没有 RESP3、Lua VM、Streams、ACL、多数据库、
Cluster、Sentinel、TLS 或网络复制。它确实支持可直接测试的重要机制，但“机制存在”、
“线协议兼容”和“生产等价”是三种不同声明。

## 动手实验：第一组命令与可观察状态

在仓库根目录运行：

```bash
uv run python - <<'PY'
import asyncio
from miniredis import CommandRequest, MiniRedis

async def main():
    async with MiniRedis.open() as server:
        client = server.direct_client()
        for request in [
            CommandRequest(b"PING"),
            CommandRequest(b"SET", (b"greeting", b"hello")),
            CommandRequest(b"GET", (b"greeting",)),
        ]:
            print(await client.execute(request))
        stats = server.debug_stats()
        print("state:", server.state.value)
        print("commits:", server.debug_commit_seq)
        print("keys:", stats.key_count)
        await client.close()

asyncio.run(main())
PY
```

实测输出：

```text
Ok(message=b'PONG')
Ok(message=b'OK')
Bytes(value=b'hello')
state: running
commits: 1
keys: 1
```

`PING` 和 `GET` 不改变 keyspace，所以只有 `SET` 创建提交。打印统计时上下文仍处于
活动状态，因此是 `state: running`。一个逻辑 key 同时对应 `keys: 1` 和提交序号 1。

## 练习

### 1. 理解题：请求与回复边界

为什么 `CommandRequest` 保留 bytes 而不立即解码字符串？为什么缺失值用
`Bytes(None)` 而不是 Python `None` 表示？

??? note "参考答案"
    bytes 让 Direct 与 RESP2 adapter 都能保留二进制安全的键和值。`Bytes(None)`
    仍是封闭 `Reply` union 内的值，可编码成 RESP nil bulk，并能与“该操作没有 reply”
    的实际 `None` 区分。

### 2. 动手题：添加生命周期 smoke test

创建 `tests/tutorial/test_getting_started.py`。启动运行时，执行 `PING`、`SET`、`GET`，
显式关闭 client，然后断言 `server.close()` 后报告的 owner 计数全为零。验收命令：

```bash
uv run pytest tests/tutorial/test_getting_started.py -q
```

必须报告 `1 passed`；完整测试套件应比基线多一个通过项。

??? note "参考答案"
    使用 `server = MiniRedis.open()`、`await server.start()`，并在 `try/finally`
    中关闭 client 与 server。关键断言是：

    ```diff
    +assert await client.execute(CommandRequest(b"PING")) == Ok(b"PONG")
    +assert await client.execute(
    +    CommandRequest(b"SET", (b"k", b"v"))
    +) == Ok()
    +assert await client.execute(
    +    CommandRequest(b"GET", (b"k",))
    +) == Bytes(b"v")
    +await client.close()
    +await server.close()
    +stats = server.debug_stats()
    +assert stats.pending_futures == stats.sessions == stats.owned_tasks == 0
    ```

### 3. 理解题：教学忠实度

把下列说法分别归为“已实现机制”“adapter 兼容”或“生产等价”：MiniRedis 通过持久性
屏障为提交排序；redis-py 可以使用 RESP2 adapter；MiniRedis 吞吐量与 Redis 相同。

??? note "参考答案"
    第一项是已实现机制。第二项是在文档所述配置下的有界 adapter 兼容。第三项为假：
    吞吐量等价明确属于非目标。

## 小结

MiniRedis 的价值在于：它让 Redis 形态的所有权和传播边界保持可执行，同时明确写出
简化点。你现在可以启动运行时，通过二进制安全 Direct API 对话，解释领域回复，并用
debug 统计验证状态。第 2 章将打开 facade，跟随一次 `SET` 穿过所有语义阶段，直到其
回复可以安全发布。
