# 第 2 章：一条命令的一生

> **语言：** [English](../../tutorial/02-life-of-a-command.md) | 简体中文

## 学习目标

学完本章后，你将能够：

- 追踪请求经过解析、mailbox 准入、规划、提交、应用、传播和回复发布；
- 区分 `CommandRequest`、类型化 `Command`、`ExecutionPlan`、
  `PreparedCommit` 与 `CommitBatch`；
- 解释为什么 planning 无副作用、commit 必须串行；
- 找到成功写入可以被观察之前的持久性点；
- 在不修改生产代码的情况下记录已应用 batch 来验证整条 pipeline。

## 五种表示，五项职责

命令在 MiniRedis 中移动时会改变形态。每种形态都会消除一种歧义或加入一项保证。

`src/miniredis/commands/request.py` 的 `CommandRequest` 与传输无关：

```python
@dataclass(frozen=True, slots=True)
class CommandRequest:
    name: bytes
    args: tuple[bytes, ...] = ()
```

它并不说明 `SET` 参数数量是否正确，那是 parser 的职责。
`src/miniredis/commands/parser.py` 的 `parse_request` 将命令名转成大写，检查 arity
和选项组合，解析严格整数或 score，并返回
`src/miniredis/commands/model.py` 中的一种 data class。例如
`CommandRequest(b"SET", (b"k", b"v"))` 会变为
`SetString(key=b"k", value=b"v", only_if=None, expire_ms=None)`。

类型化命令在并发开始前冻结了解释。planner 不必反复检查原始参数，也不用猜 `EX 2`
是否已经换算成毫秒。`Command` type alias 是封闭 union；如果新增命令尚未分类，
`is_dataset_mutating` 会 assert。

第三种形态是在 `src/miniredis/core/executor.py` 定义的 `ExecutionPlan`，包含：

- 最终应返回的回复；
- 不可变 `PutEntry` 或 `DeleteKey` 操作；
- 应触碰访问元数据的 key；
- client 或 active expiration 等 `CommitTrigger`；
- 可选的阻塞 waiter 唤醒。

execution plan 仍只是提案：没有序号，也没有跨过持久性边界。如果包含操作，它的
`prepared_commit` property 会创建 `PreparedCommit`。`_commit_prepared` 分配下一个
序号并产生最终 `CommitBatch`。第五种表示就是 AOF、`Database.apply_batch`、复制
backlog、replica sink 和恢复共同消费的原子单元。

## 准入前解析，准入后排序

Direct 路径从 `src/miniredis/adapters/direct.py` 的 `DirectClient.execute` 开始。
`DirectClient.submit` 让 runtime 提交请求。`src/miniredis/runtime.py` 的
`MiniRedis.submit_request` 调用 `MiniRedis.parse`；解析失败会成为 `Failure` 回复，
并通过 `CommandExecutor.submit_rejection` 进入 mailbox。

为什么拒绝请求也要进入 mailbox？因为顺序仍然重要。client pipeline 若依次提交有效、
无效、有效 frame，结果 slot 必须保持同一顺序。`MULTI` 中的解析失败还必须在正确的
mailbox turn 把事务标脏。`CommandExecutor._dispatch` 在同一串行 loop 处理
`RejectRequest`，先标记活动事务，再完成回复。

对有效类型化命令，`CommandExecutor._admit_request` 创建唯一 `RequestToken` 与
executor 所拥有的 future，记录请求，把 token 注册到 session endpoint，然后调用
`EventLoopMailbox.admit_user`。有界检查发生在接受之前：停止的 runtime 返回
`CLOSED`，请求集合满时返回 `BUSY`。一旦接受，executor 就拥有该 token 的终结结果。

`CommandExecutor._run` 反复调用 `mailbox.take` 并等待 `_dispatch`。这里只有一个
worker，因此一次请求的 planning、durability、apply 和 reply publication 共享唯一
全序。异步 I/O 可以在持久性屏障处挂起 worker，但第二条命令不能抢先修改数据库，
令第一份 plan 过时。

## 纯 planning 与按类型路由

在 `CommandExecutor._execute` 中，普通命令捕获 `now_ms` 并调用
`CommandPlanner.plan`。`src/miniredis/core/planner.py` 的路由器依次尝试通用/
string、hash、list、set、sorted-set 与 TTL planner，再对结果执行 `enforce_memory`。

以 `SET counter 41` 为例。`src/miniredis/core/planning.py` 的
`plan_general_and_strings` 调用 `lookup`，按需计算绝对过期时间，再调用 `make_put`。
`make_put` 把实时 Python value 冻结为 `StoredEntry` 并增加 mutation version，但不会
修改 `Database.entries`。

plan/commit 分离带来多项好处：

- 类型错误返回 `WRONGTYPE`，不会留下部分修改；
- 整数溢出可在检查后失败，无需回滚；
- 内存强制可以加入淘汰操作，或把 plan 换成 OOM；
- 懒惰过期 delete 可先提出，再在 replica 上抑制；
- 事务可把多个 planned operation 合成一个最终 batch；
- 测试可在传播前后检查决策。

这种分离并非免费。`src/miniredis/core/database.py` 的 `Database.apply_batch` 会
浅拷贝整个 key table 并重新计算逻辑用量，因此每次提交对 key 数量是 O(N)。架构
指南明确把它称为教学脚手架；真实 Redis 通常就地修改字典和 accounting。

## 提交顺序：先持久，再可见

`CommandExecutor._apply_plan` 是从提案到发布的交接点。它先附加可能的 list-push
wakeup。如果存在 prepared commit 且 runtime 不是只读 replica，就等待
`_commit_prepared`。

`CommandExecutor._commit_prepared` 内部的关键顺序是：

1. 分配 `database.commit_seq + 1`；
2. 等待 `commit_barrier.append(batch)`；
3. 拒绝失败或错误序号的 acknowledgement；
4. 调用 `Database.apply_batch`；
5. 更新过期和淘汰计数；
6. 把 batch 加入 replication backlog；
7. 把它交给已连接 replica sink。

只有 `_commit_prepared` 返回后，`_apply_plan` 才 materialize 读 touch、完成 waiter
wakeup，并为请求 token 调用 `_finish_reply`。因此变更型命令的成功回复不可能在所选
commit barrier 接受精确序号之前发布。

未配置 AOF path 时，runtime 使用 `NullCommitBarrier.append`，它返回
`AofAppendOk(batch.seq)`。这证明顺序，却不代表磁盘持久性。配置 AOF 后，第 6、7 章
会展示不同 policy 如何改变屏障强度。接口不变，所以命令语义不依赖传输层或持久化
格式。

如果 append 返回 `AofAppendFailed`，`_commit_prepared` 会抛出
`DurabilityFailure`。`_apply_plan` 用错误完成请求并让 runtime 转向 failure；它绝不
调用 `Database.apply_batch`。这就是“acknowledged 表示已跨过所选屏障”的核心契约。

## 应用一个不可变 batch

`CommitBatch` 及其操作位于 `src/miniredis/core/commit.py`。这些 data class 都是
frozen 的。batch 要求正序号和至少一个 operation。`PutEntry` 带有完整冻结 value、
绝对 expiry 和 mutation version；`DeleteKey` 带有 client、expired 或 evicted
reason。

`Database.apply_batch` 先检查序号恰好是下一个值，再把操作应用到 staged state。
client 触发的 `PutEntry` 还会 materialize access tick 与 LFU frequency 更新；复制
或 active-expiry apply 可以关闭 client access tracking。它重算逻辑用量，验证 entry
size 为正，最后才把 staged table 换成实时 database 并推进 `commit_seq`。

序号不只是测试计数器，它连接本地状态、AOF record、snapshot、replication backlog
entry 与 replica cursor。gap 或 duplicate 会被拒绝，不会悄悄形成另一段历史。

读操作通常没有 operation。实时 `GET` 返回 reply 和 touch key，所以 `_apply_plan`
更新 LRU/LFU 元数据却不产生 `CommitBatch`。发现过期 key 的读不同：`lookup` 返回
逻辑缺失和 expiry `DeleteKey`，所以读也可能产生提交，第 4 章会展开。

因此，“算出 reply”和“发布 reply”是不同事件。planner 可以立即知道答案，但只有
executor 知道全部所有权和提交步骤是否终结。正是这层分离，让同一个 planner 能服务
纯内存、AOF-backed 和 replicated 运行，而不削弱 acknowledgement 语义。

## 回复发布与所有权

`src/miniredis/core/executor.py` 的 `_finish_reply` 把 plan reply 映射回已接受 token。
Direct client resolve future；TCP session 使用 endpoint 的有序 outbox，防止后完成
的请求越过较早回复。两条路径都结束于 `_finish_request`：移除 executor 所有权，并
且只 resolve future 一次。

取消不会直接取消 executor 所拥有的 future。`DirectClient.resolve` 会 shield 它，
调用方被取消时只投递 `AbandonRequest`。这是细微但重要的 ownership 规则：client
失去耐心与 server 完成是两个事件。串行 owner 决定请求是否已经提交、正在等待，或
可以安全放弃。

## 与真实 Redis 对照

真实 Redis 也通过主事件循环串行执行普通命令，并把原子命令或事务效果传播到 AOF 和
replica。命令实现常位于 `t_string.c`、`t_hash.c`、`db.c` 等文件，传播和网络有
各自的生产机制。

MiniRedis planner 在概念上对应命令实现，但返回 `ExecutionPlan` 属于刻意简化。
Redis 通常在命令实现内部直接修改实时状态，而非先构造冻结 operation list。尽管如此，
MiniRedis 的 `CommitBatch` 保留了关键可观察课程：durability、local apply、
replication 与 recovery 看到同一个有序传播单元。

参见 [`docs/architecture.md`](../../architecture.md) 中 “per-type planners +
`CommandPlanner`”“executor mailbox”“`ExecutionPlan` / `PreparedCommit`”和
“`CommitBatch`”各行。[`docs/behavior-matrix.md`](../../behavior-matrix.md) 的
String 行指向契约与 RESP 测试，而不是声称支持完整 Redis 命令表。

## 动手实验：检查一次已提交 SET

在仓库根目录运行：

```bash
uv run python - <<'PY'
import asyncio
from miniredis import CommandRequest, MiniRedis

async def main():
    async with MiniRedis.open(debug_record_applied_batches=True) as server:
        client = server.direct_client()
        request = CommandRequest(b"SET", (b"counter", b"41"))
        print("parsed:", server.parse(request))
        print("reply:", await client.execute(request))
        print("read:", await client.execute(
            CommandRequest(b"GET", (b"counter",))
        ))
        print("batches:", server.debug_applied_batches())
        await client.close()

asyncio.run(main())
PY
```

实测输出：

```text
parsed: SetString(key=b'counter', value=b'41', only_if=None, expire_ms=None)
reply: Ok(message=b'OK')
read: Bytes(value=b'41')
batches: (CommitBatch(seq=1, operations=(PutEntry(key=b'counter', entry=StoredEntry(value=StoredString(data=b'41'), expire_at_ms=None, mutation_version=1)),), trigger=<CommitTrigger.CLIENT: 'client'>),)
```

原始请求在提交前已经类型化。写入在序号 1 中恰好产生一个 operation。后续 `GET`
返回值但不增加 batch，因此记录 tuple 仍只有一个元素。

## 练习

### 1. 理解题：找出安全回复点

为什么在 `commit_barrier.append` 前就返回 planner 的成功回复是不正确的？

??? note "参考答案"
    plan 只是提案；append 可能失败或确认错误序号。提前成功会让 client 观察到一条
    从未应用或持久接受的 acknowledged write。MiniRedis 只在 barrier 接受且
    `Database.apply_batch` 完成后发布回复。

### 2. 理解题：解析失败与顺序

parser rejection 为什么要进入 executor mailbox，而不是直接从 adapter 返回？

??? note "参考答案"
    rejection 仍占有一个有序结果 slot，也可能影响事务状态。串行化 `RejectRequest`
    能保留 pipeline 顺序，并让无效命令标脏正确的活动事务。

### 3. 动手题：测试“读不提交”

添加 `tests/tutorial/test_command_lifecycle.py`。使用
`debug_record_applied_batches=True`，执行一次 `SET` 和三次 `GET`，断言 batch 数与
序号都保持 1。验收：

```bash
uv run pytest tests/tutorial/test_command_lifecycle.py -q
```

必须报告 `1 passed`，且测试不能检查 name-mangled 私有状态。

??? note "参考答案"
    测试核心是：

    ```diff
    +async with MiniRedis.open(debug_record_applied_batches=True) as server:
    +    client = server.direct_client()
    +    await client.execute(CommandRequest(b"SET", (b"k", b"v")))
    +    for _ in range(3):
    +        assert await client.execute(
    +            CommandRequest(b"GET", (b"k",))
    +        ) == Bytes(b"v")
    +    assert server.debug_commit_seq == 1
    +    assert len(server.debug_applied_batches()) == 1
    +    await client.close()
    ```

## 小结

MiniRedis 命令会逐步变得更严格：与传输无关的 request、验证过的 typed command、
无副作用 execution plan、prepared operations，以及带序号 commit batch。单一
executor 为整个转换排序；成功写入先跨过所选持久性屏障，再应用数据库并发布回复。
第 3 章将打开为 Redis 五种核心 value type 构造 plan 的各个 planner。
