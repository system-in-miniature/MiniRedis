> **语言**: [English](../../tutorial/09-transactions-blocking.md) | 简体中文

# 事务与阻塞命令

## 学习目标

学完本章后，你将能够：

- 区分 `MULTI`/`EXEC` 中的命令排队、入队期失败和运行期失败；
- 解释 per-key revision 如何让 `WATCH` 发现“创建后删除”的竞争；
- 追踪为什么成功 `EXEC` 至多产生一个 `CommitBatch`；
- 把 `BLPOP` 建模为注册 waiter，而不是被卡住的 executor；
- 测试事务与 waiter 在 session、timeout 边界的清理。

## 两个协调问题

事务延迟到 `EXEC` 才**执行**，阻塞 pop 延迟到数据到来才**回复**。两者在单一串行状态 owner 中都很棘手：等待不能冻结 owner，组合工作不能泄漏局部 commit。

MiniRedis 显式维护状态机：

- `src/miniredis/core/transactions.py` 的 `TransactionState` 保存一个 session 的 queued command、dirty 标志与 watched revision；
- `src/miniredis/core/blocking.py` 的 `WaiterRegistry` 按 waiter ID、request token、session 和 key 索引挂起请求；
- `src/miniredis/core/executor.py` 的 `CommandExecutor` 是唯一修改这些结构和数据库的 owner。

session ID 是共同边界：`MULTI` 状态属于一个客户端，阻塞请求会在 session 关闭时取消。

## MULTI 排队，EXEC 求值

`src/miniredis/core/executor.py` 的 `CommandExecutor._route_transaction_command` 在正常 planning 前处理事务控制。

事务外：

- `MULTI` 创建 `TransactionState`；
- `WATCH` 记录各 key 当前 revision；
- `UNWATCH` 清空记录；
- `EXEC`、`DISCARD` 因不存在事务而报错。

事务内：

- 普通允许命令加入 `state.queued` 并返回 `Ok(b"QUEUED")`；
- `DISCARD` 删除队列和 watch 状态；
- 再次 `MULTI` 报错；
- parse 或入队期错误标记 `state.dirty`；
- blocking pop 和 `PUBLISH` 被拒绝并标 dirty；
- `WATCH` 被拒绝，且**会标 dirty**。

最后一点与 Redis 不同。Redis 也拒绝 `MULTI` 内 `WATCH`，但该错误不会让后续 `EXEC` 返回 `EXECABORT`；MiniRedis 会。这个语义相反的边界明确记录在[行为矩阵 Transactions 行](../behavior-matrix.md)。

`EXEC` 进入 `CommandExecutor._execute_transaction` 后有三种结果：

1. **Dirty**：返回 `EXECABORT`，不求值 queued command；
2. **watched revision 改变**：返回 `NullArray`，这是乐观并发 abort，不是错误响应；
3. **有效事务**：在 fork 数据库上求值，返回与命令一一对应的结果数组。

运行期错误保留自己的数组 slot。例如 `SET k 1`、`LPUSH k x`、`INCR k` 产生 `OK`、`WRONGTYPE`、`2`。失败的 list 操作不回滚成功的 string 操作。这保留了 Redis “入队期错误阻止执行、运行期错误占 slot” 的区别，但实现路径不同。

## WATCH 使用 revision，而不是值

仅比较值无法检测：

```text
client A: WATCH k（此时不存在）
client B: SET k v
client B: DEL k
client A: EXEC
```

watch 前后值都为空，但 key 改变过两次。MiniRedis 从 database 记录 per-key revision，每次相关 commit（包括删除）都推进 revision。`CommandExecutor._execute_transaction` 中 `_watched_keys_changed` 比较记录值和当前值，因此 create-delete 竞争会返回 `NullArray`。

watch 状态可在 `MULTI` 前建立并带入事务。成功/失败 `EXEC`、`DISCARD`、`UNWATCH` 或 session close 都会清理它。`src/miniredis/core/transactions.py` 的 `TransactionState.clear_all` 同时重置事务与 watch 状态，避免内存泄漏和下一个事务误用旧 revision。

## 一个 EXEC 成为一个传播单元

MiniRedis 在深拷贝的 database fork 上求值事务。`CommandExecutor._execute_transaction` 让每个 queued command 针对 workspace planning，把成功 operation 应用到 workspace，同时累计 operation 与 reply；不会逐命令提交 live database。

求值后，它至多创建一个 `PreparedCommit` 并只调用一次 `_commit_prepared`。trigger 仍是 `CommitTrigger.CLIENT`；事务分组由“一个 batch 含全部累计 operation”表达，而不是单独的 transaction trigger。因此 live database、AOF、replication backlog 和 replica sink 都只看到一个带序列号的 `CommitBatch`，中间状态不会被其他 mailbox turn 观察。

代价是刻意的低效率。`src/miniredis/core/database.py` 的 `Database.fork` 深拷贝 value，`Database.apply_batch` 暂存 key table 并重算逻辑内存。真实 Redis 在 event loop 串行客户端的同时对 live 数据结构执行 queued command；MiniRedis 支付 O(N + transaction work)，换取易检查的 planning 与最终 batch。[架构指南](../architecture.md)明确说明这是教学成本。

空 `EXEC` 返回 `Items(())` 且不 commit；只有读的事务同样无需 mutation batch。所以“一批”是**至多**一批。

## BLPOP 不会阻塞 executor

若 executor 内直接 `await` 等待 list 改变，就会死锁：它无法处理负责唤醒该 pop 的 `RPUSH`。

MiniRedis 先针对当前状态 planning。若任一请求 key 有 list item，就按 key 优先级和左右方向立即 pop；若都空，`CommandExecutor._apply_plan` 注册 `BlockingWaiter`，而不是完成请求。

`src/miniredis/core/blocking.py` 的 `WaiterRegistry.register` 保存：

- 唯一 `WaiterId`；
- request token 与 session ID；
- 有序 key tuple；
- 左/右方向；
- 可选 timer handle；
- 明确的 `WAITING`、`WOKEN`、`TIMED_OUT` 或 `CANCELLED` 状态。

同一个 waiter 会索引到每个请求 key 下，但仍是一个逻辑请求。`WaiterRegistry.transition` 是唯一状态转换点：返回 waiter 前移除所有索引并取消 timer。因此 timeout、push wakeup、cancel、session close 争夺同一 ID，只有第一次转换成功。

`src/miniredis/core/blocking.py` 的 `prepare_list_wakeups` 在规划 push commit 时运行。它按确定顺序检查 waiter，为每个 waiter 最多预留一个元素，返回附着在同一 execution plan 的 wakeup。`CommandExecutor._attach_push_wakeups` 把 pop operation 和 waiter reply 绑定到触发 commit。

如果在 list mutation commit 前就回复 waiter，持久化失败可能导致“客户端收到元素但元素仍在 list”。MiniRedis 只在成功 commit 路径发布 wakeup，避免这一竞争。

## Timeout、取消与方向

parser 把十进制 timeout 固化为毫秒；`BLPOP key 0` 表示永不超时。正数 timeout 安排控制事件，`TimeoutWaiter` 到达 executor 后，`_timeout_waiter` 转换 waiter 并以 null 结果完成请求。

Direct 适配器中，`src/miniredis/adapters/direct.py` 的 `DirectClient.resolve` 把 `BlockingPop` 的 transport close 映射为 `Bytes(None)`；RESP 在适用处映射为 null array。domain 与 wire 表示属于 adapter，waiter 状态机仍在 core。

方向也被冻结：`BRPOP` 被后续 push 唤醒时仍从右侧 pop，不会退化为 `BLPOP`；第一个 ready key 仍按命令顺序获胜。`tests/mechanisms/test_blpop.py` 验证这些契约。

## 与真实 Redis 对照

Redis 事务主要位于 `src/multi.c`：`MULTI` 排队、`EXEC` 串行执行、`DISCARD` 清队列、`WATCH` 提供乐观锁，运行期错误不回滚。MiniRedis 保留这些可观察要点，但在深 fork 上求值并把成功变更转换成一个显式逻辑 batch。

Redis 阻塞 list 命令分布在 blocked-client 与 list 代码中，例如不同版本的 `src/blocked.c`、`src/t_list.c`。它把 blocked client 从普通命令处理分离，key ready 时唤醒。MiniRedis waiter registry 是较小的教学对应物。

[行为矩阵的 Transactions 与 List 行](../behavior-matrix.md)列出差异：MiniRedis 禁止事务内 blocking command 与 `PUBLISH`；事务内 `WATCH` 的 dirty 行为不同；深拷贝策略也不是生产性能模型。

## 动手实验：乐观 abort 与 waiter 唤醒

保存为 `/tmp/miniredis_coordination.py`：

```python
import asyncio

from miniredis import CommandRequest, MiniRedis


async def main():
    async with MiniRedis.open() as runtime:
        owner = runtime.direct_client()
        rival = runtime.direct_client()
        producer = runtime.direct_client()
        print("WATCH:", await owner.execute(
            CommandRequest(b"WATCH", (b"counter",))
        ))
        await rival.execute(CommandRequest(b"SET", (b"counter", b"9")))
        print("MULTI:", await owner.execute(CommandRequest(b"MULTI")))
        print("queued GET:", await owner.execute(
            CommandRequest(b"GET", (b"counter",))
        ))
        print("EXEC after rival write:",
              await owner.execute(CommandRequest(b"EXEC")))
        blocked = asyncio.create_task(owner.execute(
            CommandRequest(b"BLPOP", (b"jobs", b"1"))
        ))
        await runtime.debug_wait_for_waiters(1)
        print("waiters while empty:", runtime.debug_stats().waiters)
        print("RPUSH:", await producer.execute(
            CommandRequest(b"RPUSH", (b"jobs", b"compile"))
        ))
        print("BLPOP result:", await blocked)


asyncio.run(main())
```

运行：

```bash
uv run python /tmp/miniredis_coordination.py
```

实测输出：

```text
WATCH: Ok(message=b'OK')
MULTI: Ok(message=b'OK')
queued GET: Ok(message=b'QUEUED')
EXEC after rival write: NullArray()
waiters while empty: 1
RPUSH: Number(value=1)
BLPOP result: Items(values=(Bytes(value=b'jobs'), Bytes(value=b'compile')))
```

rival 在 `WATCH` 后改变 `counter`，所以 `EXEC` 在求值前 abort。`BLPOP` 注册一个 waiter 却不阻塞 executor，另一个 client 得以提交 `RPUSH`；push commit 消费并交付 `compile`。

聚焦测试：

```bash
uv run pytest -q tests/mechanisms/test_transactions.py \
  tests/mechanisms/test_watch.py \
  tests/mechanisms/test_blpop.py
```

预期仓库结果：

```text
23 passed
```

## 练习

### 1. 理解题：错误分类

在 `MULTI` 内比较参数数量错误的 `GET` 和在 `EXEC` 时遇到 string key 的 `LPUSH`：谁终止事务，谁占结果 slot？

??? note "参考答案"

    参数错误在入队前发现，标 dirty，`EXEC` 返回 `EXECABORT`。类型错误在 queued `LPUSH` 求值时发现，只占自己的 slot；其他命令仍可成功并进入最终 batch。

### 2. 理解题：解释 create-delete watch

为何只 watch 当前值无法发现 `SET k v; DEL k`？

??? note "参考答案"

    前后都为空，值相等会丢失历史；per-key revision 对两次 commit 都推进，因此记录 revision 与当前 revision 不同。

### 3. 动手题：验证单一 transaction batch

任务边界：只在 `tests/reliability/` 增加测试，使用 `debug_record_applied_batches=True`，排队两个 mutation 和一个 read 后执行；不改 `src/`。

验收：commit seq 只加一；恰有一个新 batch 包含两个写效果，trigger 为 `CommitTrigger.CLIENT`；reply 仍有三个 slot。

??? note "参考答案"

    仿照 `tests/reliability/test_transaction_commit.py`。记录 `MULTI` 前 batch 数，选择不冲突的命令，执行后只检查新增 batch 的 trigger、效果、序列增量和 reply slot，不绑定 planner 私有细节。

### 4. 动手题：让 timeout 与 push 竞争

任务边界：使用仓库 fake scheduler 写确定性测试，在同一 executor 边界附近安排 timeout 控制事件与 push；不得使用真实 sleep。

验收：只产生一个终止 reply，waiter index 与 timer 归零；timeout 胜出时 push 值仍可读，wakeup 胜出时只消费一次。

??? note "参考答案"

    复用 `tests/concurrency/test_blpop_races.py` 的 fake clock/scheduler 和断言，为两种控制消息顺序各写一例；每例最终断言 `debug_waiter_index_counts == (0, 0, 0)`。registry 的单向 `transition` 应阻止双重完成。

## 小结

MiniRedis 事务延迟求值，以 per-key revision 检测乐观冲突，并把成功 mutation 折叠成至多一个持久且可复制的 commit batch。阻塞 pop 把请求挂进索引状态机，同时释放 executor 去处理能唤醒它的 push。两种机制都依赖单一串行 owner 与显式清理。最后一章将跨过 adapter 边界，展示同一 domain 语义如何由 RESP2/TCP 携带而不被重新实现。
