# 第 4 章：过期

> **语言：** [English](../../tutorial/04-expiration.md) | 简体中文

## 学习目标

学完本章后，你将能够：

- 解释物理存在与逻辑过期的区别；
- 追踪懒惰过期如何从读路径 lookup 变成 expiry commit；
- 描述有界、基于 cursor 的主动过期；
- 解释副本为什么隐藏过期 key，却不在本地删除；
- 使用仓库注入时钟确定性验证过期。

## 时间以绝对 deadline 存储

可过期 entry 在 `src/miniredis/core/database.py` 的 `Entry` 中保存
`expire_at_ms: int | None`。`None` 表示持久；数字表示运行时注入 `Clock` 下的绝对
毫秒 deadline。`src/miniredis/core/expiration.py` 的 `is_expired` 很小：

```python
def is_expired(entry: Entry, now_ms: int) -> bool:
    return (
        entry.expire_at_ms is not None
        and entry.expire_at_ms <= now_ms
    )
```

绝对 deadline 无需随时间推进不断递减每个 TTL。`SET ... EX/PX` 先由
`src/miniredis/commands/parser.py` 的 `_parse_set` 解析成相对毫秒 duration，再由
`plan_general_and_strings` 加上 planner 的 `now_ms`。`EXPIRE` 同样在
`src/miniredis/core/ttl_planner.py` 的 `plan_ttl` 中存入
`now_ms + seconds * 1_000`。

`TTL` 与 `PTTL` 从绝对值计算：absent 或逻辑过期返回 `-2`，persistent 返回 `-1`。
秒数采用 `(remaining_ms + 500) // 1_000` 的教学型 round。不要假设每个 Redis 边界
都被复制，应以 [`docs/behavior-matrix.md`](../../behavior-matrix.md) 的 TTL 行为准。

普通 `SET` 替换会清除旧 TTL，除非新命令提供 `EX`/`PX`。`INCR`、`HINCRBY`、
list pop/push、set 与 sorted-set update 等原地修改会把旧绝对 deadline 传给
`make_put`，所以期限保留。`PERSIST` 用无 deadline entry 重写；`EXPIRE key 0`
规划立即 client deletion。

## 逻辑缺失可以早于物理删除

数据库无法为每个 key 负担一个 timer。更重要的是，读操作绝不能只因为后台清理尚未
访问一个 key 就返回陈旧数据。因此 MiniRedis 分离逻辑可见性与物理存储。

`src/miniredis/core/planning.py` 的 `lookup` 读取 `database.entries`。若 entry 在
`now_ms` 已过期，它返回：

```python
return None, (expiry_delete(key),)
```

planner 看到 `None`，构造与 missing key 相同的 reply；额外 operation 是
`DeleteReason.EXPIRED` 的 `DeleteKey`，由
`src/miniredis/core/expiration.py` 的 `expiry_delete` 创建。

对 `GET`，结果是 `ExecutionPlan(Bytes(None), expired)`。reply 已经算出，但在 primary
上，executor 先提交 expiry delete。`CommandExecutor._apply_plan` 调用
`_commit_prepared`，让删除穿过 AOF barrier、被 apply、增加 `expired_key_count`、
进入 replication backlog 并交给 replica，最后才发布 nil reply。

这就是**懒惰过期**：发现 stale 的访问负责清理。它仍是真正的 propagated state
change，不是未追踪的 dictionary pop；commit barrier 失败时，MiniRedis 不会假装
清理成功。

命令之间有细微差别。`MGET` 会把过期或 wrong-type entry 当成 nil，却刻意丢弃
expiry operation，因此不创建 commit。multi-key planner 可以累积 expiry operation，
但若后续 key 导致 `WRONGTYPE`，error plan 会丢弃它们。
`tests/contract/test_ttl.py::test_error_discards_pending_lazy_expiry_delete`
验证失败命令不能偷偷提交部分清理。

## 主动过期是有界工作

仅有懒惰过期会让永不再读的 key 持续占据物理与逻辑内存。MiniRedis 因此加入周期性
producer 和串行 consumer。

`src/miniredis/core/expiration.py` 的 `ActiveExpireProducer` 负责 scheduling。
`start` 安排下一 deadline；`_fire` 投递 `ActiveExpireTick` control message 并安排
下一 tick，但绝不修改 database。这样 timer callback 保持轻量，executor 仍是唯一
state-changing owner。

`src/miniredis/runtime.py` 的 `MiniRedis._start_owned` 在 executor 启动后创建
producer，使用 `MiniRedisConfig.active_expire_interval_ms`，默认 100 ms。shutdown
调用 `ActiveExpireProducer.quiesce`，取消 scheduled handle，再拆除 executor。

executor 在 `CommandExecutor._dispatch` 收到 tick 并调用
`CommandExecutor._active_expire_once`。该函数：

1. 只排序带 deadline 的 key；
2. 从记住的 binary-key cursor 之后开始，到末尾时 wrap；
3. 最多取 `active_expire_sample_size` 个候选；
4. 只为 tick 时刻已过期的候选生成 expiry delete；
5. 用 `ACTIVE_EXPIRE` trigger 在一个 `CommitBatch` 中提交全部选中 delete。

默认 sample size 是 20。即使有数百万 TTL key，一次 pass 也受配置约束。cursor 提供
确定性 round-robin coverage，而不是随机采样。一次 pass 可能检查尚未过期 key 并删除
零个；后续 pass 从 cursor 继续。

这是 sampled active cleanup 的教学类比，并非 Redis 自适应时间预算算法的复刻。排序
全体相关 key 也比生产 hot cycle 能接受的成本更高。保留下来的机制是：有界后台工作
加上强制懒惰可见性。

cursor 是 progress hint，不是 durable state。重启后可再次从首个排序 key 开始，这会
改变清理时机，却不改变正确性，因为每次访问仍检查绝对 deadline。这里再次分离了语义
数据（deadline）与运维 metadata（maintenance scan position）。

## 为什么不用 per-key timer

为每个过期 key 创建异步 timer 看似简单，却带来糟糕的 ownership 与 scaling。数百万
timer 会消耗内存、scheduler work 与 cancellation bookkeeping。key 还可能在 timer
触发前被覆盖、persist 或 delete，所以每个 callback 都需要 generation check。

MiniRedis 每个过期 entry 只存一个整数，并只拥有一个周期 producer。读路径保证
correctness，producer 只负责回收物理空间。这是常见 backend 模式：廉价同步 validity
check 保护语义，有界 maintenance 控制资源滞留。

注入时间让设计可测。生产使用 `src/miniredis/clock.py` 的 `SystemClock` 与
`AsyncioTimerScheduler`；测试传入 `FakeClock` 并直接调用
`MiniRedis.debug_active_expire_once`，无需 sleep。两边运行相同 executor logic，只替换
时间与 scheduling 来源。

## 复制安全的过期

过期也是 replicated write concern。若 primary 与 replica 都用本地 clock 创建独立
deletion commit，时钟偏差或 scheduling 顺序可能让同一个 sequence 对应不同
operation，造成历史分叉。

MiniRedis 遵循 Redis 的 ownership rule：primary 创建并传播 expiration delete。只读
replica 仍把 elapsed entry 视为逻辑 absent，但不能推进自己的 sequence。

两个 executor branch 保证这一点：

- `_replica_read_only` 为 true 时，`CommandExecutor._active_expire_once` 立即返回零；
- `_replica_read_only` 为 true 时，`CommandExecutor._apply_plan` 跳过 prepared commit。
  planner 的 expired lookup 仍提供 nil reply，但不在本地删除。

物理 entry 保留到 primary 的 `DeleteReason.EXPIRED` batch 通过 replica sink 到达。
[`docs/behavior-matrix.md`](../../behavior-matrix.md) 的 Replica 行把
`tests/replication/test_sink_attach.py::test_replica_logically_hides_expired_key_without_advancing_sequence`
列为证据。

这是一项重要分离：逻辑时间决定 client 能看见什么，复制权威决定谁能发布历史。调试
时也应遵循它：若 replica 返回 nil 但物理 key 数未下降，先比较 clock 与 replicated
commit sequence，不要立即判断 active cleanup 损坏。只有 primary-originated batch
到达后才预期物理删除。

## 过期与内存强制

第 5 章的 `enforce_memory` 在牺牲 live victim 前也会扫描 expired entry。
`src/miniredis/core/eviction.py` 的 `_expired_operations` 构造排序过期 delete。触发
write、过期空间回收和任何 eviction 被 deduplicate 进同一 plan，最终进入同一 batch。

这意味着 stale 物理 entry 不会强迫本可避免的 OOM 或 live eviction，也意味着清理
始终有序且被传播；不存在脱离 committed keyspace 修改 memory accounting 的旁路。

recovery 与 snapshot 采用同一逻辑规则。`Database.export_stored_entries` 排除 deadline
小于等于 `now_ms` 的 entry，`Database.discard_expired_for_recovery` 删除 elapsed entry
并重置 operational access metadata。后续持久化章节会解释调用时机。

## 与真实 Redis 对照

真实 Redis 把 key access 时的 passive expiration 与 active expiration cycle 结合。
生产实现历史上与 `expire.c`、`server.c` 中的过期逻辑及 `activeExpireCycle` 相关。
replica 会逻辑隐藏过期数据，但等待 primary 传播删除。

MiniRedis 保留这两个语义思想，简化为 injected clock、固定 interval、配置 sample
count 和确定性 cursor traversal；它不复制 Redis 自适应 CPU budget、采样 heuristic
或全部 expiry 命令选项。

架构指南把 “`ActiveExpireProducer` + expiry planning” 标为 intentional
simplification，把 “replica expiry suppression” 在可观察契约层标为 equivalent。
行为矩阵的 TTL 与 Replica 行给出支持命令和测试。

## 动手实验：懒惰与有界主动清理

本实验复用 test clock。运行：

```bash
uv run python - <<'PY'
import asyncio
from miniredis import CommandRequest, MiniRedis
from tests.helpers.time import FakeClock

async def main():
    clock = FakeClock(1_000)
    async with MiniRedis.open(
        clock=clock,
        active_expire_sample_size=1,
        debug_record_applied_batches=True,
    ) as server:
        c = server.direct_client()
        print(await c.execute(
            CommandRequest(b"SET", (b"lazy", b"v", b"PX", b"100"))
        ))
        clock.advance(100)
        before = server.debug_commit_seq
        print("GET lazy:", await c.execute(
            CommandRequest(b"GET", (b"lazy",))
        ))
        print("lazy commit delta:", server.debug_commit_seq - before)
        for key in (b"a", b"b"):
            await c.execute(CommandRequest(b"SET", (key, b"v")))
            await c.execute(CommandRequest(b"EXPIRE", (key, b"1")))
        clock.advance(1_000)
        print("active pass 1:", await server.debug_active_expire_once())
        print("physical keys:", server.debug_physical_key_count)
        print("active pass 2:", await server.debug_active_expire_once())
        print("physical keys:", server.debug_physical_key_count)
        await c.close()

asyncio.run(main())
PY
```

实测输出：

```text
Ok(message=b'OK')
GET lazy: Bytes(value=None)
lazy commit delta: 1
active pass 1: 1
physical keys: 1
active pass 2: 1
physical keys: 0
```

`GET` 同时返回逻辑缺失并创建一个 cleanup commit。sample size 为 1，所以每次 active
pass 最多删除后面两个 key 中的一个。

## 练习

### 1. 理解题：物理状态与逻辑状态

key 能否仍在 `Database.entries` 中，但 `GET` 返回 nil？为什么这不是 correctness bug？

??? note "参考答案"
    可以。`expire_at_ms <= now_ms` 后，`lookup` 把 entry 当成 absent。物理删除可以
    等 lazy commit、active cleanup 或 primary propagation；client visibility 已正确。

### 2. 理解题：replica authority

replica 为什么同时抑制 active-expiry commit 和 lazy-expiry commit？

??? note "参考答案"
    本地 clock 与 scheduling 会让 replica sequence history 出现不同 delete。replica
    可以隐藏过期数据，但只有 primary 发布推进 replicated history 的删除 batch。

### 3. 动手题：添加有界清理回归测试

在 `tests/contract/test_ttl.py` 增加测试：五个 expired key，
`active_expire_sample_size=2`，断言三次手动 pass 依次删除 2、2、1 个，并且所有记录
的 cleanup batch 都是 `CommitTrigger.ACTIVE_EXPIRE`。验收：

```bash
uv run pytest tests/contract/test_ttl.py -q
```

该文件应比基线多一个 passing test。

??? note "参考答案"
    使用 `FakeClock` 创建并 expire 五个 key，然后一次 advance：

    ```diff
    +deleted = [
    +    await runtime.debug_active_expire_once()
    +    for _ in range(3)
    +]
    +assert deleted == [2, 2, 1]
    +assert runtime.debug_physical_key_count == 0
    +cleanup = runtime.debug_applied_batches()[-3:]
    +assert all(
    +    batch.trigger is CommitTrigger.ACTIVE_EXPIRE
    +    for batch in cleanup
    +)
    ```

    配置 `debug_record_applied_batches=True` 并显式关闭 direct client。

## 小结

MiniRedis 存储绝对 deadline，通过懒惰逻辑可见性保证正确性，再用有界 cursor-based
active pass 回收未访问 key。primary 的删除始终是普通 committed、replicated
operation；replica 隐藏 elapsed value，却不创建独立历史。第 5 章将研究 live、
non-expired 数据仍超过逻辑内存预算时会发生什么。
