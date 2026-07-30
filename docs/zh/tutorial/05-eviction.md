# 第 5 章：内存与淘汰

> **语言：** [English](../../tutorial/05-eviction.md) | 简体中文

## 学习目标

学完本章后，你将能够：

- 计算 MiniRedis 的逻辑 entry size，并解释它为何不是 RSS；
- 找到 maxmemory enforcement 改写 command plan 的精确位置；
- 对比 `noeviction`、精确 `allkeys-lru` 与确定性 `allkeys-lfu`；
- 解释 LFU decay、candidate exclusion 与确定性 tie-break；
- 验证 victim delete 与触发 write 共处一个 commit batch。

## 确定性内存模型

生产内存很难预测。对象成本包含 allocator header、fragmentation、container capacity、
shared structure 与实现细节；Python 还带来 runtime 和 garbage collector。MiniRedis
因此不把 `maxmemory` 与 RSS 或 Python allocator 相比较。

`src/miniredis/core/database.py` 的 `logical_value_size` 为各 value 分配简单、确定的
成本：string 为 `16 + len(data)`；hash field、list item、set member 和 sorted-set
member 都使用固定教学 overhead 加 byte length。`logical_entry_size` 再加入：

- 64 bytes 的 entry overhead；
- key length；
- logical value cost；
- 存在 expiration deadline 时的 16 bytes。

每个 `Entry` 保存自身 `logical_size`；`Database.logical_usage` 是全部物理 live entry
的和。`Database.apply_batch` 从 staged state 重算总和，并验证 entry size 为正。

这个模型刻意人工化，却非常有用：同一组 key、value 和 TTL 在每次运行中得到相同预算
决策；测试可选择有意义 threshold，不依赖平台 allocator。行为矩阵 Eviction 行明确称
它为 “logical byte” accounting。

切勿把“260 logical bytes”换算成“260 bytes RAM”。它只是教学 runtime 内的 policy
input，不是容量估算。

## enforcement 改写 plan，而非实时状态

`src/miniredis/core/planner.py` 的 `CommandPlanner.plan` 先得到普通类型特定
`ExecutionPlan`，再调用 `src/miniredis/core/eviction.py` 的 `enforce_memory`。
因此 eviction 是 cross-cutting planning step，可以：

- 保持 plan 不变；
- 加入 expired-key 与 victim deletion；
- 用 `Failure("OOM", "command exceeds maxmemory")` 替换 plan。

它不调用 `Database.apply_batch`；durability、atomic apply、replication 和 reply
publication 仍由 executor 拥有。

未配置 `maxmemory` 或 plan 没有 operation 时，`enforce_memory` 立即返回。接着它
判断 operation 是否真正写 client data。纯 expiry cleanup 不会递归触发 eviction；
client delete 即便在超预算时也允许，因为它能降低用量。

选择 live victim 前，`_expired_operations` 会提出删除每个逻辑过期物理 entry。
`dedupe_operations` 将其与原 plan 合并，所以 stale space 总在牺牲 live data 前回收。

函数通过 `projected_usage` 算两个值：expired cleanup 后 baseline，以及完整 proposed
write 后 usage。最终用量在预算内，或 command 没让用量超过 cleaned baseline 时，
plan 可以继续。这让 reducing write 即使面对刚设置后已经超出的 budget 也能成功。

## 原子 OOM 与 oversized target

每个目标 `PutEntry` 都会单独检查。若单 entry 的 logical size 已超过整个 budget，
`enforce_memory` 立即返回 OOM，不会徒劳淘汰无关 key。

`noeviction` 下，剩余的超预算增长一律 OOM。结果 `ExecutionPlan` 只有 failure reply，
没有 prepared commit，所以 commit sequence、keyspace、persistence history 与 replica
history 都不变。

这比“写完再发现错误”更强：planning 从结构上保证原子拒绝。
`tests/contract/test_eviction.py::test_oversized_target_does_not_evict_unrelated_key`
验证现有 key 在 oversized write 后仍存在；相邻的
`test_noeviction_allows_delete_but_rejects_growth_atomically` 验证 deletion 仍可工作。

write 的 target key 会从 victim candidate 中排除，否则 planner 可能通过立即淘汰刚
写的值来“成功”。已经计划 delete 的 key 也会排除。

## 精确 allkeys-LRU

LRU 表示 least recently used。MiniRedis 维护全局 `Database.access_tick`。
`Database.apply_batch` 对每个 client-triggered `PutEntry` 加一；
`Database.touch_if_live` 为成功 read touch 加一；每个 entry 保存
`last_access_tick`。

planner 把 touch 与 mutation 分开。例如 live `GET` 返回 `touch_keys=(key,)`，没有
write。完成可能的 commit 后，`CommandExecutor._apply_plan` 去重 key 并调用
`Database.touch_if_live`；missing 或 expired key 不会 touch。

`allkeys-lru` 下，`enforce_memory` 按 `(last_access_tick, key)` 排序 eligible
candidate。最小 tick 最冷；binary key order 让 tie 确定。它逐个加入
`DeleteKey(key, DeleteReason.EVICTED)`，直到 projected usage 可容纳。

这个 LRU 对全部 eligible key 是**精确**的。真实 Redis 使用有界 candidate sampling，
避免 write path 全 keyspace 排序。MiniRedis 接受额外成本，让读者能从可见 metadata
推导 victim，并使测试可重复。

victim deletion 与触发 `PutEntry` 留在同一 plan，commit 时共享一个 sequence。因此
在 command boundary 上，client 不会看到 victim 已消失而请求 write 尚未出现。

## 确定性衰减 LFU

LFU 表示 least frequently used，但 lifetime counter 会让曾经很热的 key 永不淘汰。
MiniRedis 将 frequency 与 decay 结合；每个 entry 有 `frequency` 与
`last_frequency_decay_ms`。

`src/miniredis/core/frequency.py` 的 `project_frequency` 计算完整 decay window：

```python
windows = (now_ms - last_decay_ms) // interval_ms
projected = (
    0 if windows >= frequency.bit_length()
    else frequency >> windows
)
```

每过一个 window，counter 右移即减半；返回的 decay timestamp 只推进完整 window。
read 与 client write 会 materialize projection，再在 `Database.touch_if_live` 或
`Database.apply_batch` 中增加 frequency。

eviction planning 不同：`_lfu_candidates` 为比较而投影每个 eligible frequency，却
不把结果写回 survivor，保持失败或无关 plan 无副作用。candidate tuple 是
`(effective_frequency, last_access_tick, key)`：先淘汰频率最低，再选访问更旧，最后
选 binary key 字典序更小者。

真实 Redis 使用近似 logarithmic counter、概率 increment 和 sampling；MiniRedis
使用精确 increment 与确定性 halving。它保留 recency/frequency trade-off 课程，但
不是 Redis metadata algorithm 的逐字节复制。

decay 会改变结果。一个旧 key 读八次后 frequency 为八，但三个完整 window 后投影为
一；最近创建 key 就可能追平或超过它，让昨日 hot item 冷却。注入 clock 使测试无需
等待就能推进这些 window。

## touch 语义与运维 metadata

正常路径中，成功 client read/write 恰好 materialize 一次 touch。no-op command 也可
touch 已存在 key，因为即便 value 未变，观察仍是访问；failed wrong-type command
通常不 touch mismatch value。

eviction planning 读取 projected metadata，却不构成 access，否则候选检查会让所有
key 变“recent”并扰乱 policy。active-expiration commit 使用
`CommitTrigger.ACTIVE_EXPIRE`，所以 `Database.apply_batch` 收到
`track_access=False`。

LRU/LFU metadata 是 operational 而非 durable semantic state。`Database.install_snapshot`
与 recovery 会把 access tick 和 frequency 初始化为 neutral；full replica sync 也
安装 neutral metadata 的 snapshot。行为矩阵记录 policy 行为，但不承诺 victim
history 跨重启保存。

## 复杂度与教学 trade-off

两种 policy 都执行生产 hot path 会避免的工作：`projected_usage` 构建 size dictionary
并求和；LRU 排序全部 candidate；LFU 投影并排序全部 candidate；随后
`Database.apply_batch` 又复制 key table 并重算 usage。

这些选择让不变量可检查：

- projected decision 不修改 survivor；
- failed OOM 不需要 rollback，因为从未 apply；
- tie-break 稳定；
- logical usage 可以重算验证；
- victim 与 trigger 是一个 propagation unit。

代价是 MiniRedis 不能用于 eviction 性能 benchmark。生产设计需要 allocator-aware
accounting、紧凑 metadata、有界 sampling 与严格预算 work。

调试意外 victim 时，改变 policy 前先记录四项输入：每个 entry 的 logical size、当前
时间、access tick，以及存储的 frequency/decay timestamp。LRU 与 LFU 是这些值的
确定性函数。检查它们会把“cache 选错了”变成可复现 ordering 问题，也避免把 TTL
cleanup 误认为 eviction。

## 与真实 Redis 对照

真实 Redis 在 `evict.c` 中 enforce maxmemory，并通过 `maxmemory-policy` 等配置暴露
policy。其选择面比 MiniRedis 的三种 policy 更广；LRU 通过 sampling 近似，LFU 使用
概率 logarithmic counter 与 decay 配置。

MiniRedis 保留决策点：触发 write 提交前必须释放内存，失败必须让 keyspace 不变。
它把 accounting 简化为 logical bytes，只支持 `noeviction`、`allkeys-lru` 和
`allkeys-lfu`，并让 victim selection 精确、确定。

架构映射的 `core/eviction.py` 行指向 Redis `evict.c` 并标为 intentional
simplification。[`docs/behavior-matrix.md`](../../behavior-matrix.md) 的 Eviction 行
引用
`tests/contract/test_eviction.py::test_lfu_planning_projects_without_materializing_survivor_decay`
作为直接证据。

## 动手实验：通过公共回复观察 LFU

运行：

```bash
uv run python - <<'PY'
import asyncio
from miniredis import CommandRequest, MiniRedis

async def main():
    async with MiniRedis.open(
        maxmemory=260,
        eviction_policy="allkeys-lfu",
    ) as server:
        c = server.direct_client()
        print("hot:", await c.execute(
            CommandRequest(b"SET", (b"hot", b"x"))
        ))
        print("cold:", await c.execute(
            CommandRequest(b"SET", (b"cold", b"x"))
        ))
        for _ in range(4):
            await c.execute(CommandRequest(b"GET", (b"hot",)))
        print("new:", await c.execute(
            CommandRequest(b"SET", (b"new", b"x" * 60))
        ))
        for key in (b"cold", b"hot", b"new"):
            print(key.decode() + ":", await c.execute(
                CommandRequest(b"GET", (key,))
            ))
        print("evictions:", server.debug_stats().evicted_key_count)
        await c.close()

asyncio.run(main())
PY
```

实测输出：

```text
hot: Ok(message=b'OK')
cold: Ok(message=b'OK')
new: Ok(message=b'OK')
cold: Bytes(value=None)
hot: Bytes(value=b'x')
new: Bytes(value=b'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx')
evictions: 1
```

两个初始 write 都给 key 一次 frequency touch；四次额外 `GET` 让 `hot` 更频繁，所以
更大的 `new` 值超预算时，`cold` 是唯一 victim。

## 练习

### 1. 理解题：逻辑内存

为什么 MiniRedis 能产生确定性 eviction 测试，而 maxmemory 数值不能用于生产主机
容量规划？

??? note "参考答案"
    它按固定公式计算 key/value，不依赖 Python 与 OS allocation，因此可重复；但省略
    allocator overhead、fragmentation、container capacity、process memory 等成本。

### 2. 理解题：LFU planning

`_lfu_candidates` 为什么只投影 decay，却不把 projected frequency 写回所有 survivor？

??? note "参考答案"
    planning 必须无副作用。materialize candidate decay 会在 write 最终 OOM 时仍改变
    operational state；survivor 只在真实 client touch 时 materialize decay。

### 3. 动手题：证明 LRU 原子性

在 `tests/contract/test_eviction.py` 添加聚焦测试：创建 `cold`、`hot`，touch `hot`，
再在 exact LRU 下写入较大的 `new`。断言最终记录 batch 同时包含 `cold` eviction 与
`new` put，commit sequence 只推进一次。验收：

```bash
uv run pytest tests/contract/test_eviction.py -q
```

该文件应比基线多一个 passing test。

??? note "参考答案"
    使用 `debug_record_applied_batches=True`，捕获 `before`，检查最后 batch：

    ```diff
    +assert runtime.debug_commit_seq == before + 1
    +batch = runtime.debug_applied_batches()[-1]
    +assert any(
    +    isinstance(op, DeleteKey)
    +    and op.key == b"cold"
    +    and op.reason is DeleteReason.EVICTED
    +    for op in batch.operations
    +)
    +assert any(
    +    isinstance(op, PutEntry) and op.key == b"new"
    +    for op in batch.operations
    +)
    ```

    与实测实验一样使用 `maxmemory=260`、一字节初始 value 与 60-byte 新 value。

### 4. 动手题：添加 tie-break 测试

增加一项测试，证明 frequency 与 recency 都相同的 LFU candidate 按 binary key 排序。
用 `Database.apply_batch(track_access=False)` 构造状态，再通过普通 write 调用
`enforce_memory`。验收：

```bash
uv run pytest tests/contract/test_eviction.py -q
```

与练习 3 合计，该文件应比基线多两个 passing test。

??? note "参考答案"
    以 neutral frequency/access metadata 插入 `b"a"` 与 `b"b"`，再触发一次必要
    eviction。关键断言是：

    ```diff
    +assert await client.execute(
    +    CommandRequest(b"GET", (b"a",))
    +) == Bytes(None)
    +assert await client.execute(
    +    CommandRequest(b"GET", (b"b",))
    +) == Bytes(b"x")
    ```

    这应保持为 policy test，不要修改 tie-break 实现。

## 小结

MiniRedis 在 planning 阶段 enforce 确定性逻辑预算：先回收 expired space，再原子拒绝
无法容纳或不允许的增长，也可把 exact LRU 或 deterministic decaying-LFU victim 加入
触发 write 的单一 commit batch。下一章会跟随该 batch 离开内存，进入 append-only
persistence barrier。
