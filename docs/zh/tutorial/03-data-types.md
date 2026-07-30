# 第 3 章：数据类型与命令面

> **语言：** [English](../../tutorial/03-data-types.md) | 简体中文

## 学习目标

学完本章后，你将能够：

- 描述 MiniRedis 的 string、hash、list、set 和 sorted-set value model；
- 找到某条命令对应的 parser model 与 planner；
- 预测缺失 key、空 key、顺序以及 `WRONGTYPE` 行为；
- 解释不可变 stored value 如何跨越 commit 边界；
- 在不改动无关层的前提下设计并测试 `APPEND` 命令。

## 一个 keyspace，五种 value variant

MiniRedis 把所有用户 key 存在同一个 `Database.entries` dictionary 中，但 entry 的
value 属于封闭 union。`src/miniredis/core/values.py` 定义五种 planning 阶段可变
wrapper：

```python
@dataclass(slots=True)
class StringValue:
    data: bytes

@dataclass(slots=True)
class HashValue:
    items: dict[bytes, bytes]
```

另外三种是 `ListValue(deque[bytes])`、`SetValue(set[bytes])` 与
`ZSetValue(dict[bytes, float])`。wrapper 在使用熟悉 Python 容器的同时，让类型
检查保持显式；`RedisValue` 是它们的 type union。

实时 value 不会直接写进 commit。`src/miniredis/core/database.py` 的
`freeze_value` 会把它变成 `src/miniredis/core/commit.py` 中的冻结类型：
`StoredString`、`StoredHash`、`StoredList`、`StoredSet` 或 `StoredZSet`。冻结
hash、set 与 sorted set 时会排序，因此持久化和快照得到确定性 tuple；apply 时，
`thaw_value` 重建新的可变容器。

这种双形态模型强化了第 2 章的边界。planner 可以在本地复制、修改 Python 容器；
`PutEntry` 携带提议状态的不可变快照；只有 `Database.apply_batch` 能发布它。planner
不会传递一个稍后还可被修改、从而绕过 commit sequence 的实时 dictionary 引用。

每个 `Entry` 还统一携带绝对 expiry、mutation version、access tick、logical size、
frequency 与最近 LFU decay time。第 4、5 章会跨类型复用这些字段。

## 解析并路由命令面

`src/miniredis/commands/parser.py` 的 `parse_request` 识别原始命令名，并验证 arity、
整数语法、score 语法和支持的选项，最终返回
`src/miniredis/commands/model.py` 的 typed data class。

`src/miniredis/core/planner.py` 的 `CommandPlanner.plan` 依次尝试：

1. `plan_general_and_strings`；
2. `plan_hash`；
3. `plan_list`；
4. `plan_set`；
5. `plan_zset`；
6. `plan_ttl`。

命令不属于自己时，planner 返回 `None`。某个 planner 一旦返回 `ExecutionPlan`，
router 就执行 memory enforcement。这样，小型命令实现靠近类型不变量，同时所有类型
共享一个 maxmemory 决策。

`src/miniredis/core/planning.py` 的 `lookup` 是公共读取 primitive：缺失 key 返回
`(None, ())`；逻辑过期 key 返回 `(None, (expiry_delete(key),))`；否则返回实时
`Entry`。每个 planner 决定“缺失”应映射成哪种命令 reply。

## String：带严格整数操作的 bytes

String 是原始 bytes，不是 Python 文本。`SET`、`GET`、`MGET`、`MSET`、`INCR`、
`DECR`、`INCRBY`，以及教学用原子命令 `COMPAREDEL`、`CHECKDECR`，都由
`src/miniredis/core/planning.py` 的 `plan_general_and_strings` 处理。

缺失 key 上的 `GET` 返回 `Bytes(None)`；`INCR` 从零开始。已有 string 的整数操作
调用 `parse_int64`，其 grammar 拒绝 `01`、`-0` 等非 canonical 表示，结果还必须
保持在 signed 64-bit 范围内。失败 plan 没有 write。

普通 `SET` 会替换值并清除旧 TTL，除非新的 `EX`/`PX` 提供新 deadline。`INCR` 等
原地操作保留已有绝对 expiry。`NX` 和 `XX` 被解析为 `SetString.only_if`，planner
无需重新解释原始选项。

## Hash：field-value map

`src/miniredis/core/hash_planner.py` 的 `plan_hash` 实现 `HSET`、`HGET`、`HDEL`、
`HGETALL` 和 `HINCRBY`。写命令可创建缺失 hash。`HSET` 统计新加入 field，而不是
赋值次数；同一请求中的重复 field 以最终 value 为准。

planner 先复制 `previous.value.items`。删除最后一个 field 会产生 `DeleteKey`，
而不是保存空 hash。`HGETALL` 按 field 排序后 materialize 交替 field/value reply。
行为矩阵把公共顺序标为 unspecified，所以调用方不能把这个确定性实现细节当兼容承诺。

`HINCRBY` 使用与 string increment 相同的严格 int64 规则。field 值非法或溢出时，
返回错误而不提交复制出的 map。

## List：有序 deque

List 使用 `collections.deque`，由 `src/miniredis/core/list_planner.py` 的
`plan_list` 规划。`LPUSH` 对每个参数调用 `appendleft`，所以
`LPUSH l a b c` 产生 `c, b, a`；`RPUSH` 在右侧 extend。最后一项被 `LPOP` 或
`RPOP` 移除时，key 也被删除。

`LRANGE` 通过 `inclusive_slice` 把 Redis 风格的 inclusive、可为负的边界转换成
Python half-open slice。越界会返回空 `Items`，而不是错误。

blocking pop 只部分属于这个 planner。`CommandPlanner.plan_blocking_pop_now` 在
某个 key 已就绪时立即 pop；否则 `CommandExecutor._execute` 注册 waiter。这样普通
list value logic 与时间/session ownership 分离，第 9 章会追踪等待路径。

## Set：唯一性与无序语义

`src/miniredis/core/set_planner.py` 的 `plan_set` 实现 `SADD`、`SREM`、
`SISMEMBER`、`SMEMBERS` 与 `SINTER`。实时模型是 Python set。`SADD` 统计真正新增
member，即使参数重复；`SREM` 去重待删 member，并在 set 变空时删除 key。

`SMEMBERS` 与 `SINTER` 在构造 `Items` 前对 bytes 排序，让测试确定。Redis set 结果
顺序并非语义保证，行为矩阵也明确标为 unspecified；此处排序是可观测性脚手架，而非
可移植排序契约。

`SINTER` 说明 planning 为什么必须检查全部相关 key：即便一个输入缺失、数学上交集
已经为空，后续 wrong-type key 仍应返回 `WRONGTYPE`，不能提前停止。

## Sorted set：score 与派生顺序

`ZSetValue` 用 dictionary 保存 `member -> float`。
`src/miniredis/core/zset_planner.py` 的 `plan_zset` 通过
`sorted(scores.items(), key=lambda item: (item[1], item[0]))` 派生顺序；score
相同时按 member bytes 排序。

支持面刻意保持小型：`ZADD`、`ZREM`、`ZSCORE`、`ZRANK`、`ZRANGE` 和
`ZRANGEBYSCORE`。score bound 可 inclusive，也可用 `(` 前缀表示 exclusive。parser
拒绝 NaN，但 range bound 接受 infinity。删除最后一个 member 会删除 key。

这里有一项已记录的 Redis 差异：`_format_score` 使用 Python `repr(float)`，所以
MiniRedis 可能返回 `b"1.0"`，而 Redis 常格式化为 `b"1"`。参见
[`docs/behavior-matrix.md`](../../behavior-matrix.md) 的 Sorted Set 行。

## 类型安全与 `WRONGTYPE`

五个 type planner 都使用 `src/miniredis/core/planning.py` 的共享 `WRONGTYPE`。
命令先区分 absence 与实时 entry，再用 `isinstance` 检查期望 wrapper。wrong-type
plan 没有 operation，所以 `_apply_plan` 发布 failure 而不推进 commit sequence。

并非所有 multi-key string read 都使用 `WRONGTYPE`：`MGET` 对缺失或非 string key
返回 nil。这就是为什么类型规则属于各 command planner，而不是全局 pre-check。

no-op write 也要谨慎处理。没有 matching member 的 `HDEL` 或 `SREM` 返回零并 touch
实时 key 的访问元数据，但不创建 commit。真正 value change 才创建 `PutEntry` 或
最终 `DeleteKey`。

## 如何安全扩展命令面

添加命令是一项纵向改动，不只是 parser 多一个 branch。先定义 frozen typed command，
使原始语法不能泄漏进 planning；把它加入 `Command` union，并分类是否修改 dataset；
在 mailbox 执行前解析精确 arity 和 option；最后把语义放进已经拥有该 value type 的
planner，返回 reply、operation 与 touch，而不是直接修改 database。

测试应独立覆盖四个边界：正常状态变更、缺失 key、wrong type 或 invalid value，以及
TTL preservation 等元数据。失败时先捕获 `debug_commit_seq`，证明它不推进；成功时
优先检查公共 reply，只有在传播形态本身是测试目标时才记录 batch。

这个 checklist 可防止常见架构漂移：实现一个绕过 parser 或 executor 的 Direct-only
helper。如果新命令从 `CommandRequest` 和共享 typed model 进入，Direct 与 RESP2
adapter 都能复用。下方练习会把这套方法应用于 `APPEND`。

## 与真实 Redis 对照

真实 Redis 在 `t_string.c`、`t_hash.c`、`t_list.c`、`t_set.c` 和 `t_zset.c`
等生产文件中实现这些命令族，并按大小与 workload 选择专门内部编码；string、
listpack、hash table、intset、skip list 等表示也会随版本演进。

MiniRedis 刻意用 Python 容器替代编码层。保留下来的课程是命令级类型契约、修改原子性
和有序传播，而非 Redis 的内存密度或算法性能。MiniRedis 还会 stage database copy，
真实 Redis 通常就地更新实时 dictionary 与 metadata。

请对照 [`docs/behavior-matrix.md`](../../behavior-matrix.md) 的 String、Hash、List、
Set、Sorted Set 行。每一行都列出支持子集、可执行测试证据和差异。架构映射将 Python
容器标为刻意简化。

## 动手实验：操作全部五种类型

运行：

```bash
uv run python - <<'PY'
import asyncio
from miniredis import CommandRequest, MiniRedis

async def main():
    async with MiniRedis.open() as server:
        c = server.direct_client()
        commands = [
            CommandRequest(b"SET", (b"s", b"hello")),
            CommandRequest(b"HSET", (b"h", b"language", b"Python")),
            CommandRequest(b"RPUSH", (b"l", b"one", b"two")),
            CommandRequest(b"SADD", (b"set", b"red", b"blue")),
            CommandRequest(
                b"ZADD", (b"z", b"2", b"second", b"1", b"first")
            ),
            CommandRequest(b"ZRANGE", (b"z", b"0", b"-1")),
            CommandRequest(b"HGET", (b"s", b"field")),
        ]
        for command in commands:
            print(command.name.decode(), "=>", await c.execute(command))
        await c.close()

asyncio.run(main())
PY
```

实测输出：

```text
SET => Ok(message=b'OK')
HSET => Number(value=1)
RPUSH => Number(value=2)
SADD => Number(value=2)
ZADD => Number(value=2)
ZRANGE => Items(values=(Bytes(value=b'first'), Bytes(value=b'second')))
HGET => Failure(code='WRONGTYPE', message='operation against a key holding the wrong kind of value')
```

sorted set 不受插入顺序影响，而按 score 排列。最后的 `HGET` 目标是 string，返回领域
错误；Python 脚本继续运行，因为命令失败是 reply。

## 练习

### 1. 理解题：live value 与 stored value

planner 为什么要把复制的 `HashValue` 冻结成 `StoredHash`，而不是直接把可变
dictionary 放进 `PutEntry`？

??? note "参考答案"
    commit 必须是提议状态的不可变快照。共享可变 planning dictionary 会让后续修改
    改变已编号 batch，破坏 durability、replication 与 recovery 的一致性。

### 2. 理解题：空 collection

`HDEL`、`LPOP`、`SREM` 或 `ZREM` 移除最后一个元素时会发生什么？

??? note "参考答案"
    planner 发出 `DeleteKey`，key 变为 absent；这些命令不会保留空 hash、list、set
    或 sorted-set entry。

### 3. 动手题：实现 String `APPEND`

在 typed model、parser、string planner、mutating classification 与测试中加入
`APPEND key value`。它必须能创建缺失 string、保留已有 TTL、返回新 byte 长度，并
在非 string 上返回 `WRONGTYPE` 且不提交。不要创建独立 planner，也不要直接修改
`Database`。验收：

```bash
uv run pytest tests/contract/test_strings.py -q
```

新增四个聚焦 case；该文件应比变更前多四个通过项。随后运行 `uv run pytest -q`
检查完整套件。

??? note "参考答案"
    关键变更是：

    ```diff
    # src/miniredis/commands/model.py
    +@dataclass(frozen=True, slots=True)
    +class Append:
    +    key: bytes
    +    value: bytes

    # src/miniredis/commands/parser.py, parse_request
    +case b"APPEND":
    +    _require_arity(name, args, 2)
    +    return Append(args[0], args[1])

    # src/miniredis/core/planning.py, plan_general_and_strings
    +case cmd.Append(key, suffix):
    +    previous, expired = lookup(database, key, now_ms)
    +    if previous is not None and not isinstance(previous.value, StringValue):
    +        return ExecutionPlan(WRONGTYPE)
    +    old = b"" if previous is None else previous.value.data
    +    value = StringValue(old + suffix)
    +    expiry = None if previous is None else previous.expire_at_ms
    +    return ExecutionPlan(
    +        Number(len(value.data)),
    +        expired + (make_put(key, value, previous, expiry),),
    +    )
    ```

    还要把 `Append` 加进 `Command` union、`_DATASET_MUTATING_TYPES` 与 parser import。
    测试覆盖 missing、existing、TTL preservation，以及 wrong type 时
    `debug_commit_seq` 不变。

## 小结

MiniRedis 用简单可变 Python 容器进行 planning，并用 frozen stored variant 进行
commit propagation，从而建模五种 Redis value family。各 type planner 负责命令特定
的 absence、ordering、integer 与 type 规则；共享 executor 负责发布。理解 value 后，
第 4 章会加入时间：entry 可以仍然物理存在，却已经逻辑缺失。
