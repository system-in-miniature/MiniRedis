> **语言**: [English](../../tutorial/08-replication.md) | 简体中文

# 复制：Backlog、重同步与晋升

## 学习目标

学完本章后，你将能够：

- 把复制 cursor 描述为“历史身份 + 已应用序列”；
- 推导 `ReplicationBacklog.missing_after` 何时允许部分重同步；
- 沿 `ReplicaSink` 追踪全量和部分 attachment；
- 解释异步确认为什么允许晋升时发生“已确认写丢失”；
- 运行 partial resume、full fallback 和已确认写丢失实验。

## 复制复用 commit 流

持久化回答“本 runtime 重启后能否重建状态”，复制回答“另一个 runtime 能否应用同一批有序变更”。MiniRedis 刻意让二者复用 `CommitBatch`。

`src/miniredis/core/executor.py` 的 `CommandExecutor._commit_prepared` 跨过持久性屏障并在本地应用 batch 后，把它加入 `ReplicationBacklog`，再调用 `_offer_replica_batch`。主节点**不会**等待副本应用后才回复客户端，这就是异步复制。

MiniRedis 的复制在进程内完成。`ReplicaSink` 模拟主节点侧 link，并调用另一个 `MiniRedis` runtime；没有 RESP `PSYNC`、复制 socket、heartbeat、ACK 流、`WAIT`、选举、Sentinel 或 Cluster。限制明确写在[行为矩阵 Replica 行](../behavior-matrix.md)。

保留下来的核心仍很丰富：身份、有序 offset、有界历史、断连、追赶、溢出、全量状态安装和手动晋升。

## Cursor 同时需要身份和位置

`src/miniredis/replication/backlog.py` 的 `ReplicationCursor` 包含：

```python
@dataclass(frozen=True, slots=True)
class ReplicationCursor:
    replication_id: str
    applied_seq: int
```

序列号单独不够。两个主节点都可能处在序列 20，却代表完全不同的历史；`replication_id` 用于隔离历史。`CommandExecutor` 为主历史创建 ID，重启会产生新 runtime 和新 ID，`CommandExecutor.promote_replica` 在副本变为可写时也创建新 ID。

`applied_seq` 表示最后一个**完整应用**的 batch，不是看见、排队或处理到一半的最新 batch。`src/miniredis/replication/sink.py` 的 `ReplicaSink._run_apply` 只在 `MiniRedis.apply_replica_batch` 成功后推进 cursor，因此断连或失败仍保留可信的恢复点。

这对应 Redis 的 replication ID + offset，但 MiniRedis offset 是逻辑 batch 序列，不是线协议字节 offset。

## 有界 backlog 与续传公式

`src/miniredis/replication/backlog.py` 的 `ReplicationBacklog` 用 deque 保存完整 batch；`append` 要求序列连续，并在超过配置容量时显式 `popleft` 最旧项。

executor 先检查 replication ID，再调用 `missing_after(applied_seq, current_seq=...)` 判断：

1. replication ID 必须匹配；
2. cursor 不能在未来；
3. cursor 等于 `current_seq` 时，是无需 batch 的合法空 partial sync；
4. 否则 backlog 不能为空；
5.第一条保留 batch 不能晚于 `applied_seq + 1`；
6. 从该点到 `current_seq` 必须连续。

若主节点在 6，backlog 保留 4、5、6：

- cursor 6：空 partial；
- cursor 5：partial，发送 6；
- cursor 3：partial，发送 4–6；
- cursor 2：full，因为 3 已丢；
- cursor 7：full，因为它声称未来历史；
- 不同 replication ID：full。

“最旧 batch 是 4 时 cursor 3 仍被覆盖”直接来自 `applied_seq` 定义：副本已有 3 及以前，下一条正是 4。

## 全量与部分 attachment

`src/miniredis/core/executor.py` 的 `CommandExecutor.attach_replica` 在主 executor mailbox 中运行。它用 backlog 检查 cursor：覆盖完整后缀时产生含缺失 batch 的 `PartialSyncAttachment`，否则产生含稳定 `SnapshotImage` 的 `FullSyncAttachment`。attachment 还带 generation，防止旧 link 实例的消息修改新状态。

`src/miniredis/replication/sink.py` 的 `ReplicaSink.attach` 走两种握手：

- **Full**：调用 `primary.install_replica_snapshot`，只有完整安装成功后才发布该镜像 cursor。
- **Partial**：调用 `primary.prepare_replica_resume` 验证副本当前状态确实匹配 cursor，再把缺失 batch 排在 live batch 之前。

只有安装成功后才发布 cursor，可避免失败快照伪装成可续传状态；partial 的准备验证也不会仅凭 sink 对象自报 cursor 就信任它，失败会 detach link。

追赶和 live delivery 共用同一有序队列。`ReplicaSink.offer` 在队列满时拒绝并标记需要重同步；`ReplicaSink._run_apply` 另行检查下一条必须正好是 `applied_seq + 1`，不连续也进入重同步。两种情况都不会静默跳过 batch。下次 attach 时，要么 backlog 提供完整后缀，要么全量安装。

sink 的 `lag = primary_seq - applied_seq` 只是观测值，不是确认 quorum。lag 为零仅表示观测时已追上，并不会把之前的客户端响应变成复制确认。

## 晋升与历史隔离

`src/miniredis/replication/sink.py` 的 `ReplicaSink.promote` 要求 sink 处于可晋升状态，并要求调用方说明 source 是否仍存活。`source_alive=True` 时它显式 detach 活 source；为 false 时从 source-lost 状态继续。随后它让副本 runtime 在当前 applied sequence 晋升。`src/miniredis/core/executor.py` 的 `CommandExecutor.promote_replica` 使数据库可写、分配新 replication ID，并清空 backlog。

新 ID 至关重要。若旧主历史 A 在序列 10，而晋升副本只应用到 8，继续使用 A 会允许其他节点提交 A/10 cursor，尽管新主从未拥有 9–10。新 ID 宣告从晋升状态开始的新谱系。

晋升是手动的，不会选择“最佳”副本；MiniRedis 没有选举或多数派 term。

## 为什么已确认写仍可能丢失

主节点在本地 commit 后回复，而不等待副本 apply。`CommandExecutor._commit_prepared` 通过 `_offer_replica_batch` 提供 batch，但 `ReplicaSink.offer` 只负责排队。sink 暂停或变慢时，主节点可以在 lag 大于零时确认。

若主节点在 sink 应用前崩溃，存活副本 cursor 仍停在最后完整序列。晋升不能凭空创造缺失值：客户端见过 `OK`，新主却没有该写入。这就是异步复制下的**已确认写丢失**，由 `tests/reliability/test_lost_acked_write.py::test_acknowledged_primary_write_can_be_lost_on_lagging_promotion` 明确验证。

这不是实现 bug，而是所建模的保证。真实 Redis 异步复制也有同类窗口。`WAIT`、`min-replicas-to-write` 和 `min-replicas-max-lag` 能提供运维控制，但不会把异步主节点变成线性一致 failover 的共识系统。

这也连接 MiniDist 谱系：复制、持久化、故障检测、leader 选择和客户端确认是不同协议决策；“状态被发往 follower”不等于“已确认写必然经 failover 存活”。

## 与真实 Redis 对照

Redis 复制主要位于 `src/replication.c`。副本使用带 replication ID 和 offset 的 `PSYNC`；主节点保留字节导向 backlog。历史仍存在时发送部分后缀，否则全量同步 RDB 后再发送缓冲命令。`REPLICAOF`、`ROLE`、`INFO replication`、`WAIT` 暴露状态与控制。

MiniRedis 保留“匹配身份 + 完整历史才允许 partial”的判断和“缺口必须 full”的安全规则，但简化了：

- batch sequence 替代字节 offset；
- `ReplicaSink` 在进程内调用 runtime；
- attachment 是 Python API，不是 `PSYNC`；
- sink 是进程内对象，而不是网络化副本机群；
- 没有周期 ACK、heartbeat、diskless sync、级联副本、认证或自动 failover；
- 晋升显式进行且总会创建 history fence。

[行为矩阵 Replica 行](../behavior-matrix.md)是兼容契约，示例输出不是网络复制证据。

## 动手实验 1：部分续传与全量回退

运行仓库示例：

```bash
uv run python examples/replication_resync.py
```

实测输出：

```text
Short disconnect: complete history is still in the backlog.
1. Initial attachment: full
   SET b'a' -> Ok(message=b'OK')
   SET b'b' -> Ok(message=b'OK')
2. Short disconnect resumed with: partial
   Expected partial: True
   Replica GET b: Bytes(value=b'2')

Long disconnect: a required batch has fallen out of the backlog.
   SET b'old' -> Ok(message=b'OK')
   SET b'k2' -> Ok(message=b'OK')
   SET b'k3' -> Ok(message=b'OK')
   SET b'k4' -> Ok(message=b'OK')
3. Cursor older than backlog resumed with: full
   Expected full: True
   Replica GET k4: Bytes(value=b'4')
```

两个主节点都设置 `replication_backlog_batches=2`。短断连只缺 batch 2 且仍被保留，所以 partial；长断连中新加三条，所需最旧 batch 被挤出两项 deque，只能 full，完整镜像因此包含 `k4`。

## 动手实验 2：演示已确认写丢失

保存为 `/tmp/miniredis_lost_ack.py`：

```python
import asyncio

from miniredis import CommandRequest, MiniRedis
from miniredis.replication.sink import ReplicaSink


async def main():
    primary = MiniRedis.open()
    replica = MiniRedis.open()
    await primary.start()
    await replica.start()
    sink = ReplicaSink(replica, queue_limit=4)
    attached = await primary.attach_replica(sink)
    print("attached:", attached.sync_mode, "seq", attached.applied_seq)
    sink.pause()
    reply = await primary.direct_client().execute(
        CommandRequest(b"SET", (b"x", b"1"))
    )
    print("primary acknowledged:", reply)
    print("replica lag before crash:", sink.status.lag)
    await primary.simulate_crash()
    promoted = await sink.promote(source_alive=False)
    print("promoted at seq:", promoted.applied_seq)
    value = await replica.direct_client().execute(
        CommandRequest(b"GET", (b"x",))
    )
    print("GET x after promotion:", value)
    print("confirmed write lost:", value.value is None)
    await replica.close()


asyncio.run(main())
```

运行：

```bash
uv run python /tmp/miniredis_lost_ack.py
```

实测输出：

```text
attached: full seq 0
primary acknowledged: Ok(message=b'OK')
replica lag before crash: 1
promoted at seq: 0
GET x after promotion: Bytes(value=None)
confirmed write lost: True
```

这是刻意的负面可靠性结果：`OK` 真实发生，晋升后缺失也真实发生。排队的 batch 1 从未应用，所以 cursor 正确停在零。

聚焦测试：

```bash
uv run pytest -q tests/replication/test_partial_resync.py \
  tests/replication/test_promotion.py \
  tests/reliability/test_lost_acked_write.py
```

预期仓库结果：

```text
18 passed
```

## 练习

### 1. 理解题：计算 backlog 覆盖

主 ID 匹配，`current_seq=20`，backlog 保留 17–20。分类 cursor 20、16、15、21。

??? note "参考答案"

    20 是空 partial；16 是发送 17–20 的 partial；15 缺少 batch 16，只能 full；21 在未来，也 full。

### 2. 理解题：解释晋升新 ID

若晋升保留旧 replication ID，为什么仅清空 backlog 不够？

??? note "参考答案"

    远端仍可能提交旧 ID 和一个指向副本从未应用 commit 的序列。新 ID 隔离旧谱系的全部 cursor；只清本地 batch 并不能消除旧 cursor 身份歧义。

### 3. 动手题：测试 exact-current partial

任务边界：只在 `tests/replication/` 增加测试，让已追平 sink 断开后在没有新写入时立即重连；不改 `src/`。

验收：断言 `ReplicaSyncMode.PARTIAL`、`applied_seq` 不变、lag 为零、副本内容不变。

??? note "参考答案"

    仿照 `tests/replication/test_partial_resync.py`：attach、写一项、等待序列 1、disconnect、再 attach。`applied_seq == current_seq` 对应空 missing suffix。

### 4. 动手题：强制队列溢出重同步

任务边界：创建 `queue_limit=1` 的 sink，暂停后执行足够多主节点写入；只用公开/debug test API，不改生产代码。

验收：证明 sink 进入 `needs-resync`，未应用 batch 不推进 cursor；之后在 backlog 完整时 partial，在产生 gap 后 full。

??? note "参考答案"

    复用 `tests/replication/test_sink_overflow.py`。暂停前保存 cursor，写两次并断言 cursor 不变。partial 变体用足够大的 backlog；full 变体用容量一并在重连前再加 batch。预期 diff 仅测试。

## 小结

MiniRedis 通过进程内异步 sink 复制有序逻辑 commit 流。只有历史 ID 与最后完整应用序列都匹配，cursor 才可信；有界 backlog 也必须保留整个缺失后缀才能 partial。晋升以新 ID 隔离旧历史，却无法恢复副本从未应用的 batch，因此异步确认会产生已确认写丢失。下一章回到单 runtime，研究另外两种协调：把命令组合成一个 commit，以及让请求等待数据而不阻塞 executor。
