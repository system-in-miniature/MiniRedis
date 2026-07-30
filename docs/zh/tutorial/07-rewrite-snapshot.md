> **语言**: [English](../../tutorial/07-rewrite-snapshot.md) | 简体中文

# 持久化 II：重写与快照

## 学习目标

学完本章后，你将能够：

- 解释 AOF 重写为什么同时需要稳定 base 和并发写 delta；
- 沿 executor 与 AOF writer 追踪无缺口的重写握手；
- 区分快照 checkpoint 与 AOF state base；
- 预测 `recover_database` 选择哪个基线、重放哪些 batch；
- 在不改 `src/` 的前提下测试原子替换与失败行为。

## 为什么追加历史需要压缩

若客户端连续执行 `SET counter 1`、2、3、4，重放四次当然正确，但描述当前状态只需要最终值。长期运行的 AOF 会不断占用磁盘并延长重启。

MiniRedis 把历史压缩为 AOF **state base**：序列 `S` 时完整的逻辑数据库镜像，再接序列大于 `S` 的普通 batch。`src/miniredis/persistence/codec.py` 的 `encode_aof_state_base_record` 为它定义独立记录类型。重写后的文件仍是带版本的 MiniRedis AOF，但可以用“checkpoint 400 的完整状态”替代 1–400 的逐条重放。

若停止接收写入，压缩很简单：捕获状态、写新文件、rename、恢复流量。真正值得学习的是**在线**重写：写 base 的慢速 I/O 期间仍继续执行命令。

## base + delta 不变量

在线重写必须满足：

```text
新 AOF = checkpoint S 时的精确状态
         + S 之后的每个已提交 batch（保持序列顺序）
```

有两个典型缺口：

1. commit 发生在捕获快照之后、开始收集 delta 之前；
2. 新文件在最后一段 delta 追加之前被安装。

MiniRedis 用同一个串行 executor mailbox turn 关闭第一个缺口。`src/miniredis/core/executor.py` 的 `CommandExecutor.begin_aof_rewrite` 投递 `BeginAofRewrite` 控制消息；`CommandExecutor._dispatch` 在处理它时捕获逻辑存活镜像，并在处理下一条命令前调用已注册的重写函数。`src/miniredis/runtime.py` 的 `MiniRedis._start_owned` 把 `AofWriter.begin_rewrite` 注册进去。

在 `src/miniredis/persistence/aof.py` 的 `AofWriter.begin_rewrite` 中，writer 先保存 `_RewriteState`，再创建 base task：

```python
self._rewrite = state
state.base_task = asyncio.create_task(
    self._write_rewrite_base(state),
    name=f"miniredis-aof-rewrite-base-{generation}",
)
```

从此以后，`AofWriter._run_writer` 处理的每次 append 都调用 `_capture_rewrite_delta`。executor commit 有全序，因此 delta 也保持该顺序。checkpoint 已存在但 writer 尚不知道重写的窗口不存在。

delta 受 `MiniRedisConfig.aof_rewrite_delta_limit_bytes` 限制。`AofWriter._capture_rewrite_delta` 超限时终止重写；旧 AOF 继续是权威历史且仍包含这些写入。与其让慢重写变成第二份无限内存日志，安全终止更合理。

## 完成阶段：让替换本身持久

`AofWriter._write_rewrite_base` 独占创建临时文件，写入 AOF header 和 state base，再把 `_FinalizeRewrite` 排到普通 append 工作之后。这关闭第二个缺口：已被 writer 接受的 commit 会先处理并进入 delta。

`AofWriter._finalize_rewrite` 的关键顺序是：

1. 把 delta 追加到临时 descriptor；
2. fsync 临时文件；
3. 用 `os.replace` 原子替换配置的 AOF 路径；
4. fsync 父目录，使目录项持久；
5. 切换活动 writer descriptor 并关闭旧 descriptor。

rename 前后失败的处理刻意不同。`replace` 前失败可删除临时文件并继续使用旧 AOF；`replace` 后路径可能已指向新文件，若目录 fsync 再失败，进程无法证明机器故障后哪个映射存活。`AofWriter._finalize_rewrite` 把这种不确定性记录为终止性 writer 失败，而不是假装安全继续。

`tests/reliability/test_aof_rewrite.py` 覆盖关键接缝：`test_write_during_paused_base_survives_rewrite_and_restart` 验证 delta；`test_rewrite_delta_overflow_preserves_old_aof_history` 验证 rename 前安全终止；`test_immediate_write_after_rewrite_request_has_no_capture_gap` 验证 mailbox 边界。

## 快照是稳定 checkpoint，不是重写文件

快照用自定义格式保存 `SnapshotImage(checkpoint_seq, entries)`。`src/miniredis/core/executor.py` 的 `CommandExecutor.capture_snapshot` 在 mailbox 屏障处导出当前序列下逻辑存活的 entry，但不会在磁盘 I/O 期间一直占用 executor。

`src/miniredis/persistence/snapshot.py` 的 `SnapshotManager.save` 同时只允许一个 job。`_run_save` 先等待捕获，再把编码镜像交给 `PosixSnapshotFileOps.write_atomic`：独占临时文件、完整写入、文件 fsync、`os.replace`、父目录 fsync。

不可变镜像捕获完成后命令即可继续，慢速文件安装在外部进行。`tests/reliability/test_snapshot_barrier.py::test_commands_continue_after_capture_while_file_write_is_blocked` 暂停文件写入，证明后续 `SET` 仍可提交，而保存镜像仍停在之前序列。

快照和 AOF state base 都包含逻辑状态，但所有权不同：

- snapshot 是由 `snapshot_path` 配置的独立 checkpoint 文件；
- state base 是 `appendonly.mraof` 中压缩后的前缀；
- 组合恢复时，后续 AOF batch 可以扩展任一基线。

两者都不是 Redis RDB。

## 组合恢复如何选基线

阅读 `src/miniredis/persistence/recovery.py` 的 `recover_database`。它分别加载 snapshot 与 AOF。若 AOF 有 state base，就选择 checkpoint 序列更大的镜像；序列相同时 AOF base 胜出，因为只有 snapshot **严格更大**时才保留 snapshot。

之后只筛选 checkpoint 之后的 AOF batch，并检查连续性：

| Snapshot | AOF base | AOF batch | 结果 |
|---|---|---|---|
| seq 5 | 无 | 1…8 | 安装 snapshot 5，重放 6…8 |
| seq 5 | seq 7 | 8…9 | 安装 AOF base 7，重放 8…9 |
| seq 9 | seq 7 | 8…9 | 安装 snapshot 9，不重放 |
| seq 7 | seq 7 | 8 | 选择 AOF base 7，重放 8 |

第三行中 AOF 也结束于 9，因此尽管无需重放仍一致。反之，若 snapshot 为 9 而 AOF 结束于 7，恢复会抛出 `RecoveryError`，因为日志明确老于已选 checkpoint。

所有重放都发生在暂存 `Database`。序列缺口、非法 mutation、错误校验和或错误起始序列都会让启动在开放 command admission 前失败。兼容边界见[行为矩阵的 Snapshot 与 Recovery 行](../behavior-matrix.md)。

## 与真实 Redis 对照

Redis 快照是 RDB，主要位于 `src/rdb.c`，通过 `SAVE`、`BGSAVE` 和 `save` 等命令/配置控制。AOF 重写主要位于 `src/aof.c`，通过 `BGREWRITEAOF` 暴露；现代 Redis 可能使用 base、增量 AOF 和 manifest，配置允许时 base 也可能采用 RDB 编码。

MiniRedis 保留了值得学习的机制：

- 捕获时点一致状态；
- 继续接收写入；
- 保留该时点后的每次写入；
- 通过原子替换安装完整文件；
- 从最新有效基线加连续后缀恢复。

它简化了子进程隔离、copy-on-write、manifest、命令兼容编码、自动重写阈值和运维进度。自定义格式边界记录在[行为矩阵](../behavior-matrix.md)中，不能交给 Redis 工具处理。

## 动手实验：观察 base 与并发 delta

把以下内容保存为 `/tmp/miniredis_rewrite_demo.py`：

```python
import asyncio
from pathlib import Path
from tempfile import TemporaryDirectory

from miniredis import CommandRequest, MiniRedis, MiniRedisConfig
from miniredis.persistence.aof import AofPolicy, load_aof


async def main():
    with TemporaryDirectory(prefix="miniredis-rewrite-") as directory:
        root = Path(directory)
        config = MiniRedisConfig(
            aof_path=root / "appendonly.mraof",
            snapshot_path=root / "dump.mrsnap",
            aof_policy=AofPolicy.ALWAYS,
        )
        runtime = MiniRedis.open(config)
        await runtime.start()
        client = runtime.direct_client()
        print("SET before:", await client.execute(
            CommandRequest(b"SET", (b"before", b"1"))
        ))
        saved = await runtime.save_snapshot()
        print("snapshot seq:", saved.checkpoint_seq)
        rewriting = asyncio.create_task(runtime.rewrite_aof())
        await asyncio.sleep(0)
        print("SET during:", await client.execute(
            CommandRequest(b"SET", (b"during", b"2"))
        ))
        rewritten = await rewriting
        print("rewrite seq:", rewritten.checkpoint_seq)
        log = load_aof(config.aof_path, repair_truncated_tail=False)
        print("AOF base seq:", log.state_base.checkpoint_seq)
        print("AOF delta seqs:", [batch.seq for batch in log.batches])
        await runtime.close()
        recovered = MiniRedis.open(config)
        await recovered.start()
        print("recovered:", await recovered.direct_client().execute(
            CommandRequest(b"MGET", (b"before", b"during"))
        ))
        print("recovered seq:", recovered.debug_commit_seq)
        await recovered.close()


asyncio.run(main())
```

运行：

```bash
uv run python /tmp/miniredis_rewrite_demo.py
```

实测输出（省略随机临时目录）：

```text
SET before: Ok(message=b'OK')
snapshot seq: 1
SET during: Ok(message=b'OK')
rewrite seq: 1
AOF base seq: 1
AOF delta seqs: [2]
recovered: Items(values=(Bytes(value=b'1'), Bytes(value=b'2')))
recovered seq: 2
```

snapshot 和 rewrite base 都描述序列 1；并发 `SET` 成为 batch 2 并进入 delta；恢复从序列 1 基线应用连续序列 2，同时保住状态与顺序。

再运行：

```bash
uv run pytest -q tests/reliability/test_aof_rewrite.py \
  tests/reliability/test_snapshot_barrier.py
```

预期仓库结果：

```text
11 passed
```

## 练习

### 1. 理解题：寻找无缺口点

为什么在一个 executor 消息中捕获 snapshot、另一个消息中调用 `AofWriter.begin_rewrite` 不安全？

??? note "参考答案"

    两条消息之间可能有命令提交。它比 base 新，但 writer 尚未设置 `_rewrite`，所以不会进入 delta。MiniRedis 在同一个串行 dispatch turn 中捕获并注册。

### 2. 理解题：选择恢复基线

snapshot 在序列 12，AOF state base 在 10 且包含 batch 11、12。恢复安装和重放什么？

??? note "参考答案"

    安装序列 12 的 snapshot；过滤后没有大于 12 的 batch，因此不重放。AOF 也结束于 12，一致性检查通过。

### 3. 动手题：测试重写压缩

任务边界：只在 `tests/reliability/` 增加测试，在 `AofPolicy.ALWAYS` 下对同一 key 写五次，再调用 `rewrite_aof` 并用 `load_aof` 检查；不改 `src/`。

验收：

```bash
uv run pytest -q tests/reliability/test_aof_rewrite.py
```

重写前有五个 batch；重写后 state base 在序列 5、无 delta；重启读到第五个值。

??? note "参考答案"

    仿照 `test_successful_rewrite_compacts_history_to_base_only`。预期 diff 仅测试：临时路径、五次 `SET`、重写前后检查、关闭重开并断言 `GET == Bytes(b"5")`。

### 4. 动手题：探索安全终止

任务边界：使用现有 test hook 暂停 base 写，设置 `aof_rewrite_delta_limit_bytes=1`，再提交写入；不改生产代码。

验收：重写返回 `AofRewriteFailed("AOF rewrite delta limit exceeded")`；重启后重写前和并发写的值都存在。

??? note "参考答案"

    复用 `open_test_runtime(..., aof_rewrite_gate=True)` 和 `test_rewrite_delta_overflow_preserves_old_aof_history` 的结构。除了失败结果，还必须验证旧 AOF 能完整重启；最后释放 gate 并清理 runtime。

## 小结

在线重写不是“拍快照后覆盖文件”，而是严格排序的 base + delta 协议：同一 mailbox turn 中捕获并注册、限制并发历史、fsync 完整替换文件、原子 rename、持久化目录项。独立快照复用稳定镜像与原子文件原则，恢复则选择最新完整基线并只重放连续后缀。下一章把同一批有序 batch 发送给另一个 runtime，研究何时可以续传而不必全量替换。
