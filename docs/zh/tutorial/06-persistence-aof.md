> **语言**: [English](../../tutorial/06-persistence-aof.md) | 简体中文

# 持久化 I：追加文件

## 学习目标

学完本章后，你将能够：

- 追踪一次成功写入如何从 `CommitBatch` 到达 AOF 持久性屏障；
- 区分 `always`、`everysec` 和 `no` 的保证，而不是笼统地把三者都称为“持久化”；
- 解释为什么 MiniRedis 只修复不完整的最后一条记录，而不修复任意损坏；
- 运行崩溃—重启实验，并指出写入究竟在哪个时刻被确认；
- 在不改命令 planner 的前提下，为 AOF 策略设计一个边界清晰的测试改动。

## 从内存提交到持久历史

纯内存数据库可能对 `SET` 返回成功，却在进程退出后忘掉它。AOF 通过按顺序记录每个**已经提交**的变更来缩小这个缺口。MiniRedis 不记录原始命令文本，而是记录应用到数据库、随后交给复制的同一个不可变 `CommitBatch`。

在 `src/miniredis/persistence/aof.py` 的 `AofWriter.append` 中，方法接收 `CommitBatch`，通过 `encode_aof_record` 编码，把 `_AppendWork` 放入 writer 队列，然后等待名为 `barrier` 的 future：

```python
barrier = asyncio.get_running_loop().create_future()
self._queue.put_nowait(
    _AppendWork(
        record=encode_aof_record(batch),
        seq=batch.seq,
        barrier=barrier,
    )
)
return await asyncio.shield(barrier)
```

关键不是入队，而是等待 future。入队只代表 AOF worker 接管了工作，并不代表字节已经达到配置要求的持久级别。`src/miniredis/core/executor.py` 的 `CommandExecutor._commit_prepared` 分配下一个序列号，等待 `commit_barrier.append(batch)`，只在得到 `AofAppendOk` 后才应用 batch。追加失败会成为 `DurabilityFailure`，因此内存状态不会在日志失败时继续向前并向客户端报告虚假成功。

顺序可以概括为：

```text
规划变更
  -> 创建 CommitBatch(seq)
  -> 等待 AOF 屏障
  -> 应用到 Database
  -> 保留并提供给复制
  -> 发布成功响应
```

未配置 AOF 时，`src/miniredis/core/executor.py` 的 `NullCommitBarrier.append` 立即返回 `AofAppendOk`。命令流水线不变，只替换屏障实现。这也说明持久化属于核心执行语义，而不属于 Python Direct 或 TCP 适配器。

## 自定义、分帧、二进制安全的文件

MiniRedis AOF **不是** Redis 兼容的命令流。文件以带版本的 header 开始，后面是逻辑记录帧。`src/miniredis/persistence/codec.py` 的 `encode_aof_record` 序列化 `CommitBatch`，`_encode_aof_payload_record` 加上长度和校验和；`scan_aof_bytes` 扫描完整帧并返回有效前缀、state base、batch 以及尾帧是否截断。

长度让任意二进制值也能被准确分隔，校验和则区分完整记录和内容损坏。序列号同样重要：`src/miniredis/persistence/recovery.py` 的 `recover_database` 安装暂存基线，只接受连续的后续 batch。若数据库在序列 7，下一条必须是 8；缺口或乱序会直接失败。

恢复过程不会先暴露半成品 runtime。它新建 `Database`，安装选中的镜像，重放连续记录，清除恢复时已过期的键，最后才返回。`src/miniredis/runtime.py` 的 `MiniRedis._start_owned` 在开放用户 admission 前安装它。

## 三种 fsync 策略，三种不同承诺

写入字节和在断电后仍保留字节不是同一件事。`src/miniredis/persistence/aof.py` 的 `AofPolicy` 明确区分：

| 策略 | `AofWriter._run_writer` 确认前的行为 | 崩溃窗口 |
|---|---|---|
| `always` | 写帧并调用 `fsync` | 已确认 batch 已跨过文件 fsync 屏障 |
| `everysec` | 写帧、标记 dirty，然后确认 | 突然崩溃可能丢失约一个尚未同步的间隔 |
| `no` | 写帧但不由应用调用 fsync | 由操作系统决定何时落到稳定存储 |

`everysec` 会在 `AofWriter.start` 中创建 `_run_everysec`，周期调用 `_sync_dirty`；优雅 `close` 还会同步剩余 dirty 数据。`AofWriter.crash_close` 刻意不做这次最终同步。因此 `MiniRedis.simulate_crash` 模拟的是突然进程丢失，而不是完美模拟所有机器、文件系统和存储控制器。

不要过度解释 `AofAppendOk`：在 `always` 下，它意味着完成配置要求的 fsync；在 `everysec` 和 `no` 下，它只意味着到达较弱的对应屏障。“已确认”描述服务端/客户端事件，策略才定义持久性承诺。`src/miniredis/config.py` 的 `MiniRedisConfig` 默认使用 `everysec`。

## 崩溃恢复与唯一安全的修复

阅读 `src/miniredis/persistence/aof.py` 的 `load_aof`。不存在或零字节的 AOF 被视为空；完整文件原样返回。若 scanner 发现**最后一帧**不完整且 `repair_truncated_tail=True`，函数把文件截到 `scan.valid_offset` 并 fsync。

这种修复很保守：进程可能在写最后一帧中途死亡，因此删除该后缀有明确含义——保留所有完整提交，丢弃从未成为完整记录的提交。完整帧的校验和错误不同，它可能表示历史中部的位损坏。此时函数抛出 `AofCorruption`，不会猜测应该删多少数据。

`tests/unit/persistence/test_aof_repair.py` 验证了这一区别：`test_repair_enabled_truncates_one_incomplete_tail` 保留第一条、截断第二条；`test_checksum_corruption_never_changes_the_file` 要求报错且文件逐字节不变。兼容性边界见[行为矩阵的 AOF 与 Recovery 行](../behavior-matrix.md)：机制被保留，但格式是自定义的。

## 与真实 Redis 对照

真实 Redis 的 AOF 主要位于 `src/aof.c`，用户配置包括 `appendonly` 以及 `appendfsync always/everysec/no`。不同版本可能使用 Redis 命令协议、多文件 manifest、base 文件和增量文件。

可迁移的核心是顺序契约：变更必须按可恢复顺序传播，服务端必须定义成功响应前完成了什么。但差异很大：

- MiniRedis 记录逻辑 `CommitBatch`，不是 Redis 命令；
- header、payload、帧和校验和均为项目自定义；
- 它只有教学用进程内 writer，没有 Redis manifest 管理；
- `everysec` 是直接的定时任务，不复制 Redis 的生产级 bio/fsync 调度与延迟控制。

这些差异记录在[行为矩阵](../behavior-matrix.md)中。`.mraof` 不能由 Redis 打开，`redis-check-aof` 也不是它的修复工具。

## 动手实验：已确认写入经模拟崩溃后仍存在

仓库已经提供 `examples/aof_crash_recovery.py`：

```bash
uv run python examples/aof_crash_recovery.py
```

实测输出：

```text
1. SET before crash: Ok(message=b'OK')
2. Simulating a crash (no graceful AOF drain)...
3. GET after restart: Bytes(value=b'durable')
4. Recovery verified: True
```

脚本配置 `AofPolicy.ALWAYS`。第一行只会在 `SET` 走过 `AofWriter._run_writer` 的写入和 fsync 路径后打印。随后 `MiniRedis.simulate_crash` 使用 `AofWriter.crash_close`，而不是优雅关闭的最终同步。第二个 runtime 在 `MiniRedis._start_owned` 中执行 `recover_database`，所以 `GET` 看到了重放后的值。

这个实验验证当前 MiniRedis 进程及其文件操作契约，不证明所有物理存储都以相同方式实现 fsync，也不使 `everysec` 等价于 `always`。

再运行聚焦回归：

```bash
uv run pytest -q tests/unit/persistence/test_aof_repair.py
```

预期仓库结果：

```text
6 passed
```

## 练习

### 1. 理解题：定位确认边界

为什么 `_queue.put_nowait` 后立即返回不安全？写出把成功响应挡在配置屏障之后的两个函数。

??? note "参考答案"

    队列只表示 worker 接管请求。`AofWriter.append` 等待每项 barrier，`CommandExecutor._commit_prepared` 等待追加结果后才应用数据库 batch 并完成响应。

### 2. 理解题：分类失败窗口

对 `always`、`everysec`、`no` 分别判断：客户端是否可能在该 batch 的应用级 fsync 前收到成功？

??? note "参考答案"

    `always` 不会；worker 先 fsync 再完成 barrier。`everysec` 会；周期任务稍后同步。`no` 会；MiniRedis 不为数据发起逐 batch 或周期 fsync。优雅关闭会缩小 `everysec` 的窗口，突然崩溃不会执行该清理。

### 3. 动手题：增加策略观察测试

任务边界：只在 `tests/unit/persistence/` 增加一个测试，复用现有 fake file-operations；不改 `src/`。记录 `always` 和 `no` 各追加一次时的调用顺序。

验收：

```bash
uv run pytest -q tests/unit/persistence/test_aof_writer.py
```

新断言必须证明 `always` 在 append future 完成前调用 `fsync`，而 `no` 不调用。若只是阅读练习，完成后恢复工作树。

??? note "参考答案"

    复用 `tests/unit/persistence/test_aof_writer.py` 的 fake `AofFileOps` 与调用日志。每种策略启动一个 writer、追加单操作 batch、等待结果，然后在关闭前检查日志。`start` 后先清空日志，以忽略 header 的 fsync。预期 diff 只有一个参数化测试。

### 4. 动手题：证明修复仅限后缀

任务边界：创建含两条完整帧和第三条截断帧的临时 AOF，调用 `load_aof(..., repair_truncated_tail=True)`；不改生产代码。

验收：返回两个 batch，修复后字节严格等于 header 加前两条完整记录。

??? note "参考答案"

    使用 `tests/unit/persistence/test_framing.py` 的 helper 构造序列 1、2、3。写入 `AOF_HEADER + first + second + third[:-1]`，同时比较 `log.batches` 和文件字节。再翻转完整帧校验和中的一个字节，应抛出 `AofCorruption` 且不改文件。

## 小结

MiniRedis AOF 是有序 `CommitBatch` 记录上的持久性屏障，而不是 Redis 线协议命令副本。三种策略在不同持久级别确认；恢复只接受完整连续历史，并只修复含义明确的不完整尾帧。日志无限增长虽正确却低效，下一章将研究如何用稳定 state base 替换历史，同时不丢失重写期间到达的写入。
