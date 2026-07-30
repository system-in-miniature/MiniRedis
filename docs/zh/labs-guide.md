# 动手实验

> [English](../labs-guide.md) · 中文版

先在仓库根目录安装：

```bash
uv sync
```

三个示例都调用 Direct API，并且只创建临时状态。

## 1. AOF 崩溃恢复

```bash
uv run python examples/aof_crash_recovery.py
```

预期关键输出：

```text
1. SET before crash: Ok(message=b'OK')
2. Simulating a crash (no graceful AOF drain)...
3. GET after restart: Bytes(value=b'durable')
4. Recovery verified: True
```

重点观察 `AofPolicy.ALWAYS` 边界：成功 `SET` 在释放回复前先跨过 AOF 持久化
闸门。`simulate_crash()` 跳过优雅排空，但重新打开并重放后仍恢复该值。

## 2. 确定性 LFU 淘汰

```bash
uv run python examples/lfu_eviction.py
```

预期关键输出：

```text
cold (least frequent, expected missing): Bytes(value=None)
hot  (expected retained): Bytes(value=b'x')
new  (expected retained): Bytes(value=b'xxx...')
```

重点通过公开回复而非内部计数观察结果。读取 `hot` 四次后，其频率高于 `cold`；
插入 `new` 超过逻辑内存预算，因此确定性的 allkeys-LFU 淘汰 `cold`。

## 3. 部分重同步与全量同步回退

```bash
uv run python examples/replication_resync.py
```

预期关键输出：

```text
Initial attachment: full
Short disconnect resumed with: partial
Cursor older than backlog resumed with: full
```

第一次断连只漏掉仍在 backlog 中的一个批次，复制游标可以续传；第二次漏掉的历史
超过两批 backlog 的保留范围，必须传输完整状态。这是进程内的 PSYNC 决策逻辑
模型，不是 Redis 线复制。

## 继续查看可执行证据

可按[行为矩阵](behavior-matrix.md)找到每个机制对应的 pytest 节点，或运行：

```bash
uv run pytest -q
```
