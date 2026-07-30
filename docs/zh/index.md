# MiniRedis 教程

> [English](../index.md) · 中文快速开始

MiniRedis 是一个直接 API 优先、二进制安全的 Redis 机制模型，覆盖命令执行、
过期、淘汰、事务、持久化、Pub/Sub 与可续传复制。类型化命令先无副作用地生成
执行计划，跨过持久化边界后，再以不可变提交批次应用并传播。

English summary: MiniRedis exposes command planning, commit, persistence, and
replication boundaries in a small executable model rather than replacing Redis.

## 安装

```bash
git clone https://github.com/system-in-miniature/mini-redis.git
cd MiniRedis
uv sync
```

需要 Python 3.12+ 与 [uv](https://docs.astral.sh/uv/)。

## 第一个实验

运行确定性的 LFU 淘汰示例：

```bash
uv run python examples/lfu_eviction.py
```

它创建同样小的 `hot` 与 `cold` 键，读取 `hot` 四次，再加入一个更大的键。
最终的公开 `GET` 观察中，`cold` 消失，而 `hot` 与 `new` 仍然存在。

接着阅读[架构指南](architecture.md)及其 [Redis 机制映射](redis-mapping.md)。
完整命令集、适配器、兼容性边界与验证命令见
[仓库中文 README](https://github.com/system-in-miniature/mini-redis/blob/main/README.zh-CN.md)。
