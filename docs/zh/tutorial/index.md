# MiniRedis 教程

> **语言：** [English](../../tutorial/index.md) | 简体中文

MiniRedis 是一个可执行的 Redis 核心机制教学内核。本教程按依赖顺序讲解类型化命令、
串行执行、数据结构、过期、淘汰、持久化、复制、事务、阻塞操作与 RESP2。请按章节
顺序学习；每章都会把源码、实测实验和 MiniRedis 与真实 Redis 的差异放在一起。

MiniRedis is an executable teaching kernel for the core mechanisms behind
Redis. The tutorial connects each mechanism to source code, measured commands,
and an explicit compatibility boundary.

## 章节目录

| # | 中文章名 | English chapter | 核心机制 |
|---:|---|---|---|
| 01 | [认识 MiniRedis](01-getting-started.md) | Meet MiniRedis | 运行时生命周期、Direct API 与全书地图 |
| 02 | [一条命令的一生](02-life-of-a-command.md) | The Life of a Command | 解析、规划、提交、应用、传播、回复 |
| 03 | [数据类型与命令面](03-data-types.md) | Data Types and the Command Surface | 五种值模型、分类 planner、`WRONGTYPE` |
| 04 | [过期](04-expiration.md) | Expiration | 懒惰可见性、有界主动清理、副本安全 |
| 05 | [内存与淘汰](05-eviction.md) | Memory and Eviction | 逻辑 maxmemory、精确 LRU、确定性 LFU |
| 06 | [持久化 I：AOF](06-persistence-aof.md) | Persistence I: AOF | 追加、fsync 策略、恢复、尾部修复 |
| 07 | [持久化 II：重写与快照](07-rewrite-snapshot.md) | Persistence II: Rewrite and Snapshots | base + delta、原子替换、分阶段恢复 |
| 08 | [复制](08-replication.md) | Replication | 身份、offset、backlog、部分/全量同步 |
| 09 | [事务与阻塞](09-transactions-blocking.md) | Transactions and Blocking | `MULTI`/`EXEC`、`WATCH`、单批次、阻塞等待者 |
| 10 | [协议层](10-protocol.md) | Protocol Layer | Direct adapter、RESP2/TCP、redis-py 互通 |

## 如何使用本教程

执行 `uv sync --dev` 后，所有命令都应从仓库根目录运行。源码锚点采用
“相对路径 + 函数或类名”的形式；指向
[行为矩阵](../../behavior-matrix.md) 的链接定义兼容性边界。需要改代码的练习只在折叠
参考答案中给出说明和关键 diff，教程本身不会修改 `src/`。

完成项目及其测试是本教程的证据基础。如果实验结果与文中的实测输出不同，先核对当前
分支、配置和 Python 环境，再使用引用的函数与聚焦 pytest 节点定位问题。
