# 自主重建

每个 Stage 都是一节可独立浏览的完整课：先理解当前问题、基本概念与必要性，再按机制板块连接相关文件和关键语句，最后用验证证据和自己的话完成理解闭环。

这是三种学习模式中的浏览器自主学习路径。按主题学习请进入[机制教程](../tutorial/index.md)；需要 CLI 互动请查看 [Agent 带教使用教程](../agent-guided.md)。

如果希望在编辑器里聚焦当前增量，运行 `python -m journey.tools.build_journey study N`，再打开 `../MiniRedis-journey-workspace`。

| Stage | 主题 | 新增测试 | 教材章节 |
|---:|---|---:|---:|
| [01](stage-01.md) | 领域状态与提交词汇 | 13 | [2](../tutorial/02-life-of-a-command.md) |
| [02](stage-02.md) | 类型化命令与严格解析 | 31 | [2](../tutorial/02-life-of-a-command.md) |
| [03](stage-03.md) | 串行 Direct 执行器 | 13 | [2](../tutorial/02-life-of-a-command.md) |
| [04](stage-04.md) | 原子 String 规划 | 6 | [3](../tutorial/03-data-types.md) |
| [05](stage-05.md) | Hash 与 List 规划 | 7 | [3](../tutorial/03-data-types.md) |
| [06](stage-06.md) | Set 与 Sorted Set 投影 | 6 | [3](../tutorial/03-data-types.md) |
| [07](stage-07.md) | 绝对 TTL 与有界过期 | 4 | [4](../tutorial/04-expiration.md) |
| [08](stage-08.md) | 确定性淘汰 | 5 | [5](../tutorial/05-eviction.md) |
| [09](stage-09.md) | 请求所有权与终态结果 | 2 | [2](../tutorial/02-life-of-a-command.md) |
| [10](stage-10.md) | 有序 Outbox 与慢 Session | 3 | [2](../tutorial/02-life-of-a-command.md) |
| [11](stage-11.md) | 阻塞 Pop 竞态所有权 | 12 | [9](../tutorial/09-transactions-blocking.md) |
| [12](stage-12.md) | Pub/Sub 与受监督关闭 | 13 | [9](../tutorial/09-transactions-blocking.md) |
| [13](stage-13.md) | 稳定 Commit Batch | 4 | [6](../tutorial/06-persistence-aof.md) |
| [14](stage-14.md) | Canonical 持久化帧 | 16 | [6](../tutorial/06-persistence-aof.md) |
| [15](stage-15.md) | AOF 提交屏障 | 11 | [6](../tutorial/06-persistence-aof.md) |
| [16](stage-16.md) | Snapshot Capture 与恢复 | 20 | [7](../tutorial/07-rewrite-snapshot.md) |
| [17](stage-17.md) | 异步复制 | 5 | [8](../tutorial/08-replication.md) |
| [18](stage-18.md) | 晋升与受监督生命周期 | 7 | [8](../tutorial/08-replication.md) |
| [19](stage-19.md) | RESP2 协议边界 | 3 | [10](../tutorial/10-protocol.md) |
| [20](stage-20.md) | TCP Runtime 一致性 | 4 | [10](../tutorial/10-protocol.md) |
| [21](stage-21.md) | 批量 String 与方向性阻塞 Pop | 5 | [3](../tutorial/03-data-types.md) |
| [22](stage-22.md) | 有序 Pipeline | 2 | [10](../tutorial/10-protocol.md) |
| [23](stage-23.md) | 事务与原子函数 | 7 | [9](../tutorial/09-transactions-blocking.md) |
| [24](stage-24.md) | 衰减式 LFU 淘汰 | 6 | [5](../tutorial/05-eviction.md) |
| [25](stage-25.md) | AOF 状态基线 | 8 | [7](../tutorial/07-rewrite-snapshot.md) |
| [26](stage-26.md) | 在线 AOF 重写 | 2 | [7](../tutorial/07-rewrite-snapshot.md) |
| [27](stage-27.md) | 复制积压日志 | 2 | [8](../tutorial/08-replication.md) |
| [28](stage-28.md) | 部分重新同步 | 4 | [8](../tutorial/08-replication.md) |
| [29](stage-29.md) | Primary 持有过期删除 | 8 | [8](../tutorial/08-replication.md) |
| [30](stage-30.md) | 公共一致性与阅读地图 | 0 | [11](../tutorial/index.md) |
