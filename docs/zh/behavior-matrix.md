> **语言**: [English](../behavior-matrix.md) | 简体中文

# MiniRedis 行为矩阵

| 领域（Area） | 已实现子集 | 直接调用证据 | RESP 证据 | 有意保留的差异 |
|---|---|---|---|---|
| 字符串（String） | `GET`、`MGET`、带条件/过期选项的 `SET`、原子 `MSET`、`INCR`、`DECR`、`INCRBY`、`COMPAREDEL`、`CHECKDECR` | `tests/contract/test_atomic_functions.py::test_checkdecr_is_single_winner_and_queues_inside_transaction` | `tests/adapters/test_tcp_async_semantics.py::test_tcp_pipeline_preserves_reply_order_with_invalid_middle_command` | 仅接受冻结选项；严格解析规范 int64；以两个显式原子函数取代 Lua |
| 哈希（Hash） | `HSET`、`HGET`、`HDEL`、`HGETALL`、`HINCRBY` | `tests/contract/test_hashes.py::test_hash_semantics_and_last_field_removal` | `tests/adapters/test_direct_resp_parity.py::test_selected_sequence_has_state_and_reply_parity` | 不规定 `HGETALL` 的顺序 |
| 列表（List） | 推入、弹出、范围查询，以及感知方向的 `BLPOP` 和 `BRPOP` | `tests/mechanisms/test_blpop.py::test_blocked_brpop_preserves_right_pop_direction_when_woken` | `tests/adapters/test_tcp_async_semantics.py::test_blpop_does_not_block_another_connection` | 阻塞等待者保留键优先级和弹出方向 |
| 流水线（Pipeline） | 有序的直接调用提交，以及由 RESP2 适配器合并的提交 | `tests/adapters/test_direct_pipeline.py::test_direct_pipeline_preserves_result_slots_and_is_not_atomic` | `tests/adapters/test_tcp_async_semantics.py::test_tcp_pipeline_submits_all_decoded_frames_before_first_executes` | 只在适配器层批处理：不提供原子执行、回滚或跨客户端隔离 |
| 事务（Transactions） | `MULTI`、`EXEC`、`DISCARD`、`WATCH`、`UNWATCH`；最终形成一个提交批次 | `tests/mechanisms/test_transactions.py::test_exec_reads_prior_write_and_keeps_runtime_error_slots` | `tests/adapters/test_resp2_mapping.py::test_domain_replies_encode_as_resp2` 中的空数组映射 | 入队期错误会中止；运行期错误保留结果槽位；成功命令不会回滚。与 Redis 不同，MiniRedis 禁止在 `MULTI` 内使用 `PUBLISH` 和 `BLPOP`/`BRPOP`；`MULTI` 内的 `WATCH` 既会报错，也会将事务标脏，因此后续 `EXEC` 返回 `EXECABORT`（Redis 会报告 `WATCH` 错误，但不会将事务标脏）。`EXEC` 在求值命令前深拷贝整个数据库，而 Redis 直接在实时数据库上执行。 |
| 集合（Set） | 添加、移除、成员判断、成员查询与交集 | `tests/contract/test_sets.py::test_smembers_sinter_and_last_member_removal` | `tests/adapters/test_direct_resp_parity.py::test_selected_sequence_has_state_and_reply_parity` | 不规定结果顺序 |
| 有序集合（Sorted Set） | 基本的添加、移除、分数、排名和分数范围查询 | `tests/contract/test_sorted_sets.py::test_zset_orders_equal_scores_by_member_bytes` | `tests/adapters/test_direct_resp_parity.py::test_selected_sequence_has_state_and_reply_parity` | 使用字典存储并按 `(score, member)` 确定性排序。`ZSCORE` 使用 Python 的 `repr(float)` 格式（例如 `1.0`），而不是 Redis 的 `%.17g` 风格输出（例如 `1`）。 |
| 生存时间（TTL） | 绝对截止时间、惰性可见性、有限主动清理 | `tests/contract/test_ttl.py::test_expire_ttl_persist_and_bounded_active_cleanup` | 不要求 | 注入时钟/调度器；不为每个键单独设置计时器 |
| 淘汰（Eviction） | `noeviction`、精确 `allkeys-lru`、确定性衰减 `allkeys-lfu`、逻辑字节预算 | `tests/contract/test_eviction.py::test_lfu_planning_projects_without_materializing_survivor_decay` | 不要求 | 使用精确排序与确定性减半，而非 Redis 的采样 LRU、概率 LFU 或 RSS |
| 发布/订阅（Pub/Sub） | 精确频道、有界、至多一次扇出 | `tests/reliability/test_phase3_invariants.py::test_waiters_and_pubsub_allocate_no_commit_or_durable_state` | `tests/adapters/test_tcp_async_semantics.py::test_subscribe_message_ping_unsubscribe_share_one_ordered_outbox` | 不支持模式匹配、确认、重试、历史或重放 |
| 追加文件（AOF） | 带版本和帧结构的 `CommitBatch` 流；`always`、`everysec`、`no`；通过状态基线加有序增量进行有界在线重写 | `tests/unit/persistence/test_aof_writer.py::test_parent_fsync_failure_after_rename_is_terminal` | `tests/reliability/test_aof_rewrite.py::test_write_during_paused_base_survives_rewrite_and_restart` | 自定义格式，并非 Redis AOF；重命名前的失败不是终止性的，重命名后的不确定状态是终止性的 |
| 快照（Snapshot） | 稳定检查点镜像与原子替换 | `tests/reliability/test_snapshot_barrier.py::test_snapshot_barrier_captures_seq_and_only_logically_live_keys` | `tests/reliability/test_final_acceptance.py::test_final_acceptance_activates_components_then_leaves_no_owners` | 自定义格式，并非 Redis RDB |
| 恢复（Recovery） | 仅快照、旧版 AOF、状态基线 AOF，或组合使用最新基线的分阶段安装 | `tests/unit/persistence/test_recovery.py::test_recovery_prefers_newer_aof_state_base` | 不适用 | 检查点相同时 AOF 基线优先于快照；仅最后一条不完整的 AOF 记录可修复 |
| 副本（Replica） | 进程内异步接收端；逻辑历史 ID；有界批次积压；手动完整/部分重新挂接；延迟、溢出与带隔离的晋升 | `tests/replication/test_partial_resync.py::test_cursor_exactly_before_oldest_backlog_batch_is_partial`; `tests/replication/test_sink_attach.py::test_replica_logically_hides_expired_key_without_advancing_sequence` | `tests/reliability/test_final_acceptance.py::test_final_acceptance_activates_components_then_leaves_no_owners` | 重启/晋升时生成新 ID；已确认写入仍可能丢失；没有 PSYNC 线协议、心跳、`WAIT`、选举或自动故障转移。与 Redis 一样，副本会在逻辑上隐藏过期键，但不会主动删除它们或创建本地过期提交；它会等待主节点传播删除操作。 |
| 生命周期（Lifecycle） | 幂等关闭且所拥有资源归零 | `tests/reliability/test_reliability_shutdown.py::test_cancelling_one_close_waiter_cannot_cancel_cleanup` | `tests/reliability/test_final_acceptance.py::test_final_acceptance_activates_components_then_leaves_no_owners` | 有界排空输出与副本；传输关闭后不保证交付 |

所有变更类行还共同具有一个实现层面的复杂度差异：
`Database.apply_batch` 每次提交都会复制整个键表并重新计算逻辑用量
（按键数量计为 O(N)）。真实 Redis 通常原地修改键空间和内存计量，因此普通单键更新的平均复杂度为 O(1)，不计命令自身的数据结构工作。MiniRedis 额外付出的成本换来了简单的暂存和不变量检查；它不是生产性能模型。
