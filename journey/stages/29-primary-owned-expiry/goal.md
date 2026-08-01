# Stage 29 · Primary-owned expiry / Primary 持有过期删除

<!-- journey: chapter=8 tests_added=8 -->

## English

### Goal

Make the primary the only owner of physical expiry commits while replicas still hide expired values logically, keeping replica sequence, backlog, AOF, transactions, and promotion aligned to one propagated history.

### Deliverable files

- `src/miniredis/core/executor.py`
- `src/miniredis/replication/sink.py`
- `src/miniredis/runtime.py`
- `tests/contract/test_domain_invariants.py`
- `tests/contract/test_eviction.py`
- `tests/contract/test_ttl.py`
- `tests/mechanisms/test_blpop_push_batch.py`
- `tests/reliability/test_final_acceptance.py`
- `tests/reliability/test_transaction_commit.py`
- `tests/replication/test_partial_resync.py`
- `tests/replication/test_sink_attach.py`

### The problem at this point

Replica reads already know an expired entry is logically absent, but their executor can still turn lazy GET or active-expire ticks into local delete commits. That independently advances replica sequence and breaks alignment with the primary's propagated batch stream. The periodic expiry producer also continues running after a runtime switches from primary to replica unless role transitions explicitly quiesce it.

### Failure preview

A replica GET at the deadline can allocate a local sequence before the primary sends DELETE; the next primary batch then appears non-contiguous. Active expiry can create the same split without user traffic. Marking the sink cursor before snapshot installation succeeds advertises state the replica does not yet hold. Failed resume validation can also leave a registered primary link, and always recording applied batches turns a debug aid into unbounded production memory.

### Test contract

<!-- journey-file: tests/replication/test_sink_attach.py -->
#### `tests/replication/test_sink_attach.py`

Locks logical expiry without local replica commit, quiesced active expiry, later primary delete propagation, full-sync cursor publication only after install, and neutral LFU metadata.

<!-- journey-file: tests/replication/test_partial_resync.py -->
#### `tests/replication/test_partial_resync.py`

Locks side-effect-free replication stats and detachment of the primary link when replica resume validation fails.

<!-- journey-file: tests/reliability/test_final_acceptance.py -->
#### `tests/reliability/test_final_acceptance.py`

Locks public replication identity, primary sequence, backlog presence, and full-sync counters alongside existing owner/durability acceptance.

<!-- journey-file: tests/contract/test_domain_invariants.py -->
#### `tests/contract/test_domain_invariants.py`

Locks applied-batch history as opt-in debug instrumentation and keeps default production recording empty.

<!-- journey-file: tests/contract/test_eviction.py -->
#### `tests/contract/test_eviction.py`

Opts eviction history assertions into explicit applied-batch recording while retaining LRU/LFU/expiry commit contracts.

<!-- journey-file: tests/contract/test_ttl.py -->
#### `tests/contract/test_ttl.py`

Opts TTL batch inspection into explicit debug recording so primary expiry evidence remains intentional.

<!-- journey-file: tests/mechanisms/test_blpop_push_batch.py -->
#### `tests/mechanisms/test_blpop_push_batch.py`

Opts the one-batch push/wakeup assertion into debug recording without changing blocking semantics.

<!-- journey-file: tests/reliability/test_transaction_commit.py -->
#### `tests/reliability/test_transaction_commit.py`

Opts the one-AOF-batch transaction assertion into debug recording while preserving durable recovery evidence.

### Basic concepts

Logical expiry is a read rule: an entry at or past its deadline behaves absent. Physical expiry is a state transition: a delete operation consumes a commit sequence and propagates. In primary–replica replication, only the primary owns that transition; replicas retain the physical entry until the propagated delete arrives. A role-aware producer must quiesce on replica install/resume and restart on promotion.

### Why this mechanism is necessary

Replication requires one authoritative transition history. Letting every node independently materialize wall-clock expiry creates multiple histories even with identical clocks. Separating logical visibility from physical deletion preserves read semantics without sacrificing sequence continuity, AOF/backlog identity, or future partial resync.

### Runtime mental model

On a primary, lazy lookup and active-expire ticks may prepare deletes and commit them normally. On a read-only replica, planning can return null for an expired key but `_apply_plan` suppresses its prepared commit, and active expiry returns zero. Snapshot install or partial resume asks the runtime to quiesce the producer before reporting attachment success. Promotion changes the executor role and restarts the producer. The primary's eventual delete batch becomes the only physical removal on both nodes.

### Mechanism blocks

<!-- journey-file: src/miniredis/core/executor.py -->
#### `src/miniredis/core/executor.py`

Suppresses prepared commits and active expiry while replica-read-only, makes applied-batch recording opt-in, and adds reading-map documentation plus replication statistics.

```python
if plan.prepared_commit is not None and not self._replica_read_only:
    await self._commit_prepared(plan.prepared_commit)
```

The reply still reflects logical expiry, but no local mutation or sequence is created on the replica.

<!-- journey-file: src/miniredis/runtime.py -->
#### `src/miniredis/runtime.py`

Owns the active-expire producer across role changes, wraps install/resume/promotion, exposes stable replication stats, and passes explicit debug-history configuration.

<!-- journey-file: src/miniredis/replication/sink.py -->
#### `src/miniredis/replication/sink.py`

Publishes replication ID/applied cursor only after install succeeds, routes role transitions through runtime wrappers, and detaches failed resume links.

### Verification evidence

Run all eight focused modules in `tests.txt`, cumulatively build Stages 1–29, and require owned-tree parity with `94109b0`.

### Durable takeaways

- Logical expiry and physical deletion are different operations.
- Only the primary creates expiry commits in replicated history.
- Expiry producers follow runtime role transitions.
- Debug commit-history retention is explicit and opt-in.

### Explain it in your own words

How can a replica return null for an expired key while keeping the physical entry and sequence unchanged, and why is that necessary for the next primary batch?

### Textbook

This is single-leader ownership of time-triggered state transitions. Followers may derive read visibility locally, but replicated mutation authority remains centralized to preserve one log.

## 中文

### 目标

让 Primary 成为 Physical Expiry Commit 的唯一 Owner，同时 Replica 仍逻辑隐藏 Expired Value，使 Replica Sequence、Backlog、AOF、Transaction 与 Promotion 对齐同一 Propagated History。

### 交付文件

- `src/miniredis/core/executor.py`
- `src/miniredis/replication/sink.py`
- `src/miniredis/runtime.py`
- `tests/contract/test_domain_invariants.py`
- `tests/contract/test_eviction.py`
- `tests/contract/test_ttl.py`
- `tests/mechanisms/test_blpop_push_batch.py`
- `tests/reliability/test_final_acceptance.py`
- `tests/reliability/test_transaction_commit.py`
- `tests/replication/test_partial_resync.py`
- `tests/replication/test_sink_attach.py`

### 当前遇到的问题

Replica Read 已知道 Expired Entry 在逻辑上缺失，但 Executor 仍可能把 Lazy GET 或 Active-expire Tick 变成 Local Delete Commit。这会独立推进 Replica Sequence，破坏与 Primary Propagated Batch Stream 的对齐。Runtime 从 Primary 切到 Replica 后，Periodic Expiry Producer 也会继续运行，除非 Role Transition 显式 Quiesce。

### 先看会坏在哪里

Replica 在 Deadline GET 可先分配 Local Sequence，Primary 随后的 DELETE 就显得不连续。Active Expiry 无需用户流量也会造成同一分叉。Snapshot Install 成功前设置 Sink Cursor 会宣称 Replica 已持有尚未安装的 State。Resume Validation 失败还可能遗留 Registered Primary Link，而始终记录 Applied Batch 会让 Debug Aid 变成无界 Production Memory。

### 测试契约

<!-- journey-file: tests/replication/test_sink_attach.py -->
#### `tests/replication/test_sink_attach.py`

锁定 Logical Expiry 不产生 Local Replica Commit、Quiesced Active Expiry、Later Primary Delete Propagation、Full-sync Cursor 仅在 Install 后发布，以及中性 LFU Metadata。

<!-- journey-file: tests/replication/test_partial_resync.py -->
#### `tests/replication/test_partial_resync.py`

锁定无副作用 Replication Stats，以及 Replica Resume Validation 失败时 Detach Primary Link。

<!-- journey-file: tests/reliability/test_final_acceptance.py -->
#### `tests/reliability/test_final_acceptance.py`

在既有 Owner/Durability Acceptance 旁锁定 Public Replication Identity、Primary Sequence、Backlog Presence 与 Full-sync Counter。

<!-- journey-file: tests/contract/test_domain_invariants.py -->
#### `tests/contract/test_domain_invariants.py`

锁定 Applied-batch History 是 Opt-in Debug Instrumentation，默认 Production Recording 为空。

<!-- journey-file: tests/contract/test_eviction.py -->
#### `tests/contract/test_eviction.py`

让 Eviction History Assertion 显式开启 Applied-batch Recording，同时保留 LRU/LFU/Expiry Commit Contract。

<!-- journey-file: tests/contract/test_ttl.py -->
#### `tests/contract/test_ttl.py`

让 TTL Batch Inspection 显式开启 Debug Recording，使 Primary Expiry Evidence 保持有意图。

<!-- journey-file: tests/mechanisms/test_blpop_push_batch.py -->
#### `tests/mechanisms/test_blpop_push_batch.py`

让 One-batch Push/Wakeup Assertion 开启 Debug Recording，不改变 Blocking Semantics。

<!-- journey-file: tests/reliability/test_transaction_commit.py -->
#### `tests/reliability/test_transaction_commit.py`

让 One-AOF-batch Transaction Assertion 开启 Debug Recording，同时保留 Durable Recovery Evidence。

### 基本概念

Logical Expiry 是 Read Rule：Entry 到达 Deadline 后表现为 Absent。Physical Expiry 是 State Transition：Delete Operation 消耗 Commit Sequence 并传播。在 Primary–replica Replication 中只有 Primary 持有该 Transition；Replica 保留 Physical Entry，直到 Propagated Delete 到达。Role-aware Producer 必须在 Replica Install/Resume 时 Quiesce，在 Promotion 时 Restart。

### 为什么需要这个机制

Replication 需要一条 Authoritative Transition History。让每个 Node 按 Wall Clock 独立 Materialize Expiry，即使 Clock 一致也会产生多条 History。分离 Logical Visibility 与 Physical Deletion，既保持 Read Semantics，也不牺牲 Sequence Continuity、AOF/Backlog Identity 与后续 Partial Resync。

### 运行时心智模型

Primary 上，Lazy Lookup 与 Active-expire Tick 可以准备 Delete 并正常 Commit。Read-only Replica 上，Planning 可为 Expired Key 返回 Null，但 `_apply_plan` 抑制 Prepared Commit，Active Expiry 返回零。Snapshot Install 或 Partial Resume 请求 Runtime 在报告 Attachment Success 前 Quiesce Producer。Promotion 改变 Executor Role 并 Restart Producer。Primary 最终 Delete Batch 成为两个 Node 上唯一 Physical Removal。

### 机制板块

<!-- journey-file: src/miniredis/core/executor.py -->
#### `src/miniredis/core/executor.py`

在 Replica-read-only 时抑制 Prepared Commit 与 Active Expiry，让 Applied-batch Recording Opt-in，并加入 Reading-map Documentation 与 Replication Stats。

```python
if plan.prepared_commit is not None and not self._replica_read_only:
    await self._commit_prepared(plan.prepared_commit)
```

Reply 仍反映 Logical Expiry，但 Replica 不创建 Local Mutation 或 Sequence。

<!-- journey-file: src/miniredis/runtime.py -->
#### `src/miniredis/runtime.py`

跨 Role Change 持有 Active-expire Producer，包装 Install/Resume/Promotion，暴露稳定 Replication Stats，并传递显式 Debug-history Config。

<!-- journey-file: src/miniredis/replication/sink.py -->
#### `src/miniredis/replication/sink.py`

只在 Install 成功后发布 Replication ID/Applied Cursor，通过 Runtime Wrapper 路由 Role Transition，并 Detach Failed Resume Link。

### 验证证据

运行 `tests.txt` 中八个聚焦模块，累计构建 Stage 1–29，并要求 Owned-tree 与 `94109b0` 一致。

### 需要真正记住的内容

- Logical Expiry 与 Physical Deletion 是不同操作。
- 只有 Primary 在 Replicated History 中创建 Expiry Commit。
- Expiry Producer 跟随 Runtime Role Transition。
- Debug Commit-history Retention 显式 Opt-in。

### 用自己的话讲清楚

Replica 如何为 Expired Key 返回 Null，却保持 Physical Entry 与 Sequence 不变？为什么这对下一条 Primary Batch 必要？

### 教材

这是 Single-leader Ownership of Time-triggered State Transition。Follower 可以本地派生 Read Visibility，但 Replicated Mutation Authority 保持集中，从而维护单一 Log。
