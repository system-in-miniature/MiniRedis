# Stage 24 · Decaying LFU eviction / 衰减式 LFU 淘汰

<!-- journey: chapter=5 tests_added=6 -->

## English

### Goal

Evict the least frequently used logical entry with deterministic time decay, without mutating survivors during planning or inventing access history during recovery and replication.

### Deliverable files

- `src/miniredis/config.py`
- `src/miniredis/core/database.py`
- `src/miniredis/core/eviction.py`
- `src/miniredis/core/executor.py`
- `src/miniredis/core/frequency.py`
- `src/miniredis/runtime.py`
- `tests/contract/test_eviction.py`
- `tests/mechanisms/test_transactions.py`
- `tests/mechanisms/test_watch.py`
- `tests/reliability/test_final_acceptance.py`
- `tests/unit/core/test_domain_types.py`
- `tests/unit/core/test_frequency.py`

### The problem at this point

An ever-increasing hit counter makes an old hot key permanently hot. LFU therefore needs elapsed-time decay, but eviction planning must remain pure: comparing candidates cannot materialize decay on survivors. Metadata updates also need identical clock/config semantics through direct commands, transaction forks, replica apply, snapshots, and recovery.

### Failure preview

Without decay, old traffic dominates forever. If planning writes projected counters back, merely considering eviction changes future victims even when a key survives. If recovery restores live frequency, restart fabricates policy history. If transaction forks share Entry objects, speculative reads mutate live LFU state. Unstable ties make equal-frequency victims depend on map iteration.

### Test contract

<!-- journey-file: tests/unit/core/test_frequency.py -->
#### `tests/unit/core/test_frequency.py`

Locks exact right-shift decay by complete windows, anchor advancement, no time reversal, and input validation. Failure means policy time is ambiguous or non-deterministic.

<!-- journey-file: tests/contract/test_eviction.py -->
#### `tests/contract/test_eviction.py`

Locks lowest effective-frequency eviction, cooling of an old hot key, deterministic survivor selection, and no materialization of projected decay on survivors. It also exposes committed expired/evicted counters.

<!-- journey-file: tests/unit/core/test_domain_types.py -->
#### `tests/unit/core/test_domain_types.py`

Locks client PUT/touch frequency updates, decay-anchor preservation, neutral recovery metadata, and deep-fork independence.

<!-- journey-file: tests/mechanisms/test_transactions.py -->
#### `tests/mechanisms/test_transactions.py`

Locks the transaction-abort counter on dirty EXEC so new observability reports terminal outcomes, not guesses from active state.

<!-- journey-file: tests/mechanisms/test_watch.py -->
#### `tests/mechanisms/test_watch.py`

Locks the WATCH-abort counter when revision validation returns a null array.

<!-- journey-file: tests/reliability/test_final_acceptance.py -->
#### `tests/reliability/test_final_acceptance.py`

Extends zero-owner acceptance with active transaction and watched-key ownership, ensuring LFU observability changes do not weaken lifecycle settlement.

### Basic concepts

Raw frequency counts real accesses. Effective frequency is the raw counter projected through complete decay windows: each window halves it. The decay anchor advances only by elapsed complete windows. LFU candidate order is `(effective_frequency, last_access_tick, key)`, giving deterministic tie-breaking. Projection is observation; materialization happens only on a real touch or put.

### Why this mechanism is necessary

LFU captures popularity better than recency but needs aging to adapt. A pure projection separates policy comparison from state mutation, preserving repeatable planning and transaction speculation. Neutral recovery acknowledges that access policy metadata is runtime-local rather than durable logical data.

### Runtime mental model

Every live Entry owns raw frequency and last-decay time. A client touch projects then increments; a client replacement carries prior metadata through the same rule; replica/recovery puts start neutral. When memory enforcement needs victims, it projects every eligible entry at one `now_ms`, sorts deterministically, and adds eviction deletes until the planned batch fits. Only the committed batch updates counters and state.

### Mechanism blocks

<!-- journey-file: src/miniredis/core/frequency.py -->
#### `src/miniredis/core/frequency.py`

Provides the pure decay function. `frequency >> windows` gives deterministic exponential cooling while `bit_length()` avoids pointless huge shifts.

<!-- journey-file: src/miniredis/core/database.py -->
#### `src/miniredis/core/database.py`

Owns LFU fields and updates them only on real client access/commit; deep forks copy fields, while recovery/replica application starts at zero.

<!-- journey-file: src/miniredis/core/eviction.py -->
#### `src/miniredis/core/eviction.py`

Builds LFU candidates from projected values and stable tie-breakers without assigning those projections back to entries.

```python
return sorted((effective, entry.last_access_tick, key) ...)
```

Candidate comparison is referentially transparent: running it twice at the same time yields the same state and order.

<!-- journey-file: src/miniredis/config.py -->
#### `src/miniredis/config.py`

Adds `allkeys-lfu` and a positive decay interval as explicit validated policy inputs.

<!-- journey-file: src/miniredis/core/executor.py -->
#### `src/miniredis/core/executor.py`

Threads the same clock/decay interval through normal commits, transaction workspaces, replica apply, and touches; counts committed expiry/eviction plus transaction/watch aborts.

<!-- journey-file: src/miniredis/runtime.py -->
#### `src/miniredis/runtime.py`

Wires configuration and exposes key, logical-memory, eviction, expiry, transaction, and WATCH counters in one runtime snapshot.

### Verification evidence

Run all six focused modules in `tests.txt`, cumulatively build Stages 1–24, and require owned-tree parity with `b25b473`.

### Durable takeaways

- Effective frequency decays; raw history does not dominate forever.
- Eviction projection must not mutate survivors.
- LFU metadata is runtime policy state, not recovered logical state.
- Stable tie-breakers are part of deterministic eviction.

### Explain it in your own words

Why does candidate planning project frequency without storing it, while a real GET both materializes decay and increments the counter?

### Textbook

This is an aging LFU cache policy. Lazy projection avoids periodic full-table mutation, while deterministic secondary ordering converts a partial preference into a reproducible total order.

## 中文

### 目标

用确定性时间衰减淘汰逻辑上最不常使用的 Entry，同时不在 Planning 时修改 Survivor，也不在 Recovery/Replication 时虚构 Access History。

### 交付文件

- `src/miniredis/config.py`
- `src/miniredis/core/database.py`
- `src/miniredis/core/eviction.py`
- `src/miniredis/core/executor.py`
- `src/miniredis/core/frequency.py`
- `src/miniredis/runtime.py`
- `tests/contract/test_eviction.py`
- `tests/mechanisms/test_transactions.py`
- `tests/mechanisms/test_watch.py`
- `tests/reliability/test_final_acceptance.py`
- `tests/unit/core/test_domain_types.py`
- `tests/unit/core/test_frequency.py`

### 当前遇到的问题

只增不减的 Hit Counter 会让旧 Hot Key 永远保持 Hot。LFU 因此需要按时间衰减，但 Eviction Planning 必须保持纯函数：比较 Candidate 不能把 Decay 实体化到 Survivor。Metadata Update 还必须在 Direct Command、Transaction Fork、Replica Apply、Snapshot 与 Recovery 中共享同一 Clock/Config 语义。

### 先看会坏在哪里

没有 Decay 时旧流量永久占优。Planning 写回 Projected Counter 会让“仅仅考虑过淘汰”改变后续 Victim，即使 Key 存活。Recovery 恢复 Live Frequency 会在 Restart 时虚构 Policy History。Transaction Fork 共享 Entry 会让推测 Read 修改 Live LFU State。不稳定 Tie 会让等 Frequency Victim 依赖 Map Iteration。

### 测试契约

<!-- journey-file: tests/unit/core/test_frequency.py -->
#### `tests/unit/core/test_frequency.py`

锁定按完整 Window 右移衰减、Anchor 推进、不回退时间与输入校验。失败说明 Policy Time 含糊或不确定。

<!-- journey-file: tests/contract/test_eviction.py -->
#### `tests/contract/test_eviction.py`

锁定最低 Effective-frequency 淘汰、旧 Hot Key 冷却、确定性 Survivor 选择，以及不实体化 Survivor Projected Decay；同时暴露已提交 Expired/Evicted Counter。

<!-- journey-file: tests/unit/core/test_domain_types.py -->
#### `tests/unit/core/test_domain_types.py`

锁定 Client PUT/Touch Frequency Update、Decay-anchor 保留、Recovery 中性 Metadata 与 Deep-fork 独立性。

<!-- journey-file: tests/mechanisms/test_transactions.py -->
#### `tests/mechanisms/test_transactions.py`

锁定 Dirty EXEC 的 Transaction-abort Counter，让新可观测性报告 Terminal Outcome，而不是根据 Active State 猜测。

<!-- journey-file: tests/mechanisms/test_watch.py -->
#### `tests/mechanisms/test_watch.py`

锁定 Revision Validation 返回 Null Array 时的 WATCH-abort Counter。

<!-- journey-file: tests/reliability/test_final_acceptance.py -->
#### `tests/reliability/test_final_acceptance.py`

用 Active Transaction 与 Watched-key Ownership 扩展 Zero-owner Acceptance，保证 LFU 可观测性变更不削弱 Lifecycle Settlement。

### 基本概念

Raw Frequency 统计真实 Access。Effective Frequency 是 Raw Counter 穿过完整 Decay Window 后的 Projection：每个 Window 减半。Decay Anchor 只按已经过去的完整 Window 推进。LFU Candidate Order 是 `(effective_frequency, last_access_tick, key)`，形成确定性 Tie-break。Projection 是观察；只有真实 Touch/Put 才 Materialize。

### 为什么需要这个机制

LFU 比 Recency 更能表达 Popularity，但需要 Aging 才能适应变化。Pure Projection 分离 Policy Comparison 与 State Mutation，保持可重复 Planning 与 Transaction Speculation。Neutral Recovery 承认 Access-policy Metadata 是 Runtime-local，而非 Durable Logical Data。

### 运行时心智模型

每个 Live Entry 持有 Raw Frequency 与 Last-decay Time。Client Touch 先 Project 再 Increment；Client Replacement 用同一规则携带旧 Metadata；Replica/Recovery Put 从 Neutral 开始。Memory Enforcement 需要 Victim 时，在同一 `now_ms` 投影所有 Eligible Entry，确定性排序，并加入 Eviction Delete 直到 Planned Batch 可容纳。只有 Committed Batch 更新 Counter 与 State。

### 机制板块

<!-- journey-file: src/miniredis/core/frequency.py -->
#### `src/miniredis/core/frequency.py`

提供 Pure Decay Function。`frequency >> windows` 给出确定性指数冷却，`bit_length()` 避免无意义的大 Shift。

<!-- journey-file: src/miniredis/core/database.py -->
#### `src/miniredis/core/database.py`

持有 LFU Field，只在真实 Client Access/Commit 时更新；Deep Fork 复制 Field，Recovery/Replica Apply 则从零开始。

<!-- journey-file: src/miniredis/core/eviction.py -->
#### `src/miniredis/core/eviction.py`

用 Projected Value 与稳定 Tie-breaker 建立 LFU Candidate，不把 Projection 赋回 Entry。

```python
return sorted((effective, entry.last_access_tick, key) ...)
```

Candidate Comparison 具有 Referential Transparency：同一时刻执行两次得到相同 State 与 Order。

<!-- journey-file: src/miniredis/config.py -->
#### `src/miniredis/config.py`

加入 `allkeys-lfu` 与正数 Decay Interval 作为显式校验的 Policy Input。

<!-- journey-file: src/miniredis/core/executor.py -->
#### `src/miniredis/core/executor.py`

让同一 Clock/Decay Interval 贯穿 Normal Commit、Transaction Workspace、Replica Apply 与 Touch；统计 Committed Expiry/Eviction 及 Transaction/WATCH Abort。

<!-- journey-file: src/miniredis/runtime.py -->
#### `src/miniredis/runtime.py`

接线配置，并在一个 Runtime Snapshot 中暴露 Key、Logical-memory、Eviction、Expiry、Transaction 与 WATCH Counter。

### 验证证据

运行 `tests.txt` 中六个聚焦模块，累计构建 Stage 1–24，并要求 Owned-tree 与 `b25b473` 一致。

### 需要真正记住的内容

- Effective Frequency 会衰减；Raw History 不会永久占优。
- Eviction Projection 不能修改 Survivor。
- LFU Metadata 是 Runtime Policy State，不是 Recovered Logical State。
- 稳定 Tie-breaker 属于确定性 Eviction 契约。

### 用自己的话讲清楚

为什么 Candidate Planning 只 Project Frequency 而不存储它，但真实 GET 会同时 Materialize Decay 并 Increment Counter？

### 教材

这是 Aging LFU Cache Policy。Lazy Projection 避免周期性全表修改，确定性 Secondary Ordering 则把 Partial Preference 转成可复现 Total Order。
