# Stage 08 · Deterministic eviction / 确定性淘汰

<!-- journey: chapter=5 tests_added=5 -->

## English

### Goal

Enforce a logical maxmemory budget with atomic noeviction and exact-LRU decisions.

### Deliverable files

- `src/miniredis/core/eviction.py`
- `src/miniredis/core/planner.py`
- `src/miniredis/runtime.py`
- `tests/contract/test_domain_invariants.py`
- `tests/contract/test_eviction.py`

### The problem at this point

Atomic command plans can still grow without a budget. Reading process RSS would make outcomes allocator-dependent, while evicting first and discovering that the target itself is oversized would destroy unrelated data for a command that ultimately fails.

### Failure preview

An oversized target must return OOM without removing an existing key. Under exact LRU, the cold-key delete and the triggering put must share one commit; under noeviction, growth fails but a client delete remains legal. Expired bytes must be reclaimed before any of these choices.

### Test contract

<!-- journey-file: tests/contract/test_domain_invariants.py -->
#### `tests/contract/test_domain_invariants.py`

##### What this test locks

It locks no-commit WRONGTYPE behavior across value families and proves that emitted batches rebuild the same logical database.

##### How it constructs the counterexample

It sends each read command to the wrong value type, then separately replays every observed batch into a fresh `Database`.

##### Key test statement

```python
for batch in batches:
    replay.apply_batch(batch, track_access=False)
assert replay.logical_items() == expected
```

##### What a failure means

A semantic failure allocated a commit, or live database changes escaped the operation log and cannot be replayed later.

<!-- journey-file: tests/contract/test_eviction.py -->
#### `tests/contract/test_eviction.py`

##### What this test locks

It locks oversized-target safety, exact LRU, one-batch victim publication, noeviction shrink behavior, and expired-budget reclamation.

##### How it constructs the counterexample

It makes one key hot, attempts an impossible target, and inspects both commit sequence and the operations inside the accepted write batch.

##### Key test statement

```python
assert r.debug_commit_seq == before + 1
```

##### What a failure means

Eviction occurred as a separate visible mutation, an OOM command caused damage, or policy rejected an operation that reduces usage.

### Basic concepts

MiniRedis budgets a deterministic logical size derived from keys, values, and expiry metadata; it does not promise process-memory accounting. Exact LRU orders candidates by access tick and key. `noeviction` prevents net growth over budget but does not forbid deletes or other usage-reducing plans.

### Why this mechanism is necessary

Eviction is part of accepting one command, not background cleanup. Planning the target, expiry cleanup, victim deletes, and final put together preserves all-or-nothing publication and creates one replayable decision for future persistence and replication.

### Runtime mental model

The normal family planner first produces semantic reply and operations. The memory policy projects their post-commit usage over a copied size map. It rejects an individually oversized target immediately, includes expired deletes, and if necessary adds deterministic cold victims until the whole plan fits. Only then does the executor allocate one commit sequence.

### Mechanism blocks

<!-- journey-file: src/miniredis/core/eviction.py -->
#### `src/miniredis/core/eviction.py`

##### What it is and why it appears

This policy layer transforms a successful semantic plan into either an OOM failure or another complete plan containing required cleanup and victims.

##### Runtime role

It calculates projected usage without mutation, refuses impossible target entries, reclaims expired entries first, and selects exact-LRU candidates outside the target set.

##### Key code

```python
candidates = sorted(
    (entry.last_access_tick, key)
    for key, entry in database.entries.items()
    if key not in target_keys
    and key not in already_deleted
    and not is_expired(entry, now_ms)
)
```

##### Statement understanding

Sorting `(tick, key)` provides deterministic oldest-first selection and a byte-key tie-break. Target keys cannot be evicted to pretend their own write fits.

<!-- journey-file: src/miniredis/core/planner.py -->
#### `src/miniredis/core/planner.py`

##### What it is and why it appears

The planner facade becomes the composition point between command semantics and global memory policy.

##### Runtime role

Every recognized family plan passes through the same budget enforcement before reaching the executor.

##### Key code

```python
if plan is not None:
    return enforce_memory(plan, database, self.config, now_ms)
```

##### Statement understanding

Policy runs after command semantics are known but before a commit exists, so rejection remains side-effect free.

<!-- journey-file: src/miniredis/runtime.py -->
#### `src/miniredis/runtime.py`

##### What it is and why it appears

The runtime exposes a frozen logical view for the replay invariant without making its mutable entry map a public API.

##### Runtime role

Tests compare the live logical state with a fresh database reconstructed only from applied batches.

##### Key code

```python
def debug_logical_items(self) -> tuple[tuple[bytes, StoredEntry], ...]:
    return self.database.logical_items()
```

##### Statement understanding

The diagnostic observes the result; it does not provide a bypass around the executor's single-writer boundary.

### Verification evidence

Run `uv run pytest -q $(cat journey/stages/08-deterministic-eviction/tests.txt)`. It proves policy behavior, atomic batch composition, wrong-type no-commit behavior, and operation-log replay through public commands plus narrow diagnostics.

### Durable takeaways

Budget logical state, not RSS; reject impossible targets before choosing victims; purge expired entries first; allow shrinking plans; publish victim deletes and the accepted mutation in one batch; keep all live state reconstructible from commits.

### Explain it in your own words

Eviction wraps an already planned command. It asks what the complete post-commit database would cost, then either returns OOM unchanged or adds enough deterministic deletes to make that exact command fit. The executor still sees and publishes only one plan.

### Textbook

[Chapter 5](https://github.com/system-in-miniature/mini-redis/blob/main/docs/tutorial/05-eviction.md)

## 中文

### 目标

用原子 Noeviction 和精确 LRU 决策执行逻辑 Maxmemory 预算。

### 交付文件

- `src/miniredis/core/eviction.py`
- `src/miniredis/core/planner.py`
- `src/miniredis/runtime.py`
- `tests/contract/test_domain_invariants.py`
- `tests/contract/test_eviction.py`

### 当前遇到的问题

原子命令 Plan 仍可无限增长。如果读取进程 RSS，结果会依赖 Allocator；如果先淘汰、后发现目标本身过大，则一个最终失败的命令会毁掉无关数据。

### 先看会坏在哪里

过大目标必须返回 OOM，且不删除已有 Key。精确 LRU 下，冷 Key 删除与触发它的 Put 必须属于同一 Commit；Noeviction 下，增长失败，但客户端删除仍合法。所有决策前还要先回收过期字节。

### 测试契约

<!-- journey-file: tests/contract/test_domain_invariants.py -->
#### `tests/contract/test_domain_invariants.py`

##### 测试锁定什么

它锁定跨值族 WRONGTYPE 不提交，并证明已发出 Batch 可重建同一逻辑 Database。

##### 如何构造反例

它把每个读命令发给错误值类型，再单独把所有观察到的 Batch 重放到全新 `Database`。

##### 关键测试语句

```python
for batch in batches:
    replay.apply_batch(batch, track_access=False)
assert replay.logical_items() == expected
```

##### 失败意味着什么

语义失败分配了 Commit，或实时 Database 变化逃离操作日志，无法在后续重放。

<!-- journey-file: tests/contract/test_eviction.py -->
#### `tests/contract/test_eviction.py`

##### 测试锁定什么

它锁定过大目标安全性、精确 LRU、单 Batch Victim 发布、Noeviction 缩减行为与过期预算回收。

##### 如何构造反例

它把一个 Key 变热，尝试一个不可能容纳的目标，并同时检查 Commit Sequence 与已接受写入 Batch 内的操作。

##### 关键测试语句

```python
assert r.debug_commit_seq == before + 1
```

##### 失败意味着什么

淘汰作为独立可见变更发生，OOM 命令造成了破坏，或 Policy 拒绝了减少用量的操作。

### 基本概念

MiniRedis 预算由 Key、Value 与过期元数据导出的确定逻辑大小，不承诺进程内存计量。精确 LRU 按 Access Tick 与 Key 排序候选者。`noeviction` 阻止超预算净增长，但不禁止删除或其他降低用量的 Plan。

### 为什么需要这个机制

淘汰是接受一条命令的一部分，不是后台清理。把目标、过期清理、Victim 删除与最终 Put 一起规划，才能保持全有全无发布，并为未来持久化与复制留下单个可重放决策。

### 运行时心智模型

普通命令族 Planner 先生成语义 Reply 与操作。Memory Policy 在复制的 Size Map 上投影提交后用量，立即拒绝单个过大目标，包入过期删除，并在需要时追加确定冷 Victim，直到完整 Plan 可容纳。只有此后 Executor 才分配一个 Commit Sequence。

### 机制板块

<!-- journey-file: src/miniredis/core/eviction.py -->
#### `src/miniredis/core/eviction.py`

##### 是什么，为什么现在需要

这个 Policy 层把成功语义 Plan 变成 OOM 失败，或另一个包含必要清理与 Victim 的完整 Plan。

##### 在运行时做什么

它在不变更状态的前提下计算投影用量，拒绝不可能的目标 Entry，先回收过期 Entry，再从目标集外选择精确 LRU 候选者。

##### 关键代码

```python
candidates = sorted(
    (entry.last_access_tick, key)
    for key, entry in database.entries.items()
    if key not in target_keys
    and key not in already_deleted
    and not is_expired(entry, now_ms)
)
```

##### 关键语句理解

对 `(tick, key)` 排序提供确定的最旧优先选择和 Bytes Key Tie-break。目标 Key 不能被淘汰来伪装其自身写入可容纳。

<!-- journey-file: src/miniredis/core/planner.py -->
#### `src/miniredis/core/planner.py`

##### 是什么，为什么现在需要

Planner 门面成为命令语义与全局 Memory Policy 的组合点。

##### 在运行时做什么

每个已识别命令族 Plan 在到达 Executor 以前都经过同一预算执行。

##### 关键代码

```python
if plan is not None:
    return enforce_memory(plan, database, self.config, now_ms)
```

##### 关键语句理解

Policy 在命令语义确定后、Commit 存在前运行，因此拒绝仍无副作用。

<!-- journey-file: src/miniredis/runtime.py -->
#### `src/miniredis/runtime.py`

##### 是什么，为什么现在需要

Runtime 暴露一份冻结逻辑视图供重放不变量使用，而不把可变 Entry Map 变成公开 API。

##### 在运行时做什么

测试比较实时逻辑状态与仅由已应用 Batch 重建的全新 Database。

##### 关键代码

```python
def debug_logical_items(self) -> tuple[tuple[bytes, StoredEntry], ...]:
    return self.database.logical_items()
```

##### 关键语句理解

该诊断只观察结果，不提供绕过 Executor 单 Writer 边界的通道。

### 验证证据

运行 `uv run pytest -q $(cat journey/stages/08-deterministic-eviction/tests.txt)`。它通过公开命令与窄化诊断，证明 Policy 行为、原子 Batch 组成、Wrong-type 不提交与操作日志重放。

### 需要真正记住的内容

预算逻辑状态而非 RSS；选 Victim 前拒绝不可能目标；先清理过期 Entry；允许缩减 Plan；在一个 Batch 中发布 Victim 删除与已接受变更；保持所有实时状态可由 Commit 重建。

### 用自己的话讲清楚

淘汰包裹一个已规划命令。它询问完整提交后 Database 需要多少成本，再要么原样返回 OOM，要么追加足够的确定删除使这条精确命令可容纳。Executor 仍只看见并发布一个 Plan。

### 教材

[第 5 章](https://github.com/system-in-miniature/mini-redis/blob/main/docs/zh/tutorial/05-eviction.md)
