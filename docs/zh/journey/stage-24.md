# Stage 24 · 衰减式 LFU 淘汰

### 目标

用确定性时间衰减淘汰逻辑上最不常使用的 Entry，同时不在 Planning 时修改 Survivor，也不在 Recovery/Replication 时虚构 Access History。

??? note "交付文件"
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

### 测试契约

#### 先看会坏在哪里

没有 Decay 时旧流量永久占优。Planning 写回 Projected Counter 会让“仅仅考虑过淘汰”改变后续 Victim，即使 Key 存活。Recovery 恢复 Live Frequency 会在 Restart 时虚构 Policy History。Transaction Fork 共享 Entry 会让推测 Read 修改 Live LFU State。不稳定 Tie 会让等 Frequency Victim 依赖 Map Iteration。

??? note "文件差异：tests/contract/test_eviction.py"
    ```diff
    diff --git a/tests/contract/test_eviction.py b/tests/contract/test_eviction.py
    index 5973527b9f17d7c4afef29ee883ebe507dc996a6..90110b37e83bb85d7ab688a26991b21c6d348ca3 100644
    --- a/tests/contract/test_eviction.py
    +++ b/tests/contract/test_eviction.py
    @@ -85,3 +85,82 @@ async def test_expired_budget_is_purged_in_same_batch_before_noeviction_check():
                 for operation in batch.operations
             )
             assert r.debug_physical_key_count == 1
    +        assert r.debug_stats().expired_key_count == 1
    +
    +
    +@pytest.mark.asyncio
    +async def test_lfu_evicts_lowest_effective_frequency():
    +    clock = FakeClock(0)
    +    async with MiniRedis.open(
    +        clock=clock,
    +        maxmemory=260,
    +        eviction_policy="allkeys-lfu",
    +        lfu_decay_interval_ms=1000,
    +    ) as runtime:
    +        client = runtime.direct_client()
    +        await client.execute(CommandRequest(b"SET", (b"hot", b"x")))
    +        for _ in range(4):
    +            await client.execute(CommandRequest(b"GET", (b"hot",)))
    +        await client.execute(CommandRequest(b"SET", (b"cold", b"x")))
    +
    +        assert await client.execute(
    +            CommandRequest(b"SET", (b"new", b"x" * 60))
    +        ) == Ok()
    +        assert await client.execute(CommandRequest(b"GET", (b"cold",))) == Bytes(None)
    +        assert await client.execute(CommandRequest(b"GET", (b"hot",))) == Bytes(b"x")
    +        assert runtime.debug_stats().evicted_key_count == 1
    +
    +
    +@pytest.mark.asyncio
    +async def test_lfu_decay_can_cool_an_old_hot_key_below_recent_key():
    +    clock = FakeClock(0)
    +    async with MiniRedis.open(
    +        clock=clock,
    +        maxmemory=260,
    +        eviction_policy="allkeys-lfu",
    +        lfu_decay_interval_ms=1000,
    +    ) as runtime:
    +        client = runtime.direct_client()
    +        await client.execute(CommandRequest(b"SET", (b"old", b"x")))
    +        for _ in range(7):
    +            await client.execute(CommandRequest(b"GET", (b"old",)))
    +        old_anchor = runtime.database.entries[b"old"].last_frequency_decay_ms
    +        clock.advance(3_000)
    +        await client.execute(CommandRequest(b"SET", (b"recent", b"x")))
    +
    +        assert await client.execute(
    +            CommandRequest(b"SET", (b"new", b"x" * 60))
    +        ) == Ok()
    +        assert await client.execute(CommandRequest(b"GET", (b"old",))) == Bytes(None)
    +        assert await client.execute(CommandRequest(b"GET", (b"recent",))) == Bytes(
    +            b"x"
    +        )
    +        assert old_anchor == 0
    +
    +
    +@pytest.mark.asyncio
    +async def test_lfu_planning_projects_without_materializing_survivor_decay():
    +    clock = FakeClock(0)
    +    async with MiniRedis.open(
    +        clock=clock,
    +        maxmemory=260,
    +        eviction_policy="allkeys-lfu",
    +        lfu_decay_interval_ms=1000,
    +    ) as runtime:
    +        client = runtime.direct_client()
    +        await client.execute(CommandRequest(b"SET", (b"hot", b"x")))
    +        for _ in range(7):
    +            await client.execute(CommandRequest(b"GET", (b"hot",)))
    +        await client.execute(CommandRequest(b"SET", (b"cold", b"x")))
    +        clock.advance(2_000)
    +        before = (
    +            runtime.database.entries[b"hot"].frequency,
    +            runtime.database.entries[b"hot"].last_frequency_decay_ms,
    +        )
    +
    +        assert await client.execute(
    +            CommandRequest(b"SET", (b"new", b"x" * 60))
    +        ) == Ok()
    +
    +        survivor = runtime.database.entries[b"hot"]
    +        assert (survivor.frequency, survivor.last_frequency_decay_ms) == before
    ```

锁定最低 Effective-frequency 淘汰、旧 Hot Key 冷却、确定性 Survivor 选择，以及不实体化 Survivor Projected Decay；同时暴露已提交 Expired/Evicted Counter。

??? note "文件差异：tests/mechanisms/test_transactions.py"
    ```diff
    diff --git a/tests/mechanisms/test_transactions.py b/tests/mechanisms/test_transactions.py
    index 3c0dc8e00dd197da91ffb5cf445bcdca2e316adc..7fad3ebf3ce1cba96656cc030b1a1aa4e05d1ef7 100644
    --- a/tests/mechanisms/test_transactions.py
    +++ b/tests/mechanisms/test_transactions.py
    @@ -90,6 +90,7 @@ async def test_dirty_exec_aborts_without_applying_queued_commands():
             assert await client.execute(CommandRequest(b"EXEC")) == Failure(
                 "EXECABORT", "transaction discarded because of previous errors"
             )
    +        assert runtime.debug_stats().transaction_aborts == 1
             assert await client.execute(CommandRequest(b"GET", (b"k",))) == Bytes(None)


    ```

锁定 Dirty EXEC 的 Transaction-abort Counter，让新可观测性报告 Terminal Outcome，而不是根据 Active State 猜测。

??? note "文件差异：tests/mechanisms/test_watch.py"
    ```diff
    diff --git a/tests/mechanisms/test_watch.py b/tests/mechanisms/test_watch.py
    index d42b382c70408468110e9685c60701d5f67d830e..a7b1a739ecd10fc295957a3226ea18845fb88812 100644
    --- a/tests/mechanisms/test_watch.py
    +++ b/tests/mechanisms/test_watch.py
    @@ -45,5 +45,6 @@ async def test_watch_detects_create_then_delete():
             await owner.execute(CommandRequest(b"GET", (b"k",)))

             assert await owner.execute(CommandRequest(b"EXEC")) == NullArray()
    +        assert runtime.debug_stats().watch_aborts == 1
             assert runtime.executor.watched_key_count == 0
             assert await owner.execute(CommandRequest(b"GET", (b"k",))) == Bytes(None)
    ```

锁定 Revision Validation 返回 Null Array 时的 WATCH-abort Counter。

??? note "文件差异：tests/reliability/test_final_acceptance.py"
    ```diff
    diff --git a/tests/reliability/test_final_acceptance.py b/tests/reliability/test_final_acceptance.py
    index 6441eab6a1cbd0db08e687f54d3b7b0ee0b9ae60..58e5f09984ea10543252b483cbda4c912150f18e 100644
    --- a/tests/reliability/test_final_acceptance.py
    +++ b/tests/reliability/test_final_acceptance.py
    @@ -15,6 +15,7 @@ from tests.helpers.runtime import open_test_runtime

     OWNER_FIELDS = (
         "accepted_requests",
    +    "active_transactions",
         "aof_tasks",
         "control_producers",
         "executor_tasks",
    @@ -30,6 +31,7 @@ OWNER_FIELDS = (
         "tcp_tasks",
         "timer_handles",
         "waiters",
    +    "watched_keys",
     )


    ```

用 Active Transaction 与 Watched-key Ownership 扩展 Zero-owner Acceptance，保证 LFU 可观测性变更不削弱 Lifecycle Settlement。

??? note "文件差异：tests/unit/core/test_domain_types.py"
    ```diff
    diff --git a/tests/unit/core/test_domain_types.py b/tests/unit/core/test_domain_types.py
    index 905d09eb5c319fe6d7518ed27101e4eac173aca2..33384f8eacdd6f33c8a2f47a157e5588940816d9 100644
    --- a/tests/unit/core/test_domain_types.py
    +++ b/tests/unit/core/test_domain_types.py
    @@ -324,6 +324,56 @@ def test_database_fork_is_deep_and_preserves_runtime_metadata():
         assert fork.logical_usage == 0


    +def test_client_updates_preserve_decay_and_increment_frequency():
    +    database = Database()
    +    database.apply_batch(
    +        CommitBatch(
    +            1,
    +            (PutEntry(b"k", StoredEntry(StoredString(b"one"), None, 1)),),
    +            CommitTrigger.CLIENT,
    +        ),
    +        track_access=True,
    +        now_ms=0,
    +        lfu_decay_interval_ms=1000,
    +    )
    +    assert database.entries[b"k"].frequency == 1
    +    database.apply_batch(
    +        CommitBatch(
    +            2,
    +            (PutEntry(b"k", StoredEntry(StoredString(b"two"), None, 2)),),
    +            CommitTrigger.CLIENT,
    +        ),
    +        track_access=True,
    +        now_ms=2000,
    +        lfu_decay_interval_ms=1000,
    +    )
    +
    +    assert database.entries[b"k"].frequency == 1
    +    assert database.entries[b"k"].last_frequency_decay_ms == 2000
    +
    +
    +def test_recovery_puts_start_neutral_and_fork_copies_lfu_metadata():
    +    database = Database()
    +    database.apply_batch(
    +        CommitBatch(
    +            1,
    +            (PutEntry(b"k", StoredEntry(StoredString(b"v"), None, 1)),),
    +            CommitTrigger.CLIENT,
    +        ),
    +        track_access=False,
    +        now_ms=5000,
    +        lfu_decay_interval_ms=1000,
    +    )
    +    assert database.entries[b"k"].frequency == 0
    +    assert database.entries[b"k"].last_access_tick == 0
    +    assert database.entries[b"k"].last_frequency_decay_ms == 5000
    +
    +    fork = database.fork()
    +    assert fork.touch_if_live(b"k", 5000, 1000) is True
    +    assert fork.entries[b"k"].frequency == 1
    +    assert database.entries[b"k"].frequency == 0
    +
    +
     def test_apply_batch_tracks_each_put_and_touch_only_live_entries() -> None:
         database = Database()
         database.apply_batch(
    ```

锁定 Client PUT/Touch Frequency Update、Decay-anchor 保留、Recovery 中性 Metadata 与 Deep-fork 独立性。

??? note "文件差异：tests/unit/core/test_frequency.py"
    ```diff
    diff --git a/tests/unit/core/test_frequency.py b/tests/unit/core/test_frequency.py
    new file mode 100644
    index 0000000000000000000000000000000000000000..91586aa497af8ae0eb1babdf42000275a346890d
    --- /dev/null
    +++ b/tests/unit/core/test_frequency.py
    @@ -0,0 +1,32 @@
    +import pytest
    +
    +from miniredis.core.frequency import project_frequency
    +
    +
    +@pytest.mark.parametrize(
    +    ("frequency", "last_ms", "now_ms", "interval_ms", "expected"),
    +    [
    +        (8, 0, 999, 1000, (8, 0)),
    +        (8, 0, 1000, 1000, (4, 1000)),
    +        (9, 0, 3000, 1000, (1, 3000)),
    +        (1, 0, 5000, 1000, (0, 5000)),
    +        (8, 2000, 1000, 1000, (8, 2000)),
    +    ],
    +)
    +def test_project_frequency_decay(
    +    frequency,
    +    last_ms,
    +    now_ms,
    +    interval_ms,
    +    expected,
    +):
    +    assert project_frequency(frequency, last_ms, now_ms, interval_ms) == expected
    +
    +
    +@pytest.mark.parametrize(
    +    ("frequency", "interval"),
    +    [(-1, 1000), (1, 0), (1, -1)],
    +)
    +def test_project_frequency_rejects_invalid_inputs(frequency, interval):
    +    with pytest.raises(ValueError):
    +        project_frequency(frequency, 0, 0, interval)
    ```

锁定按完整 Window 右移衰减、Anchor 推进、不回退时间与输入校验。失败说明 Policy Time 含糊或不确定。

### 基本概念

Raw Frequency 统计真实 Access。Effective Frequency 是 Raw Counter 穿过完整 Decay Window 后的 Projection：每个 Window 减半。Decay Anchor 只按已经过去的完整 Window 推进。LFU Candidate Order 是 `(effective_frequency, last_access_tick, key)`，形成确定性 Tie-break。Projection 是观察；只有真实 Touch/Put 才 Materialize。

### 为什么需要这个机制

LFU 比 Recency 更能表达 Popularity，但需要 Aging 才能适应变化。Pure Projection 分离 Policy Comparison 与 State Mutation，保持可重复 Planning 与 Transaction Speculation。Neutral Recovery 承认 Access-policy Metadata 是 Runtime-local，而非 Durable Logical Data。

### 运行时心智模型

每个 Live Entry 持有 Raw Frequency 与 Last-decay Time。Client Touch 先 Project 再 Increment；Client Replacement 用同一规则携带旧 Metadata；Replica/Recovery Put 从 Neutral 开始。Memory Enforcement 需要 Victim 时，在同一 `now_ms` 投影所有 Eligible Entry，确定性排序，并加入 Eviction Delete 直到 Planned Batch 可容纳。只有 Committed Batch 更新 Counter 与 State。

### 机制板块

#### 纯函数衰减 Frequency Projection

让访问 Counter 穿过已过去的 Decay Window 做 Projection，Candidate 比较期间不修改 Database Entry。

??? note "文件差异：src/miniredis/core/frequency.py"
    ```diff
    diff --git a/src/miniredis/core/frequency.py b/src/miniredis/core/frequency.py
    new file mode 100644
    index 0000000000000000000000000000000000000000..47e07b71a165ecb140e3f853623cf7507893db08
    --- /dev/null
    +++ b/src/miniredis/core/frequency.py
    @@ -0,0 +1,17 @@
    +def project_frequency(
    +    frequency: int,
    +    last_decay_ms: int,
    +    now_ms: int,
    +    interval_ms: int,
    +) -> tuple[int, int]:
    +    if frequency < 0:
    +        raise ValueError("frequency cannot be negative")
    +    if interval_ms <= 0:
    +        raise ValueError("LFU decay interval must be positive")
    +    if now_ms <= last_decay_ms:
    +        return frequency, last_decay_ms
    +    windows = (now_ms - last_decay_ms) // interval_ms
    +    if windows == 0:
    +        return frequency, last_decay_ms
    +    projected = 0 if windows >= frequency.bit_length() else frequency >> windows
    +    return projected, last_decay_ms + windows * interval_ms
    ```

提供 Pure Decay Function。`frequency >> windows` 给出确定性指数冷却，`bit_length()` 避免无意义的大 Shift。

#### LFU Metadata 所有权

在每个 Live Entry 上记录 Frequency 与 Decay Anchor，只在真实 Access/Commit 时更新，并让 Recovery 保持中性、Fork 相互独立。

??? note "文件差异：src/miniredis/core/database.py"
    ```diff
    diff --git a/src/miniredis/core/database.py b/src/miniredis/core/database.py
    index e6dcaaa5bdd2318978c561533cc2c2f7ddbb1c36..df62f5427e2609031bd7ca13a7c74ba214761b24 100644
    --- a/src/miniredis/core/database.py
    +++ b/src/miniredis/core/database.py
    @@ -16,6 +16,7 @@ from miniredis.core.commit import (
         StoredValue,
         StoredZSet,
     )
    +from miniredis.core.frequency import project_frequency
     from miniredis.core.values import (
         HashValue,
         ListValue,
    @@ -37,6 +38,8 @@ class Entry:
         mutation_version: int
         last_access_tick: int
         logical_size: int
    +    frequency: int = 0
    +    last_frequency_decay_ms: int = 0


     def logical_value_size(value: RedisValue | StoredValue) -> int:
    @@ -127,7 +130,14 @@ class Database:
         def revision(self, key: bytes) -> int:
             return self.key_revisions.get(key, 0)

    -    def apply_batch(self, batch: CommitBatch, *, track_access: bool) -> None:
    +    def apply_batch(
    +        self,
    +        batch: CommitBatch,
    +        *,
    +        track_access: bool,
    +        now_ms: int = 0,
    +        lfu_decay_interval_ms: int = 60_000,
    +    ) -> None:
             next_seq = self.commit_seq + 1
             if batch.seq != next_seq:
                 raise ValueError(f"expected commit seq {next_seq}, got {batch.seq}")
    @@ -142,8 +152,23 @@ class Database:
                     case DeleteKey(key=key):
                         staged.pop(key, None)
                     case PutEntry(key=key, entry=entry):
    +                    previous = staged.get(key)
                         if track_access:
                             staged_access_tick += 1
    +                        if previous is None:
    +                            frequency = 1
    +                            last_frequency_decay_ms = now_ms
    +                        else:
    +                            frequency, last_frequency_decay_ms = project_frequency(
    +                                previous.frequency,
    +                                previous.last_frequency_decay_ms,
    +                                now_ms,
    +                                lfu_decay_interval_ms,
    +                            )
    +                            frequency += 1
    +                    else:
    +                        frequency = 0
    +                        last_frequency_decay_ms = now_ms
                         value = thaw_value(entry.value)
                         staged[key] = Entry(
                             value=value,
    @@ -151,6 +176,8 @@ class Database:
                             mutation_version=entry.mutation_version,
                             last_access_tick=staged_access_tick if track_access else 0,
                             logical_size=logical_entry_size(key, value, entry.expire_at_ms),
    +                        frequency=frequency,
    +                        last_frequency_decay_ms=last_frequency_decay_ms,
                         )
                     case _:
                         raise TypeError(
    @@ -181,6 +208,8 @@ class Database:
                     mutation_version=entry.mutation_version,
                     last_access_tick=entry.last_access_tick,
                     logical_size=entry.logical_size,
    +                frequency=entry.frequency,
    +                last_frequency_decay_ms=entry.last_frequency_decay_ms,
                 )
                 for key, entry in self.entries.items()
             }
    @@ -191,7 +220,12 @@ class Database:
             forked.revision_clock = self.revision_clock
             return forked

    -    def touch_if_live(self, key: bytes, now_ms: int) -> bool:
    +    def touch_if_live(
    +        self,
    +        key: bytes,
    +        now_ms: int,
    +        lfu_decay_interval_ms: int = 60_000,
    +    ) -> bool:
             entry = self.entries.get(key)
             if entry is None or (
                 entry.expire_at_ms is not None and entry.expire_at_ms <= now_ms
    @@ -199,6 +233,13 @@ class Database:
                 return False
             self.access_tick += 1
             entry.last_access_tick = self.access_tick
    +        entry.frequency, entry.last_frequency_decay_ms = project_frequency(
    +            entry.frequency,
    +            entry.last_frequency_decay_ms,
    +            now_ms,
    +            lfu_decay_interval_ms,
    +        )
    +        entry.frequency += 1
             return True

         def logical_items(self) -> tuple[tuple[bytes, StoredEntry], ...]:
    @@ -244,6 +285,8 @@ class Database:
                     mutation_version=stored.mutation_version,
                     last_access_tick=0,
                     logical_size=size,
    +                frequency=0,
    +                last_frequency_decay_ms=now_ms,
                 )
                 staged_usage += size

    ```

持有 LFU Field，只在真实 Client Access/Commit 时更新；Deep Fork 复制 Field，Recovery/Replica Apply 则从零开始。

#### 确定性 LFU Victim Planning

按 Projected Frequency、Access Tick、Key 排序 Candidate，同时不改动 Survivor Metadata。

??? note "文件差异：src/miniredis/core/eviction.py"
    ```diff
    diff --git a/src/miniredis/core/eviction.py b/src/miniredis/core/eviction.py
    index 141b42622f1f1c6f08926cfb12e06b94c31d48d8..0e4f39c1626cdaed68838b9c75f9ed0f6c1bcef2 100644
    --- a/src/miniredis/core/eviction.py
    +++ b/src/miniredis/core/eviction.py
    @@ -12,6 +12,7 @@ from miniredis.core.commit import (
     from miniredis.core.database import Database, logical_entry_size
     from miniredis.core.executor import ExecutionPlan
     from miniredis.core.expiration import expiry_delete, is_expired
    +from miniredis.core.frequency import project_frequency
     from miniredis.core.planning import dedupe_operations
     from miniredis.core.reply import Failure

    @@ -50,6 +51,29 @@ def _expired_operations(
         )


    +def _lfu_candidates(
    +    database: Database,
    +    *,
    +    now_ms: int,
    +    decay_interval_ms: int,
    +    excluded: set[bytes],
    +) -> list[tuple[int, int, bytes]]:
    +    return sorted(
    +        (
    +            project_frequency(
    +                entry.frequency,
    +                entry.last_frequency_decay_ms,
    +                now_ms,
    +                decay_interval_ms,
    +            )[0],
    +            entry.last_access_tick,
    +            key,
    +        )
    +        for key, entry in database.entries.items()
    +        if key not in excluded and not is_expired(entry, now_ms)
    +    )
    +
    +
     def enforce_memory(
         plan: ExecutionPlan,
         database: Database,
    @@ -102,15 +126,28 @@ def enforce_memory(
         already_deleted = {
             operation.key for operation in operations if isinstance(operation, DeleteKey)
         }
    -    candidates = sorted(
    -        (entry.last_access_tick, key)
    -        for key, entry in database.entries.items()
    -        if key not in target_keys
    -        and key not in already_deleted
    -        and not is_expired(entry, now_ms)
    -    )
    +    excluded = target_keys | already_deleted
    +    if config.eviction_policy == "allkeys-lfu":
    +        candidate_keys = [
    +            key
    +            for _frequency, _tick, key in _lfu_candidates(
    +                database,
    +                now_ms=now_ms,
    +                decay_interval_ms=config.lfu_decay_interval_ms,
    +                excluded=excluded,
    +            )
    +        ]
    +    else:
    +        candidate_keys = [
    +            key
    +            for _tick, key in sorted(
    +                (entry.last_access_tick, key)
    +                for key, entry in database.entries.items()
    +                if key not in excluded and not is_expired(entry, now_ms)
    +            )
    +        ]
         victims: list[CommitOperation] = []
    -    for _tick, key in candidates:
    +    for key in candidate_keys:
             victims.append(DeleteKey(key, DeleteReason.EVICTED))
             candidate_operations = dedupe_operations(
                 expired + tuple(victims) + plan.operations
    ```

用 Projected Value 与稳定 Tie-breaker 建立 LFU Candidate，不把 Projection 赋回 Entry。

```python
return sorted((effective, entry.last_access_tick, key) ...)
```

Candidate Comparison 具有 Referential Transparency：同一时刻执行两次得到相同 State 与 Order。

#### LFU 执行与可观测性

校验 Policy/Decay 配置，让同一个 Clock 贯穿 Live、Transaction、Replica 与 Recovery 路径，并统计终态 Eviction/Abort Outcome。

??? note "文件差异：src/miniredis/config.py"
    ```diff
    diff --git a/src/miniredis/config.py b/src/miniredis/config.py
    index 98ad63d78a39025f944287c267c6916ee4bf7a03..c6e0e548b1f324adb75e7abefff094090dd8b0fc 100644
    --- a/src/miniredis/config.py
    +++ b/src/miniredis/config.py
    @@ -6,7 +6,7 @@ from typing import Literal

     from miniredis.persistence.aof import AofPolicy

    -EvictionPolicy = Literal["noeviction", "allkeys-lru"]
    +EvictionPolicy = Literal["noeviction", "allkeys-lru", "allkeys-lfu"]


     @dataclass(frozen=True, slots=True)
    @@ -15,6 +15,7 @@ class MiniRedisConfig:
         active_expire_sample_size: int = 20
         maxmemory: int | None = None
         eviction_policy: EvictionPolicy = "noeviction"
    +    lfu_decay_interval_ms: int = 60_000
         outbox_limit: int = 64
         outbox_drain_grace_ms: int = 100
         active_expire_interval_ms: int = 100
    @@ -34,8 +35,17 @@ class MiniRedisConfig:
                 raise ValueError("active_expire_sample_size must be positive")
             if self.maxmemory is not None and self.maxmemory <= 0:
                 raise ValueError("maxmemory must be positive")
    -        if self.eviction_policy not in {"noeviction", "allkeys-lru"}:
    -            raise ValueError("eviction_policy must be 'noeviction' or 'allkeys-lru'")
    +        if self.eviction_policy not in {
    +            "noeviction",
    +            "allkeys-lru",
    +            "allkeys-lfu",
    +        }:
    +            raise ValueError(
    +                "eviction_policy must be 'noeviction', 'allkeys-lru', "
    +                "or 'allkeys-lfu'"
    +            )
    +        if self.lfu_decay_interval_ms <= 0:
    +            raise ValueError("lfu_decay_interval_ms must be positive")
             if self.outbox_limit <= 0:
                 raise ValueError("outbox_limit must be positive")
             if self.outbox_drain_grace_ms < 0:
    ```

加入 `allkeys-lfu` 与正数 Decay Interval 作为显式校验的 Policy Input。

??? note "文件差异：src/miniredis/core/executor.py"
    ```diff
    diff --git a/src/miniredis/core/executor.py b/src/miniredis/core/executor.py
    index a573de1fa794592770df57981ae5515b6499d716..5391570d0166cc8310db6b7a4d6df62f2147178e 100644
    --- a/src/miniredis/core/executor.py
    +++ b/src/miniredis/core/executor.py
    @@ -34,6 +34,8 @@ from miniredis.core.commit import (
         CommitBatch,
         CommitOperation,
         CommitTrigger,
    +    DeleteKey,
    +    DeleteReason,
         PreparedCommit,
         PutEntry,
         SnapshotImage,
    @@ -240,6 +242,7 @@ class CommandExecutor:
             clock: Clock,
             commit_barrier: CommitBarrier,
             max_pending_commands: int,
    +        lfu_decay_interval_ms: int = 60_000,
             active_expire_sample_size: int = 20,
             scheduler: TimerScheduler,
             on_debug_change: Callable[[], None],
    @@ -255,6 +258,9 @@ class CommandExecutor:
             self.clock = clock
             self.commit_barrier = commit_barrier
             self.max_pending_commands = max_pending_commands
    +        if lfu_decay_interval_ms <= 0:
    +            raise ValueError("lfu_decay_interval_ms must be positive")
    +        self.lfu_decay_interval_ms = lfu_decay_interval_ms
             if active_expire_sample_size <= 0:
                 raise ValueError("active_expire_sample_size must be positive")
             self.active_expire_sample_size = active_expire_sample_size
    @@ -286,6 +292,10 @@ class CommandExecutor:
             self._active_source_generation: int | None = None
             self._replica_read_only = False
             self._transactions: dict[int, TransactionState] = {}
    +        self._transaction_aborts = 0
    +        self._watch_aborts = 0
    +        self.expired_key_count = 0
    +        self.evicted_key_count = 0
             self._replica_apply_failure = replica_apply_failure
             if (replica_apply_entered is None) != (replica_apply_release is None):
                 raise ValueError(
    @@ -543,7 +553,12 @@ class CommandExecutor:
                         error = self._replica_apply_failure
                         self._replica_apply_failure = None
                         raise error
    -                self.database.apply_batch(message.batch, track_access=False)
    +                self.database.apply_batch(
    +                    message.batch,
    +                    track_access=False,
    +                    now_ms=self.clock.now_ms(),
    +                    lfu_decay_interval_ms=self.lfu_decay_interval_ms,
    +                )
                 except BaseException as exc:
                     message.future.set_exception(exc)
                 else:
    @@ -806,6 +821,7 @@ class CommandExecutor:
                 return
             try:
                 if state.dirty:
    +                self._transaction_aborts += 1
                     self._finish_reply(
                         request.token,
                         Failure(
    @@ -818,6 +834,7 @@ class CommandExecutor:
                     self.database.revision(key) != revision
                     for key, revision in state.watched.items()
                 ):
    +                self._watch_aborts += 1
                     self._finish_reply(request.token, NullArray())
                     return

    @@ -850,10 +867,16 @@ class CommandExecutor:
                                 plan.trigger,
                             ),
                             track_access=plan.trigger is CommitTrigger.CLIENT,
    +                        now_ms=now_ms,
    +                        lfu_decay_interval_ms=self.lfu_decay_interval_ms,
                         )
                         workspace.operations.extend(plan.operations)
                     for key in dict.fromkeys(plan.touch_keys):
    -                    workspace.database.touch_if_live(key, now_ms)
    +                    workspace.database.touch_if_live(
    +                        key,
    +                        now_ms,
    +                        self.lfu_decay_interval_ms,
    +                    )

                 if workspace.operations:
                     try:
    @@ -871,7 +894,11 @@ class CommandExecutor:
                         self._on_fatal(str(exc))
                         return
                 for key in dict.fromkeys(workspace.touch_keys):
    -                self.database.touch_if_live(key, now_ms)
    +                self.database.touch_if_live(
    +                    key,
    +                    now_ms,
    +                    self.lfu_decay_interval_ms,
    +                )
                 for wakeup in workspace.wakeups:
                     waiter = self.waiters.transition(
                         wakeup.waiter_id,
    @@ -986,7 +1013,11 @@ class CommandExecutor:
                 return

             for key in dict.fromkeys(plan.touch_keys):
    -            self.database.touch_if_live(key, now_ms)
    +            self.database.touch_if_live(
    +                key,
    +                now_ms,
    +                self.lfu_decay_interval_ms,
    +            )

             for wakeup in plan.waiter_wakeups:
                 waiter = self.waiters.transition(
    @@ -1015,6 +1046,18 @@ class CommandExecutor:
             self.database.apply_batch(
                 batch,
                 track_access=prepared.trigger is CommitTrigger.CLIENT,
    +            now_ms=self.clock.now_ms(),
    +            lfu_decay_interval_ms=self.lfu_decay_interval_ms,
    +        )
    +        self.expired_key_count += sum(
    +            isinstance(operation, DeleteKey)
    +            and operation.reason is DeleteReason.EXPIRED
    +            for operation in batch.operations
    +        )
    +        self.evicted_key_count += sum(
    +            isinstance(operation, DeleteKey)
    +            and operation.reason is DeleteReason.EVICTED
    +            for operation in batch.operations
             )
             self._applied_batches.append(batch)
             self._offer_replica_batch(batch)
    @@ -1203,6 +1246,14 @@ class CommandExecutor:
         def watched_key_count(self) -> int:
             return sum(len(state.watched) for state in self._transactions.values())

    +    @property
    +    def transaction_abort_count(self) -> int:
    +        return self._transaction_aborts
    +
    +    @property
    +    def watch_abort_count(self) -> int:
    +        return self._watch_aborts
    +
         @property
         def accepted_tokens(self) -> tuple[RequestToken, ...]:
             return tuple(self._accepted_tokens)
    ```

让同一 Clock/Decay Interval 贯穿 Normal Commit、Transaction Workspace、Replica Apply 与 Touch；统计 Committed Expiry/Eviction 及 Transaction/WATCH Abort。

??? note "文件差异：src/miniredis/runtime.py"
    ```diff
    diff --git a/src/miniredis/runtime.py b/src/miniredis/runtime.py
    index 82d59947fc55c6352ba6292cb9142cf5a8d55135..47019905cf08d4c97370b3451e5500a3c146091f 100644
    --- a/src/miniredis/runtime.py
    +++ b/src/miniredis/runtime.py
    @@ -80,6 +80,14 @@ class RuntimeStats:
         tcp_servers: int
         tcp_sessions: int
         tcp_tasks: int
    +    active_transactions: int
    +    watched_keys: int
    +    transaction_aborts: int
    +    watch_aborts: int
    +    key_count: int
    +    logical_memory_usage: int
    +    expired_key_count: int
    +    evicted_key_count: int


     @dataclass(slots=True)
    @@ -130,6 +138,7 @@ class MiniRedis:
                 clock=clock,
                 commit_barrier=actual_barrier,
                 max_pending_commands=config.max_pending_commands,
    +            lfu_decay_interval_ms=config.lfu_decay_interval_ms,
                 active_expire_sample_size=config.active_expire_sample_size,
                 scheduler=self.scheduler,
                 on_debug_change=self._debug_notify,
    @@ -739,6 +748,14 @@ class MiniRedis:
                 tcp_servers=len(servers),
                 tcp_sessions=sum(server.session_count for server in servers),
                 tcp_tasks=sum(server.owned_task_count for server in servers),
    +            active_transactions=self.executor.active_transaction_count,
    +            watched_keys=self.executor.watched_key_count,
    +            transaction_aborts=self.executor.transaction_abort_count,
    +            watch_aborts=self.executor.watch_abort_count,
    +            key_count=len(self.database.entries),
    +            logical_memory_usage=self.database.logical_usage,
    +            expired_key_count=self.executor.expired_key_count,
    +            evicted_key_count=self.executor.evicted_key_count,
             )

         def _debug_notify(self) -> None:
    ```

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

[在 GitHub 查看阶段差异](https://github.com/system-in-miniature/mini-redis/compare/b195a43...b25b473)

完成后可运行 `python -m journey.tools.build_journey check 24` 验收学习工作区。

[Complete reference patch / 完整参考补丁](https://github.com/system-in-miniature/mini-redis/blob/main/journey/stages/24-decaying-lfu/stage.patch)
