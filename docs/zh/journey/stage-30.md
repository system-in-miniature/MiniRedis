# Stage 30 · 公共一致性与阅读地图

### 目标

通过记录每个 Public/Runtime Module Boundary，并提供 Durability、LFU Eviction、Replica Resynchronization 的可执行 Public-API 场景完成重建，而不再加入隐藏机制。

??? note "交付文件"
    - `examples/aof_crash_recovery.py`
    - `examples/lfu_eviction.py`
    - `examples/replication_resync.py`
    - `src/miniredis/__init__.py`
    - `src/miniredis/adapters/direct.py`
    - `src/miniredis/adapters/resp2.py`
    - `src/miniredis/adapters/tcp.py`
    - `src/miniredis/clock.py`
    - `src/miniredis/commands/model.py`
    - `src/miniredis/commands/parser.py`
    - `src/miniredis/commands/request.py`
    - `src/miniredis/config.py`
    - `src/miniredis/core/blocking.py`
    - `src/miniredis/core/commit.py`
    - `src/miniredis/core/database.py`
    - `src/miniredis/core/eviction.py`
    - `src/miniredis/core/expiration.py`
    - `src/miniredis/core/frequency.py`
    - `src/miniredis/core/hash_planner.py`
    - `src/miniredis/core/list_planner.py`
    - `src/miniredis/core/mailbox.py`
    - `src/miniredis/core/outbound.py`
    - `src/miniredis/core/planner.py`
    - `src/miniredis/core/planning.py`
    - `src/miniredis/core/pubsub.py`
    - `src/miniredis/core/reply.py`
    - `src/miniredis/core/set_planner.py`
    - `src/miniredis/core/transactions.py`
    - `src/miniredis/core/ttl_planner.py`
    - `src/miniredis/core/values.py`
    - `src/miniredis/core/zset_planner.py`
    - `src/miniredis/persistence/aof.py`
    - `src/miniredis/persistence/codec.py`
    - `src/miniredis/persistence/recovery.py`
    - `src/miniredis/persistence/snapshot.py`
    - `src/miniredis/replication/backlog.py`

### 当前遇到的问题

所有机制已经存在，但完整 Learning Model 还需要从 Public Behavior 到 Internal Ownership 的稳定地图。缺少 Module Contract 与 Runnable Scenario 时，学习者即使通过孤立 Test，仍不清楚 Ordering、Durability、Expiry、Transaction、Resynchronization 分别由谁持有，也无法区分 MiniRedis 与 Redis 的差异是有意简化还是意外缺口。

### 测试契约

#### 先看会坏在哪里

Documentation-only Final Stage 可能变成与 Executable Behavior 脱节的空评论。导入 Test Helper 或 Debug Internal 的 Example 不能证明 Public Surface。声称 Byte-level PSYNC、Allocator Memory Accounting 或 Redis Exact In-place Algorithm 会夸大 Parity。反过来，逐个平铺文件又会隐藏少数真正重要的 Architectural Boundary。

本阶段没有修改 Test File。聚焦 Final-acceptance Suite 被有意复用为锁定的 Whole-system Evidence：同时激活 Command、Blocking Waiter、TCP、Pub/Sub、AOF、Snapshot、Replication、Stats 与 Shutdown，再证明 Durable Artifact 与 Zero Remaining Owner。新 Example 提供 Public Demonstration，但不替代该契约。

### 基本概念

Public Parity 表示 Documented API 与 Executable Example 经过前面 Journey 已锁定的同一 Production Path。Reading-map Parity 表示每个 Module 都说明 Responsibility 与对应 Redis Concept，却不声称内部完全相同。MiniRedis 的有意取舍包括 Logical 而非 Allocator Memory、Exact 而非 Sampled Eviction、Staged-copy Atomicity、Whole-batch 而非 Byte-ring Backlog，以及 In-process Replica Transport。

### 为什么需要这个机制

Polished Miniature 应该作为系统可理解，而不只是通过测试的机制堆。Module-level Contract 让学习者从 Behavior 找到 Owner，Public-only Example 证明 Facade 足以完成有意义实验。显式 Trade-off 防止“小型实现”被误解为“内部完全相同”。

### 运行时心智模型

最终系统有四个边界：Adapter 把 Direct/TCP Input 变成 Typed Request 与 Ordered Outbound；Core Single-writer 规划并应用 Atomic Batch；Persistence 发布并恢复 Durable Checkpoint/Log；Replication 在 Epoch/Cursor Fencing 下传输同一 Batch。Example 只经过 `MiniRedis`、`MiniRedisConfig`、`CommandRequest` 与 Documented Replica Sink，再观察 Reply/Status。

### 机制板块

#### 可执行公共场景

通过 Public API 与可见 Reply 演示 Crash Recovery、确定性 LFU，以及 Partial-vs-full Replica Resynchronization。

??? note "文件差异：examples/aof_crash_recovery.py"
    ```diff
    diff --git a/examples/aof_crash_recovery.py b/examples/aof_crash_recovery.py
    new file mode 100644
    index 0000000000000000000000000000000000000000..68ed8a9da3cd5fbd8b102fee68297a2414fdc825
    --- /dev/null
    +++ b/examples/aof_crash_recovery.py
    @@ -0,0 +1,37 @@
    +"""Demonstrate that acknowledged AOF writes survive a simulated crash."""
    +
    +from __future__ import annotations
    +
    +import asyncio
    +from pathlib import Path
    +from tempfile import TemporaryDirectory
    +
    +from miniredis import CommandRequest, MiniRedis, MiniRedisConfig
    +from miniredis.persistence.aof import AofPolicy
    +
    +
    +async def main() -> None:
    +    with TemporaryDirectory(prefix="miniredis-aof-") as directory:
    +        aof_path = Path(directory) / "appendonly.mraof"
    +        config = MiniRedisConfig(aof_path=aof_path, aof_policy=AofPolicy.ALWAYS)
    +
    +        first = MiniRedis.open(config)
    +        await first.start()
    +        writer = first.direct_client()
    +        print("1. SET before crash:", await writer.execute(
    +            CommandRequest(b"SET", (b"lesson", b"durable"))
    +        ))
    +        print("2. Simulating a crash (no graceful AOF drain)...")
    +        await first.simulate_crash()
    +
    +        recovered = MiniRedis.open(config)
    +        await recovered.start()
    +        reader = recovered.direct_client()
    +        value = await reader.execute(CommandRequest(b"GET", (b"lesson",)))
    +        print("3. GET after restart:", value)
    +        print("4. Recovery verified:", getattr(value, "value", None) == b"durable")
    +        await recovered.close()
    +
    +
    +if __name__ == "__main__":
    +    asyncio.run(main())
    ```

??? note "文件差异：examples/lfu_eviction.py"
    ```diff
    diff --git a/examples/lfu_eviction.py b/examples/lfu_eviction.py
    new file mode 100644
    index 0000000000000000000000000000000000000000..99f14476d2b065fdef3c0959f6dba61a74a158dc
    --- /dev/null
    +++ b/examples/lfu_eviction.py
    @@ -0,0 +1,44 @@
    +"""Observe deterministic allkeys-LFU eviction through command replies only."""
    +
    +from __future__ import annotations
    +
    +import asyncio
    +
    +from miniredis import CommandRequest, MiniRedis
    +
    +
    +async def main() -> None:
    +    async with MiniRedis.open(
    +        maxmemory=260,
    +        eviction_policy="allkeys-lfu",
    +    ) as redis:
    +        client = redis.direct_client()
    +        print("1. Create equally small 'hot' and 'cold' keys.")
    +        print("   hot:", await client.execute(
    +            CommandRequest(b"SET", (b"hot", b"x"))
    +        ))
    +        print("   cold:", await client.execute(
    +            CommandRequest(b"SET", (b"cold", b"x"))
    +        ))
    +
    +        print("2. Read 'hot' four times to raise its frequency.")
    +        for attempt in range(1, 5):
    +            reply = await client.execute(CommandRequest(b"GET", (b"hot",)))
    +            print(f"   read {attempt}: {reply!r}")
    +
    +        print("3. Add a larger key so maxmemory requires one victim.")
    +        print("   new:", await client.execute(
    +            CommandRequest(b"SET", (b"new", b"x" * 60))
    +        ))
    +
    +        cold = await client.execute(CommandRequest(b"GET", (b"cold",)))
    +        hot = await client.execute(CommandRequest(b"GET", (b"hot",)))
    +        new = await client.execute(CommandRequest(b"GET", (b"new",)))
    +        print("4. Public GET observations:")
    +        print("   cold (least frequent, expected missing):", cold)
    +        print("   hot  (expected retained):", hot)
    +        print("   new  (expected retained):", new)
    +
    +
    +if __name__ == "__main__":
    +    asyncio.run(main())
    ```

??? note "文件差异：examples/replication_resync.py"
    ```diff
    diff --git a/examples/replication_resync.py b/examples/replication_resync.py
    new file mode 100644
    index 0000000000000000000000000000000000000000..dd28ab71297052bd5bd8b2215d1a8b78bdf4bb00
    --- /dev/null
    +++ b/examples/replication_resync.py
    @@ -0,0 +1,76 @@
    +"""Compare partial replica resume with full-sync fallback after a backlog gap."""
    +
    +from __future__ import annotations
    +
    +import asyncio
    +
    +from miniredis import CommandRequest, MiniRedis, MiniRedisConfig
    +from miniredis.replication.sink import ReplicaSink, ReplicaSyncMode
    +
    +
    +async def set_value(runtime: MiniRedis, key: bytes, value: bytes) -> None:
    +    reply = await runtime.direct_client().execute(
    +        CommandRequest(b"SET", (key, value))
    +    )
    +    print(f"   SET {key!r} -> {reply!r}")
    +
    +
    +async def short_disconnect() -> None:
    +    primary = MiniRedis.open(MiniRedisConfig(replication_backlog_batches=2))
    +    replica = MiniRedis.open()
    +    await primary.start()
    +    await replica.start()
    +    sink = ReplicaSink(replica, queue_limit=4)
    +
    +    initial = await primary.attach_replica(sink)
    +    print("1. Initial attachment:", initial.sync_mode)
    +    await set_value(primary, b"a", b"1")
    +    await sink.wait_until_applied(initial.primary_seq + 1)
    +    await sink.disconnect()
    +    await set_value(primary, b"b", b"2")
    +
    +    resumed = await primary.attach_replica(sink)
    +    await sink.wait_until_applied(resumed.primary_seq)
    +    print("2. Short disconnect resumed with:", resumed.sync_mode)
    +    print("   Expected partial:", resumed.sync_mode is ReplicaSyncMode.PARTIAL)
    +    print("   Replica GET b:", await replica.direct_client().execute(
    +        CommandRequest(b"GET", (b"b",))
    +    ))
    +    await primary.close()
    +    await replica.close()
    +
    +
    +async def backlog_gap() -> None:
    +    primary = MiniRedis.open(MiniRedisConfig(replication_backlog_batches=2))
    +    replica = MiniRedis.open()
    +    await primary.start()
    +    await replica.start()
    +    sink = ReplicaSink(replica, queue_limit=4)
    +
    +    initial = await primary.attach_replica(sink)
    +    await set_value(primary, b"old", b"present")
    +    await sink.wait_until_applied(initial.primary_seq + 1)
    +    await sink.disconnect()
    +    await set_value(primary, b"k2", b"2")
    +    await set_value(primary, b"k3", b"3")
    +    await set_value(primary, b"k4", b"4")
    +
    +    resumed = await primary.attach_replica(sink)
    +    print("3. Cursor older than backlog resumed with:", resumed.sync_mode)
    +    print("   Expected full:", resumed.sync_mode is ReplicaSyncMode.FULL)
    +    print("   Replica GET k4:", await replica.direct_client().execute(
    +        CommandRequest(b"GET", (b"k4",))
    +    ))
    +    await primary.close()
    +    await replica.close()
    +
    +
    +async def main() -> None:
    +    print("Short disconnect: complete history is still in the backlog.")
    +    await short_disconnect()
    +    print("\nLong disconnect: a required batch has fallen out of the backlog.")
    +    await backlog_gap()
    +
    +
    +if __name__ == "__main__":
    +    asyncio.run(main())
    ```

三个 Example 分别演示 Simulated Crash 后 Acknowledged AOF Recovery、Exact allkeys-LFU Victim，以及 Short-disconnect Partial Resume 与 Backlog-gap Full Fallback。它们使用 Production Lifecycle 与 Public Reply，不依赖 Test Fixture。

#### 公共接口与协议模块契约

记录 Direct-first API、Adapter Ownership、Binary Request/Reply Boundary、Clock/Config Input 与 Typed Command Surface。

??? note "文件差异：src/miniredis/__init__.py"
    ```diff
    diff --git a/src/miniredis/__init__.py b/src/miniredis/__init__.py
    index 02570d909b840aa544ccda4ba82632e3ce144160..8dddcc2aee8b43c48d55a6db1ed911c76d0862a7 100644
    --- a/src/miniredis/__init__.py
    +++ b/src/miniredis/__init__.py
    @@ -1,3 +1,5 @@
    +"""Public Direct-first API for the MiniRedis teaching runtime."""
    +
     from miniredis.adapters.direct import DirectPipeline
     from miniredis.commands.request import CommandRequest
     from miniredis.config import MiniRedisConfig
    ```

??? note "文件差异：src/miniredis/adapters/direct.py"
    ```diff
    diff --git a/src/miniredis/adapters/direct.py b/src/miniredis/adapters/direct.py
    index b93fb5a0a318c2a480dbadacdfbc209bdcef32dc..d797749c209ca3a348c1e560f6230636f7c01479 100644
    --- a/src/miniredis/adapters/direct.py
    +++ b/src/miniredis/adapters/direct.py
    @@ -1,3 +1,5 @@
    +"""Expose binary-safe in-process clients and ordered, non-atomic pipelines."""
    +
     from __future__ import annotations

     import asyncio
    ```

??? note "文件差异：src/miniredis/adapters/resp2.py"
    ```diff
    diff --git a/src/miniredis/adapters/resp2.py b/src/miniredis/adapters/resp2.py
    index ee40d126969a857e5921eaa5a14d3b672d36bca3..171a0732e0807b6951ff42da10977388e2e66827 100644
    --- a/src/miniredis/adapters/resp2.py
    +++ b/src/miniredis/adapters/resp2.py
    @@ -1,3 +1,5 @@
    +"""Incrementally decode bounded RESP2 requests and encode domain replies."""
    +
     from __future__ import annotations

     from dataclasses import dataclass
    ```

??? note "文件差异：src/miniredis/adapters/tcp.py"
    ```diff
    diff --git a/src/miniredis/adapters/tcp.py b/src/miniredis/adapters/tcp.py
    index 3b5e2f6cc99e6d652cdd166e9c446ef9130bc31e..366152205762fd2671f052482cb62abc9cf6b436 100644
    --- a/src/miniredis/adapters/tcp.py
    +++ b/src/miniredis/adapters/tcp.py
    @@ -1,3 +1,5 @@
    +"""Adapt asyncio TCP sessions to the shared parser, executor, and outbox."""
    +
     from __future__ import annotations

     import asyncio
    ```

??? note "文件差异：src/miniredis/clock.py"
    ```diff
    diff --git a/src/miniredis/clock.py b/src/miniredis/clock.py
    index 18d8ffc37a9c21622f60be0f13e907fa30e1e2aa..b7abcbab397cb96a15bb9f22f868e9623e765910 100644
    --- a/src/miniredis/clock.py
    +++ b/src/miniredis/clock.py
    @@ -1,3 +1,5 @@
    +"""Abstract time and timer scheduling for deterministic expiry and blocking."""
    +
     from __future__ import annotations

     import asyncio
    ```

??? note "文件差异：src/miniredis/commands/model.py"
    ```diff
    diff --git a/src/miniredis/commands/model.py b/src/miniredis/commands/model.py
    index fa071fad55d9367d92633bbbe49278f1b688e208..a719362c11cf35e670db383c74de7ff69b09840e 100644
    --- a/src/miniredis/commands/model.py
    +++ b/src/miniredis/commands/model.py
    @@ -1,3 +1,5 @@
    +"""Define the closed typed-command vocabulary accepted by core planning."""
    +
     from __future__ import annotations

     from dataclasses import dataclass
    ```

??? note "文件差异：src/miniredis/commands/parser.py"
    ```diff
    diff --git a/src/miniredis/commands/parser.py b/src/miniredis/commands/parser.py
    index 475d9c5f86d9f909a3017158f8027afd3d2f8976..55771e36c809848527238f071a8bba910ed90595 100644
    --- a/src/miniredis/commands/parser.py
    +++ b/src/miniredis/commands/parser.py
    @@ -1,3 +1,5 @@
    +"""Validate binary command requests and freeze options into typed commands."""
    +
     from __future__ import annotations

     import math
    ```

??? note "文件差异：src/miniredis/commands/request.py"
    ```diff
    diff --git a/src/miniredis/commands/request.py b/src/miniredis/commands/request.py
    index ba02069b3dd4bc716d2795ef55690b75eddf8978..ebde183f4873d8de5f87f3b70b100e5f4e656463 100644
    --- a/src/miniredis/commands/request.py
    +++ b/src/miniredis/commands/request.py
    @@ -1,3 +1,5 @@
    +"""Represent one transport-neutral command name and binary argument tuple."""
    +
     from dataclasses import dataclass


    ```

??? note "文件差异：src/miniredis/config.py"
    ```diff
    diff --git a/src/miniredis/config.py b/src/miniredis/config.py
    index ce49f2da3fc8d2a9f55284f5bdab265fceeff6f7..1fdaee85956c7f728b83b2c10b3c1ff4ee962a90 100644
    --- a/src/miniredis/config.py
    +++ b/src/miniredis/config.py
    @@ -1,3 +1,5 @@
    +"""Collect bounded runtime, persistence, eviction, and replication settings."""
    +
     from __future__ import annotations

     from dataclasses import dataclass
    ```

这些 Module 记录 Direct-first Facade、TCP Session Pump、Binary-safe RESP2 Codec、Injectable Time、Immutable Request/Command Language、Strict Parser 与 Validated Runtime Policy Input。合并理解能看出 Transport Syntax 在 Semantic Planning 前结束。

#### 核心状态机阅读地图

说明 Commit、Database State、Planning、Queue、Transaction、Pub/Sub、Expiry、Eviction 与各数据类型 Planner 的职责、Redis 对应物和有意教学取舍。

??? note "文件差异：src/miniredis/core/blocking.py"
    ```diff
    diff --git a/src/miniredis/core/blocking.py b/src/miniredis/core/blocking.py
    index 150a6afab8c80fa0db6d1f7c0f615074db12faab..b10d0a608ff995a3e43025f5cbbca45e78f43af2 100644
    --- a/src/miniredis/core/blocking.py
    +++ b/src/miniredis/core/blocking.py
    @@ -1,3 +1,9 @@
    +"""Track blocking list-pop waiters without making planners asynchronous.
    +
    +This is the teaching analogue of Redis's blocked-client and ready-key machinery:
    +the executor owns wakeup order while the registry owns timeout/cancellation state.
    +"""
    +
     from __future__ import annotations

     from collections import defaultdict, deque
    ```

??? note "文件差异：src/miniredis/core/commit.py"
    ```diff
    diff --git a/src/miniredis/core/commit.py b/src/miniredis/core/commit.py
    index d262f838ea45d57befcaae1e5e2de3aeb6139aff..c1deb906183ba9aa7e38d3f1684da1a43b90a8a5 100644
    --- a/src/miniredis/core/commit.py
    +++ b/src/miniredis/core/commit.py
    @@ -1,3 +1,9 @@
    +"""Define immutable state and the ordered unit used for durability and propagation.
    +
    +``CommitBatch`` corresponds to one atomic Redis propagation unit: it crosses
    +AOF, local apply, replication, and recovery without exposing partial operations.
    +"""
    +
     from __future__ import annotations

     from dataclasses import dataclass
    ```

??? note "文件差异：src/miniredis/core/database.py"
    ```diff
    diff --git a/src/miniredis/core/database.py b/src/miniredis/core/database.py
    index df62f5427e2609031bd7ca13a7c74ba214761b24..49c8fb8fe13d3009784cab426086425fbff8511e 100644
    --- a/src/miniredis/core/database.py
    +++ b/src/miniredis/core/database.py
    @@ -1,3 +1,9 @@
    +"""Own the live keyspace and apply immutable commit batches.
    +
    +The database favors staged copies and explicit invariant checks over Redis's
    +in-place dictionary updates, making atomicity visible at the cost of O(N) writes.
    +"""
    +
     from __future__ import annotations

     from collections import deque
    @@ -142,6 +148,9 @@ class Database:
             if batch.seq != next_seq:
                 raise ValueError(f"expected commit seq {next_seq}, got {batch.seq}")

    +        # Unlike Redis's in-place dictionary updates, staging the tables makes
    +        # a failed invariant check non-mutating. The explicit teaching trade-off
    +        # is O(N) copying and usage recomputation for every committed batch.
             staged = dict(self.entries)
             staged_access_tick = self.access_tick
             staged_key_revisions = dict(self.key_revisions)
    @@ -200,6 +209,9 @@ class Database:
             self.commit_seq = batch.seq

         def fork(self) -> Database:
    +        # EXEC evaluates against this deep copy so runtime errors can retain
    +        # reply slots without leaking partial planning. Real Redis executes its
    +        # queued commands directly and does not pay this whole-database copy.
             forked = Database()
             forked.entries = {
                 key: Entry(
    ```

??? note "文件差异：src/miniredis/core/eviction.py"
    ```diff
    diff --git a/src/miniredis/core/eviction.py b/src/miniredis/core/eviction.py
    index 0e4f39c1626cdaed68838b9c75f9ed0f6c1bcef2..ce2e745bcae22f017d59712e8ec44d11c1012711 100644
    --- a/src/miniredis/core/eviction.py
    +++ b/src/miniredis/core/eviction.py
    @@ -1,3 +1,9 @@
    +"""Plan maxmemory eviction in the same commit as the triggering write.
    +
    +This corresponds to Redis ``evict.c`` while using logical bytes and exact,
    +deterministic victim ordering instead of allocator memory and sampled candidates.
    +"""
    +
     from __future__ import annotations

     from collections.abc import Iterable
    @@ -94,6 +100,9 @@ def enforce_memory(
         if not writes_data:
             return plan

    +    # Redis evict.c also reclaims logically expired data before sacrificing a
    +    # live victim. Keeping those deletes in this plan makes the triggering write
    +    # and all space reclamation one atomic propagation unit.
         expired = _expired_operations(database, now_ms)
         operations = dedupe_operations(expired + plan.operations)

    @@ -128,6 +137,9 @@ def enforce_memory(
         }
         excluded = target_keys | already_deleted
         if config.eviction_policy == "allkeys-lfu":
    +        # Redis samples candidates and uses an approximate counter. Exact
    +        # projection plus stable tie-breaks is deliberate here: readers can
    +        # derive the victim without depending on randomness.
             candidate_keys = [
                 key
                 for _frequency, _tick, key in _lfu_candidates(
    @@ -138,6 +150,8 @@ def enforce_memory(
                 )
             ]
         else:
    +        # This is the allkeys-LRU lesson from evict.c with a global exact order
    +        # instead of Redis's bounded candidate sampling.
             candidate_keys = [
                 key
                 for _tick, key in sorted(
    ```

??? note "文件差异：src/miniredis/core/expiration.py"
    ```diff
    diff --git a/src/miniredis/core/expiration.py b/src/miniredis/core/expiration.py
    index 67c746aca7d520a88f5fb6c10bab91cc299a2a6e..89746f846d4abd63dd385e72f42f39a80cde84a1 100644
    --- a/src/miniredis/core/expiration.py
    +++ b/src/miniredis/core/expiration.py
    @@ -1,3 +1,9 @@
    +"""Model lazy expiry tests and the bounded active-expiry tick producer.
    +
    +``ActiveExpireProducer`` is the scheduling half of Redis ``activeExpireCycle``;
    +the executor remains the only component allowed to turn a tick into deletion.
    +"""
    +
     from collections.abc import Callable

     from miniredis.clock import Clock, ScheduledHandle, TimerScheduler
    ```

??? note "文件差异：src/miniredis/core/frequency.py"
    ```diff
    diff --git a/src/miniredis/core/frequency.py b/src/miniredis/core/frequency.py
    index 47e07b71a165ecb140e3f853623cf7507893db08..459e9e7a43f9592a83112dc8c423b534262d35bd 100644
    --- a/src/miniredis/core/frequency.py
    +++ b/src/miniredis/core/frequency.py
    @@ -1,3 +1,9 @@
    +"""Project deterministic LFU counters through elapsed decay windows.
    +
    +Redis uses an approximate logarithmic counter; MiniRedis uses exact halving so
    +tests and lessons can predict the same victim every time.
    +"""
    +
     def project_frequency(
         frequency: int,
         last_decay_ms: int,
    ```

??? note "文件差异：src/miniredis/core/hash_planner.py"
    ```diff
    diff --git a/src/miniredis/core/hash_planner.py b/src/miniredis/core/hash_planner.py
    index 479755793f0246af9143ec8f7faf8edfabacc052..da0500e7fc91161061084121ca5df57bfa72f766 100644
    --- a/src/miniredis/core/hash_planner.py
    +++ b/src/miniredis/core/hash_planner.py
    @@ -1,3 +1,5 @@
    +"""Plan hash commands as replies plus immutable commit operations."""
    +
     from miniredis.commands import model as cmd
     from miniredis.commands.parser import (
         INT64_MAX,
    ```

??? note "文件差异：src/miniredis/core/list_planner.py"
    ```diff
    diff --git a/src/miniredis/core/list_planner.py b/src/miniredis/core/list_planner.py
    index ead234743a406735df6e8ca5750510bd7a53a6aa..26981c0cc81c44d3c2f83a0d84ed04aba1c4e915 100644
    --- a/src/miniredis/core/list_planner.py
    +++ b/src/miniredis/core/list_planner.py
    @@ -1,3 +1,5 @@
    +"""Plan non-blocking list commands; waiter registration stays in the executor."""
    +
     from collections import deque

     from miniredis.commands import model as cmd
    ```

??? note "文件差异：src/miniredis/core/mailbox.py"
    ```diff
    diff --git a/src/miniredis/core/mailbox.py b/src/miniredis/core/mailbox.py
    index 92a92de7719200fece94570e0e9a4be9e169a33e..7756c97d0050cd8b387ee92b303733f995d571b1 100644
    --- a/src/miniredis/core/mailbox.py
    +++ b/src/miniredis/core/mailbox.py
    @@ -1,3 +1,5 @@
    +"""Provide the bounded event-loop mailbox that serializes all state ownership."""
    +
     from __future__ import annotations

     import asyncio
    ```

??? note "文件差异：src/miniredis/core/outbound.py"
    ```diff
    diff --git a/src/miniredis/core/outbound.py b/src/miniredis/core/outbound.py
    index 178ff18c625d6585c8a3389c48acc064aa7748fb..fd7b0f6349d9b030aac1cf2c4275c8222956f0d0 100644
    --- a/src/miniredis/core/outbound.py
    +++ b/src/miniredis/core/outbound.py
    @@ -1,3 +1,9 @@
    +"""Represent request outcomes and bounded per-session outbound delivery.
    +
    +Replies and Pub/Sub pushes share an ordered outbox, mirroring Redis's need to
    +preserve per-client output order while bounding slow-consumer memory.
    +"""
    +
     from __future__ import annotations

     import asyncio
    ```

??? note "文件差异：src/miniredis/core/planner.py"
    ```diff
    diff --git a/src/miniredis/core/planner.py b/src/miniredis/core/planner.py
    index 9f408c8bd2c9cefb5b0275c86dabc0d60c363ba5..ef470457207f6af3118145d758d4ae53aea950b9 100644
    --- a/src/miniredis/core/planner.py
    +++ b/src/miniredis/core/planner.py
    @@ -1,3 +1,9 @@
    +"""Route typed commands to pure per-type planners and maxmemory enforcement.
    +
    +The planner layer corresponds to Redis command implementations, but returns an
    +``ExecutionPlan`` rather than mutating the keyspace in place.
    +"""
    +
     from collections import deque

     from miniredis.commands.model import BlockingPop
    ```

??? note "文件差异：src/miniredis/core/planning.py"
    ```diff
    diff --git a/src/miniredis/core/planning.py b/src/miniredis/core/planning.py
    index a022a95636988ef86141838711a724d5cac722f0..5816cc4b87decfdcff5ea0e1bb032f1998ac680b 100644
    --- a/src/miniredis/core/planning.py
    +++ b/src/miniredis/core/planning.py
    @@ -1,3 +1,9 @@
    +"""Plan general and string commands and share lookup/building primitives.
    +
    +Lazy expiry is represented as a proposed delete operation, allowing the
    +executor to suppress replica-local expiry commits while keeping reads logical.
    +"""
    +
     from __future__ import annotations

     from collections.abc import Iterable
    ```

??? note "文件差异：src/miniredis/core/pubsub.py"
    ```diff
    diff --git a/src/miniredis/core/pubsub.py b/src/miniredis/core/pubsub.py
    index cd9877e7d139502f5cc7e71d10e7ff29dea1f9fe..e89df4f8561d1f425be9451e830522f8765253e6 100644
    --- a/src/miniredis/core/pubsub.py
    +++ b/src/miniredis/core/pubsub.py
    @@ -1,3 +1,5 @@
    +"""Index exact-channel subscriptions for bounded at-most-once fan-out."""
    +
     from collections import defaultdict
     from collections.abc import Callable

    ```

??? note "文件差异：src/miniredis/core/reply.py"
    ```diff
    diff --git a/src/miniredis/core/reply.py b/src/miniredis/core/reply.py
    index c1744d7cc9cb63f49e0816a26ebb9e8a274028e3..73d4883a754b5d9bb2a1e098fee4d768b86f84f4 100644
    --- a/src/miniredis/core/reply.py
    +++ b/src/miniredis/core/reply.py
    @@ -1,3 +1,5 @@
    +"""Define transport-neutral command replies before RESP2 encoding."""
    +
     from __future__ import annotations

     from dataclasses import dataclass
    ```

??? note "文件差异：src/miniredis/core/set_planner.py"
    ```diff
    diff --git a/src/miniredis/core/set_planner.py b/src/miniredis/core/set_planner.py
    index ba915cb4fb370fbc39fd26c0b29d42d6066af9fa..3aebb60efe2fbd48831ad6aa6546f33addec2018 100644
    --- a/src/miniredis/core/set_planner.py
    +++ b/src/miniredis/core/set_planner.py
    @@ -1,3 +1,5 @@
    +"""Plan set commands with deterministic reply materialization where required."""
    +
     from miniredis.commands import model as cmd
     from miniredis.core.commit import (
         CommitOperation,
    ```

??? note "文件差异：src/miniredis/core/transactions.py"
    ```diff
    diff --git a/src/miniredis/core/transactions.py b/src/miniredis/core/transactions.py
    index 259b7611361fa188e68acdea7303be91f5c31682..3a8bf597a7285b9dbe41f2906c8e9a58d9299ff1 100644
    --- a/src/miniredis/core/transactions.py
    +++ b/src/miniredis/core/transactions.py
    @@ -1,3 +1,9 @@
    +"""Hold per-session MULTI/WATCH state and transaction evaluation workspaces.
    +
    +MiniRedis evaluates EXEC on a deep database fork to make rollback-by-discard
    +simple; real Redis runs queued commands against the live keyspace.
    +"""
    +
     from dataclasses import dataclass, field

     from miniredis.commands.model import Command
    ```

??? note "文件差异：src/miniredis/core/ttl_planner.py"
    ```diff
    diff --git a/src/miniredis/core/ttl_planner.py b/src/miniredis/core/ttl_planner.py
    index bb1276ef96495974bf07f7c859250f75b7be45fe..f3455f851a6cf08c8eac637e009daea7ebae614d 100644
    --- a/src/miniredis/core/ttl_planner.py
    +++ b/src/miniredis/core/ttl_planner.py
    @@ -1,3 +1,5 @@
    +"""Plan EXPIRE, TTL/PTTL, and PERSIST against absolute millisecond deadlines."""
    +
     from miniredis.commands import model as cmd
     from miniredis.core.commit import DeleteKey, DeleteReason
     from miniredis.core.database import Database
    ```

??? note "文件差异：src/miniredis/core/values.py"
    ```diff
    diff --git a/src/miniredis/core/values.py b/src/miniredis/core/values.py
    index 3c0069ece455c305f21c7ea64767aef5ac716ad4..3ee2421967ee6237aacf53e5b32414e173ec1149 100644
    --- a/src/miniredis/core/values.py
    +++ b/src/miniredis/core/values.py
    @@ -1,3 +1,5 @@
    +"""Define mutable in-memory representations for the five supported value types."""
    +
     from __future__ import annotations

     from collections import deque
    ```

??? note "文件差异：src/miniredis/core/zset_planner.py"
    ```diff
    diff --git a/src/miniredis/core/zset_planner.py b/src/miniredis/core/zset_planner.py
    index 07d1b1646d48b659818d53ec35b868460a4d3b5e..c9a899e4f335ba18ebc3d89a44d6015f501edd28 100644
    --- a/src/miniredis/core/zset_planner.py
    +++ b/src/miniredis/core/zset_planner.py
    @@ -1,3 +1,5 @@
    +"""Plan sorted-set commands over a simple member-to-float dictionary."""
    +
     import math

     from miniredis.commands import model as cmd
    ```

这些 Module Contract 标出 Immutable Propagation Unit、Staged Database Owner、Pure Planner、Waiter/PubSub Registry、Mailbox/Outbox Ordering、Transaction Workspace、Expiry/LRU/LFU Policy、Typed Reply/Value 与 Per-data-type Planning。关键 Comment 说明有意差异：O(N) Staged Copy 与 Exact Deterministic Eviction，而不是 Redis 的 In-place 与 Sampled Production Algorithm。

#### Durability 与 Replication 阅读地图

记录 AOF、Codec、Recovery、Snapshot 与 Logical Replication-backlog Boundary，包括 MiniRedis 有意不同于 Redis 实现细节的位置。

??? note "文件差异：src/miniredis/persistence/aof.py"
    ```diff
    diff --git a/src/miniredis/persistence/aof.py b/src/miniredis/persistence/aof.py
    index 1f8ab9060aa86632f7df77be36e21665e4cd68ba..c88b372aa4d3d7ca5ccf4d8b29bb8d65d9882d04 100644
    --- a/src/miniredis/persistence/aof.py
    +++ b/src/miniredis/persistence/aof.py
    @@ -1,3 +1,9 @@
    +"""Append framed commit batches and compact them with online base-plus-delta rewrite.
    +
    +The design corresponds to Redis ``aof.c`` but uses a custom logical record
    +format and treats post-rename durability ambiguity as terminal.
    +"""
    +
     from __future__ import annotations

     import asyncio
    @@ -365,6 +371,9 @@ class AofWriter:
                 completion=completion,
                 delta=bytearray(),
             )
    +        # Redis BGREWRITEAOF likewise needs a stable base plus writes that race
    +        # with rewriting. Registration precedes the base task so no accepted
    +        # commit can fall between the checkpoint and delta capture.
             self._rewrite = state
             state.base_task = asyncio.create_task(
                 self._write_rewrite_base(state),
    @@ -452,6 +461,9 @@ class AofWriter:
                 len(state.delta) + len(record)
                 > self._rewrite_delta_limit_bytes
             ):
    +            # A bounded delta prevents an online rewrite from becoming a second
    +            # unbounded AOF in memory. Before rename, aborting safely leaves the
    +            # original append-only file authoritative.
                 state.abort_reason = "AOF rewrite delta limit exceeded"
                 self._settle_rewrite(
                     state.completion,
    @@ -484,6 +496,10 @@ class AofWriter:
                     self._path,
                 )
                 state.renamed = True
    +            # Matching durable-file replacement practice in Redis aof.c: fsync
    +            # the new file before rename, then fsync the directory entry before
    +            # switching the writer fd. A failure after rename is terminal because
    +            # the durable path owner can no longer be proved safely.
                 await asyncio.to_thread(self._ops.fsync_parent, self._path)
                 self._fd = temporary_fd
                 state.temporary_fd = None
    ```

??? note "文件差异：src/miniredis/persistence/codec.py"
    ```diff
    diff --git a/src/miniredis/persistence/codec.py b/src/miniredis/persistence/codec.py
    index c05f7609d7e5f3c47df400482bdce4e27ba5d161..7f38c6b0f1c5cae5e8821e2d822b720ce3f5c8cb 100644
    --- a/src/miniredis/persistence/codec.py
    +++ b/src/miniredis/persistence/codec.py
    @@ -1,3 +1,5 @@
    +"""Encode and validate custom snapshot, AOF-base, and commit-batch records."""
    +
     from __future__ import annotations

     import base64
    ```

??? note "文件差异：src/miniredis/persistence/recovery.py"
    ```diff
    diff --git a/src/miniredis/persistence/recovery.py b/src/miniredis/persistence/recovery.py
    index 6d7642ab422f2ccb35beade233f1a9b33d7ff176..875b5a2d56af26b0cd3a7673469cb58288cf841e 100644
    --- a/src/miniredis/persistence/recovery.py
    +++ b/src/miniredis/persistence/recovery.py
    @@ -1,3 +1,5 @@
    +"""Choose a complete checkpoint and replay only contiguous later AOF batches."""
    +
     from __future__ import annotations

     from pathlib import Path
    ```

??? note "文件差异：src/miniredis/persistence/snapshot.py"
    ```diff
    diff --git a/src/miniredis/persistence/snapshot.py b/src/miniredis/persistence/snapshot.py
    index 9fd51be96a79b501f41e0f1991f16300148cd924..ed3ddbf1b309bf62fa313c5f15e814fadde1c2d1 100644
    --- a/src/miniredis/persistence/snapshot.py
    +++ b/src/miniredis/persistence/snapshot.py
    @@ -1,3 +1,5 @@
    +"""Capture and atomically install custom stable keyspace checkpoints."""
    +
     from __future__ import annotations

     import asyncio
    ```

??? note "文件差异：src/miniredis/replication/backlog.py"
    ```diff
    diff --git a/src/miniredis/replication/backlog.py b/src/miniredis/replication/backlog.py
    index 868a725825d2b0d0471753ad735d3c9d33abd620..50b550d511cfc2e3ab36e8b7fbb5e362e5427df8 100644
    --- a/src/miniredis/replication/backlog.py
    +++ b/src/miniredis/replication/backlog.py
    @@ -1,3 +1,9 @@
    +"""Retain bounded contiguous commit history for logical partial resynchronization.
    +
    +This models Redis's replication backlog at whole-batch granularity rather than
    +as a byte ring carrying the PSYNC wire stream.
    +"""
    +
     from __future__ import annotations

     from collections import deque
    @@ -74,6 +80,9 @@ class ReplicationBacklog:
             if applied_seq == current_seq:
                 return ()
             expected = applied_seq + 1
    +        # This is the logical-batch form of Redis PSYNC backlog coverage: a
    +        # matching endpoint is insufficient unless every intervening unit is
    +        # retained contiguously through the captured primary boundary.
             selected = tuple(
                 batch for batch in self._batches if batch.seq >= expected
             )
    ```

这些 Module 记录 Framed Logical Record、AOF Base-plus-delta Rewrite、Atomic Snapshot Publication、Strict Recovery Composition 与 Bounded Whole-batch Replication Backlog。Comment 把它们连接到 Redis AOF/PSYNC Concept，同时明确保留 MiniRedis Custom Record Format 与 Batch-level In-process Model。

### 验证证据

运行聚焦 Final-acceptance Suite，执行全部三个 Example，累计构建 Stage 1–30，并要求每个 Journey-owned Source/Example/Test Path 与端点 `8151fae` 字节一致。

### 需要真正记住的内容

- Final Stage 映射既有机制，不发明新机制。
- Example 只使用 Public Production Path。
- Redis Correspondence 与 MiniRedis Trade-off 都显式说明。
- Full Parity 表示 Test、Example、Source Tree 与 Documented Ownership 一致。

### 用自己的话讲清楚

从 Public Request 到 Recovery 追踪一次带 AOF 与 Replication 的 SET，指出每个 Boundary 的 Owner，以及 MiniRedis 与 Redis 的一个有意差异。

### 教材

这是 Architectural Closeout：Executable Example 充当 Usage-level Proof，Module Contract 形成 Responsibility Map，显式 Abstraction Gap 定义 Miniature Model Boundary。

[在 GitHub 查看阶段差异](https://github.com/system-in-miniature/mini-redis/compare/94109b0...8151fae)

完成后可运行 `python -m journey.tools.build_journey check 30` 验收学习工作区。

[Complete reference patch / 完整参考补丁](https://github.com/system-in-miniature/mini-redis/blob/main/journey/stages/30-public-parity/stage.patch)
