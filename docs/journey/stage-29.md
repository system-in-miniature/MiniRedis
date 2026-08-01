# Stage 29 · Primary-owned expiry

### Goal

Make the primary the only owner of physical expiry commits while replicas still hide expired values logically, keeping replica sequence, backlog, AOF, transactions, and promotion aligned to one propagated history.

??? note "Deliverable files"
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

### Test contract

#### See the failure first

A replica GET at the deadline can allocate a local sequence before the primary sends DELETE; the next primary batch then appears non-contiguous. Active expiry can create the same split without user traffic. Marking the sink cursor before snapshot installation succeeds advertises state the replica does not yet hold. Failed resume validation can also leave a registered primary link, and always recording applied batches turns a debug aid into unbounded production memory.

??? note "File diff: tests/contract/test_domain_invariants.py"
    ```diff
    diff --git a/tests/contract/test_domain_invariants.py b/tests/contract/test_domain_invariants.py
    index cf5194487d4a80dfaa3b1c9640153796e0577706..1d47755667e740bb74b7a12d9845dca7df03ac0f 100644
    --- a/tests/contract/test_domain_invariants.py
    +++ b/tests/contract/test_domain_invariants.py
    @@ -32,7 +32,7 @@ async def test_wrongtype_never_allocates_commit(command_request):

     @pytest.mark.asyncio
     async def test_commits_rebuild_the_same_logical_database():
    -    runtime = MiniRedis.open()
    +    runtime = MiniRedis.open(debug_record_applied_batches=True)
         await runtime.start()
         client = runtime.direct_client()
         await client.execute(CommandRequest(b"SET", (b"s", b"1")))
    @@ -48,3 +48,16 @@ async def test_commits_rebuild_the_same_logical_database():
         for batch in batches:
             replay.apply_batch(batch, track_access=False)
         assert replay.logical_items() == expected
    +
    +
    +@pytest.mark.asyncio
    +async def test_applied_batches_are_not_recorded_by_default():
    +    runtime = MiniRedis.open()
    +    await runtime.start()
    +    await runtime.direct_client().execute(
    +        CommandRequest(b"SET", (b"k", b"v"))
    +    )
    +    batches = runtime.debug_applied_batches()
    +    await runtime.close()
    +
    +    assert batches == ()
    ```

Locks applied-batch history as opt-in debug instrumentation and keeps default production recording empty.

??? note "File diff: tests/contract/test_eviction.py"
    ```diff
    diff --git a/tests/contract/test_eviction.py b/tests/contract/test_eviction.py
    index 90110b37e83bb85d7ab688a26991b21c6d348ca3..f22694e3290ca843e07b70ea94e34f248e9c1003 100644
    --- a/tests/contract/test_eviction.py
    +++ b/tests/contract/test_eviction.py
    @@ -20,7 +20,11 @@ async def test_oversized_target_does_not_evict_unrelated_key():

     @pytest.mark.asyncio
     async def test_exact_lru_evicts_cold_key_in_same_commit_as_write():
    -    async with MiniRedis.open(maxmemory=260, eviction_policy="allkeys-lru") as r:
    +    async with MiniRedis.open(
    +        maxmemory=260,
    +        eviction_policy="allkeys-lru",
    +        debug_record_applied_batches=True,
    +    ) as r:
             c = r.direct_client()
             await c.execute(CommandRequest(b"SET", (b"cold", b"x")))
             await c.execute(CommandRequest(b"SET", (b"hot", b"x")))
    @@ -65,6 +69,7 @@ async def test_expired_budget_is_purged_in_same_batch_before_noeviction_check():
             clock=clock,
             maxmemory=100,
             eviction_policy="noeviction",
    +        debug_record_applied_batches=True,
         ) as r:
             c = r.direct_client()
             assert await c.execute(CommandRequest(b"SET", (b"old", b"x"))) == Ok()
    ```

Opts eviction history assertions into explicit applied-batch recording while retaining LRU/LFU/expiry commit contracts.

??? note "File diff: tests/contract/test_ttl.py"
    ```diff
    diff --git a/tests/contract/test_ttl.py b/tests/contract/test_ttl.py
    index 0de350ebf6ddd7606305414b10363f714d10dc51..db7b5c64bfd229dd51ab0b23c4756e9098cf0b64 100644
    --- a/tests/contract/test_ttl.py
    +++ b/tests/contract/test_ttl.py
    @@ -31,6 +31,7 @@ async def test_expire_ttl_persist_and_bounded_active_cleanup():
         async with MiniRedis.open(
             clock=clock,
             active_expire_sample_size=1,
    +        debug_record_applied_batches=True,
         ) as runtime:
             c = runtime.direct_client()
             await c.execute(CommandRequest(b"SET", (b"a", b"1")))
    ```

Opts TTL batch inspection into explicit debug recording so primary expiry evidence remains intentional.

??? note "File diff: tests/mechanisms/test_blpop_push_batch.py"
    ```diff
    diff --git a/tests/mechanisms/test_blpop_push_batch.py b/tests/mechanisms/test_blpop_push_batch.py
    index 4acd1ac4efba899523bc585cbdba084813d3de6e..85da15917b9d3ec219515d0b7e56d9b8b854cbf6 100644
    --- a/tests/mechanisms/test_blpop_push_batch.py
    +++ b/tests/mechanisms/test_blpop_push_batch.py
    @@ -9,7 +9,9 @@ from miniredis.core.reply import Bytes, Items, Number

     @pytest.mark.asyncio
     async def test_full_push_then_fifo_pops_are_one_commit_batch():
    -    async with MiniRedis.open() as runtime:
    +    async with MiniRedis.open(
    +        debug_record_applied_batches=True
    +    ) as runtime:
             first_client = runtime.direct_client()
             second_client = runtime.direct_client()
             producer = runtime.direct_client()
    ```

Opts the one-batch push/wakeup assertion into debug recording without changing blocking semantics.

??? note "File diff: tests/reliability/test_final_acceptance.py"
    ```diff
    diff --git a/tests/reliability/test_final_acceptance.py b/tests/reliability/test_final_acceptance.py
    index a0124be399116d5c8f10ead1fa6720fe5df7ed7b..94b2735df216119a1ef28d889ca5fe266217326b 100644
    --- a/tests/reliability/test_final_acceptance.py
    +++ b/tests/reliability/test_final_acceptance.py
    @@ -147,6 +147,10 @@ async def test_final_acceptance_activates_components_then_leaves_no_owners(
         assert active.tcp_tasks >= 6
         assert active.timer_handles == 0
         assert active.waiters == 0
    +    assert active.primary_seq == primary.debug_commit_seq
    +    assert active.backlog_batch_count > 0
    +    assert active.full_sync_count == 1
    +    assert active.replication_id

         await sink.wait_until_applied(primary.debug_commit_seq)
         assert await replica.direct_client().execute(
    ```

Locks public replication identity, primary sequence, backlog presence, and full-sync counters alongside existing owner/durability acceptance.

??? note "File diff: tests/reliability/test_transaction_commit.py"
    ```diff
    diff --git a/tests/reliability/test_transaction_commit.py b/tests/reliability/test_transaction_commit.py
    index 7533410c3190fcbe8927536ddb3451037ef41e13..368ee628aece397d833a2c9ee6f3e15ab6e2933a 100644
    --- a/tests/reliability/test_transaction_commit.py
    +++ b/tests/reliability/test_transaction_commit.py
    @@ -11,7 +11,10 @@ async def test_transaction_is_one_aof_batch_and_recovers_as_one_commit(tmp_path)
             aof_path=tmp_path / "appendonly.mraof",
             aof_policy=AofPolicy.ALWAYS,
         )
    -    first = MiniRedis.open(config)
    +    first = MiniRedis.open(
    +        config,
    +        debug_record_applied_batches=True,
    +    )
         await first.start()
         client = first.direct_client()
         before = first.debug_commit_seq
    ```

Opts the one-AOF-batch transaction assertion into debug recording while preserving durable recovery evidence.

??? note "File diff: tests/replication/test_partial_resync.py"
    ```diff
    diff --git a/tests/replication/test_partial_resync.py b/tests/replication/test_partial_resync.py
    index a396f88667432a20f673e1a23814178c5e5e06ed..4eb3a6ac494b85c07342cfb2edfccd39e7b3d6dc 100644
    --- a/tests/replication/test_partial_resync.py
    +++ b/tests/replication/test_partial_resync.py
    @@ -17,6 +17,7 @@ from miniredis.replication.sink import (
         ReplicaSyncMode,
     )
     from tests.helpers.runtime import open_test_runtime
    +from tests.unit.persistence.test_framing import batch


     @dataclass
    @@ -257,3 +258,56 @@ async def test_backlog_gap_falls_back_to_full_and_replaces_stale_keys():
         assert tuple(replica.database.entries) == (b"b", b"c")
         await primary.close()
         await replica.close()
    +
    +
    +@pytest.mark.asyncio
    +async def test_replication_stats_report_history_without_mutating_it():
    +    primary = await open_test_runtime(
    +        config=MiniRedisConfig(replication_backlog_batches=4),
    +        replication_id_factory=lambda: "primary-A",
    +    )
    +    replica = await open_test_runtime()
    +    sink = ReplicaSink(replica, queue_limit=4)
    +    first = await primary.attach_replica(sink)
    +    client = primary.direct_client()
    +    await client.execute(CommandRequest(b"SET", (b"a", b"1")))
    +    await sink.wait_until_applied(1)
    +    await sink.disconnect()
    +    await client.execute(CommandRequest(b"SET", (b"b", b"2")))
    +    resumed = await primary.attach_replica(sink)
    +    await sink.wait_until_applied(2)
    +
    +    before = primary.debug_stats()
    +    after = primary.debug_stats()
    +
    +    assert first.sync_mode is ReplicaSyncMode.FULL
    +    assert resumed.sync_mode is ReplicaSyncMode.PARTIAL
    +    assert before.replication_id == "primary-A"
    +    assert before.primary_seq == 2
    +    assert before.backlog_oldest_seq == 1
    +    assert before.backlog_newest_seq == 2
    +    assert before.backlog_batch_count == 2
    +    assert before.full_sync_count == 1
    +    assert before.partial_sync_count == 1
    +    assert after == before
    +    await primary.close()
    +    await replica.close()
    +
    +
    +@pytest.mark.asyncio
    +async def test_failed_replica_resume_validation_detaches_primary_link():
    +    primary = await open_test_runtime(
    +        replication_id_factory=lambda: "primary-A"
    +    )
    +    replica = await open_test_runtime()
    +    sink = ReplicaSink(replica, queue_limit=4)
    +    await primary.attach_replica(sink)
    +    await sink.disconnect()
    +    replica.database.apply_batch(batch(1), track_access=False)
    +
    +    status = await primary.attach_replica(sink)
    +
    +    assert status.state is ReplicaSinkState.NEEDS_RESYNC
    +    assert primary.debug_stats().replica_links == 0
    +    await primary.close()
    +    await replica.close()
    ```

Locks side-effect-free replication stats and detachment of the primary link when replica resume validation fails.

??? note "File diff: tests/replication/test_sink_attach.py"
    ```diff
    diff --git a/tests/replication/test_sink_attach.py b/tests/replication/test_sink_attach.py
    index 8e84dbfa9212f34f5d130ba3e13ce1745ffd9b14..5fcb589a281a8d824b1af7feff37f4df86857b16 100644
    --- a/tests/replication/test_sink_attach.py
    +++ b/tests/replication/test_sink_attach.py
    @@ -6,7 +6,9 @@ from miniredis import CommandRequest
     from miniredis.config import MiniRedisConfig
     from miniredis.core.reply import Bytes, Ok
     from miniredis.replication.sink import ReplicaSink, ReplicaSinkState
    +from miniredis.replication.backlog import ReplicationCursor
     from tests.helpers.runtime import open_test_runtime
    +from tests.helpers.time import FakeClock, ManualScheduler


     @pytest.mark.asyncio
    @@ -67,6 +69,56 @@ async def test_attached_replica_rejects_user_writes():
         await replica.close()


    +@pytest.mark.asyncio
    +async def test_replica_logically_hides_expired_key_until_primary_delete_arrives():
    +    clock = FakeClock()
    +    replica_scheduler = ManualScheduler(clock)
    +    primary = await open_test_runtime(clock=clock)
    +    replica = await open_test_runtime(
    +        clock=clock,
    +        scheduler=replica_scheduler,
    +    )
    +    try:
    +        primary_client = primary.direct_client()
    +        replica_client = replica.direct_client()
    +        assert await primary_client.execute(
    +            CommandRequest(b"SET", (b"k", b"v", b"PX", b"10"))
    +        ) == Ok()
    +
    +        sink = ReplicaSink(replica, queue_limit=4)
    +        await primary.attach_replica(sink)
    +        assert replica_scheduler.pending_count == 0
    +        baseline = replica.debug_commit_seq
    +
    +        clock.advance(10)
    +        assert await replica.debug_active_expire_once() == 0
    +        assert replica.debug_commit_seq == baseline
    +        assert await replica_client.execute(
    +            CommandRequest(b"GET", (b"k",))
    +        ) == Bytes(None)
    +        assert replica.debug_commit_seq == baseline
    +        assert replica.debug_physical_key_count == 1
    +
    +        assert await primary_client.execute(
    +            CommandRequest(b"GET", (b"k",))
    +        ) == Bytes(None)
    +        await sink.wait_until_applied(primary.debug_commit_seq)
    +        assert await primary_client.execute(
    +            CommandRequest(b"SET", (b"after", b"write"))
    +        ) == Ok()
    +        await sink.wait_until_applied(primary.debug_commit_seq)
    +
    +        assert sink.status.state is ReplicaSinkState.STREAMING
    +        assert replica.debug_commit_seq == primary.debug_commit_seq
    +        assert replica.debug_physical_key_count == 1
    +        assert await replica_client.execute(
    +            CommandRequest(b"GET", (b"after",))
    +        ) == Bytes(b"write")
    +    finally:
    +        await primary.close()
    +        await replica.close()
    +
    +
     @pytest.mark.asyncio
     async def test_full_sync_resets_volatile_lfu_metadata():
         config = MiniRedisConfig(eviction_policy="allkeys-lfu")
    @@ -85,3 +137,30 @@ async def test_full_sync_resets_volatile_lfu_metadata():
         assert replica.database.entries[b"k"].last_access_tick == 0
         await primary.close()
         await replica.close()
    +
    +
    +@pytest.mark.asyncio
    +async def test_full_sync_cursor_exists_only_after_snapshot_install():
    +    primary = await open_test_runtime(
    +        replication_id_factory=lambda: "primary-A"
    +    )
    +    replica = await open_test_runtime()
    +    await primary.direct_client().execute(
    +        CommandRequest(b"SET", (b"k", b"v"))
    +    )
    +    install_gate = asyncio.Event()
    +    sink = ReplicaSink(
    +        replica,
    +        queue_limit=4,
    +        install_gate=install_gate,
    +    )
    +    attaching = asyncio.create_task(primary.attach_replica(sink))
    +    await sink.attachment_captured.wait()
    +
    +    assert sink.status.cursor is None
    +
    +    install_gate.set()
    +    await attaching
    +    assert sink.status.cursor == ReplicationCursor("primary-A", 1)
    +    await primary.close()
    +    await replica.close()
    ```

Locks logical expiry without local replica commit, quiesced active expiry, later primary delete propagation, full-sync cursor publication only after install, and neutral LFU metadata.

### Basic concepts

Logical expiry is a read rule: an entry at or past its deadline behaves absent. Physical expiry is a state transition: a delete operation consumes a commit sequence and propagates. In primary–replica replication, only the primary owns that transition; replicas retain the physical entry until the propagated delete arrives. A role-aware producer must quiesce on replica install/resume and restart on promotion.

### Why this mechanism is necessary

Replication requires one authoritative transition history. Letting every node independently materialize wall-clock expiry creates multiple histories even with identical clocks. Separating logical visibility from physical deletion preserves read semantics without sacrificing sequence continuity, AOF/backlog identity, or future partial resync.

### Runtime mental model

On a primary, lazy lookup and active-expire ticks may prepare deletes and commit them normally. On a read-only replica, planning can return null for an expired key but `_apply_plan` suppresses its prepared commit, and active expiry returns zero. Snapshot install or partial resume asks the runtime to quiesce the producer before reporting attachment success. Promotion changes the executor role and restarts the producer. The primary's eventual delete batch becomes the only physical removal on both nodes.

### Mechanism blocks

#### Primary-owned physical expiry

Let replicas hide expired values logically but suppress lazy and active expiry commits so only primary-propagated deletes advance replicated history.

??? note "File diff: src/miniredis/core/executor.py"
    ```diff
    diff --git a/src/miniredis/core/executor.py b/src/miniredis/core/executor.py
    index c9183e339d9ad551649027ba2f25f69c2fde24fb..318bf8309e83ab01bee7a234a016d2125fe622cc 100644
    --- a/src/miniredis/core/executor.py
    +++ b/src/miniredis/core/executor.py
    @@ -1,3 +1,10 @@
    +"""Serialize commands and control events through MiniRedis's single state owner.
    +
    +This module combines the roles of Redis's command execution loop, transaction
    +coordinator, propagation boundary, replica control plane, and shutdown barrier.
    +The section markers below are the intended reading map for this large module.
    +"""
    +
     from __future__ import annotations

     import asyncio
    @@ -290,6 +297,7 @@ class CommandExecutor:
             ) = None,
             replication_backlog_batches: int = 1024,
             replication_id_factory: Callable[[], str] | None = None,
    +        record_applied_batches: bool = False,
         ) -> None:
             self.database = database
             self.planner = planner
    @@ -324,7 +332,9 @@ class CommandExecutor:
             self._accepted_tokens: list[RequestToken] = []
             self._endpoints: dict[int, SessionEndpoint] = {}
             self._accepted_changed = asyncio.Event()
    -        self._applied_batches: list[CommitBatch] = []
    +        self._applied_batches: list[CommitBatch] | None = (
    +            [] if record_applied_batches else None
    +        )
             self._replica_sinks: dict[int, ReplicaSink] = {}
             self._next_replica_generation = 1
             self._active_source_generation: int | None = None
    @@ -549,6 +559,10 @@ class CommandExecutor:
                         self._finish_request(token, RuntimeClosed())
                 self._on_debug_change()

    +    # --- Command execution -------------------------------------------------
    +    # Requests enter through this dispatch loop so planning, durability, apply,
    +    # and reply publication share one total order, like Redis's main command loop.
    +
         async def _dispatch(self, message: object) -> None:
             if isinstance(message, ExecuteRequest):
                 await self._execute(message)
    @@ -604,6 +618,9 @@ class CommandExecutor:
                         )
                     )
             elif isinstance(message, AttachReplica):
    +            # Redis PSYNC makes the same choice: resume only when the history
    +            # identity matches and the backlog covers every byte/batch after
    +            # the replica cursor; any uncertainty requires a full snapshot.
                 generation = self._next_replica_generation
                 self._next_replica_generation += 1
                 boundary = self.database.commit_seq
    @@ -873,6 +890,10 @@ class CommandExecutor:
                 plan = self.planner.plan(command, self.database, now_ms)
             await self._apply_plan(request, plan, now_ms)

    +    # --- Transactions ------------------------------------------------------
    +    # MULTI/WATCH state is per session. EXEC plans on an isolated database fork
    +    # and collapses successful mutations into one propagation CommitBatch.
    +
         def _route_transaction_command(
             self,
             request: ExecuteRequest,
    @@ -1052,6 +1073,10 @@ class CommandExecutor:
                 state.clear_all()
                 self._drop_empty_transaction_state(request.session_id, state)

    +    # --- Pub/Sub -----------------------------------------------------------
    +    # Pub/Sub bypasses database commits: it routes ephemeral pushes through the
    +    # same ordered per-session outboxes used for command replies.
    +
         def _subscribe(self, request: ExecuteRequest, command: Subscribe) -> None:
             items: list[SubscriptionAck] = []
             for channel in command.channels:
    @@ -1139,7 +1164,13 @@ class CommandExecutor:
         ) -> None:
             plan = self._attach_push_wakeups(request.command, plan)
             try:
    -            if plan.prepared_commit is not None:
    +            # Redis replicas logically hide expired data but wait for the
    +            # primary's DEL propagation. Dropping a lazy-expiry commit here
    +            # keeps the replica sequence aligned with that primary history.
    +            if (
    +                plan.prepared_commit is not None
    +                and not self._replica_read_only
    +            ):
                     await self._commit_prepared(plan.prepared_commit)
             except DurabilityFailure as exc:
                 self._finish_reply(
    @@ -1196,7 +1227,8 @@ class CommandExecutor:
                 and operation.reason is DeleteReason.EVICTED
                 for operation in batch.operations
             )
    -        self._applied_batches.append(batch)
    +        if self._applied_batches is not None:
    +            self._applied_batches.append(batch)
             self.replication_backlog.append(batch)
             self._offer_replica_batch(batch)
             return batch
    @@ -1249,6 +1281,10 @@ class CommandExecutor:
                 return AofRewriteFailed("executor control admission is closed")
             return await asyncio.shield(future)

    +    # --- Replication control plane ----------------------------------------
    +    # Attach, snapshot install, cursor resume, ordered apply, and promotion all
    +    # re-enter the mailbox so no user command can split a control-plane decision.
    +
         async def attach_replica(
             self,
             sink: ReplicaSink,
    @@ -1326,6 +1362,10 @@ class CommandExecutor:
             return await asyncio.shield(future)

         async def _active_expire_once(self, now_ms: int) -> int:
    +        # Redis replicas do not run active expiry deletion; the primary owns
    +        # expiry propagation. Returning zero also prevents a local seq advance.
    +        if self._replica_read_only:
    +            return 0
             keys = sorted(
                 key
                 for key, entry in self.database.entries.items()
    @@ -1360,6 +1400,10 @@ class CommandExecutor:
                 return 0
             return len(operations)

    +    # --- Shutdown state machine -------------------------------------------
    +    # Admission closes first, then the executor resolves every accepted owner
    +    # before the runtime releases the held barrier and joins this worker.
    +
         async def close(self) -> None:
             if self._close_task is None:
                 self._close_task = asyncio.create_task(
    @@ -1528,4 +1572,8 @@ class CommandExecutor:
             return self._failure

         def debug_applied_batches(self) -> tuple[CommitBatch, ...]:
    -        return tuple(self._applied_batches)
    +        return (
    +            ()
    +            if self._applied_batches is None
    +            else tuple(self._applied_batches)
    +        )
    ```

Suppresses prepared commits and active expiry while replica-read-only, makes applied-batch recording opt-in, and adds reading-map documentation plus replication statistics.

```python
if plan.prepared_commit is not None and not self._replica_read_only:
    await self._commit_prepared(plan.prepared_commit)
```

The reply still reflects logical expiry, but no local mutation or sequence is created on the replica.

#### Role-aware expiry producer lifecycle

Quiesce the active-expire producer after full install or partial resume and restart it only after successful promotion.

??? note "File diff: src/miniredis/runtime.py"
    ```diff
    diff --git a/src/miniredis/runtime.py b/src/miniredis/runtime.py
    index 3358d8aebebab9ca8cf39627aa492a3f13d8d15e..bf6efe09144d59170fefc1a0f78f676ed2b69559 100644
    --- a/src/miniredis/runtime.py
    +++ b/src/miniredis/runtime.py
    @@ -1,3 +1,9 @@
    +"""Assemble MiniRedis subsystems and own their startup-to-shutdown lifecycle.
    +
    +The runtime is the public facade; semantic execution remains in the serialized
    +executor, while this layer owns tasks, persistence workers, servers, and sinks.
    +"""
    +
     from __future__ import annotations

     import asyncio
    @@ -19,7 +25,7 @@ from miniredis.commands.model import Command
     from miniredis.commands.parser import CommandParseError, parse_command_request
     from miniredis.commands.request import CommandRequest
     from miniredis.core.blocking import WaiterId
    -from miniredis.core.commit import CommitBatch, StoredEntry
    +from miniredis.core.commit import CommitBatch, SnapshotImage, StoredEntry
     from miniredis.core.database import Database
     from miniredis.core.executor import (
         ActiveExpireTick,
    @@ -28,6 +34,7 @@ from miniredis.core.executor import (
         CommandExecutor,
         CommitBarrier,
         NullCommitBarrier,
    +    PromotionResult,
         SessionClosed,
         SubmittedRequest,
     )
    @@ -96,6 +103,13 @@ class RuntimeStats:
         aof_rewrite_active: bool
         aof_rewrite_delta_bytes: int
         aof_rewrite_checkpoint_seq: int | None
    +    replication_id: str
    +    primary_seq: int
    +    backlog_oldest_seq: int | None
    +    backlog_newest_seq: int | None
    +    backlog_batch_count: int
    +    full_sync_count: int
    +    partial_sync_count: int


     @dataclass(slots=True)
    @@ -124,6 +138,7 @@ class MiniRedis:
             commit_barrier: CommitBarrier,
             scheduler: TimerScheduler | None,
             test_hooks: _RuntimeTestHooks | None = None,
    +        debug_record_applied_batches: bool = False,
         ) -> None:
             self.config = config
             self.clock = clock
    @@ -175,6 +190,7 @@ class MiniRedis:
                     if self._test_hooks is None
                     else self._test_hooks.replication_id_factory
                 ),
    +            record_applied_batches=debug_record_applied_batches,
             )
             self.executor.mailbox.close_user_admission()
             self._snapshot_manager = (
    @@ -194,6 +210,7 @@ class MiniRedis:
             self._lifecycle_lock = asyncio.Lock()
             self._shutdown_task: asyncio.Task[None] | None = None
             self._control_producers: set[object] = set()
    +        self._active_expire_producer: ActiveExpireProducer | None = None
             self._owned_tasks: set[asyncio.Task[object]] = set()
             self._failure_reason: str | None = None
             self._shutdown_complete = False
    @@ -208,6 +225,7 @@ class MiniRedis:
             clock: Clock | None = None,
             scheduler: TimerScheduler | None = None,
             commit_barrier: CommitBarrier | None = None,
    +        debug_record_applied_batches: bool = False,
             **options: Any,
         ) -> MiniRedis:
             if config is not None and options:
    @@ -221,6 +239,7 @@ class MiniRedis:
                     commit_barrier if commit_barrier is not None else NullCommitBarrier()
                 ),
                 test_hooks=None,
    +            debug_record_applied_batches=debug_record_applied_batches,
             )

         @classmethod
    @@ -232,6 +251,7 @@ class MiniRedis:
             scheduler: TimerScheduler | None = None,
             commit_barrier: CommitBarrier | None = None,
             test_hooks: _RuntimeTestHooks | None = None,
    +        debug_record_applied_batches: bool = False,
             **options: Any,
         ) -> MiniRedis:
             if config is not None and options:
    @@ -245,6 +265,7 @@ class MiniRedis:
                     commit_barrier if commit_barrier is not None else NullCommitBarrier()
                 ),
                 test_hooks=test_hooks,
    +            debug_record_applied_batches=debug_record_applied_batches,
             )

         async def start(self) -> None:
    @@ -314,6 +335,7 @@ class MiniRedis:
                         self.executor.post_control,
                         lambda now_ms: ActiveExpireTick(now_ms, None),
                     )
    +                self._active_expire_producer = producer
                     self._control_producers.add(producer)
                     producer.start()
                     self.executor.mailbox.open_user_admission()
    @@ -564,6 +586,7 @@ class MiniRedis:
             self._trace_lifecycle("executor-stopped")

             self._control_producers.clear()
    +        self._active_expire_producer = None
             current = asyncio.current_task()
             for owned in tuple(self._owned_tasks):
                 if owned.done() or owned is current:
    @@ -652,6 +675,44 @@ class MiniRedis:
                     self._owned_replica_sinks.discard(sink)
                 raise

    +    async def install_replica_snapshot(
    +        self,
    +        sink: ReplicaSink,
    +        generation: int,
    +        replication_id: str,
    +        image: SnapshotImage,
    +    ) -> bool:
    +        installed = await self.executor.install_replica_snapshot(
    +            sink,
    +            generation,
    +            replication_id,
    +            image,
    +        )
    +        if installed and self._active_expire_producer is not None:
    +            await self._active_expire_producer.quiesce()
    +        return installed
    +
    +    async def prepare_replica_resume(
    +        self,
    +        generation: int,
    +        replication_id: str,
    +        expected_applied_seq: int,
    +    ) -> bool:
    +        prepared = await self.executor.prepare_replica_resume(
    +            generation,
    +            replication_id,
    +            expected_applied_seq,
    +        )
    +        if prepared and self._active_expire_producer is not None:
    +            await self._active_expire_producer.quiesce()
    +        return prepared
    +
    +    async def promote_replica(self, generation: int) -> PromotionResult:
    +        result = await self.executor.promote_replica(generation)
    +        if result.writable and self._active_expire_producer is not None:
    +            self._active_expire_producer.start()
    +        return result
    +
         def _replica_attach_done(
             self,
             sink: ReplicaSink,
    @@ -806,6 +867,19 @@ class MiniRedis:
                     if self._aof_writer is None
                     else self._aof_writer.rewrite_checkpoint_seq
                 ),
    +            replication_id=self.executor.replication_id,
    +            primary_seq=self.database.commit_seq,
    +            backlog_oldest_seq=(
    +                self.executor.replication_backlog.oldest_seq
    +            ),
    +            backlog_newest_seq=(
    +                self.executor.replication_backlog.newest_seq
    +            ),
    +            backlog_batch_count=(
    +                self.executor.replication_backlog.batch_count
    +            ),
    +            full_sync_count=self.executor.full_sync_count,
    +            partial_sync_count=self.executor.partial_sync_count,
             )

         def _debug_notify(self) -> None:
    ```

Owns the active-expire producer across role changes, wraps install/resume/promotion, exposes stable replication stats, and passes explicit debug-history configuration.

??? note "File diff: src/miniredis/replication/sink.py"
    ```diff
    diff --git a/src/miniredis/replication/sink.py b/src/miniredis/replication/sink.py
    index 34a27652947c6e542371ae8d0750e7678dbc2ff2..b2b1c08346eebe79201de6498fe69f3575aee9a0 100644
    --- a/src/miniredis/replication/sink.py
    +++ b/src/miniredis/replication/sink.py
    @@ -1,3 +1,9 @@
    +"""Model one asynchronous primary-to-replica link and its resync state machine.
    +
    +``ReplicaSink`` corresponds to a Redis replica connection, but delivery stays
    +in process and records only the last fully applied logical cursor.
    +"""
    +
     from __future__ import annotations

     import asyncio
    @@ -132,17 +138,16 @@ class ReplicaSink:
             if self._state is not ReplicaSinkState.BOOTSTRAPPING:
                 raise RuntimeError("sink is not bootstrapping")
             self._generation = attachment.generation
    -        self._replication_id = attachment.replication_id
             if isinstance(attachment, FullSyncAttachment):
                 self._sync_mode = ReplicaSyncMode.FULL
                 self._baseline_seq = attachment.image.checkpoint_seq
    -            self._applied_seq = attachment.image.checkpoint_seq
                 self._primary_seq = attachment.image.checkpoint_seq
                 self._catch_up.clear()
             else:
    +            if self.cursor != attachment.cursor:
    +                raise RuntimeError("partial attachment cursor changed")
                 self._sync_mode = ReplicaSyncMode.PARTIAL
                 self._baseline_seq = attachment.cursor.applied_seq
    -            self._applied_seq = attachment.cursor.applied_seq
                 self._primary_seq = attachment.boundary_seq
                 self._catch_up = deque(attachment.batches)
             self._attachment_captured.set()
    @@ -184,7 +189,7 @@ class ReplicaSink:
                     return self.status
                 if isinstance(attachment, FullSyncAttachment):
                     installed = (
    -                    await self._replica.executor.install_replica_snapshot(
    +                    await self._replica.install_replica_snapshot(
                             self,
                             attachment.generation,
                             attachment.replication_id,
    @@ -193,7 +198,7 @@ class ReplicaSink:
                     )
                 else:
                     installed = (
    -                    await self._replica.executor.prepare_replica_resume(
    +                    await self._replica.prepare_replica_resume(
                             attachment.generation,
                             attachment.replication_id,
                             attachment.cursor.applied_seq,
    @@ -201,10 +206,26 @@ class ReplicaSink:
                     )
                 if not installed:
                     self._state = ReplicaSinkState.NEEDS_RESYNC
    +                await primary.executor.detach_replica(
    +                    attachment.generation
    +                )
    +                primary._release_replica_sink(self)
                     self._signal_status_change()
                     return self.status
                 if self._state is not ReplicaSinkState.BOOTSTRAPPING:
                     return self.status
    +            if isinstance(attachment, FullSyncAttachment):
    +                self._replication_id = attachment.replication_id
    +                self._applied_seq = attachment.image.checkpoint_seq
    +                self._primary_seq = max(
    +                    self._primary_seq,
    +                    attachment.image.checkpoint_seq,
    +                )
    +            else:
    +                self._replication_id = attachment.replication_id
    +            # As in Redis partial resynchronization, backlog catch-up must drain
    +            # before writes observed live during attachment; otherwise sequence
    +            # order could invert at the replica.
                 self._state = (
                     ReplicaSinkState.CATCHING_UP
                     if self._catch_up
    @@ -378,7 +399,7 @@ class ReplicaSink:
                     pass
             self._queue.clear()
             self._catch_up.clear()
    -        result = await self._replica.executor.promote_replica(self._generation)
    +        result = await self._replica.promote_replica(self._generation)
             if not result.writable:
                 self._state = ReplicaSinkState.FAILED
                 self._signal_status_change()
    ```

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

[Compare this stage on GitHub](https://github.com/system-in-miniature/mini-redis/compare/c07182f...94109b0)

After finishing, run `python -m journey.tools.build_journey check 29` to verify the learner workspace.

[Complete reference patch / 完整参考补丁](https://github.com/system-in-miniature/mini-redis/blob/main/journey/stages/29-primary-owned-expiry/stage.patch)
