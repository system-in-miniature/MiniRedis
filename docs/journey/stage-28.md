# Stage 28 · Partial resynchronization

### Goal

Reconnect a detached or overflowed replica by applying an available missing backlog suffix before live batches, while fencing stale source generations and falling back to full replacement when continuity cannot be proven.

??? note "Deliverable files"
    - `src/miniredis/core/executor.py`
    - `src/miniredis/replication/sink.py`
    - `src/miniredis/runtime.py`
    - `tests/reliability/test_restart.py`
    - `tests/replication/test_partial_resync.py`
    - `tests/replication/test_promotion.py`
    - `tests/replication/test_sink_overflow.py`

### The problem at this point

Stage 27 can select a partial attachment, but the sink still cannot install it. Resume must verify the replica still holds exactly the cursor state, apply frozen catch-up batches before any concurrently offered live batch, preserve a reconnectable cursor on disconnect/overflow, and reset source identity/backlog when a replica becomes a new primary.

### Test contract

#### See the failure first

Applying live batch N+2 before catch-up N+1 creates divergence. Resuming onto a replica whose local sequence or source identity changed corrupts state. Reusing the old primary ID after promotion allows descendants to request unrelated history. Treating queue overflow as permanently dead wastes a still-covered backlog; forcing partial sync after rotation leaves stale keys that only full snapshot replacement removes.

??? note "File diff: tests/reliability/test_restart.py"
    ```diff
    diff --git a/tests/reliability/test_restart.py b/tests/reliability/test_restart.py
    index 2ebe298c08176aafd238be5b8671ceeb9c3d629d..6771acdd39eeaf3a44c5603764e00fd04b3e77d4 100644
    --- a/tests/reliability/test_restart.py
    +++ b/tests/reliability/test_restart.py
    @@ -6,6 +6,8 @@ from miniredis.core.reply import Bytes, Ok
     from miniredis.persistence.aof import AofPolicy
     from miniredis.persistence.recovery import RecoveryError
     from miniredis.runtime import RuntimeState
    +from miniredis.replication.sink import ReplicaSink, ReplicaSyncMode
    +from tests.helpers.runtime import open_test_runtime


     @pytest.mark.asyncio
    @@ -76,3 +78,38 @@ async def test_restart_resets_volatile_lfu_metadata(tmp_path):
         assert second.database.entries[b"k"].frequency == 0
         assert second.database.entries[b"k"].last_access_tick == 0
         await second.close()
    +
    +
    +@pytest.mark.asyncio
    +async def test_primary_restart_changes_identity_and_forces_full_sync(
    +    tmp_path,
    +):
    +    config = MiniRedisConfig(
    +        aof_path=tmp_path / "appendonly.mraof",
    +        aof_policy=AofPolicy.ALWAYS,
    +    )
    +    first = await open_test_runtime(
    +        config=config,
    +        replication_id_factory=lambda: "primary-A",
    +    )
    +    replica = await open_test_runtime()
    +    sink = ReplicaSink(replica, queue_limit=4)
    +    await first.attach_replica(sink)
    +    await first.direct_client().execute(
    +        CommandRequest(b"SET", (b"k", b"v"))
    +    )
    +    await sink.wait_until_applied(1)
    +    await sink.disconnect()
    +    await first.close()
    +
    +    restarted = await open_test_runtime(
    +        config=config,
    +        replication_id_factory=lambda: "primary-B",
    +    )
    +    status = await restarted.attach_replica(sink)
    +
    +    assert status.sync_mode is ReplicaSyncMode.FULL
    +    assert status.replication_id == "primary-B"
    +    assert status.applied_seq == 1
    +    await restarted.close()
    +    await replica.close()
    ```

Locks runtime restart as a new primary identity that forces full sync even when recovered commit sequence equals the replica cursor.

??? note "File diff: tests/replication/test_partial_resync.py"
    ```diff
    diff --git a/tests/replication/test_partial_resync.py b/tests/replication/test_partial_resync.py
    index bea308e1178575344627cc68c46ca8dff40a901f..a396f88667432a20f673e1a23814178c5e5e06ed 100644
    --- a/tests/replication/test_partial_resync.py
    +++ b/tests/replication/test_partial_resync.py
    @@ -1,3 +1,4 @@
    +import asyncio
     from dataclasses import dataclass

     import pytest
    @@ -10,6 +11,11 @@ from miniredis.replication.backlog import (
         PartialSyncAttachment,
         ReplicationCursor,
     )
    +from miniredis.replication.sink import (
    +    ReplicaSink,
    +    ReplicaSinkState,
    +    ReplicaSyncMode,
    +)
     from tests.helpers.runtime import open_test_runtime


    @@ -90,6 +96,24 @@ async def test_matching_current_cursor_uses_empty_partial_sync(tmp_path):
         await primary.close()


    +@pytest.mark.asyncio
    +async def test_empty_backlog_accepts_current_cursor():
    +    primary = await open_test_runtime(
    +        replication_id_factory=lambda: "primary-A"
    +    )
    +    probe = AttachmentProbe()
    +
    +    attachment = await primary.executor.attach_replica(
    +        probe,
    +        ReplicationCursor("primary-A", 0),
    +    )
    +
    +    assert isinstance(attachment, PartialSyncAttachment)
    +    assert attachment.batches == ()
    +    await primary.executor.detach_replica(attachment.generation)
    +    await primary.close()
    +
    +
     @pytest.mark.asyncio
     @pytest.mark.parametrize(
         "cursor",
    @@ -113,3 +137,123 @@ async def test_diverged_or_uncovered_cursor_falls_back_to_full_sync(
         assert attachment.image.checkpoint_seq == 3
         await primary.executor.detach_replica(attachment.generation)
         await primary.close()
    +
    +
    +@pytest.mark.asyncio
    +async def test_short_disconnect_resumes_only_missing_batches():
    +    primary = await open_test_runtime(
    +        replication_id_factory=lambda: "primary-A"
    +    )
    +    replica = await open_test_runtime()
    +    sink = ReplicaSink(replica, queue_limit=4)
    +    await primary.attach_replica(sink)
    +    client = primary.direct_client()
    +    assert await client.execute(
    +        CommandRequest(b"SET", (b"a", b"1"))
    +    ) == Ok()
    +    await sink.wait_until_applied(primary.debug_commit_seq)
    +
    +    retained = await sink.disconnect()
    +    assert retained.state is ReplicaSinkState.DETACHED
    +    assert retained.cursor == ReplicationCursor("primary-A", 1)
    +    assert await client.execute(
    +        CommandRequest(b"SET", (b"b", b"2"))
    +    ) == Ok()
    +
    +    status = await primary.attach_replica(sink)
    +    await sink.wait_until_applied(primary.debug_commit_seq)
    +
    +    assert status.sync_mode is ReplicaSyncMode.PARTIAL
    +    assert sink.status.applied_seq == primary.debug_commit_seq
    +    assert tuple(replica.database.entries) == (b"a", b"b")
    +    await primary.close()
    +    await replica.close()
    +
    +
    +@pytest.mark.asyncio
    +async def test_partial_catchup_precedes_concurrent_live_batch():
    +    install_gate = asyncio.Event()
    +    install_gate.set()
    +    primary = await open_test_runtime(
    +        replication_id_factory=lambda: "primary-A"
    +    )
    +    replica = await open_test_runtime()
    +    sink = ReplicaSink(
    +        replica,
    +        queue_limit=4,
    +        install_gate=install_gate,
    +    )
    +    await primary.attach_replica(sink)
    +    client = primary.direct_client()
    +    await client.execute(CommandRequest(b"SET", (b"a", b"1")))
    +    await sink.wait_until_applied(1)
    +    await sink.disconnect()
    +    await client.execute(CommandRequest(b"SET", (b"b", b"2")))
    +
    +    install_gate.clear()
    +    attaching = asyncio.create_task(primary.attach_replica(sink))
    +    await sink.attachment_captured.wait()
    +    assert sink.status.primary_seq == 2
    +    await client.execute(CommandRequest(b"SET", (b"c", b"3")))
    +    assert sink.status.queued == 1
    +
    +    install_gate.set()
    +    status = await attaching
    +    await sink.wait_until_applied(3)
    +
    +    assert status.sync_mode is ReplicaSyncMode.PARTIAL
    +    assert replica.debug_commit_seq == 3
    +    assert tuple(replica.database.entries) == (b"a", b"b", b"c")
    +    assert sink.status.state is ReplicaSinkState.STREAMING
    +    await primary.close()
    +    await replica.close()
    +
    +
    +@pytest.mark.asyncio
    +async def test_cursor_exactly_before_oldest_backlog_batch_is_partial():
    +    primary = await open_test_runtime(
    +        config=MiniRedisConfig(replication_backlog_batches=2),
    +        replication_id_factory=lambda: "primary-A",
    +    )
    +    replica = await open_test_runtime()
    +    sink = ReplicaSink(replica, queue_limit=4)
    +    await primary.attach_replica(sink)
    +    client = primary.direct_client()
    +    await client.execute(CommandRequest(b"SET", (b"a", b"1")))
    +    await sink.wait_until_applied(1)
    +    await sink.disconnect()
    +    await client.execute(CommandRequest(b"SET", (b"b", b"2")))
    +    await client.execute(CommandRequest(b"SET", (b"c", b"3")))
    +
    +    status = await primary.attach_replica(sink)
    +    await sink.wait_until_applied(3)
    +
    +    assert status.sync_mode is ReplicaSyncMode.PARTIAL
    +    await primary.close()
    +    await replica.close()
    +
    +
    +@pytest.mark.asyncio
    +async def test_backlog_gap_falls_back_to_full_and_replaces_stale_keys():
    +    primary = await open_test_runtime(
    +        config=MiniRedisConfig(replication_backlog_batches=2),
    +        replication_id_factory=lambda: "primary-A",
    +    )
    +    replica = await open_test_runtime()
    +    sink = ReplicaSink(replica, queue_limit=4)
    +    await primary.attach_replica(sink)
    +    client = primary.direct_client()
    +    await client.execute(CommandRequest(b"SET", (b"obsolete", b"1")))
    +    await sink.wait_until_applied(1)
    +    await sink.disconnect()
    +    await client.execute(CommandRequest(b"DEL", (b"obsolete",)))
    +    await client.execute(CommandRequest(b"SET", (b"b", b"3")))
    +    await client.execute(CommandRequest(b"SET", (b"c", b"4")))
    +
    +    status = await primary.attach_replica(sink)
    +
    +    assert status.sync_mode is ReplicaSyncMode.FULL
    +    assert b"obsolete" not in replica.database.entries
    +    assert tuple(replica.database.entries) == (b"b", b"c")
    +    await primary.close()
    +    await replica.close()
    ```

Locks current-cursor empty resume, short disconnect delta-only catch-up, catch-up-before-concurrent-live ordering, exact oldest-boundary coverage, and full replacement after backlog gaps.

??? note "File diff: tests/replication/test_promotion.py"
    ```diff
    diff --git a/tests/replication/test_promotion.py b/tests/replication/test_promotion.py
    index 51406f5ca1557311343cf026d2a6bd7dcf3e8909..58379bb229215f8955ea21453b3b7488693b2dda 100644
    --- a/tests/replication/test_promotion.py
    +++ b/tests/replication/test_promotion.py
    @@ -1,4 +1,5 @@
     import asyncio
    +from dataclasses import dataclass

     import pytest

    @@ -12,6 +13,10 @@ from miniredis.core.commit import (
     )
     from miniredis.core.reply import Bytes, Ok
     from miniredis.replication.sink import ReplicaSink, ReplicaSinkState
    +from miniredis.replication.backlog import (
    +    FullSyncAttachment,
    +    ReplicationCursor,
    +)
     from tests.helpers.runtime import open_test_runtime


    @@ -28,6 +33,17 @@ def batch(seq: int, value: bytes) -> CommitBatch:
         )


    +@dataclass
    +class AttachmentProbe:
    +    attachment: object | None = None
    +
    +    def register_attachment(self, attachment) -> None:
    +        self.attachment = attachment
    +
    +    def offer(self, _batch) -> bool:
    +        return True
    +
    +
     @pytest.mark.asyncio
     async def test_apply_accepted_before_promotion_finishes_before_barrier():
         primary = await open_test_runtime()
    @@ -99,3 +115,43 @@ async def test_link_generations_are_never_reused():
         await primary.close()
         await first_replica.close()
         await second_replica.close()
    +
    +
    +@pytest.mark.asyncio
    +async def test_promotion_fences_old_history_and_starts_new_backlog():
    +    promoted_ids = iter(("replica-seed", "promoted-B"))
    +    primary = await open_test_runtime(
    +        replication_id_factory=lambda: "primary-A"
    +    )
    +    promoted = await open_test_runtime(
    +        replication_id_factory=lambda: next(promoted_ids)
    +    )
    +    sink = ReplicaSink(promoted, queue_limit=4)
    +    await primary.attach_replica(sink)
    +    old_id = sink.status.replication_id
    +
    +    result = await sink.promote(source_alive=True)
    +
    +    assert old_id == "primary-A"
    +    assert result.replication_id == "promoted-B"
    +    assert result.replication_id != old_id
    +    assert sink.status.replication_id == "promoted-B"
    +    assert promoted.debug_replication_backlog_count == 0
    +
    +    probe = AttachmentProbe()
    +    attachment = await promoted.executor.attach_replica(
    +        probe,
    +        ReplicationCursor(old_id, result.applied_seq),
    +    )
    +    assert isinstance(attachment, FullSyncAttachment)
    +    await promoted.executor.detach_replica(attachment.generation)
    +
    +    assert await promoted.direct_client().execute(
    +        CommandRequest(b"SET", (b"k", b"new"))
    +    ) == Ok()
    +    assert promoted.debug_replication_backlog_count == 1
    +    assert promoted.debug_replication_backlog_oldest_seq == (
    +        result.applied_seq + 1
    +    )
    +    await primary.close()
    +    await promoted.close()
    ```

Locks promotion as a new replication identity with empty backlog, rejects cursors from the old source, then starts a new backlog at the next local commit.

??? note "File diff: tests/replication/test_sink_overflow.py"
    ```diff
    diff --git a/tests/replication/test_sink_overflow.py b/tests/replication/test_sink_overflow.py
    index 7348b563b30c1d6039ade2eb529f7690db0d2d14..fdb8f9382b1788bd4bfb7bb8598c70656d720b51 100644
    --- a/tests/replication/test_sink_overflow.py
    +++ b/tests/replication/test_sink_overflow.py
    @@ -4,7 +4,12 @@ import pytest

     from miniredis import CommandRequest
     from miniredis.core.reply import Bytes, Ok
    -from miniredis.replication.sink import ReplicaSink, ReplicaSinkState
    +from miniredis.config import MiniRedisConfig
    +from miniredis.replication.sink import (
    +    ReplicaSink,
    +    ReplicaSinkState,
    +    ReplicaSyncMode,
    +)
     from tests.helpers.runtime import open_test_runtime


    @@ -94,3 +99,57 @@ async def test_bootstrap_overflow_never_installs_the_stale_snapshot():
         assert primary.debug_stats().replica_links == 0
         await primary.close()
         await replica.close()
    +
    +
    +@pytest.mark.asyncio
    +async def test_queue_overflow_can_reattach_from_backlog():
    +    primary = await open_test_runtime(
    +        config=MiniRedisConfig(replication_backlog_batches=4),
    +        replication_id_factory=lambda: "primary-A",
    +    )
    +    replica = await open_test_runtime()
    +    sink = ReplicaSink(replica, queue_limit=1)
    +    await primary.attach_replica(sink)
    +    sink.pause()
    +    client = primary.direct_client()
    +    await client.execute(CommandRequest(b"SET", (b"a", b"1")))
    +    await client.execute(CommandRequest(b"SET", (b"b", b"2")))
    +    assert sink.status.state is ReplicaSinkState.NEEDS_RESYNC
    +
    +    sink.resume()
    +    status = await primary.attach_replica(sink)
    +    await sink.wait_until_applied(2)
    +
    +    assert status.sync_mode is ReplicaSyncMode.PARTIAL
    +    assert tuple(replica.database.entries) == (b"a", b"b")
    +    await primary.close()
    +    await replica.close()
    +
    +
    +@pytest.mark.asyncio
    +async def test_queue_overflow_falls_back_after_backlog_rotates():
    +    primary = await open_test_runtime(
    +        config=MiniRedisConfig(replication_backlog_batches=2),
    +        replication_id_factory=lambda: "primary-A",
    +    )
    +    replica = await open_test_runtime()
    +    sink = ReplicaSink(replica, queue_limit=1)
    +    await primary.attach_replica(sink)
    +    sink.pause()
    +    client = primary.direct_client()
    +    for seq in range(1, 5):
    +        await client.execute(
    +            CommandRequest(
    +                b"SET",
    +                (f"k{seq}".encode(), str(seq).encode()),
    +            )
    +        )
    +    assert sink.status.state is ReplicaSinkState.NEEDS_RESYNC
    +
    +    sink.resume()
    +    status = await primary.attach_replica(sink)
    +
    +    assert status.sync_mode is ReplicaSyncMode.FULL
    +    assert sink.status.applied_seq == 4
    +    await primary.close()
    +    await replica.close()
    ```

Locks overflow recovery through partial backlog when covered and full fallback after backlog rotation.

### Basic concepts

Partial resume has two ordered inputs: a frozen catch-up deque through attachment boundary B and a live queue containing commits after B. `CATCHING_UP` drains the former before `STREAMING` drains the latter. The replica executor stores active source ID and generation; resume is allowed only when read-only state, source ID, and current sequence match the cursor. Promotion creates a new epoch and clears inherited backlog.

### Why this mechanism is necessary

The backlog optimization is useful only when the sink can turn retained history into a verified state transition. Explicit catch-up state closes the race with concurrent commits, while executor-side source/sequence validation prevents applying a correct suffix to the wrong local base. Identity rotation on restart/promotion fences separate histories that reuse sequence numbers.

### Runtime mental model

Disconnect stops the apply task and clears volatile queues but retains `(replication_id, applied_seq)`. Reattach sends that cursor. Full attachment replaces the database and source identity; partial attachment asks the replica executor to fence the expected base, then seeds `_catch_up`. New primary commits offered during installation enter `_queue`. The apply worker enforces `batch.seq == applied_seq + 1`, drains catch-up fully, transitions to streaming, then drains live queue.

### Mechanism blocks

#### Replica resume fencing

Install source identity with full snapshots, validate identity and exact applied sequence before partial resume, and mint a new identity with an empty backlog on promotion.

??? note "File diff: src/miniredis/core/executor.py"
    ```diff
    diff --git a/src/miniredis/core/executor.py b/src/miniredis/core/executor.py
    index c8ade4b1ae6adac06444fc53bea1fd23c195d309..c9183e339d9ad551649027ba2f25f69c2fde24fb 100644
    --- a/src/miniredis/core/executor.py
    +++ b/src/miniredis/core/executor.py
    @@ -165,10 +165,19 @@ class DetachReplica:
     class InstallReplicaSnapshot:
         sink: ReplicaSink
         generation: int
    +    replication_id: str
         image: SnapshotImage
         future: asyncio.Future[bool]


    +@dataclass(slots=True)
    +class PrepareReplicaResume:
    +    generation: int
    +    replication_id: str
    +    expected_applied_seq: int
    +    future: asyncio.Future[bool]
    +
    +
     @dataclass(slots=True)
     class ApplyReplicaBatch:
         generation: int
    @@ -180,6 +189,7 @@ class ApplyReplicaBatch:
     class PromotionResult:
         applied_seq: int
         writable: bool
    +    replication_id: str


     @dataclass(slots=True)
    @@ -318,6 +328,7 @@ class CommandExecutor:
             self._replica_sinks: dict[int, ReplicaSink] = {}
             self._next_replica_generation = 1
             self._active_source_generation: int | None = None
    +        self._active_source_id: str | None = None
             self._replica_read_only = False
             self._transactions: dict[int, TransactionState] = {}
             self._transaction_aborts = 0
    @@ -640,8 +651,19 @@ class CommandExecutor:
                     now_ms=self.clock.now_ms(),
                 )
                 self._active_source_generation = message.generation
    +            self._active_source_id = message.replication_id
                 self._replica_read_only = True
                 message.future.set_result(True)
    +        elif isinstance(message, PrepareReplicaResume):
    +            allowed = (
    +                self._replica_read_only
    +                and self._active_source_id == message.replication_id
    +                and self.database.commit_seq
    +                == message.expected_applied_seq
    +            )
    +            if allowed:
    +                self._active_source_generation = message.generation
    +            message.future.set_result(allowed)
             elif isinstance(message, ApplyReplicaBatch):
                 if message.generation != self._active_source_generation:
                     message.future.set_result(False)
    @@ -668,12 +690,25 @@ class CommandExecutor:
             elif isinstance(message, PromoteReplica):
                 if message.generation != self._active_source_generation:
                     message.future.set_result(
    -                    PromotionResult(self.database.commit_seq, False)
    +                    PromotionResult(
    +                        self.database.commit_seq,
    +                        False,
    +                        self.replication_id,
    +                    )
                     )
                     return
                 self._active_source_generation = None
    +            self._active_source_id = None
                 self._replica_read_only = False
    -            message.future.set_result(PromotionResult(self.database.commit_seq, True))
    +            self.replication_id = self._replication_id_factory()
    +            self.replication_backlog.clear()
    +            message.future.set_result(
    +                PromotionResult(
    +                    self.database.commit_seq,
    +                    True,
    +                    self.replication_id,
    +                )
    +            )
             else:
                 raise AssertionError(f"unknown executor message: {message!r}")

    @@ -1236,11 +1271,38 @@ class CommandExecutor:
             self,
             sink: ReplicaSink,
             generation: int,
    +        replication_id: str,
             image: SnapshotImage,
         ) -> bool:
             future: asyncio.Future[bool] = asyncio.get_running_loop().create_future()
             if not self.post_control(
    -            InstallReplicaSnapshot(sink, generation, image, future)
    +            InstallReplicaSnapshot(
    +                sink,
    +                generation,
    +                replication_id,
    +                image,
    +                future,
    +            )
    +        ):
    +            return False
    +        return await asyncio.shield(future)
    +
    +    async def prepare_replica_resume(
    +        self,
    +        generation: int,
    +        replication_id: str,
    +        expected_applied_seq: int,
    +    ) -> bool:
    +        future: asyncio.Future[bool] = (
    +            asyncio.get_running_loop().create_future()
    +        )
    +        if not self.post_control(
    +            PrepareReplicaResume(
    +                generation,
    +                replication_id,
    +                expected_applied_seq,
    +                future,
    +            )
             ):
                 return False
             return await asyncio.shield(future)
    ```

Tracks active source identity beside generation, validates exact partial-resume base, and on promotion clears source fencing, mints a new replication ID, and empties inherited backlog.

```python
allowed = self._replica_read_only and self._active_source_id == replication_id and self.database.commit_seq == expected_applied_seq
```

All three facts are required: role, history epoch, and exact offset.

#### Catch-up before live streaming

Retain a cursor across disconnect, apply the frozen backlog suffix before concurrently queued live batches, detect sequence gaps, and reattach after overflow or source loss.

??? note "File diff: src/miniredis/replication/sink.py"
    ```diff
    diff --git a/src/miniredis/replication/sink.py b/src/miniredis/replication/sink.py
    index 52beb456710365921404722cdb101baa2134638e..34a27652947c6e542371ae8d0750e7678dbc2ff2 100644
    --- a/src/miniredis/replication/sink.py
    +++ b/src/miniredis/replication/sink.py
    @@ -10,6 +10,7 @@ from miniredis.core.commit import CommitBatch
     from miniredis.replication.backlog import (
         FullSyncAttachment,
         ReplicaAttachment,
    +    ReplicationCursor,
     )

     if TYPE_CHECKING:
    @@ -20,6 +21,7 @@ if TYPE_CHECKING:
     class ReplicaSinkState(StrEnum):
         DETACHED = "detached"
         BOOTSTRAPPING = "bootstrapping"
    +    CATCHING_UP = "catching_up"
         STREAMING = "streaming"
         NEEDS_RESYNC = "needs_resync"
         FAILED = "failed"
    @@ -29,6 +31,11 @@ class ReplicaSinkState(StrEnum):
         STOPPED = "stopped"


    +class ReplicaSyncMode(StrEnum):
    +    FULL = "full"
    +    PARTIAL = "partial"
    +
    +
     @dataclass(frozen=True, slots=True)
     class ReplicaStatus:
         generation: int | None
    @@ -38,6 +45,9 @@ class ReplicaStatus:
         primary_seq: int
         lag: int
         queued: int
    +    replication_id: str | None
    +    sync_mode: ReplicaSyncMode | None
    +    cursor: ReplicationCursor | None


     class ReplicaSink:
    @@ -58,7 +68,10 @@ class ReplicaSink:
             self._baseline_seq = 0
             self._applied_seq = 0
             self._primary_seq = 0
    +        self._replication_id: str | None = None
    +        self._sync_mode: ReplicaSyncMode | None = None
             self._state = ReplicaSinkState.DETACHED
    +        self._catch_up: deque[CommitBatch] = deque()
             self._queue: deque[CommitBatch] = deque()
             self._queue_ready = asyncio.Event()
             self._apply_allowed = asyncio.Event()
    @@ -85,6 +98,18 @@ class ReplicaSink:
                 primary_seq=self._primary_seq,
                 lag=max(0, self._primary_seq - self._applied_seq),
                 queued=len(self._queue),
    +            replication_id=self._replication_id,
    +            sync_mode=self._sync_mode,
    +            cursor=self.cursor,
    +        )
    +
    +    @property
    +    def cursor(self) -> ReplicationCursor | None:
    +        if self._replication_id is None:
    +            return None
    +        return ReplicationCursor(
    +            self._replication_id,
    +            self._applied_seq,
             )

         @property
    @@ -106,18 +131,42 @@ class ReplicaSink:
         ) -> None:
             if self._state is not ReplicaSinkState.BOOTSTRAPPING:
                 raise RuntimeError("sink is not bootstrapping")
    -        if not isinstance(attachment, FullSyncAttachment):
    -            raise RuntimeError("partial sync is not supported by this sink")
             self._generation = attachment.generation
    -        self._baseline_seq = attachment.image.checkpoint_seq
    -        self._applied_seq = attachment.image.checkpoint_seq
    -        self._primary_seq = attachment.image.checkpoint_seq
    +        self._replication_id = attachment.replication_id
    +        if isinstance(attachment, FullSyncAttachment):
    +            self._sync_mode = ReplicaSyncMode.FULL
    +            self._baseline_seq = attachment.image.checkpoint_seq
    +            self._applied_seq = attachment.image.checkpoint_seq
    +            self._primary_seq = attachment.image.checkpoint_seq
    +            self._catch_up.clear()
    +        else:
    +            self._sync_mode = ReplicaSyncMode.PARTIAL
    +            self._baseline_seq = attachment.cursor.applied_seq
    +            self._applied_seq = attachment.cursor.applied_seq
    +            self._primary_seq = attachment.boundary_seq
    +            self._catch_up = deque(attachment.batches)
             self._attachment_captured.set()
             self._signal_status_change()

         async def attach(self, primary: MiniRedis) -> ReplicaStatus:
    -        if self._state is not ReplicaSinkState.DETACHED:
    +        if self._state not in {
    +            ReplicaSinkState.DETACHED,
    +            ReplicaSinkState.NEEDS_RESYNC,
    +            ReplicaSinkState.SOURCE_LOST,
    +        }:
                 raise RuntimeError("replica sink is already attached")
    +        previous_task = self._task
    +        if previous_task is not None and not previous_task.done():
    +            previous_task.cancel()
    +            try:
    +                await previous_task
    +            except asyncio.CancelledError:
    +                pass
    +        self._task = None
    +        self._queue.clear()
    +        self._catch_up.clear()
    +        self._queue_ready.clear()
    +        self._attachment_captured.clear()
             current = asyncio.current_task()
             assert current is not None
             self._attach_task = current
    @@ -125,21 +174,42 @@ class ReplicaSink:
             self._state = ReplicaSinkState.BOOTSTRAPPING
             self._signal_status_change()
             try:
    -            attachment = await primary.executor.attach_replica(self)
    +            attachment = await primary.executor.attach_replica(
    +                self,
    +                self.cursor,
    +            )
                 if self._install_gate is not None:
                     await self._install_gate.wait()
                 if self._state is not ReplicaSinkState.BOOTSTRAPPING:
                     return self.status
    -            installed = await self._replica.executor.install_replica_snapshot(
    -                self,
    -                attachment.generation,
    -                attachment.image,
    -            )
    +            if isinstance(attachment, FullSyncAttachment):
    +                installed = (
    +                    await self._replica.executor.install_replica_snapshot(
    +                        self,
    +                        attachment.generation,
    +                        attachment.replication_id,
    +                        attachment.image,
    +                    )
    +                )
    +            else:
    +                installed = (
    +                    await self._replica.executor.prepare_replica_resume(
    +                        attachment.generation,
    +                        attachment.replication_id,
    +                        attachment.cursor.applied_seq,
    +                    )
    +                )
                 if not installed:
    +                self._state = ReplicaSinkState.NEEDS_RESYNC
    +                self._signal_status_change()
                     return self.status
                 if self._state is not ReplicaSinkState.BOOTSTRAPPING:
                     return self.status
    -            self._state = ReplicaSinkState.STREAMING
    +            self._state = (
    +                ReplicaSinkState.CATCHING_UP
    +                if self._catch_up
    +                else ReplicaSinkState.STREAMING
    +            )
                 self._signal_status_change()
                 self._task = asyncio.create_task(
                     self._run_apply(),
    @@ -158,12 +228,14 @@ class ReplicaSink:
         def offer(self, batch: CommitBatch) -> bool:
             if self._state not in {
                 ReplicaSinkState.BOOTSTRAPPING,
    +            ReplicaSinkState.CATCHING_UP,
                 ReplicaSinkState.STREAMING,
             }:
                 return False
             self._primary_seq = batch.seq
             if len(self._queue) >= self._queue_limit:
                 self._queue.clear()
    +            self._catch_up.clear()
                 self._state = ReplicaSinkState.NEEDS_RESYNC
                 self._queue_ready.set()
                 self._signal_status_change()
    @@ -175,26 +247,36 @@ class ReplicaSink:

         async def _run_apply(self) -> None:
             try:
    -            while self._state is ReplicaSinkState.STREAMING:
    -                await self._queue_ready.wait()
    -                await self._apply_allowed.wait()
    -                if not self._queue:
    -                    self._queue_ready.clear()
    -                    continue
    -                batch = self._queue.popleft()
    -                if not self._queue:
    -                    self._queue_ready.clear()
    +            while self._state in {
    +                ReplicaSinkState.CATCHING_UP,
    +                ReplicaSinkState.STREAMING,
    +            }:
    +                if self._state is ReplicaSinkState.CATCHING_UP:
    +                    await self._apply_allowed.wait()
    +                    if not self._catch_up:
    +                        self._state = ReplicaSinkState.STREAMING
    +                        self._signal_status_change()
    +                        continue
    +                    batch = self._catch_up.popleft()
    +                else:
    +                    await self._queue_ready.wait()
    +                    await self._apply_allowed.wait()
    +                    if not self._queue:
    +                        self._queue_ready.clear()
    +                        continue
    +                    batch = self._queue.popleft()
    +                    if not self._queue:
    +                        self._queue_ready.clear()
    +                if batch.seq != self._applied_seq + 1:
    +                    await self._mark_needs_resync()
    +                    return
                     assert self._generation is not None
                     applied = await self._replica.executor.apply_replica_batch(
                         self._generation,
                         batch,
                     )
                     if not applied:
    -                    self._queue.clear()
    -                    self._state = ReplicaSinkState.NEEDS_RESYNC
    -                    self._signal_status_change()
    -                    if self._primary is not None:
    -                        await self._primary.executor.detach_replica(self._generation)
    +                    await self._mark_needs_resync()
                         return
                     self._applied_seq = batch.seq
                     self._signal_status_change()
    @@ -209,6 +291,40 @@ class ReplicaSink:
             finally:
                 self._signal_status_change()

    +    async def _mark_needs_resync(self) -> None:
    +        self._queue.clear()
    +        self._catch_up.clear()
    +        self._state = ReplicaSinkState.NEEDS_RESYNC
    +        self._signal_status_change()
    +        if self._primary is not None and self._generation is not None:
    +            await self._primary.executor.detach_replica(
    +                self._generation
    +            )
    +
    +    async def disconnect(self) -> ReplicaStatus:
    +        generation = self._generation
    +        primary = self._primary
    +        if primary is not None and generation is not None:
    +            await primary.executor.detach_replica(generation)
    +            primary._release_replica_sink(self)
    +        task = self._task
    +        if task is not None and not task.done():
    +            task.cancel()
    +            try:
    +                await task
    +            except asyncio.CancelledError:
    +                pass
    +        self._task = None
    +        self._queue.clear()
    +        self._catch_up.clear()
    +        self._queue_ready.clear()
    +        self._attachment_captured.clear()
    +        self._generation = None
    +        self._primary = None
    +        self._state = ReplicaSinkState.DETACHED
    +        self._signal_status_change()
    +        return self.status
    +
         async def wait_until_applied(self, seq: int) -> None:
             terminal = {
                 ReplicaSinkState.NEEDS_RESYNC,
    @@ -261,6 +377,7 @@ class ReplicaSink:
                 except asyncio.CancelledError:
                     pass
             self._queue.clear()
    +        self._catch_up.clear()
             result = await self._replica.executor.promote_replica(self._generation)
             if not result.writable:
                 self._state = ReplicaSinkState.FAILED
    @@ -268,6 +385,7 @@ class ReplicaSink:
                 raise RuntimeError("replica generation is no longer promotable")
             self._applied_seq = result.applied_seq
             self._primary_seq = max(self._primary_seq, result.applied_seq)
    +        self._replication_id = result.replication_id
             self._state = ReplicaSinkState.PROMOTED
             self._primary = None
             self._signal_status_change()
    @@ -296,6 +414,7 @@ class ReplicaSink:
                 except asyncio.CancelledError:
                     pass
             self._queue.clear()
    +        self._catch_up.clear()
             self._primary = None
             self._signal_status_change()

    @@ -333,6 +452,7 @@ class ReplicaSink:
                 except asyncio.CancelledError:
                     pass
             self._queue.clear()
    +        self._catch_up.clear()
             self._state = ReplicaSinkState.STOPPED
             self._primary = None
             self._signal_status_change()
    ```

Adds cursor/sync-mode status, reconnectable states, disconnect, catch-up deque, live queue separation, strict next-sequence validation, and shared NEEDS_RESYNC cleanup.

```python
if batch.seq != self._applied_seq + 1:
    await self._mark_needs_resync()
```

Even a selected partial suffix is revalidated at application time; continuity is never inferred from container order alone.

#### Resume observability

Expose retained backlog count and oldest sequence so tests and operators can explain partial versus full fallback.

??? note "File diff: src/miniredis/runtime.py"
    ```diff
    diff --git a/src/miniredis/runtime.py b/src/miniredis/runtime.py
    index 8a6f5d01aa5ed003c564e5ebaa2e7be7b11b19e2..3358d8aebebab9ca8cf39627aa492a3f13d8d15e 100644
    --- a/src/miniredis/runtime.py
    +++ b/src/miniredis/runtime.py
    @@ -689,6 +689,14 @@ class MiniRedis:
         def debug_physical_key_count(self) -> int:
             return len(self.database.entries)

    +    @property
    +    def debug_replication_backlog_count(self) -> int:
    +        return self.executor.replication_backlog.batch_count
    +
    +    @property
    +    def debug_replication_backlog_oldest_seq(self) -> int | None:
    +        return self.executor.replication_backlog.oldest_seq
    +
         @property
         def closed(self) -> bool:
             return self.state is RuntimeState.CLOSED
    ```

Exposes backlog count and oldest retained sequence to explain why a reconnect selected partial or full sync.

### Verification evidence

Run all four focused modules from `tests.txt`, cumulatively build Stages 1–28, and require owned-tree parity with `c07182f`.

### Durable takeaways

- Catch-up suffix must retire before live queued batches.
- Resume validates role, source epoch, and exact local offset.
- Overflow may resume if backlog still covers the cursor.
- Restart and promotion create new source identities.

### Explain it in your own words

Why must a promoted replica clear its inherited backlog and mint a new ID even though its commit sequence continues monotonically?

### Textbook

Partial resynchronization is log catch-up under epoch fencing. The two-queue sink is a handoff protocol: a frozen historical suffix joins a concurrent live stream at one boundary.

[Compare this stage on GitHub](https://github.com/system-in-miniature/mini-redis/compare/e65b568...c07182f)

After finishing, run `python -m journey.tools.build_journey check 28` to verify the learner workspace.

[Complete reference patch / 完整参考补丁](https://github.com/system-in-miniature/mini-redis/blob/main/journey/stages/28-partial-resynchronization/stage.patch)
