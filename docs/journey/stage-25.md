# Stage 25 · AOF state base

### Goal

Allow an AOF to begin from a complete checkpoint image followed by contiguous delta commits, so later online rewrite can replace old history without requiring a separate snapshot file.

??? note "Deliverable files"
    - `src/miniredis/persistence/aof.py`
    - `src/miniredis/persistence/codec.py`
    - `src/miniredis/persistence/recovery.py`
    - `tests/reliability/test_final_acceptance.py`
    - `tests/reliability/test_phase3_invariants.py`
    - `tests/reliability/test_restart.py`
    - `tests/replication/test_sink_attach.py`
    - `tests/unit/persistence/test_aof_repair.py`
    - `tests/unit/persistence/test_codec.py`
    - `tests/unit/persistence/test_framing.py`
    - `tests/unit/persistence/test_recovery.py`

### The problem at this point

The current AOF is only a sequence of commit deltas. Compacting it cannot discard early records unless another artifact supplies their resulting state. An online rewrite therefore needs one self-contained first record: state through checkpoint N, followed only by N+1, N+2, and so on. Recovery may also see an independent snapshot and must choose one base without double replay.

### Test contract

#### See the failure first

A state base after a commit or a duplicate base makes history ambiguous. A first delta that skips or repeats the checkpoint boundary is incomplete. Choosing an older base over a newer snapshot loses state; applying deltas at or before the chosen base duplicates history. Tail repair that truncates into a partial base must fall back to the AOF header, not decode half a checkpoint.

??? note "File diff: tests/reliability/test_final_acceptance.py"
    ```diff
    diff --git a/tests/reliability/test_final_acceptance.py b/tests/reliability/test_final_acceptance.py
    index 58e5f09984ea10543252b483cbda4c912150f18e..a0124be399116d5c8f10ead1fa6720fe5df7ed7b 100644
    --- a/tests/reliability/test_final_acceptance.py
    +++ b/tests/reliability/test_final_acceptance.py
    @@ -167,7 +167,9 @@ async def test_final_acceptance_activates_components_then_leaves_no_owners(
             writer.close()
             await writer.wait_closed()

    -    batches = load_aof(aof_path, repair_truncated_tail=False)
    +    batches = load_aof(
    +        aof_path, repair_truncated_tail=False
    +    ).batches
         assert batches[-1].seq == primary.debug_commit_seq
         aof_entries = {
             operation.key: operation.entry
    ```

Updates final durable-state evidence to inspect `AofLog.batches` without confusing a state base with a commit batch.

??? note "File diff: tests/reliability/test_phase3_invariants.py"
    ```diff
    diff --git a/tests/reliability/test_phase3_invariants.py b/tests/reliability/test_phase3_invariants.py
    index 2e336c157d3ebd51d80c646b64ce960f4ab47f47..3520bbf19c7fed662df27a7fcbd394da6fb4c9fb 100644
    --- a/tests/reliability/test_phase3_invariants.py
    +++ b/tests/reliability/test_phase3_invariants.py
    @@ -75,7 +75,9 @@ async def test_expiration_and_eviction_reasons_are_in_the_same_aof_stream(

         reasons = tuple(
             operation.reason
    -        for batch in load_aof(path, repair_truncated_tail=False)
    +        for batch in load_aof(
    +            path, repair_truncated_tail=False
    +        ).batches
             for operation in batch.operations
             if isinstance(operation, DeleteKey)
         )
    ```

Updates invariant inspection to read commit deltas from the structured AOF log, preserving expiry/eviction evidence.

??? note "File diff: tests/reliability/test_restart.py"
    ```diff
    diff --git a/tests/reliability/test_restart.py b/tests/reliability/test_restart.py
    index 31eddea5a8fd912ef3dce617e08f60c8caed2bfb..2ebe298c08176aafd238be5b8671ceeb9c3d629d 100644
    --- a/tests/reliability/test_restart.py
    +++ b/tests/reliability/test_restart.py
    @@ -53,3 +53,26 @@ async def test_corrupt_startup_never_accepts_clients_or_leaks_workers(
         assert stats.owned_tasks == 0
         assert stats.sessions == 0
         await runtime.close()
    +
    +
    +@pytest.mark.asyncio
    +async def test_restart_resets_volatile_lfu_metadata(tmp_path):
    +    config = MiniRedisConfig(
    +        aof_path=tmp_path / "appendonly.mraof",
    +        aof_policy=AofPolicy.ALWAYS,
    +        eviction_policy="allkeys-lfu",
    +    )
    +    first = MiniRedis.open(config)
    +    await first.start()
    +    client = first.direct_client()
    +    await client.execute(CommandRequest(b"SET", (b"k", b"v")))
    +    for _ in range(4):
    +        await client.execute(CommandRequest(b"GET", (b"k",)))
    +    assert first.database.entries[b"k"].frequency == 5
    +    await first.close()
    +
    +    second = MiniRedis.open(config)
    +    await second.start()
    +    assert second.database.entries[b"k"].frequency == 0
    +    assert second.database.entries[b"k"].last_access_tick == 0
    +    await second.close()
    ```

Locks restart as logical-state recovery while volatile LFU/access metadata resets to neutral.

??? note "File diff: tests/replication/test_sink_attach.py"
    ```diff
    diff --git a/tests/replication/test_sink_attach.py b/tests/replication/test_sink_attach.py
    index 8d585882b79399e5686b5be3bca84cce2b2280e0..8e84dbfa9212f34f5d130ba3e13ce1745ffd9b14 100644
    --- a/tests/replication/test_sink_attach.py
    +++ b/tests/replication/test_sink_attach.py
    @@ -3,6 +3,7 @@ import asyncio
     import pytest

     from miniredis import CommandRequest
    +from miniredis.config import MiniRedisConfig
     from miniredis.core.reply import Bytes, Ok
     from miniredis.replication.sink import ReplicaSink, ReplicaSinkState
     from tests.helpers.runtime import open_test_runtime
    @@ -64,3 +65,23 @@ async def test_attached_replica_rejects_user_writes():
         assert replica.debug_commit_seq == sink.status.applied_seq
         await primary.close()
         await replica.close()
    +
    +
    +@pytest.mark.asyncio
    +async def test_full_sync_resets_volatile_lfu_metadata():
    +    config = MiniRedisConfig(eviction_policy="allkeys-lfu")
    +    primary = await open_test_runtime(config=config)
    +    replica = await open_test_runtime(config=config)
    +    client = primary.direct_client()
    +    await client.execute(CommandRequest(b"SET", (b"k", b"v")))
    +    for _ in range(4):
    +        await client.execute(CommandRequest(b"GET", (b"k",)))
    +    assert primary.database.entries[b"k"].frequency == 5
    +
    +    sink = ReplicaSink(replica, queue_limit=4)
    +    await primary.attach_replica(sink)
    +
    +    assert replica.database.entries[b"k"].frequency == 0
    +    assert replica.database.entries[b"k"].last_access_tick == 0
    +    await primary.close()
    +    await replica.close()
    ```

Locks full-sync snapshot installation with neutral LFU/access metadata on the follower.

??? note "File diff: tests/unit/persistence/test_aof_repair.py"
    ```diff
    diff --git a/tests/unit/persistence/test_aof_repair.py b/tests/unit/persistence/test_aof_repair.py
    index a7aac38335c6359abe2451474f08f98b6af2f681..ed30c96d52029da7f726b823f61cf0c8afbc3291 100644
    --- a/tests/unit/persistence/test_aof_repair.py
    +++ b/tests/unit/persistence/test_aof_repair.py
    @@ -1,6 +1,6 @@
     import pytest

    -from miniredis.persistence.aof import AofCorruption, load_aof
    +from miniredis.persistence.aof import AofCorruption, AofLog, load_aof
     from miniredis.persistence.codec import AOF_HEADER, encode_aof_record

     from tests.unit.persistence.test_framing import batch
    @@ -12,9 +12,9 @@ def test_repair_enabled_truncates_one_incomplete_tail(tmp_path):
         second = encode_aof_record(batch(2, b"two"))
         path.write_bytes(AOF_HEADER + first + second[:-3])

    -    batches = load_aof(path, repair_truncated_tail=True)
    +    log = load_aof(path, repair_truncated_tail=True)

    -    assert batches == (batch(1, b"one"),)
    +    assert log == AofLog(None, (batch(1, b"one"),))
         assert path.read_bytes() == AOF_HEADER + first


    @@ -42,18 +42,18 @@ def test_missing_aof_is_an_empty_stream(tmp_path):
         assert load_aof(
             tmp_path / "missing.mraof",
             repair_truncated_tail=True,
    -    ) == ()
    +    ) == AofLog(None, ())


     def test_existing_zero_byte_aof_is_an_empty_stream(tmp_path):
         path = tmp_path / "empty.mraof"
         path.write_bytes(b"")
    -    assert load_aof(path, repair_truncated_tail=True) == ()
    +    assert load_aof(path, repair_truncated_tail=True) == AofLog(None, ())
         assert path.read_bytes() == b""


     def test_header_only_aof_is_an_empty_stream(tmp_path):
         path = tmp_path / "header-only.mraof"
         path.write_bytes(AOF_HEADER)
    -    assert load_aof(path, repair_truncated_tail=True) == ()
    +    assert load_aof(path, repair_truncated_tail=True) == AofLog(None, ())
         assert path.read_bytes() == AOF_HEADER
    ```

Locks the new `AofLog(state_base, batches)` return contract for repaired, missing, empty, and header-only logs.

??? note "File diff: tests/unit/persistence/test_codec.py"
    ```diff
    diff --git a/tests/unit/persistence/test_codec.py b/tests/unit/persistence/test_codec.py
    index 7f459c65a84a2989c84b2e82d2a93bc83c6fcd70..7cedbb571331e8b95c15434194d825b51e1ee324 100644
    --- a/tests/unit/persistence/test_codec.py
    +++ b/tests/unit/persistence/test_codec.py
    @@ -16,8 +16,10 @@ from miniredis.core.commit import (
     )
     from miniredis.persistence.codec import (
         CodecError,
    +    decode_aof_state_base_payload,
         decode_commit_payload,
         decode_snapshot_payload,
    +    encode_aof_state_base_payload,
         encode_commit_payload,
         encode_snapshot_payload,
     )
    @@ -96,6 +98,45 @@ def test_snapshot_payload_round_trips_sorted_entries():
         assert decode_snapshot_payload(encode_snapshot_payload(image)) == image


    +def test_aof_state_base_payload_round_trips_sorted_entries():
    +    image = SnapshotImage(
    +        checkpoint_seq=7,
    +        entries=(
    +            (b"a", StoredEntry(StoredString(b"1"), None, 1)),
    +            (b"z", StoredEntry(StoredSet((b"a", b"b")), 8000, 2)),
    +        ),
    +    )
    +
    +    assert (
    +        decode_aof_state_base_payload(encode_aof_state_base_payload(image))
    +        == image
    +    )
    +
    +
    +@pytest.mark.parametrize(
    +    "payload",
    +    [
    +        b"{}",
    +        b'{"checkpoint_seq":0,"entries":[],"record":"state_base"}',
    +        (
    +            b'{"checkpoint_seq":0,"entries":[],"record":"state_base",'
    +            b'"version":2}'
    +        ),
    +        (
    +            b'{"checkpoint_seq":true,"entries":[],"record":"state_base",'
    +            b'"version":1}'
    +        ),
    +        (
    +            b'{"checkpoint_seq":0,"entries":[],"record":"commit",'
    +            b'"version":1}'
    +        ),
    +    ],
    +)
    +def test_invalid_aof_state_base_schema_is_rejected(payload):
    +    with pytest.raises(CodecError):
    +        decode_aof_state_base_payload(payload)
    +
    +
     @pytest.mark.parametrize(
         "payload",
         [
    ```

Locks deterministic state-base payload round trip and strict schema/version/record-type validation.

??? note "File diff: tests/unit/persistence/test_framing.py"
    ```diff
    diff --git a/tests/unit/persistence/test_framing.py b/tests/unit/persistence/test_framing.py
    index 62494a18eb1e92c4977a82daec8e242cadd52199..31496f9eaa50d7e13e24d2dfd9e98d40bcb344d8 100644
    --- a/tests/unit/persistence/test_framing.py
    +++ b/tests/unit/persistence/test_framing.py
    @@ -17,6 +17,7 @@ from miniredis.persistence.codec import (
         CodecError,
         decode_snapshot_file,
         encode_aof_record,
    +    encode_aof_state_base_record,
         encode_commit_payload,
         encode_snapshot_file,
         scan_aof_bytes,
    @@ -48,6 +49,7 @@ def test_aof_record_has_length_payload_and_crc32():
         assert scan.batches == (batch(1),)
         assert scan.valid_offset == len(AOF_HEADER + record)
         assert scan.has_truncated_tail is False
    +    assert scan.state_base is None


     def test_snapshot_file_has_versioned_header_length_and_crc():
    @@ -85,3 +87,83 @@ def test_aof_segment_may_start_after_a_snapshot_checkpoint():
         )

         assert scan_aof_bytes(encoded).batches == (batch(8), batch(9))
    +
    +
    +def test_aof_state_base_round_trips_before_contiguous_batches():
    +    image = SnapshotImage(
    +        7,
    +        ((b"k", StoredEntry(StoredString(b"v"), None, 3)),),
    +    )
    +    data = (
    +        AOF_HEADER
    +        + encode_aof_state_base_record(image)
    +        + encode_aof_record(batch(8))
    +    )
    +
    +    scan = scan_aof_bytes(data)
    +
    +    assert scan.state_base == image
    +    assert scan.batches == (batch(8),)
    +    assert not scan.has_truncated_tail
    +
    +
    +def test_aof_state_base_may_be_the_only_record():
    +    image = SnapshotImage(7, ())
    +
    +    scan = scan_aof_bytes(
    +        AOF_HEADER + encode_aof_state_base_record(image)
    +    )
    +
    +    assert scan.state_base == image
    +    assert scan.batches == ()
    +    assert not scan.has_truncated_tail
    +
    +
    +def test_state_base_after_commit_is_corruption():
    +    data = (
    +        AOF_HEADER
    +        + encode_aof_record(batch(1))
    +        + encode_aof_state_base_record(SnapshotImage(1, ()))
    +    )
    +
    +    with pytest.raises(CodecError, match="state base must be first"):
    +        scan_aof_bytes(data)
    +
    +
    +def test_duplicate_state_base_is_corruption():
    +    base = encode_aof_state_base_record(SnapshotImage(1, ()))
    +
    +    with pytest.raises(CodecError, match="state base must be first"):
    +        scan_aof_bytes(AOF_HEADER + base + base)
    +
    +
    +@pytest.mark.parametrize("seq", [6, 7, 9])
    +def test_first_batch_after_state_base_must_follow_checkpoint(seq):
    +    data = (
    +        AOF_HEADER
    +        + encode_aof_state_base_record(SnapshotImage(7, ()))
    +        + encode_aof_record(batch(seq))
    +    )
    +
    +    with pytest.raises(CodecError, match=f"expected AOF seq 8, got {seq}"):
    +        scan_aof_bytes(data)
    +
    +
    +def test_truncated_state_base_tail_repairs_to_header_boundary():
    +    record = encode_aof_state_base_record(SnapshotImage(7, ()))
    +
    +    scan = scan_aof_bytes(AOF_HEADER + record[:-1])
    +
    +    assert scan.state_base is None
    +    assert scan.batches == ()
    +    assert scan.valid_offset == len(AOF_HEADER)
    +    assert scan.has_truncated_tail
    +
    +
    +def test_legacy_batch_only_aof_has_no_state_base():
    +    data = AOF_HEADER + encode_aof_record(batch(4))
    +
    +    scan = scan_aof_bytes(data)
    +
    +    assert scan.state_base is None
    +    assert scan.batches == (batch(4),)
    ```

Locks first-only placement, uniqueness, base-to-delta sequence continuity, base-only logs, legacy batch-only compatibility, and truncated-base boundaries.

??? note "File diff: tests/unit/persistence/test_recovery.py"
    ```diff
    diff --git a/tests/unit/persistence/test_recovery.py b/tests/unit/persistence/test_recovery.py
    index 5bbdc703d6b0fd661035099765fd3f47349062d8..1ef5ec6c75cac244b2e3f27a3e095eb6dbc195f1 100644
    --- a/tests/unit/persistence/test_recovery.py
    +++ b/tests/unit/persistence/test_recovery.py
    @@ -1,3 +1,5 @@
    +import pytest
    +
     from miniredis.core.commit import (
         CommitBatch,
         CommitTrigger,
    @@ -9,9 +11,10 @@ from miniredis.core.commit import (
     from miniredis.persistence.codec import (
         AOF_HEADER,
         encode_aof_record,
    +    encode_aof_state_base_record,
         encode_snapshot_file,
     )
    -from miniredis.persistence.recovery import recover_database
    +from miniredis.persistence.recovery import RecoveryError, recover_database


     def put(seq: int, key: bytes, value: bytes, expire_at_ms=None):
    @@ -37,6 +40,14 @@ def write_aof(path, *batches):
         )


    +def write_aof_with_base(path, image, *batches):
    +    path.write_bytes(
    +        AOF_HEADER
    +        + encode_aof_state_base_record(image)
    +        + b"".join(encode_aof_record(item) for item in batches)
    +    )
    +
    +
     def test_aof_only_recovery_replays_without_reappend(tmp_path):
         aof = tmp_path / "appendonly.mraof"
         write_aof(aof, put(1, b"a", b"1"), put(2, b"b", b"2"))
    @@ -187,3 +198,150 @@ def test_startup_clock_discards_expired_values_and_resets_lru(tmp_path):
         assert recovered.entries[b"live"].last_access_tick == 0
         assert recovered.access_tick == 0
         assert recovered.commit_seq == 2
    +
    +
    +def test_recovery_prefers_newer_aof_state_base(tmp_path):
    +    snapshot_path = tmp_path / "dump.snapshot"
    +    aof_path = tmp_path / "appendonly.mraof"
    +    snapshot_path.write_bytes(
    +        encode_snapshot_file(
    +            SnapshotImage(
    +                1,
    +                ((b"k", StoredEntry(StoredString(b"snapshot"), None, 1)),),
    +            )
    +        )
    +    )
    +    base = SnapshotImage(
    +        2,
    +        ((b"k", StoredEntry(StoredString(b"base"), None, 2)),),
    +    )
    +    later = put(3, b"later", b"value")
    +    write_aof_with_base(aof_path, base, later)
    +
    +    recovered = recover_database(
    +        snapshot_path=snapshot_path,
    +        aof_path=aof_path,
    +        now_ms=0,
    +        repair_truncated_tail=False,
    +    )
    +
    +    assert recovered.commit_seq == 3
    +    assert recovered.export_stored_entries(0) == (
    +        (b"k", StoredEntry(StoredString(b"base"), None, 2)),
    +        (b"later", StoredEntry(StoredString(b"value"), None, 3)),
    +    )
    +
    +
    +def test_recovery_prefers_newer_snapshot_over_aof_state_base(tmp_path):
    +    snapshot_path = tmp_path / "dump.snapshot"
    +    aof_path = tmp_path / "appendonly.mraof"
    +    snapshot = SnapshotImage(
    +        4,
    +        ((b"k", StoredEntry(StoredString(b"snapshot"), None, 4)),),
    +    )
    +    snapshot_path.write_bytes(encode_snapshot_file(snapshot))
    +    write_aof_with_base(
    +        aof_path,
    +        SnapshotImage(
    +            2,
    +            ((b"k", StoredEntry(StoredString(b"base"), None, 2)),),
    +        ),
    +        put(3, b"k", b"three"),
    +        put(4, b"k", b"four"),
    +    )
    +
    +    recovered = recover_database(
    +        snapshot_path=snapshot_path,
    +        aof_path=aof_path,
    +        now_ms=0,
    +        repair_truncated_tail=False,
    +    )
    +
    +    assert recovered.commit_seq == 4
    +    assert recovered.export_stored_entries(0) == snapshot.entries
    +
    +
    +def test_equal_checkpoint_prefers_aof_state_base(tmp_path):
    +    snapshot_path = tmp_path / "dump.snapshot"
    +    aof_path = tmp_path / "appendonly.mraof"
    +    snapshot_path.write_bytes(
    +        encode_snapshot_file(
    +            SnapshotImage(
    +                2,
    +                ((b"k", StoredEntry(StoredString(b"snapshot"), None, 2)),),
    +            )
    +        )
    +    )
    +    base = SnapshotImage(
    +        2,
    +        ((b"k", StoredEntry(StoredString(b"base"), None, 2)),),
    +    )
    +    write_aof_with_base(aof_path, base)
    +
    +    recovered = recover_database(
    +        snapshot_path=snapshot_path,
    +        aof_path=aof_path,
    +        now_ms=0,
    +        repair_truncated_tail=False,
    +    )
    +
    +    assert recovered.export_stored_entries(0) == base.entries
    +
    +
    +def test_aof_state_base_recovers_without_snapshot(tmp_path):
    +    aof_path = tmp_path / "appendonly.mraof"
    +    base = SnapshotImage(
    +        2,
    +        ((b"k", StoredEntry(StoredString(b"base"), None, 2)),),
    +    )
    +    write_aof_with_base(aof_path, base)
    +
    +    recovered = recover_database(
    +        snapshot_path=None,
    +        aof_path=aof_path,
    +        now_ms=0,
    +        repair_truncated_tail=False,
    +    )
    +
    +    assert recovered.commit_seq == 2
    +    assert recovered.export_stored_entries(0) == base.entries
    +
    +
    +def test_missing_post_base_sequence_is_rejected(tmp_path):
    +    aof_path = tmp_path / "appendonly.mraof"
    +    aof_path.write_bytes(
    +        AOF_HEADER
    +        + encode_aof_state_base_record(SnapshotImage(2, ()))
    +        + encode_aof_record(put(4, b"k", b"value"))
    +    )
    +
    +    with pytest.raises(RecoveryError, match="expected AOF seq 3, got 4"):
    +        recover_database(
    +            snapshot_path=None,
    +            aof_path=aof_path,
    +            now_ms=0,
    +            repair_truncated_tail=False,
    +        )
    +
    +
    +def test_truncated_final_delta_after_base_is_repaired(tmp_path):
    +    aof_path = tmp_path / "appendonly.mraof"
    +    base = SnapshotImage(
    +        2,
    +        ((b"k", StoredEntry(StoredString(b"base"), None, 2)),),
    +    )
    +    first = encode_aof_record(put(3, b"first", b"1"))
    +    truncated = encode_aof_record(put(4, b"lost", b"2"))
    +    expected = AOF_HEADER + encode_aof_state_base_record(base) + first
    +    aof_path.write_bytes(expected + truncated[:-3])
    +
    +    recovered = recover_database(
    +        snapshot_path=None,
    +        aof_path=aof_path,
    +        now_ms=0,
    +        repair_truncated_tail=True,
    +    )
    +
    +    assert recovered.commit_seq == 3
    +    assert tuple(recovered.entries) == (b"k", b"first")
    +    assert aof_path.read_bytes() == expected
    ```

Locks newer-base selection, equal-checkpoint AOF preference, AOF-only base recovery, contiguous suffix replay, old-log rejection, and tail repair after a base.

### Basic concepts

An AOF state base is a `SnapshotImage(checkpoint_seq, entries)` encoded inside ordinary length/CRC framing. It is a logical checkpoint, not a commit. `AofLog` separates the optional base from delta batches. The chosen recovery base is the newer of snapshot file and AOF state base; equal sequence prefers the AOF base because it belongs to the same log generation as its suffix.

### Why this mechanism is necessary

Online rewrite must publish one standalone AOF that recovers without coordinating two file replacements. A first-record state base gives the new file a complete starting state, while strict placement and sequence rules make rewritten and legacy logs equally auditable.

### Runtime mental model

The scanner verifies the header and each framed payload. If the first payload declares `state_base`, it decodes one checkpoint and seeds expected sequence to N+1; any later base is corruption. Loading returns `(base, batches)`. Recovery chooses the newest available base, validates the log end against it, and replays only batches with sequence greater than the chosen checkpoint.

### Mechanism blocks

#### First-record AOF state base

Encode a checkpoint image as a checksummed AOF record that may appear exactly once and only before contiguous commit batches.

??? note "File diff: src/miniredis/persistence/codec.py"
    ```diff
    diff --git a/src/miniredis/persistence/codec.py b/src/miniredis/persistence/codec.py
    index 77f12a2a506b4f16de2e87833e2fadd349656aca..c05f7609d7e5f3c47df400482bdce4e27ba5d161 100644
    --- a/src/miniredis/persistence/codec.py
    +++ b/src/miniredis/persistence/codec.py
    @@ -38,6 +38,7 @@ class CodecError(ValueError):

     @dataclass(frozen=True, slots=True)
     class AofScan:
    +    state_base: SnapshotImage | None
         batches: tuple[CommitBatch, ...]
         valid_offset: int
         has_truncated_tail: bool
    @@ -440,33 +441,104 @@ def decode_snapshot_payload(payload: bytes) -> SnapshotImage:
             raise CodecError(str(exc)) from exc


    +def encode_aof_state_base_payload(image: SnapshotImage) -> bytes:
    +    return _dumps(
    +        {
    +            "record": "state_base",
    +            "version": PAYLOAD_VERSION,
    +            "checkpoint_seq": image.checkpoint_seq,
    +            "entries": [
    +                {"key": _bytes(key), "entry": _encode_entry(entry)}
    +                for key, entry in image.entries
    +            ],
    +        }
    +    )
    +
    +
    +def decode_aof_state_base_payload(payload: bytes) -> SnapshotImage:
    +    root = _object(
    +        _loads(payload),
    +        frozenset(
    +            {"record", "version", "checkpoint_seq", "entries"}
    +        ),
    +        "AOF state base",
    +    )
    +    if _text(root["record"], "record") != "state_base":
    +        _fail("unknown AOF record type")
    +    if _integer(root["version"], "version", minimum=1) != PAYLOAD_VERSION:
    +        _fail("unsupported AOF state base payload version")
    +    entries = []
    +    for index, raw_entry in enumerate(_array(root["entries"], "entries")):
    +        item = _object(
    +            raw_entry,
    +            frozenset({"key", "entry"}),
    +            f"entries[{index}]",
    +        )
    +        entries.append(
    +            (
    +                _decode_bytes(item["key"], f"entries[{index}].key"),
    +                _decode_entry(item["entry"]),
    +            )
    +        )
    +    try:
    +        return SnapshotImage(
    +            checkpoint_seq=_integer(
    +                root["checkpoint_seq"],
    +                "checkpoint_seq",
    +            ),
    +            entries=tuple(entries),
    +        )
    +    except ValueError as exc:
    +        raise CodecError(str(exc)) from exc
    +
    +
     def _crc(payload: bytes) -> bytes:
         return struct.pack(">I", zlib.crc32(payload))


    -def encode_aof_record(batch: CommitBatch) -> bytes:
    -    payload = encode_commit_payload(batch)
    +def _encode_aof_payload_record(payload: bytes) -> bytes:
         if len(payload) > MAX_PAYLOAD_BYTES:
             raise CodecError("AOF payload exceeds limit")
         return struct.pack(">I", len(payload)) + payload + _crc(payload)


    +def encode_aof_record(batch: CommitBatch) -> bytes:
    +    return _encode_aof_payload_record(encode_commit_payload(batch))
    +
    +
    +def encode_aof_state_base_record(image: SnapshotImage) -> bytes:
    +    return _encode_aof_payload_record(
    +        encode_aof_state_base_payload(image)
    +    )
    +
    +
     def scan_aof_bytes(data: bytes) -> AofScan:
         if not data.startswith(AOF_HEADER):
             raise CodecError("invalid AOF header")
         offset = len(AOF_HEADER)
         valid_offset = offset
    +    state_base: SnapshotImage | None = None
         batches: list[CommitBatch] = []
         previous_seq: int | None = None
         while offset < len(data):
             if len(data) - offset < 4:
    -            return AofScan(tuple(batches), valid_offset, True)
    +            return AofScan(
    +                state_base,
    +                tuple(batches),
    +                valid_offset,
    +                True,
    +            )
             payload_length = struct.unpack_from(">I", data, offset)[0]
             if payload_length > MAX_PAYLOAD_BYTES:
                 raise CodecError("AOF payload exceeds limit")
             end = offset + 4 + payload_length + 4
             if end > len(data):
    -            return AofScan(tuple(batches), valid_offset, True)
    +            return AofScan(
    +                state_base,
    +                tuple(batches),
    +                valid_offset,
    +                True,
    +            )
             payload_start = offset + 4
             payload_end = payload_start + payload_length
             payload = data[payload_start:payload_end]
    @@ -474,6 +546,19 @@ def scan_aof_bytes(data: bytes) -> AofScan:
             actual_crc = zlib.crc32(payload)
             if actual_crc != expected_crc:
                 raise CodecError(f"AOF checksum failure at offset {offset}")
    +        decoded_payload = _loads(payload)
    +        is_state_base = (
    +            isinstance(decoded_payload, dict)
    +            and decoded_payload.get("record") == "state_base"
    +        )
    +        if is_state_base:
    +            if state_base is not None or batches:
    +                raise CodecError("AOF state base must be first")
    +            state_base = decode_aof_state_base_payload(payload)
    +            previous_seq = state_base.checkpoint_seq
    +            offset = end
    +            valid_offset = end
    +            continue
             batch = decode_commit_payload(payload)
             if previous_seq is not None and batch.seq != previous_seq + 1:
                 raise CodecError(
    @@ -483,7 +568,7 @@ def scan_aof_bytes(data: bytes) -> AofScan:
             previous_seq = batch.seq
             offset = end
             valid_offset = end
    -    return AofScan(tuple(batches), valid_offset, False)
    +    return AofScan(state_base, tuple(batches), valid_offset, False)


     def encode_snapshot_file(image: SnapshotImage) -> bytes:
    ```

Adds strict payload encode/decode plus framed state-base records, and teaches the scanner the first-record-only state machine.

```python
if state_base is not None or batches:
    raise CodecError("AOF state base must be first")
```

The condition simultaneously forbids duplicates and bases after commits.

#### Structured AOF log loading

Return an explicit optional state base plus delta batches while preserving truncated-tail repair boundaries.

??? note "File diff: src/miniredis/persistence/aof.py"
    ```diff
    diff --git a/src/miniredis/persistence/aof.py b/src/miniredis/persistence/aof.py
    index 2fc237a340657b585e2cd7f364f2826faac41aa2..fea7ff4a4d608819e194384d2dfff1c4d10b8dd3 100644
    --- a/src/miniredis/persistence/aof.py
    +++ b/src/miniredis/persistence/aof.py
    @@ -8,7 +8,7 @@ from enum import StrEnum
     from pathlib import Path
     from typing import Protocol, TypeAlias

    -from miniredis.core.commit import CommitBatch
    +from miniredis.core.commit import CommitBatch, SnapshotImage
     from miniredis.persistence.codec import (
         AOF_HEADER,
         CodecError,
    @@ -40,6 +40,12 @@ class AofAppendFailed:
     AofAppendOutcome: TypeAlias = AofAppendOk | AofAppendFailed


    +@dataclass(frozen=True, slots=True)
    +class AofLog:
    +    state_base: SnapshotImage | None
    +    batches: tuple[CommitBatch, ...]
    +
    +
     class AofFileOps(Protocol):
         def open_append(self, path: Path) -> int:
             raise NotImplementedError
    @@ -94,19 +100,19 @@ def load_aof(
         path: Path,
         *,
         repair_truncated_tail: bool,
    -) -> tuple[CommitBatch, ...]:
    +) -> AofLog:
         try:
             data = path.read_bytes()
         except FileNotFoundError:
    -        return ()
    +        return AofLog(None, ())
         if data == b"":
    -        return ()
    +        return AofLog(None, ())
         try:
             scan = scan_aof_bytes(data)
         except CodecError as exc:
             raise AofCorruption(str(exc)) from exc
         if not scan.has_truncated_tail:
    -        return scan.batches
    +        return AofLog(scan.state_base, scan.batches)
         if not repair_truncated_tail:
             raise AofCorruption("incomplete final AOF record")

    @@ -116,7 +122,7 @@ def load_aof(
             os.fsync(fd)
         finally:
             os.close(fd)
    -    return scan.batches
    +    return AofLog(scan.state_base, scan.batches)


     @dataclass(slots=True)
    ```

Introduces `AofLog` and preserves both base and batches across normal load and truncated-tail repair.

#### Newest compatible recovery base

Choose the newer snapshot or AOF state base, replay only later contiguous deltas, and reject an AOF that ends before the chosen checkpoint.

??? note "File diff: src/miniredis/persistence/recovery.py"
    ```diff
    diff --git a/src/miniredis/persistence/recovery.py b/src/miniredis/persistence/recovery.py
    index 1721753330c0142f3159b9031736697f4736dad0..6d7642ab422f2ccb35beade233f1a9b33d7ff176 100644
    --- a/src/miniredis/persistence/recovery.py
    +++ b/src/miniredis/persistence/recovery.py
    @@ -4,7 +4,7 @@ from pathlib import Path

     from miniredis.core.commit import SnapshotImage
     from miniredis.core.database import Database
    -from miniredis.persistence.aof import AofCorruption, load_aof
    +from miniredis.persistence.aof import AofCorruption, AofLog, load_aof
     from miniredis.persistence.codec import CodecError, decode_snapshot_file


    @@ -34,23 +34,39 @@ def recover_database(
     ) -> Database:
         image = _load_snapshot(snapshot_path)
         try:
    -        batches = (
    +        log = (
                 load_aof(
                     aof_path,
                     repair_truncated_tail=repair_truncated_tail,
                 )
                 if aof_path is not None
    -            else ()
    +            else AofLog(None, ())
             )
         except AofCorruption as exc:
             raise RecoveryError(str(exc)) from exc

    +    base = log.state_base
    +    image = (
    +        image
    +        if base is None or image.checkpoint_seq > base.checkpoint_seq
    +        else base
    +    )
    +    batches = log.batches
         post_checkpoint = tuple(
    -        batch for batch in batches if batch.seq > image.checkpoint_seq
    +        batch
    +        for batch in batches
    +        if batch.seq > image.checkpoint_seq
    +    )
    +    aof_end = (
    +        batches[-1].seq
    +        if batches
    +        else base.checkpoint_seq
    +        if base is not None
    +        else None
         )
    -    if batches and batches[-1].seq < image.checkpoint_seq:
    +    if aof_end is not None and aof_end < image.checkpoint_seq:
             raise RecoveryError(
    -            f"AOF ends at seq {batches[-1].seq} before "
    +            f"AOF ends at seq {aof_end} before "
                 f"snapshot checkpoint {image.checkpoint_seq}"
             )
         if image.checkpoint_seq == 0 and batches and batches[0].seq != 1:
    ```

Selects the newer compatible checkpoint source, derives the AOF end from either base or final batch, and replays only the post-checkpoint suffix.

### Verification evidence

Run all eight focused modules in `tests.txt`, cumulatively build Stages 1–25, and require owned-tree parity with `b9b363e`.

### Durable takeaways

- A state base is checkpoint state, not commit delta.
- It may occur once, first, before a contiguous suffix.
- Recovery chooses one newest base and never double replays.
- Rewritten AOF can be self-contained.

### Explain it in your own words

Why does equal checkpoint sequence prefer the AOF state base, and why must the first following batch be exactly checkpoint plus one?

### Textbook

This is log compaction by checkpoint-plus-suffix. The base summarizes a prefix of state transitions; the remaining deltas retain causal order after that prefix.

[Compare this stage on GitHub](https://github.com/system-in-miniature/mini-redis/compare/b25b473...b9b363e)

After finishing, run `python -m journey.tools.build_journey check 25` to verify the learner workspace.

[Complete reference patch / 完整参考补丁](https://github.com/system-in-miniature/mini-redis/blob/main/journey/stages/25-aof-state-base/stage.patch)
