from miniredis.core.commit import (
    CommitBatch,
    CommitTrigger,
    PutEntry,
    SnapshotImage,
    StoredEntry,
    StoredString,
)
from miniredis.persistence.codec import (
    AOF_HEADER,
    encode_aof_record,
    encode_snapshot_file,
)
from miniredis.persistence.recovery import recover_database


def put(seq: int, key: bytes, value: bytes, expire_at_ms=None):
    return CommitBatch(
        seq,
        (
            PutEntry(
                key,
                StoredEntry(
                    StoredString(value),
                    expire_at_ms,
                    seq,
                ),
            ),
        ),
        CommitTrigger.CLIENT,
    )


def write_aof(path, *batches):
    path.write_bytes(
        AOF_HEADER + b"".join(encode_aof_record(item) for item in batches)
    )


def test_aof_only_recovery_replays_without_reappend(tmp_path):
    aof = tmp_path / "appendonly.mraof"
    write_aof(aof, put(1, b"a", b"1"), put(2, b"b", b"2"))

    recovered = recover_database(
        snapshot_path=None,
        aof_path=aof,
        now_ms=0,
        repair_truncated_tail=True,
    )

    assert recovered.commit_seq == 2
    assert recovered.logical_items() == (
        (b"a", StoredEntry(StoredString(b"1"), None, 1)),
        (b"b", StoredEntry(StoredString(b"2"), None, 2)),
    )
    assert aof.read_bytes() == (
        AOF_HEADER
        + encode_aof_record(put(1, b"a", b"1"))
        + encode_aof_record(put(2, b"b", b"2"))
    )


def test_snapshot_only_recovery_restores_checkpoint(tmp_path):
    snapshot = tmp_path / "dump.mrsnap"
    image = SnapshotImage(
        7,
        ((b"k", StoredEntry(StoredString(b"v"), None, 3)),),
    )
    snapshot.write_bytes(encode_snapshot_file(image))

    recovered = recover_database(
        snapshot_path=snapshot,
        aof_path=None,
        now_ms=0,
        repair_truncated_tail=True,
    )

    assert recovered.commit_seq == 7
    assert recovered.logical_items() == image.entries


def test_combined_recovery_replays_only_after_checkpoint(tmp_path):
    snapshot = tmp_path / "dump.mrsnap"
    aof = tmp_path / "appendonly.mraof"
    snapshot.write_bytes(
        encode_snapshot_file(
            SnapshotImage(
                1,
                ((b"a", StoredEntry(StoredString(b"1"), None, 1)),),
            )
        )
    )
    write_aof(
        aof,
        put(1, b"a", b"1"),
        put(2, b"a", b"2"),
        put(3, b"b", b"3"),
    )

    recovered = recover_database(
        snapshot_path=snapshot,
        aof_path=aof,
        now_ms=0,
        repair_truncated_tail=True,
    )

    assert recovered.commit_seq == 3
    assert recovered.logical_items() == (
        (b"a", StoredEntry(StoredString(b"2"), None, 2)),
        (b"b", StoredEntry(StoredString(b"3"), None, 3)),
    )


def test_checkpoint_7_accepts_aof_segment_starting_at_8_on_restarts(
    tmp_path,
):
    snapshot = tmp_path / "dump.mrsnap"
    aof = tmp_path / "appendonly.mraof"
    snapshot.write_bytes(
        encode_snapshot_file(
            SnapshotImage(
                7,
                ((b"base", StoredEntry(StoredString(b"7"), None, 7)),),
            )
        )
    )
    write_aof(aof, put(8, b"after", b"8"))

    first = recover_database(
        snapshot_path=snapshot,
        aof_path=aof,
        now_ms=0,
        repair_truncated_tail=True,
    )
    assert first.commit_seq == 8
    with aof.open("ab") as stream:
        stream.write(encode_aof_record(put(9, b"later", b"9")))

    second = recover_database(
        snapshot_path=snapshot,
        aof_path=aof,
        now_ms=0,
        repair_truncated_tail=True,
    )
    assert second.commit_seq == 9
    assert tuple(second.entries) == (b"base", b"after", b"later")


def test_checkpoint_7_accepts_an_existing_zero_byte_aof(tmp_path):
    snapshot = tmp_path / "dump.mrsnap"
    aof = tmp_path / "appendonly.mraof"
    image = SnapshotImage(
        7,
        ((b"base", StoredEntry(StoredString(b"7"), None, 7)),),
    )
    snapshot.write_bytes(encode_snapshot_file(image))
    aof.write_bytes(b"")

    recovered = recover_database(
        snapshot_path=snapshot,
        aof_path=aof,
        now_ms=0,
        repair_truncated_tail=True,
    )

    assert recovered.commit_seq == 7
    assert recovered.logical_items() == image.entries
    assert aof.read_bytes() == b""


def test_startup_clock_discards_expired_values_and_resets_lru(tmp_path):
    aof = tmp_path / "appendonly.mraof"
    write_aof(
        aof,
        put(1, b"expired", b"x", expire_at_ms=100),
        put(2, b"live", b"y", expire_at_ms=101),
    )

    recovered = recover_database(
        snapshot_path=None,
        aof_path=aof,
        now_ms=100,
        repair_truncated_tail=True,
    )

    assert tuple(recovered.entries) == (b"live",)
    assert recovered.entries[b"live"].last_access_tick == 0
    assert recovered.access_tick == 0
    assert recovered.commit_seq == 2
