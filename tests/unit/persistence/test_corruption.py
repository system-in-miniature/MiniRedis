import pytest

from miniredis.core.commit import SnapshotImage
from miniredis.persistence.codec import (
    AOF_HEADER,
    encode_aof_record,
    encode_snapshot_file,
)
from miniredis.persistence.recovery import RecoveryError, recover_database
from tests.unit.persistence.test_recovery import put


def test_bad_snapshot_checksum_is_not_an_empty_database(tmp_path):
    path = tmp_path / "dump.mrsnap"
    encoded = bytearray(encode_snapshot_file(SnapshotImage(0, ())))
    encoded[-1] ^= 0x01
    path.write_bytes(encoded)

    with pytest.raises(RecoveryError, match="snapshot checksum"):
        recover_database(
            snapshot_path=path,
            aof_path=None,
            now_ms=0,
            repair_truncated_tail=True,
        )


def test_corrupt_aof_after_valid_snapshot_still_fails_startup(tmp_path):
    snapshot = tmp_path / "dump.mrsnap"
    aof = tmp_path / "appendonly.mraof"
    snapshot.write_bytes(encode_snapshot_file(SnapshotImage(0, ())))
    aof.write_bytes(b"not-an-aof")

    with pytest.raises(RecoveryError, match="invalid AOF header"):
        recover_database(
            snapshot_path=snapshot,
            aof_path=aof,
            now_ms=0,
            repair_truncated_tail=True,
        )


def test_aof_only_segment_must_start_at_one(tmp_path):
    aof = tmp_path / "appendonly.mraof"
    aof.write_bytes(AOF_HEADER + encode_aof_record(put(2, b"k", b"v")))

    with pytest.raises(RecoveryError, match="expected replay seq 1, got 2"):
        recover_database(
            snapshot_path=None,
            aof_path=aof,
            now_ms=0,
            repair_truncated_tail=True,
        )


def test_first_post_checkpoint_record_must_be_checkpoint_plus_one(tmp_path):
    snapshot = tmp_path / "dump.mrsnap"
    aof = tmp_path / "appendonly.mraof"
    snapshot.write_bytes(encode_snapshot_file(SnapshotImage(7, ())))
    aof.write_bytes(AOF_HEADER + encode_aof_record(put(9, b"k", b"v")))

    with pytest.raises(RecoveryError, match="expected replay seq 8, got 9"):
        recover_database(
            snapshot_path=snapshot,
            aof_path=aof,
            now_ms=0,
            repair_truncated_tail=True,
        )


def test_nonempty_aof_ending_before_checkpoint_is_rejected(tmp_path):
    snapshot = tmp_path / "dump.mrsnap"
    aof = tmp_path / "appendonly.mraof"
    snapshot.write_bytes(encode_snapshot_file(SnapshotImage(7, ())))
    aof.write_bytes(AOF_HEADER + encode_aof_record(put(5, b"k", b"v")))

    with pytest.raises(
        RecoveryError,
        match="AOF ends at seq 5 before snapshot checkpoint 7",
    ):
        recover_database(
            snapshot_path=snapshot,
            aof_path=aof,
            now_ms=0,
            repair_truncated_tail=True,
        )
