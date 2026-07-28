import struct
import zlib

import pytest

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
    SNAPSHOT_HEADER,
    CodecError,
    decode_snapshot_file,
    encode_aof_record,
    encode_aof_state_base_record,
    encode_commit_payload,
    encode_snapshot_file,
    scan_aof_bytes,
)


def batch(seq: int, value: bytes = b"v") -> CommitBatch:
    return CommitBatch(
        seq,
        (
            PutEntry(
                b"k",
                StoredEntry(StoredString(value), None, seq),
            ),
        ),
        CommitTrigger.CLIENT,
    )


def test_aof_record_has_length_payload_and_crc32():
    payload = encode_commit_payload(batch(1))
    record = encode_aof_record(batch(1))
    assert record == (
        struct.pack(">I", len(payload))
        + payload
        + struct.pack(">I", zlib.crc32(payload))
    )
    scan = scan_aof_bytes(AOF_HEADER + record)
    assert scan.batches == (batch(1),)
    assert scan.valid_offset == len(AOF_HEADER + record)
    assert scan.has_truncated_tail is False
    assert scan.state_base is None


def test_snapshot_file_has_versioned_header_length_and_crc():
    image = SnapshotImage(
        1,
        ((b"k", StoredEntry(StoredString(b"a\x00b"), 7000, 2)),),
    )
    encoded = encode_snapshot_file(image)
    assert encoded.startswith(SNAPSHOT_HEADER)
    assert decode_snapshot_file(encoded) == image


def test_complete_checksum_failure_is_never_a_repairable_tail():
    encoded = bytearray(AOF_HEADER + encode_aof_record(batch(1)))
    encoded[-1] ^= 0xFF
    with pytest.raises(CodecError, match="AOF checksum"):
        scan_aof_bytes(bytes(encoded))


def test_sequence_gap_or_regression_is_corruption():
    encoded = (
        AOF_HEADER
        + encode_aof_record(batch(1))
        + encode_aof_record(batch(3))
    )
    with pytest.raises(CodecError, match="expected AOF seq 2, got 3"):
        scan_aof_bytes(encoded)


def test_aof_segment_may_start_after_a_snapshot_checkpoint():
    encoded = (
        AOF_HEADER
        + encode_aof_record(batch(8))
        + encode_aof_record(batch(9))
    )

    assert scan_aof_bytes(encoded).batches == (batch(8), batch(9))


def test_aof_state_base_round_trips_before_contiguous_batches():
    image = SnapshotImage(
        7,
        ((b"k", StoredEntry(StoredString(b"v"), None, 3)),),
    )
    data = (
        AOF_HEADER
        + encode_aof_state_base_record(image)
        + encode_aof_record(batch(8))
    )

    scan = scan_aof_bytes(data)

    assert scan.state_base == image
    assert scan.batches == (batch(8),)
    assert not scan.has_truncated_tail


def test_aof_state_base_may_be_the_only_record():
    image = SnapshotImage(7, ())

    scan = scan_aof_bytes(
        AOF_HEADER + encode_aof_state_base_record(image)
    )

    assert scan.state_base == image
    assert scan.batches == ()
    assert not scan.has_truncated_tail


def test_state_base_after_commit_is_corruption():
    data = (
        AOF_HEADER
        + encode_aof_record(batch(1))
        + encode_aof_state_base_record(SnapshotImage(1, ()))
    )

    with pytest.raises(CodecError, match="state base must be first"):
        scan_aof_bytes(data)


def test_duplicate_state_base_is_corruption():
    base = encode_aof_state_base_record(SnapshotImage(1, ()))

    with pytest.raises(CodecError, match="state base must be first"):
        scan_aof_bytes(AOF_HEADER + base + base)


@pytest.mark.parametrize("seq", [6, 7, 9])
def test_first_batch_after_state_base_must_follow_checkpoint(seq):
    data = (
        AOF_HEADER
        + encode_aof_state_base_record(SnapshotImage(7, ()))
        + encode_aof_record(batch(seq))
    )

    with pytest.raises(CodecError, match=f"expected AOF seq 8, got {seq}"):
        scan_aof_bytes(data)


def test_truncated_state_base_tail_repairs_to_header_boundary():
    record = encode_aof_state_base_record(SnapshotImage(7, ()))

    scan = scan_aof_bytes(AOF_HEADER + record[:-1])

    assert scan.state_base is None
    assert scan.batches == ()
    assert scan.valid_offset == len(AOF_HEADER)
    assert scan.has_truncated_tail


def test_legacy_batch_only_aof_has_no_state_base():
    data = AOF_HEADER + encode_aof_record(batch(4))

    scan = scan_aof_bytes(data)

    assert scan.state_base is None
    assert scan.batches == (batch(4),)
