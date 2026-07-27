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
