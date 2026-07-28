import pytest

from miniredis.core.commit import (
    CommitBatch,
    CommitTrigger,
    DeleteKey,
    DeleteReason,
    PutEntry,
    SnapshotImage,
    StoredEntry,
    StoredHash,
    StoredList,
    StoredSet,
    StoredString,
    StoredZSet,
)
from miniredis.persistence.codec import (
    CodecError,
    decode_aof_state_base_payload,
    decode_commit_payload,
    decode_snapshot_payload,
    encode_aof_state_base_payload,
    encode_commit_payload,
    encode_snapshot_payload,
)


def test_commit_payload_is_exact_canonical_json():
    batch = CommitBatch(
        seq=1,
        operations=(
            PutEntry(
                b"k",
                StoredEntry(StoredString(b"v"), None, 1),
            ),
        ),
        trigger=CommitTrigger.CLIENT,
    )

    assert encode_commit_payload(batch) == (
        b'{"operations":[{"entry":{"expire_at_ms":null,'
        b'"mutation_version":1,"value":{"data":"dg==","type":"string"}},'
        b'"key":"aw==","op":"put"}],"seq":1,"trigger":"client",'
        b'"version":1}'
    )


def test_commit_round_trip_covers_binary_data_ordering_and_scores():
    batch = CommitBatch(
        seq=9,
        operations=(
            PutEntry(
                b"\xffhash",
                StoredEntry(
                    StoredHash(((b"\x00", b"v"), (b"z", b"\xff"))),
                    123456,
                    4,
                ),
            ),
            PutEntry(
                b"list",
                StoredEntry(StoredList((b"a", b"\x00")), None, 2),
            ),
            PutEntry(
                b"set",
                StoredEntry(StoredSet((b"a", b"z")), None, 3),
            ),
            PutEntry(
                b"zset",
                StoredEntry(
                    StoredZSet(
                        (
                            (b"a", -1.5),
                            (b"n", float("-inf")),
                            (b"p", float("inf")),
                        )
                    ),
                    None,
                    7,
                ),
            ),
            DeleteKey(b"gone", DeleteReason.EVICTED),
        ),
        trigger=CommitTrigger.ACTIVE_EXPIRE,
    )

    assert decode_commit_payload(encode_commit_payload(batch)) == batch


def test_snapshot_payload_round_trips_sorted_entries():
    image = SnapshotImage(
        checkpoint_seq=9,
        entries=(
            (b"a", StoredEntry(StoredString(b"1"), None, 1)),
            (b"z", StoredEntry(StoredSet((b"a", b"b")), 8000, 2)),
        ),
    )
    assert decode_snapshot_payload(encode_snapshot_payload(image)) == image


def test_aof_state_base_payload_round_trips_sorted_entries():
    image = SnapshotImage(
        checkpoint_seq=7,
        entries=(
            (b"a", StoredEntry(StoredString(b"1"), None, 1)),
            (b"z", StoredEntry(StoredSet((b"a", b"b")), 8000, 2)),
        ),
    )

    assert (
        decode_aof_state_base_payload(encode_aof_state_base_payload(image))
        == image
    )


@pytest.mark.parametrize(
    "payload",
    [
        b"{}",
        b'{"checkpoint_seq":0,"entries":[],"record":"state_base"}',
        (
            b'{"checkpoint_seq":0,"entries":[],"record":"state_base",'
            b'"version":2}'
        ),
        (
            b'{"checkpoint_seq":true,"entries":[],"record":"state_base",'
            b'"version":1}'
        ),
        (
            b'{"checkpoint_seq":0,"entries":[],"record":"commit",'
            b'"version":1}'
        ),
    ],
)
def test_invalid_aof_state_base_schema_is_rejected(payload):
    with pytest.raises(CodecError):
        decode_aof_state_base_payload(payload)


@pytest.mark.parametrize(
    "payload",
    [
        b"{}",
        b'{"operations":[],"seq":1,"trigger":"client","version":1}',
        b'{"operations":[],"seq":1,"trigger":"client","version":2}',
        b'{"operations":[],"seq":true,"trigger":"client","version":1}',
        b'{"operations":[],"seq":1,"seq":1,"trigger":"client","version":1}',
        b'{"operations":[],"seq":1,"trigger":"unknown","version":1}',
    ],
)
def test_invalid_schema_or_duplicate_json_keys_are_rejected(payload):
    with pytest.raises(CodecError):
        decode_commit_payload(payload)
