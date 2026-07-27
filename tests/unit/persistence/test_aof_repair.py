import pytest

from miniredis.persistence.aof import AofCorruption, load_aof
from miniredis.persistence.codec import AOF_HEADER, encode_aof_record

from tests.unit.persistence.test_framing import batch


def test_repair_enabled_truncates_one_incomplete_tail(tmp_path):
    path = tmp_path / "appendonly.mraof"
    first = encode_aof_record(batch(1, b"one"))
    second = encode_aof_record(batch(2, b"two"))
    path.write_bytes(AOF_HEADER + first + second[:-3])

    batches = load_aof(path, repair_truncated_tail=True)

    assert batches == (batch(1, b"one"),)
    assert path.read_bytes() == AOF_HEADER + first


def test_repair_disabled_rejects_the_same_incomplete_tail(tmp_path):
    path = tmp_path / "appendonly.mraof"
    path.write_bytes(AOF_HEADER + encode_aof_record(batch(1))[:-1])
    with pytest.raises(AofCorruption, match="incomplete final AOF record"):
        load_aof(path, repair_truncated_tail=False)


def test_checksum_corruption_never_changes_the_file(tmp_path):
    path = tmp_path / "appendonly.mraof"
    encoded = bytearray(AOF_HEADER + encode_aof_record(batch(1)))
    encoded[-1] ^= 0x01
    original = bytes(encoded)
    path.write_bytes(original)

    with pytest.raises(AofCorruption, match="AOF checksum"):
        load_aof(path, repair_truncated_tail=True)

    assert path.read_bytes() == original


def test_missing_aof_is_an_empty_stream(tmp_path):
    assert load_aof(
        tmp_path / "missing.mraof",
        repair_truncated_tail=True,
    ) == ()


def test_existing_zero_byte_aof_is_an_empty_stream(tmp_path):
    path = tmp_path / "empty.mraof"
    path.write_bytes(b"")
    assert load_aof(path, repair_truncated_tail=True) == ()
    assert path.read_bytes() == b""


def test_header_only_aof_is_an_empty_stream(tmp_path):
    path = tmp_path / "header-only.mraof"
    path.write_bytes(AOF_HEADER)
    assert load_aof(path, repair_truncated_tail=True) == ()
    assert path.read_bytes() == AOF_HEADER
