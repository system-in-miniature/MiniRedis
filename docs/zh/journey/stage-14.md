# Stage 14 · Canonical 持久化帧

### 目标

Canonical 编码 Commit 与 Snapshot，检测完整损坏，并只修复不完整的最终 AOF Frame。

??? note "交付文件"
    - `src/miniredis/persistence/aof.py`
    - `src/miniredis/persistence/codec.py`
    - `tests/unit/persistence/test_aof_repair.py`
    - `tests/unit/persistence/test_codec.py`
    - `tests/unit/persistence/test_framing.py`

### 当前遇到的问题

稳定 Python Value 仍需 Byte Contract。通用 JSON 允许重复 Key、非有限 Number 与多个等价编码；原始拼接 Payload 无法区分 Crash-truncated Tail 与完整但已损坏数据。

### 测试契约

#### 先看会坏在哪里

Checksum 不匹配、重复 JSON Field、非 Canonical Base64/Float、Sequence Gap 或非法 Schema 必须失败且不改 Disk。只有已声明 Bytes 不完整的最终 Frame，才可在启用 Repair 时截回最后 Verified Offset。

??? note "文件差异：tests/unit/persistence/test_aof_repair.py"
    ```diff
    diff --git a/tests/unit/persistence/test_aof_repair.py b/tests/unit/persistence/test_aof_repair.py
    new file mode 100644
    index 0000000000000000000000000000000000000000..a7aac38335c6359abe2451474f08f98b6af2f681
    --- /dev/null
    +++ b/tests/unit/persistence/test_aof_repair.py
    @@ -0,0 +1,59 @@
    +import pytest
    +
    +from miniredis.persistence.aof import AofCorruption, load_aof
    +from miniredis.persistence.codec import AOF_HEADER, encode_aof_record
    +
    +from tests.unit.persistence.test_framing import batch
    +
    +
    +def test_repair_enabled_truncates_one_incomplete_tail(tmp_path):
    +    path = tmp_path / "appendonly.mraof"
    +    first = encode_aof_record(batch(1, b"one"))
    +    second = encode_aof_record(batch(2, b"two"))
    +    path.write_bytes(AOF_HEADER + first + second[:-3])
    +
    +    batches = load_aof(path, repair_truncated_tail=True)
    +
    +    assert batches == (batch(1, b"one"),)
    +    assert path.read_bytes() == AOF_HEADER + first
    +
    +
    +def test_repair_disabled_rejects_the_same_incomplete_tail(tmp_path):
    +    path = tmp_path / "appendonly.mraof"
    +    path.write_bytes(AOF_HEADER + encode_aof_record(batch(1))[:-1])
    +    with pytest.raises(AofCorruption, match="incomplete final AOF record"):
    +        load_aof(path, repair_truncated_tail=False)
    +
    +
    +def test_checksum_corruption_never_changes_the_file(tmp_path):
    +    path = tmp_path / "appendonly.mraof"
    +    encoded = bytearray(AOF_HEADER + encode_aof_record(batch(1)))
    +    encoded[-1] ^= 0x01
    +    original = bytes(encoded)
    +    path.write_bytes(original)
    +
    +    with pytest.raises(AofCorruption, match="AOF checksum"):
    +        load_aof(path, repair_truncated_tail=True)
    +
    +    assert path.read_bytes() == original
    +
    +
    +def test_missing_aof_is_an_empty_stream(tmp_path):
    +    assert load_aof(
    +        tmp_path / "missing.mraof",
    +        repair_truncated_tail=True,
    +    ) == ()
    +
    +
    +def test_existing_zero_byte_aof_is_an_empty_stream(tmp_path):
    +    path = tmp_path / "empty.mraof"
    +    path.write_bytes(b"")
    +    assert load_aof(path, repair_truncated_tail=True) == ()
    +    assert path.read_bytes() == b""
    +
    +
    +def test_header_only_aof_is_an_empty_stream(tmp_path):
    +    path = tmp_path / "header-only.mraof"
    +    path.write_bytes(AOF_HEADER)
    +    assert load_aof(path, repair_truncated_tail=True) == ()
    +    assert path.read_bytes() == AOF_HEADER
    ```

**测试锁定什么**

它锁定 Opt-in Tail Truncation、Checksum Corruption 不修复、以及 Empty/Missing/Header-only Stream 的显式行为。

**如何构造反例**

它只从最终 Frame 删 Bytes，对比 Repair Enabled/Disabled，并证明 Corrupted File 保持 Byte-identical。

**关键测试语句**

```python
assert path.read_bytes() == AOF_HEADER + first
```

**失败意味着什么**

Repair 毁掉 Verified History、静默重写 Complete Corruption，或把空部署当作非法状态。

??? note "文件差异：tests/unit/persistence/test_codec.py"
    ```diff
    diff --git a/tests/unit/persistence/test_codec.py b/tests/unit/persistence/test_codec.py
    new file mode 100644
    index 0000000000000000000000000000000000000000..7f459c65a84a2989c84b2e82d2a93bc83c6fcd70
    --- /dev/null
    +++ b/tests/unit/persistence/test_codec.py
    @@ -0,0 +1,112 @@
    +import pytest
    +
    +from miniredis.core.commit import (
    +    CommitBatch,
    +    CommitTrigger,
    +    DeleteKey,
    +    DeleteReason,
    +    PutEntry,
    +    SnapshotImage,
    +    StoredEntry,
    +    StoredHash,
    +    StoredList,
    +    StoredSet,
    +    StoredString,
    +    StoredZSet,
    +)
    +from miniredis.persistence.codec import (
    +    CodecError,
    +    decode_commit_payload,
    +    decode_snapshot_payload,
    +    encode_commit_payload,
    +    encode_snapshot_payload,
    +)
    +
    +
    +def test_commit_payload_is_exact_canonical_json():
    +    batch = CommitBatch(
    +        seq=1,
    +        operations=(
    +            PutEntry(
    +                b"k",
    +                StoredEntry(StoredString(b"v"), None, 1),
    +            ),
    +        ),
    +        trigger=CommitTrigger.CLIENT,
    +    )
    +
    +    assert encode_commit_payload(batch) == (
    +        b'{"operations":[{"entry":{"expire_at_ms":null,'
    +        b'"mutation_version":1,"value":{"data":"dg==","type":"string"}},'
    +        b'"key":"aw==","op":"put"}],"seq":1,"trigger":"client",'
    +        b'"version":1}'
    +    )
    +
    +
    +def test_commit_round_trip_covers_binary_data_ordering_and_scores():
    +    batch = CommitBatch(
    +        seq=9,
    +        operations=(
    +            PutEntry(
    +                b"\xffhash",
    +                StoredEntry(
    +                    StoredHash(((b"\x00", b"v"), (b"z", b"\xff"))),
    +                    123456,
    +                    4,
    +                ),
    +            ),
    +            PutEntry(
    +                b"list",
    +                StoredEntry(StoredList((b"a", b"\x00")), None, 2),
    +            ),
    +            PutEntry(
    +                b"set",
    +                StoredEntry(StoredSet((b"a", b"z")), None, 3),
    +            ),
    +            PutEntry(
    +                b"zset",
    +                StoredEntry(
    +                    StoredZSet(
    +                        (
    +                            (b"a", -1.5),
    +                            (b"n", float("-inf")),
    +                            (b"p", float("inf")),
    +                        )
    +                    ),
    +                    None,
    +                    7,
    +                ),
    +            ),
    +            DeleteKey(b"gone", DeleteReason.EVICTED),
    +        ),
    +        trigger=CommitTrigger.ACTIVE_EXPIRE,
    +    )
    +
    +    assert decode_commit_payload(encode_commit_payload(batch)) == batch
    +
    +
    +def test_snapshot_payload_round_trips_sorted_entries():
    +    image = SnapshotImage(
    +        checkpoint_seq=9,
    +        entries=(
    +            (b"a", StoredEntry(StoredString(b"1"), None, 1)),
    +            (b"z", StoredEntry(StoredSet((b"a", b"b")), 8000, 2)),
    +        ),
    +    )
    +    assert decode_snapshot_payload(encode_snapshot_payload(image)) == image
    +
    +
    +@pytest.mark.parametrize(
    +    "payload",
    +    [
    +        b"{}",
    +        b'{"operations":[],"seq":1,"trigger":"client","version":1}',
    +        b'{"operations":[],"seq":1,"trigger":"client","version":2}',
    +        b'{"operations":[],"seq":true,"trigger":"client","version":1}',
    +        b'{"operations":[],"seq":1,"seq":1,"trigger":"client","version":1}',
    +        b'{"operations":[],"seq":1,"trigger":"unknown","version":1}',
    +    ],
    +)
    +def test_invalid_schema_or_duplicate_json_keys_are_rejected(payload):
    +    with pytest.raises(CodecError):
    +        decode_commit_payload(payload)
    ```

**测试锁定什么**

它锁定精确 Canonical JSON、全值族二进制 Round-trip、Score Encoding、Snapshot Ordering、严格 Schema、Duplicate-key Rejection 与 Payload Version。

**如何构造反例**

它断言精确 Bytes，Round-trip 非 UTF-8 数据与 Infinity，再提供非法 Object 和重复 Field。

**关键测试语句**

```python
assert decode_commit_payload(encode_commit_payload(batch)) == batch
```

**失败意味着什么**

等价逻辑状态可生成不同 Bytes，或非法 Bytes 可进耐久词汇。

??? note "文件差异：tests/unit/persistence/test_framing.py"
    ```diff
    diff --git a/tests/unit/persistence/test_framing.py b/tests/unit/persistence/test_framing.py
    new file mode 100644
    index 0000000000000000000000000000000000000000..62494a18eb1e92c4977a82daec8e242cadd52199
    --- /dev/null
    +++ b/tests/unit/persistence/test_framing.py
    @@ -0,0 +1,87 @@
    +import struct
    +import zlib
    +
    +import pytest
    +
    +from miniredis.core.commit import (
    +    CommitBatch,
    +    CommitTrigger,
    +    PutEntry,
    +    SnapshotImage,
    +    StoredEntry,
    +    StoredString,
    +)
    +from miniredis.persistence.codec import (
    +    AOF_HEADER,
    +    SNAPSHOT_HEADER,
    +    CodecError,
    +    decode_snapshot_file,
    +    encode_aof_record,
    +    encode_commit_payload,
    +    encode_snapshot_file,
    +    scan_aof_bytes,
    +)
    +
    +
    +def batch(seq: int, value: bytes = b"v") -> CommitBatch:
    +    return CommitBatch(
    +        seq,
    +        (
    +            PutEntry(
    +                b"k",
    +                StoredEntry(StoredString(value), None, seq),
    +            ),
    +        ),
    +        CommitTrigger.CLIENT,
    +    )
    +
    +
    +def test_aof_record_has_length_payload_and_crc32():
    +    payload = encode_commit_payload(batch(1))
    +    record = encode_aof_record(batch(1))
    +    assert record == (
    +        struct.pack(">I", len(payload))
    +        + payload
    +        + struct.pack(">I", zlib.crc32(payload))
    +    )
    +    scan = scan_aof_bytes(AOF_HEADER + record)
    +    assert scan.batches == (batch(1),)
    +    assert scan.valid_offset == len(AOF_HEADER + record)
    +    assert scan.has_truncated_tail is False
    +
    +
    +def test_snapshot_file_has_versioned_header_length_and_crc():
    +    image = SnapshotImage(
    +        1,
    +        ((b"k", StoredEntry(StoredString(b"a\x00b"), 7000, 2)),),
    +    )
    +    encoded = encode_snapshot_file(image)
    +    assert encoded.startswith(SNAPSHOT_HEADER)
    +    assert decode_snapshot_file(encoded) == image
    +
    +
    +def test_complete_checksum_failure_is_never_a_repairable_tail():
    +    encoded = bytearray(AOF_HEADER + encode_aof_record(batch(1)))
    +    encoded[-1] ^= 0xFF
    +    with pytest.raises(CodecError, match="AOF checksum"):
    +        scan_aof_bytes(bytes(encoded))
    +
    +
    +def test_sequence_gap_or_regression_is_corruption():
    +    encoded = (
    +        AOF_HEADER
    +        + encode_aof_record(batch(1))
    +        + encode_aof_record(batch(3))
    +    )
    +    with pytest.raises(CodecError, match="expected AOF seq 2, got 3"):
    +        scan_aof_bytes(encoded)
    +
    +
    +def test_aof_segment_may_start_after_a_snapshot_checkpoint():
    +    encoded = (
    +        AOF_HEADER
    +        + encode_aof_record(batch(8))
    +        + encode_aof_record(batch(9))
    +    )
    +
    +    assert scan_aof_bytes(encoded).batches == (batch(8), batch(9))
    ```

**测试锁定什么**

它锁定 Versioned Header、Length/Payload/CRC Frame、Snapshot Checksum、连续 AOF Sequence、Valid Tail Offset 与 Checkpoint 后 Segment Start。

**如何构造反例**

它翻转完整 Frame Checksum Byte，构造 Sequence Gap，并扫描完整流与 Checkpoint-following Stream。

**关键测试语句**

```python
with pytest.raises(CodecError, match="AOF checksum"):
```

**失败意味着什么**

Scanner 混淆 Corruption 与 Truncation，或无法证明哪个 Byte Offset 已完整耐久。

### 基本概念

Canonical Encoding 给每个 Stable Value 唯一 Byte 表示：有序 JSON Key、精确 Field、Canonical Base64、十六进制 Finite Score、显式 Infinity Token，且无 NaN。Frame 增加 Payload Length 与 CRC32。Scan Result 携带 Verified Batch、最后 Valid Offset 与是否只有最终 Frame 不完整。

### 为什么需要这个机制

Checksum 只有在 Framing 精确声明覆盖哪些 Bytes 时才证明 Integrity。严格 Canonical Payload 使 Replay、Comparison、Test 与未来 Replication 确定。窄 Repair Policy 保留证据，而不是把任意 Corruption 变成已接受 History。

### 运行时心智模型

Encoding 先创建 Canonical Payload Bytes，再包装为 `length + payload + crc`。AOF Scan 按序校验 Header、Bounds、Checksum、Schema 与 Sequence。EOF 落在最终已声明 Frame 内时返回 Repairable Tail；任何完整非法 Frame 抛错。Repair 只截断并 Fsync 到 Scanner 的 Verified Offset。

### 机制板块

#### Canonical Payload 与校验帧

把稳定值编码为严格 Canonical JSON，再用版本 Header、Length 与 CRC32 包装 AOF/Snapshot Payload。

??? note "文件差异：src/miniredis/persistence/codec.py"
    ```diff
    diff --git a/src/miniredis/persistence/codec.py b/src/miniredis/persistence/codec.py
    new file mode 100644
    index 0000000000000000000000000000000000000000..77f12a2a506b4f16de2e87833e2fadd349656aca
    --- /dev/null
    +++ b/src/miniredis/persistence/codec.py
    @@ -0,0 +1,519 @@
    +from __future__ import annotations
    +
    +import base64
    +import binascii
    +import json
    +import math
    +import struct
    +import zlib
    +from dataclasses import dataclass
    +from typing import Any, NoReturn
    +
    +from miniredis.core.commit import (
    +    CommitBatch,
    +    CommitTrigger,
    +    DeleteKey,
    +    DeleteReason,
    +    PutEntry,
    +    SnapshotImage,
    +    StoredEntry,
    +    StoredHash,
    +    StoredList,
    +    StoredSet,
    +    StoredString,
    +    StoredValue,
    +    StoredZSet,
    +)
    +
    +
    +PAYLOAD_VERSION = 1
    +AOF_HEADER = b"MR-AOF\x01"
    +SNAPSHOT_HEADER = b"MR-SNAP\x01"
    +MAX_PAYLOAD_BYTES = 64 * 1024 * 1024
    +
    +
    +class CodecError(ValueError):
    +    pass
    +
    +
    +@dataclass(frozen=True, slots=True)
    +class AofScan:
    +    batches: tuple[CommitBatch, ...]
    +    valid_offset: int
    +    has_truncated_tail: bool
    +
    +
    +def _fail(message: str) -> NoReturn:
    +    raise CodecError(message)
    +
    +
    +def _strict_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    +    result: dict[str, Any] = {}
    +    for key, value in pairs:
    +        if key in result:
    +            _fail(f"duplicate JSON key: {key}")
    +        result[key] = value
    +    return result
    +
    +
    +def _parse_constant(value: str) -> NoReturn:
    +    _fail(f"non-finite JSON number: {value}")
    +
    +
    +def _loads(payload: bytes) -> Any:
    +    try:
    +        text = payload.decode("utf-8")
    +        return json.loads(
    +            text,
    +            object_pairs_hook=_strict_object,
    +            parse_constant=_parse_constant,
    +        )
    +    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
    +        raise CodecError("invalid UTF-8 JSON payload") from exc
    +
    +
    +def _dumps(value: Any) -> bytes:
    +    try:
    +        return json.dumps(
    +            value,
    +            allow_nan=False,
    +            ensure_ascii=True,
    +            separators=(",", ":"),
    +            sort_keys=True,
    +        ).encode("ascii")
    +    except (TypeError, ValueError) as exc:
    +        raise CodecError("value cannot be encoded") from exc
    +
    +
    +def _object(
    +    value: Any,
    +    fields: frozenset[str],
    +    label: str,
    +) -> dict[str, Any]:
    +    if not isinstance(value, dict) or frozenset(value) != fields:
    +        _fail(f"invalid {label} fields")
    +    return value
    +
    +
    +def _array(value: Any, label: str) -> list[Any]:
    +    if not isinstance(value, list):
    +        _fail(f"{label} must be an array")
    +    return value
    +
    +
    +def _integer(value: Any, label: str, *, minimum: int = 0) -> int:
    +    if isinstance(value, bool) or not isinstance(value, int):
    +        _fail(f"{label} must be an integer")
    +    if value < minimum:
    +        _fail(f"{label} must be at least {minimum}")
    +    return value
    +
    +
    +def _optional_integer(value: Any, label: str) -> int | None:
    +    if value is None:
    +        return None
    +    return _integer(value, label)
    +
    +
    +def _text(value: Any, label: str) -> str:
    +    if not isinstance(value, str):
    +        _fail(f"{label} must be text")
    +    return value
    +
    +
    +def _bytes(value: bytes) -> str:
    +    return base64.b64encode(value).decode("ascii")
    +
    +
    +def _decode_bytes(value: Any, label: str) -> bytes:
    +    text = _text(value, label)
    +    try:
    +        decoded = base64.b64decode(text.encode("ascii"), validate=True)
    +    except (UnicodeEncodeError, binascii.Error) as exc:
    +        raise CodecError(f"invalid Base64 for {label}") from exc
    +    if _bytes(decoded) != text:
    +        _fail(f"non-canonical Base64 for {label}")
    +    return decoded
    +
    +
    +def _score(value: float) -> str:
    +    if math.isnan(value):
    +        _fail("NaN score is not encodable")
    +    if value == float("inf"):
    +        return "+inf"
    +    if value == float("-inf"):
    +        return "-inf"
    +    return value.hex()
    +
    +
    +def _decode_score(value: Any) -> float:
    +    token = _text(value, "score")
    +    if token == "+inf":
    +        return float("inf")
    +    if token == "-inf":
    +        return float("-inf")
    +    try:
    +        score = float.fromhex(token)
    +    except ValueError as exc:
    +        raise CodecError("invalid hexadecimal score") from exc
    +    if not math.isfinite(score) or score.hex() != token:
    +        _fail("non-canonical finite score")
    +    return score
    +
    +
    +def _encode_value(value: StoredValue) -> dict[str, Any]:
    +    match value:
    +        case StoredString(data):
    +            return {"type": "string", "data": _bytes(data)}
    +        case StoredHash(items):
    +            return {
    +                "type": "hash",
    +                "items": [
    +                    [_bytes(field), _bytes(item)] for field, item in items
    +                ],
    +            }
    +        case StoredList(items):
    +            return {
    +                "type": "list",
    +                "items": [_bytes(item) for item in items],
    +            }
    +        case StoredSet(items):
    +            return {
    +                "type": "set",
    +                "members": [_bytes(member) for member in items],
    +            }
    +        case StoredZSet(items):
    +            return {
    +                "type": "zset",
    +                "scores": [
    +                    [_bytes(member), _score(score)]
    +                    for member, score in items
    +                ],
    +            }
    +    raise TypeError(f"unsupported stored value: {type(value)!r}")
    +
    +
    +def _pairs(
    +    value: Any,
    +    label: str,
    +) -> tuple[tuple[bytes, bytes], ...]:
    +    result: list[tuple[bytes, bytes]] = []
    +    for index, pair in enumerate(_array(value, label)):
    +        items = _array(pair, f"{label}[{index}]")
    +        if len(items) != 2:
    +            _fail(f"{label}[{index}] must have two items")
    +        result.append(
    +            (
    +                _decode_bytes(items[0], f"{label}[{index}].key"),
    +                _decode_bytes(items[1], f"{label}[{index}].value"),
    +            )
    +        )
    +    frozen = tuple(result)
    +    if frozen != tuple(sorted(frozen)) or len(
    +        {key for key, _ in frozen}
    +    ) != len(frozen):
    +        _fail(f"{label} must have unique binary-sorted keys")
    +    return frozen
    +
    +
    +def _byte_array(value: Any, label: str) -> tuple[bytes, ...]:
    +    return tuple(
    +        _decode_bytes(item, f"{label}[{index}]")
    +        for index, item in enumerate(_array(value, label))
    +    )
    +
    +
    +def _decode_value(value: Any) -> StoredValue:
    +    if not isinstance(value, dict):
    +        _fail("stored value must be an object")
    +    kind = _text(value.get("type"), "value type")
    +    if kind == "string":
    +        item = _object(value, frozenset({"type", "data"}), "string")
    +        return StoredString(_decode_bytes(item["data"], "string data"))
    +    if kind == "hash":
    +        item = _object(value, frozenset({"type", "items"}), "hash")
    +        return StoredHash(_pairs(item["items"], "hash items"))
    +    if kind == "list":
    +        item = _object(value, frozenset({"type", "items"}), "list")
    +        return StoredList(_byte_array(item["items"], "list items"))
    +    if kind == "set":
    +        item = _object(value, frozenset({"type", "members"}), "set")
    +        items = _byte_array(item["members"], "set members")
    +        if items != tuple(sorted(items)) or len(set(items)) != len(items):
    +            _fail("set members must be unique and binary sorted")
    +        return StoredSet(items)
    +    if kind == "zset":
    +        item = _object(value, frozenset({"type", "scores"}), "zset")
    +        items: list[tuple[bytes, float]] = []
    +        for index, pair in enumerate(_array(item["scores"], "zset scores")):
    +            members = _array(pair, f"zset scores[{index}]")
    +            if len(members) != 2:
    +                _fail(f"zset scores[{index}] must have two items")
    +            items.append(
    +                (
    +                    _decode_bytes(
    +                        members[0],
    +                        f"zset scores[{index}].member",
    +                    ),
    +                    _decode_score(members[1]),
    +                )
    +            )
    +        frozen_items = tuple(items)
    +        keys = tuple(member for member, _score_value in frozen_items)
    +        if keys != tuple(sorted(keys)) or len(set(keys)) != len(keys):
    +            _fail("zset members must be unique and binary sorted")
    +        return StoredZSet(frozen_items)
    +    _fail(f"unknown stored value type: {kind}")
    +
    +
    +def _encode_entry(entry: StoredEntry) -> dict[str, Any]:
    +    return {
    +        "value": _encode_value(entry.value),
    +        "expire_at_ms": entry.expire_at_ms,
    +        "mutation_version": entry.mutation_version,
    +    }
    +
    +
    +def _decode_entry(value: Any) -> StoredEntry:
    +    item = _object(
    +        value,
    +        frozenset({"value", "expire_at_ms", "mutation_version"}),
    +        "stored entry",
    +    )
    +    return StoredEntry(
    +        value=_decode_value(item["value"]),
    +        expire_at_ms=_optional_integer(
    +            item["expire_at_ms"],
    +            "expire_at_ms",
    +        ),
    +        mutation_version=_integer(
    +            item["mutation_version"],
    +            "mutation_version",
    +        ),
    +    )
    +
    +
    +def encode_commit_payload(batch: CommitBatch) -> bytes:
    +    operations: list[dict[str, Any]] = []
    +    for operation in batch.operations:
    +        if isinstance(operation, PutEntry):
    +            operations.append(
    +                {
    +                    "op": "put",
    +                    "key": _bytes(operation.key),
    +                    "entry": _encode_entry(operation.entry),
    +                }
    +            )
    +        else:
    +            operations.append(
    +                {
    +                    "op": "delete",
    +                    "key": _bytes(operation.key),
    +                    "reason": operation.reason.value,
    +                }
    +            )
    +    return _dumps(
    +        {
    +            "version": PAYLOAD_VERSION,
    +            "seq": batch.seq,
    +            "trigger": batch.trigger.value,
    +            "operations": operations,
    +        }
    +    )
    +
    +
    +def decode_commit_payload(payload: bytes) -> CommitBatch:
    +    root = _object(
    +        _loads(payload),
    +        frozenset({"version", "seq", "trigger", "operations"}),
    +        "commit",
    +    )
    +    if _integer(root["version"], "version", minimum=1) != PAYLOAD_VERSION:
    +        _fail("unsupported commit payload version")
    +    try:
    +        trigger = CommitTrigger(_text(root["trigger"], "trigger"))
    +    except ValueError as exc:
    +        raise CodecError("unknown commit trigger") from exc
    +
    +    raw_operations = _array(root["operations"], "operations")
    +    if not raw_operations:
    +        _fail("commit must contain an operation")
    +    operations = []
    +    for index, raw_operation in enumerate(raw_operations):
    +        if not isinstance(raw_operation, dict):
    +            _fail(f"operations[{index}] must be an object")
    +        kind = _text(raw_operation.get("op"), f"operations[{index}].op")
    +        if kind == "put":
    +            operation = _object(
    +                raw_operation,
    +                frozenset({"op", "key", "entry"}),
    +                f"operations[{index}]",
    +            )
    +            operations.append(
    +                PutEntry(
    +                    _decode_bytes(
    +                        operation["key"],
    +                        f"operations[{index}].key",
    +                    ),
    +                    _decode_entry(operation["entry"]),
    +                )
    +            )
    +        elif kind == "delete":
    +            operation = _object(
    +                raw_operation,
    +                frozenset({"op", "key", "reason"}),
    +                f"operations[{index}]",
    +            )
    +            try:
    +                reason = DeleteReason(
    +                    _text(
    +                        operation["reason"],
    +                        f"operations[{index}].reason",
    +                    )
    +                )
    +            except ValueError as exc:
    +                raise CodecError("unknown delete reason") from exc
    +            operations.append(
    +                DeleteKey(
    +                    _decode_bytes(
    +                        operation["key"],
    +                        f"operations[{index}].key",
    +                    ),
    +                    reason,
    +                )
    +            )
    +        else:
    +            _fail(f"unknown operation type: {kind}")
    +    try:
    +        return CommitBatch(
    +            seq=_integer(root["seq"], "seq", minimum=1),
    +            operations=tuple(operations),
    +            trigger=trigger,
    +        )
    +    except ValueError as exc:
    +        raise CodecError(str(exc)) from exc
    +
    +
    +def encode_snapshot_payload(image: SnapshotImage) -> bytes:
    +    return _dumps(
    +        {
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
    +def decode_snapshot_payload(payload: bytes) -> SnapshotImage:
    +    root = _object(
    +        _loads(payload),
    +        frozenset({"version", "checkpoint_seq", "entries"}),
    +        "snapshot",
    +    )
    +    if _integer(root["version"], "version", minimum=1) != PAYLOAD_VERSION:
    +        _fail("unsupported snapshot payload version")
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
    +def _crc(payload: bytes) -> bytes:
    +    return struct.pack(">I", zlib.crc32(payload))
    +
    +
    +def encode_aof_record(batch: CommitBatch) -> bytes:
    +    payload = encode_commit_payload(batch)
    +    if len(payload) > MAX_PAYLOAD_BYTES:
    +        raise CodecError("AOF payload exceeds limit")
    +    return struct.pack(">I", len(payload)) + payload + _crc(payload)
    +
    +
    +def scan_aof_bytes(data: bytes) -> AofScan:
    +    if not data.startswith(AOF_HEADER):
    +        raise CodecError("invalid AOF header")
    +    offset = len(AOF_HEADER)
    +    valid_offset = offset
    +    batches: list[CommitBatch] = []
    +    previous_seq: int | None = None
    +    while offset < len(data):
    +        if len(data) - offset < 4:
    +            return AofScan(tuple(batches), valid_offset, True)
    +        payload_length = struct.unpack_from(">I", data, offset)[0]
    +        if payload_length > MAX_PAYLOAD_BYTES:
    +            raise CodecError("AOF payload exceeds limit")
    +        end = offset + 4 + payload_length + 4
    +        if end > len(data):
    +            return AofScan(tuple(batches), valid_offset, True)
    +        payload_start = offset + 4
    +        payload_end = payload_start + payload_length
    +        payload = data[payload_start:payload_end]
    +        expected_crc = struct.unpack_from(">I", data, payload_end)[0]
    +        actual_crc = zlib.crc32(payload)
    +        if actual_crc != expected_crc:
    +            raise CodecError(f"AOF checksum failure at offset {offset}")
    +        batch = decode_commit_payload(payload)
    +        if previous_seq is not None and batch.seq != previous_seq + 1:
    +            raise CodecError(
    +                f"expected AOF seq {previous_seq + 1}, got {batch.seq}"
    +            )
    +        batches.append(batch)
    +        previous_seq = batch.seq
    +        offset = end
    +        valid_offset = end
    +    return AofScan(tuple(batches), valid_offset, False)
    +
    +
    +def encode_snapshot_file(image: SnapshotImage) -> bytes:
    +    payload = encode_snapshot_payload(image)
    +    if len(payload) > MAX_PAYLOAD_BYTES:
    +        raise CodecError("snapshot payload exceeds limit")
    +    return (
    +        SNAPSHOT_HEADER
    +        + struct.pack(">Q", len(payload))
    +        + payload
    +        + _crc(payload)
    +    )
    +
    +
    +def decode_snapshot_file(data: bytes) -> SnapshotImage:
    +    if not data.startswith(SNAPSHOT_HEADER):
    +        raise CodecError("invalid snapshot header")
    +    prefix = len(SNAPSHOT_HEADER)
    +    if len(data) < prefix + 8 + 4:
    +        raise CodecError("truncated snapshot")
    +    payload_length = struct.unpack_from(">Q", data, prefix)[0]
    +    if payload_length > MAX_PAYLOAD_BYTES:
    +        raise CodecError("snapshot payload exceeds limit")
    +    expected_size = prefix + 8 + payload_length + 4
    +    if len(data) != expected_size:
    +        raise CodecError("invalid snapshot length")
    +    payload_start = prefix + 8
    +    payload_end = payload_start + payload_length
    +    payload = data[payload_start:payload_end]
    +    expected_crc = struct.unpack_from(">I", data, payload_end)[0]
    +    if zlib.crc32(payload) != expected_crc:
    +        raise CodecError("snapshot checksum failure")
    +    return decode_snapshot_payload(payload)
    ```

**是什么，为什么现在需要**

该模块拥有严格 Payload Schema、Canonical Scalar Encoding、AOF Record、Snapshot File、Checksum 与 Scanning。

**在运行时做什么**

它把 Stable Commit/Snapshot Value 变成唯一 Byte Form，并在 Replay 前拒绝模糊或损坏 Form。

**关键代码**

```python
return json.dumps(
    value,
    allow_nan=False,
    ensure_ascii=True,
    separators=(",", ":"),
    sort_keys=True,
).encode("ascii")
```

**关键语句理解**

有序 Compact ASCII JSON 消除 Formatting 与 Key-order 自由度；Field Validator 消除 Schema 自由度。

#### 有界 AOF Tail 修复

只把一个不完整末帧视为可修复；Checksum、Schema、Header 与 Sequence 损坏都拒绝且不改写证据。

??? note "文件差异：src/miniredis/persistence/aof.py"
    ```diff
    diff --git a/src/miniredis/persistence/aof.py b/src/miniredis/persistence/aof.py
    new file mode 100644
    index 0000000000000000000000000000000000000000..32808fb70d31265dccf90ed8bc62b4ef33598b8c
    --- /dev/null
    +++ b/src/miniredis/persistence/aof.py
    @@ -0,0 +1,40 @@
    +from __future__ import annotations
    +
    +import os
    +from pathlib import Path
    +
    +from miniredis.core.commit import CommitBatch
    +from miniredis.persistence.codec import CodecError, scan_aof_bytes
    +
    +
    +class AofCorruption(RuntimeError):
    +    pass
    +
    +
    +def load_aof(
    +    path: Path,
    +    *,
    +    repair_truncated_tail: bool,
    +) -> tuple[CommitBatch, ...]:
    +    try:
    +        data = path.read_bytes()
    +    except FileNotFoundError:
    +        return ()
    +    if data == b"":
    +        return ()
    +    try:
    +        scan = scan_aof_bytes(data)
    +    except CodecError as exc:
    +        raise AofCorruption(str(exc)) from exc
    +    if not scan.has_truncated_tail:
    +        return scan.batches
    +    if not repair_truncated_tail:
    +        raise AofCorruption("incomplete final AOF record")
    +
    +    fd = os.open(path, os.O_WRONLY)
    +    try:
    +        os.ftruncate(fd, scan.valid_offset)
    +        os.fsync(fd)
    +    finally:
    +        os.close(fd)
    +    return scan.batches
    ```

**是什么，为什么现在需要**

AOF Loader 把 Codec Failure 翻译为 Storage-level Corruption，并拥有 Opt-in Physical Tail Repair。

**在运行时做什么**

它接受 Missing/Empty History，返回 Complete Batch，拒绝 Non-tail Corruption，或精确截至 `valid_offset` 并 Fsync。

**关键代码**

```python
os.ftruncate(fd, scan.valid_offset)
os.fsync(fd)
```

**关键语句理解**

Truncate + Fsync 使修复后 File 本身耐久；后续 Load 不依赖记住 In-memory Offset。

### 验证证据

运行 `uv run pytest -q $(cat journey/stages/14-framed-persistence-codec/tests.txt)`。它覆盖 Canonical Payload、全值族、Frame Integrity、Sequence Validation 与完整 Tail-repair Matrix。

### 需要真正记住的内容

一个逻辑值需一个 Byte Form；Checksum 前先 Frame；拒绝重复或额外 Schema Field；区分 Incomplete Tail 与 Complete Corruption；只修复到 Verified Offset；Fsync Repair。

### 用自己的话讲清楚

Codec 决定 Bytes 意义，Framing 决定一个 Durable Record 在哪结束。Crash 可留下未完成最终 Record，它可被移除。已完成但 Checksum 或含义错误的 Record 是 Corruption 证据，绝不能通过猜测“修复”。

### 教材

[第 6 章](https://github.com/system-in-miniature/mini-redis/blob/main/docs/zh/tutorial/06-aof.md)

[在 GitHub 查看阶段差异](https://github.com/system-in-miniature/mini-redis/compare/5a40b5f...633bbe6)

完成后可运行 `python -m journey.tools.build_journey check 14` 验收学习工作区。

[Complete reference patch / 完整参考补丁](https://github.com/system-in-miniature/mini-redis/blob/main/journey/stages/14-framed-persistence-codec/stage.patch)
