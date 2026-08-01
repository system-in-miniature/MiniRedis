# Stage 14 · Canonical persistence frames / Canonical 持久化帧

<!-- journey: chapter=6 tests_added=16 -->

## English

### Goal

Encode commits and snapshots canonically, detect complete corruption, and repair only an incomplete final AOF frame.

### Deliverable files

- `src/miniredis/persistence/aof.py`
- `src/miniredis/persistence/codec.py`
- `tests/unit/persistence/test_aof_repair.py`
- `tests/unit/persistence/test_codec.py`
- `tests/unit/persistence/test_framing.py`

### The problem at this point

Stable Python values still need a byte contract. Generic JSON accepts duplicate keys, non-finite numbers, and multiple equivalent encodings; raw concatenated payloads cannot distinguish a crash-truncated tail from complete but corrupted data.

### Failure preview

A checksum mismatch, duplicate JSON field, noncanonical Base64/float, sequence gap, or invalid schema must fail without modifying disk. Only a final frame whose declared bytes are incomplete may be truncated back to the last verified offset when repair is enabled.

### Test contract

<!-- journey-file: tests/unit/persistence/test_codec.py -->
#### `tests/unit/persistence/test_codec.py`

##### What this test locks

It locks exact canonical JSON, binary round trips for all values, score encoding, snapshot ordering, strict schemas, duplicate-key rejection, and payload versions.

##### How it constructs the counterexample

It asserts exact bytes, round-trips non-UTF8 data and infinities, then supplies malformed objects and duplicate fields.

##### Key test statement

```python
assert decode_commit_payload(encode_commit_payload(batch)) == batch
```

##### What a failure means

Equivalent logical state can produce different bytes or invalid bytes can enter the durable vocabulary.

<!-- journey-file: tests/unit/persistence/test_framing.py -->
#### `tests/unit/persistence/test_framing.py`

##### What this test locks

It locks versioned headers, length/payload/CRC framing, snapshot checksums, contiguous AOF sequences, valid tail offsets, and segment starts after checkpoints.

##### How it constructs the counterexample

It flips complete-frame checksum bytes, creates a sequence gap, and scans both full and checkpoint-following streams.

##### Key test statement

```python
with pytest.raises(CodecError, match="AOF checksum"):
```

##### What a failure means

The scanner confused corruption with truncation or could not prove which byte offset is fully durable.

<!-- journey-file: tests/unit/persistence/test_aof_repair.py -->
#### `tests/unit/persistence/test_aof_repair.py`

##### What this test locks

It locks opt-in tail truncation, no repair for checksum corruption, and explicit empty/missing/header-only stream behavior.

##### How it constructs the counterexample

It removes bytes only from the final frame, contrasts repair enabled/disabled, and verifies corrupted files remain byte-identical.

##### Key test statement

```python
assert path.read_bytes() == AOF_HEADER + first
```

##### What a failure means

Repair destroyed verified history, silently rewrote complete corruption, or treated an empty deployment as invalid state.

### Basic concepts

Canonical encoding gives one byte representation per stable value: sorted JSON keys, exact fields, canonical Base64, hexadecimal finite scores, explicit infinity tokens, and no NaN. A frame adds payload length and CRC32. A scan result carries verified batches, the last valid offset, and whether only the final frame is incomplete.

### Why this mechanism is necessary

Checksums prove integrity only when framing says exactly which bytes they cover. Strict canonical payloads make replay, comparison, tests, and future replication deterministic. Narrow repair policy preserves evidence instead of converting arbitrary corruption into accepted history.

### Runtime mental model

Encoding first creates canonical payload bytes, then wraps them as `length + payload + crc`. AOF scanning verifies header, bounds, checksum, schema, and sequence in order. End-of-file inside the final declared frame returns a repairable tail; any complete invalid frame raises. Repair truncates and fsyncs only to the scanner's verified offset.

### Mechanism blocks

<!-- journey-file: src/miniredis/persistence/codec.py -->
#### `src/miniredis/persistence/codec.py`

##### What it is and why it appears

This module owns strict payload schemas, canonical scalar encodings, AOF records, snapshot files, checksums, and scanning.

##### Runtime role

It converts stable commit/snapshot values to one byte form and refuses ambiguous or corrupted forms before replay.

##### Key code

```python
return json.dumps(
    value,
    allow_nan=False,
    ensure_ascii=True,
    separators=(",", ":"),
    sort_keys=True,
).encode("ascii")
```

##### Statement understanding

Sorted compact ASCII JSON removes formatting and key-order degrees of freedom; field validators remove schema degrees of freedom.

<!-- journey-file: src/miniredis/persistence/aof.py -->
#### `src/miniredis/persistence/aof.py`

##### What it is and why it appears

The AOF loader translates codec failures to storage-level corruption and owns the opt-in physical tail repair.

##### Runtime role

It accepts missing/empty history, returns complete batches, rejects non-tail corruption, or truncates exactly to `valid_offset` and fsyncs.

##### Key code

```python
os.ftruncate(fd, scan.valid_offset)
os.fsync(fd)
```

##### Statement understanding

Truncation and fsync make the repaired file itself durable; no later load depends on remembering an in-memory offset.

### Verification evidence

Run `uv run pytest -q $(cat journey/stages/14-framed-persistence-codec/tests.txt)`. It covers canonical payloads, all value families, frame integrity, sequence validation, and the complete tail-repair matrix.

### Durable takeaways

One logical value needs one byte form; frame before checksumming; reject duplicate or extra schema fields; distinguish incomplete tail from complete corruption; repair only to a verified offset; fsync the repair.

### Explain it in your own words

The codec decides what bytes mean, while framing decides where one durable record ends. A crash may leave the last record unfinished, which can be removed. A finished record with bad checksum or meaning is evidence of corruption and must never be “repaired” by guessing.

### Textbook

[Chapter 6](https://github.com/system-in-miniature/mini-redis/blob/main/docs/tutorial/06-aof.md)

## 中文

### 目标

Canonical 编码 Commit 与 Snapshot，检测完整损坏，并只修复不完整的最终 AOF Frame。

### 交付文件

- `src/miniredis/persistence/aof.py`
- `src/miniredis/persistence/codec.py`
- `tests/unit/persistence/test_aof_repair.py`
- `tests/unit/persistence/test_codec.py`
- `tests/unit/persistence/test_framing.py`

### 当前遇到的问题

稳定 Python Value 仍需 Byte Contract。通用 JSON 允许重复 Key、非有限 Number 与多个等价编码；原始拼接 Payload 无法区分 Crash-truncated Tail 与完整但已损坏数据。

### 先看会坏在哪里

Checksum 不匹配、重复 JSON Field、非 Canonical Base64/Float、Sequence Gap 或非法 Schema 必须失败且不改 Disk。只有已声明 Bytes 不完整的最终 Frame，才可在启用 Repair 时截回最后 Verified Offset。

### 测试契约

<!-- journey-file: tests/unit/persistence/test_codec.py -->
#### `tests/unit/persistence/test_codec.py`

##### 测试锁定什么

它锁定精确 Canonical JSON、全值族二进制 Round-trip、Score Encoding、Snapshot Ordering、严格 Schema、Duplicate-key Rejection 与 Payload Version。

##### 如何构造反例

它断言精确 Bytes，Round-trip 非 UTF-8 数据与 Infinity，再提供非法 Object 和重复 Field。

##### 关键测试语句

```python
assert decode_commit_payload(encode_commit_payload(batch)) == batch
```

##### 失败意味着什么

等价逻辑状态可生成不同 Bytes，或非法 Bytes 可进耐久词汇。

<!-- journey-file: tests/unit/persistence/test_framing.py -->
#### `tests/unit/persistence/test_framing.py`

##### 测试锁定什么

它锁定 Versioned Header、Length/Payload/CRC Frame、Snapshot Checksum、连续 AOF Sequence、Valid Tail Offset 与 Checkpoint 后 Segment Start。

##### 如何构造反例

它翻转完整 Frame Checksum Byte，构造 Sequence Gap，并扫描完整流与 Checkpoint-following Stream。

##### 关键测试语句

```python
with pytest.raises(CodecError, match="AOF checksum"):
```

##### 失败意味着什么

Scanner 混淆 Corruption 与 Truncation，或无法证明哪个 Byte Offset 已完整耐久。

<!-- journey-file: tests/unit/persistence/test_aof_repair.py -->
#### `tests/unit/persistence/test_aof_repair.py`

##### 测试锁定什么

它锁定 Opt-in Tail Truncation、Checksum Corruption 不修复、以及 Empty/Missing/Header-only Stream 的显式行为。

##### 如何构造反例

它只从最终 Frame 删 Bytes，对比 Repair Enabled/Disabled，并证明 Corrupted File 保持 Byte-identical。

##### 关键测试语句

```python
assert path.read_bytes() == AOF_HEADER + first
```

##### 失败意味着什么

Repair 毁掉 Verified History、静默重写 Complete Corruption，或把空部署当作非法状态。

### 基本概念

Canonical Encoding 给每个 Stable Value 唯一 Byte 表示：有序 JSON Key、精确 Field、Canonical Base64、十六进制 Finite Score、显式 Infinity Token，且无 NaN。Frame 增加 Payload Length 与 CRC32。Scan Result 携带 Verified Batch、最后 Valid Offset 与是否只有最终 Frame 不完整。

### 为什么需要这个机制

Checksum 只有在 Framing 精确声明覆盖哪些 Bytes 时才证明 Integrity。严格 Canonical Payload 使 Replay、Comparison、Test 与未来 Replication 确定。窄 Repair Policy 保留证据，而不是把任意 Corruption 变成已接受 History。

### 运行时心智模型

Encoding 先创建 Canonical Payload Bytes，再包装为 `length + payload + crc`。AOF Scan 按序校验 Header、Bounds、Checksum、Schema 与 Sequence。EOF 落在最终已声明 Frame 内时返回 Repairable Tail；任何完整非法 Frame 抛错。Repair 只截断并 Fsync 到 Scanner 的 Verified Offset。

### 机制板块

<!-- journey-file: src/miniredis/persistence/codec.py -->
#### `src/miniredis/persistence/codec.py`

##### 是什么，为什么现在需要

该模块拥有严格 Payload Schema、Canonical Scalar Encoding、AOF Record、Snapshot File、Checksum 与 Scanning。

##### 在运行时做什么

它把 Stable Commit/Snapshot Value 变成唯一 Byte Form，并在 Replay 前拒绝模糊或损坏 Form。

##### 关键代码

```python
return json.dumps(
    value,
    allow_nan=False,
    ensure_ascii=True,
    separators=(",", ":"),
    sort_keys=True,
).encode("ascii")
```

##### 关键语句理解

有序 Compact ASCII JSON 消除 Formatting 与 Key-order 自由度；Field Validator 消除 Schema 自由度。

<!-- journey-file: src/miniredis/persistence/aof.py -->
#### `src/miniredis/persistence/aof.py`

##### 是什么，为什么现在需要

AOF Loader 把 Codec Failure 翻译为 Storage-level Corruption，并拥有 Opt-in Physical Tail Repair。

##### 在运行时做什么

它接受 Missing/Empty History，返回 Complete Batch，拒绝 Non-tail Corruption，或精确截至 `valid_offset` 并 Fsync。

##### 关键代码

```python
os.ftruncate(fd, scan.valid_offset)
os.fsync(fd)
```

##### 关键语句理解

Truncate + Fsync 使修复后 File 本身耐久；后续 Load 不依赖记住 In-memory Offset。

### 验证证据

运行 `uv run pytest -q $(cat journey/stages/14-framed-persistence-codec/tests.txt)`。它覆盖 Canonical Payload、全值族、Frame Integrity、Sequence Validation 与完整 Tail-repair Matrix。

### 需要真正记住的内容

一个逻辑值需一个 Byte Form；Checksum 前先 Frame；拒绝重复或额外 Schema Field；区分 Incomplete Tail 与 Complete Corruption；只修复到 Verified Offset；Fsync Repair。

### 用自己的话讲清楚

Codec 决定 Bytes 意义，Framing 决定一个 Durable Record 在哪结束。Crash 可留下未完成最终 Record，它可被移除。已完成但 Checksum 或含义错误的 Record 是 Corruption 证据，绝不能通过猜测“修复”。

### 教材

[第 6 章](https://github.com/system-in-miniature/mini-redis/blob/main/docs/zh/tutorial/06-aof.md)
