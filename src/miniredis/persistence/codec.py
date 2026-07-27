from __future__ import annotations

import base64
import binascii
import json
import math
from typing import Any, NoReturn

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
    StoredValue,
    StoredZSet,
)


PAYLOAD_VERSION = 1


class CodecError(ValueError):
    pass


def _fail(message: str) -> NoReturn:
    raise CodecError(message)


def _strict_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            _fail(f"duplicate JSON key: {key}")
        result[key] = value
    return result


def _parse_constant(value: str) -> NoReturn:
    _fail(f"non-finite JSON number: {value}")


def _loads(payload: bytes) -> Any:
    try:
        text = payload.decode("utf-8")
        return json.loads(
            text,
            object_pairs_hook=_strict_object,
            parse_constant=_parse_constant,
        )
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise CodecError("invalid UTF-8 JSON payload") from exc


def _dumps(value: Any) -> bytes:
    try:
        return json.dumps(
            value,
            allow_nan=False,
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("ascii")
    except (TypeError, ValueError) as exc:
        raise CodecError("value cannot be encoded") from exc


def _object(
    value: Any,
    fields: frozenset[str],
    label: str,
) -> dict[str, Any]:
    if not isinstance(value, dict) or frozenset(value) != fields:
        _fail(f"invalid {label} fields")
    return value


def _array(value: Any, label: str) -> list[Any]:
    if not isinstance(value, list):
        _fail(f"{label} must be an array")
    return value


def _integer(value: Any, label: str, *, minimum: int = 0) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        _fail(f"{label} must be an integer")
    if value < minimum:
        _fail(f"{label} must be at least {minimum}")
    return value


def _optional_integer(value: Any, label: str) -> int | None:
    if value is None:
        return None
    return _integer(value, label)


def _text(value: Any, label: str) -> str:
    if not isinstance(value, str):
        _fail(f"{label} must be text")
    return value


def _bytes(value: bytes) -> str:
    return base64.b64encode(value).decode("ascii")


def _decode_bytes(value: Any, label: str) -> bytes:
    text = _text(value, label)
    try:
        decoded = base64.b64decode(text.encode("ascii"), validate=True)
    except (UnicodeEncodeError, binascii.Error) as exc:
        raise CodecError(f"invalid Base64 for {label}") from exc
    if _bytes(decoded) != text:
        _fail(f"non-canonical Base64 for {label}")
    return decoded


def _score(value: float) -> str:
    if math.isnan(value):
        _fail("NaN score is not encodable")
    if value == float("inf"):
        return "+inf"
    if value == float("-inf"):
        return "-inf"
    return value.hex()


def _decode_score(value: Any) -> float:
    token = _text(value, "score")
    if token == "+inf":
        return float("inf")
    if token == "-inf":
        return float("-inf")
    try:
        score = float.fromhex(token)
    except ValueError as exc:
        raise CodecError("invalid hexadecimal score") from exc
    if not math.isfinite(score) or score.hex() != token:
        _fail("non-canonical finite score")
    return score


def _encode_value(value: StoredValue) -> dict[str, Any]:
    match value:
        case StoredString(data):
            return {"type": "string", "data": _bytes(data)}
        case StoredHash(items):
            return {
                "type": "hash",
                "items": [
                    [_bytes(field), _bytes(item)] for field, item in items
                ],
            }
        case StoredList(items):
            return {
                "type": "list",
                "items": [_bytes(item) for item in items],
            }
        case StoredSet(items):
            return {
                "type": "set",
                "members": [_bytes(member) for member in items],
            }
        case StoredZSet(items):
            return {
                "type": "zset",
                "scores": [
                    [_bytes(member), _score(score)]
                    for member, score in items
                ],
            }
    raise TypeError(f"unsupported stored value: {type(value)!r}")


def _pairs(
    value: Any,
    label: str,
) -> tuple[tuple[bytes, bytes], ...]:
    result: list[tuple[bytes, bytes]] = []
    for index, pair in enumerate(_array(value, label)):
        items = _array(pair, f"{label}[{index}]")
        if len(items) != 2:
            _fail(f"{label}[{index}] must have two items")
        result.append(
            (
                _decode_bytes(items[0], f"{label}[{index}].key"),
                _decode_bytes(items[1], f"{label}[{index}].value"),
            )
        )
    frozen = tuple(result)
    if frozen != tuple(sorted(frozen)) or len(
        {key for key, _ in frozen}
    ) != len(frozen):
        _fail(f"{label} must have unique binary-sorted keys")
    return frozen


def _byte_array(value: Any, label: str) -> tuple[bytes, ...]:
    return tuple(
        _decode_bytes(item, f"{label}[{index}]")
        for index, item in enumerate(_array(value, label))
    )


def _decode_value(value: Any) -> StoredValue:
    if not isinstance(value, dict):
        _fail("stored value must be an object")
    kind = _text(value.get("type"), "value type")
    if kind == "string":
        item = _object(value, frozenset({"type", "data"}), "string")
        return StoredString(_decode_bytes(item["data"], "string data"))
    if kind == "hash":
        item = _object(value, frozenset({"type", "items"}), "hash")
        return StoredHash(_pairs(item["items"], "hash items"))
    if kind == "list":
        item = _object(value, frozenset({"type", "items"}), "list")
        return StoredList(_byte_array(item["items"], "list items"))
    if kind == "set":
        item = _object(value, frozenset({"type", "members"}), "set")
        items = _byte_array(item["members"], "set members")
        if items != tuple(sorted(items)) or len(set(items)) != len(items):
            _fail("set members must be unique and binary sorted")
        return StoredSet(items)
    if kind == "zset":
        item = _object(value, frozenset({"type", "scores"}), "zset")
        items: list[tuple[bytes, float]] = []
        for index, pair in enumerate(_array(item["scores"], "zset scores")):
            members = _array(pair, f"zset scores[{index}]")
            if len(members) != 2:
                _fail(f"zset scores[{index}] must have two items")
            items.append(
                (
                    _decode_bytes(
                        members[0],
                        f"zset scores[{index}].member",
                    ),
                    _decode_score(members[1]),
                )
            )
        frozen_items = tuple(items)
        keys = tuple(member for member, _score_value in frozen_items)
        if keys != tuple(sorted(keys)) or len(set(keys)) != len(keys):
            _fail("zset members must be unique and binary sorted")
        return StoredZSet(frozen_items)
    _fail(f"unknown stored value type: {kind}")


def _encode_entry(entry: StoredEntry) -> dict[str, Any]:
    return {
        "value": _encode_value(entry.value),
        "expire_at_ms": entry.expire_at_ms,
        "mutation_version": entry.mutation_version,
    }


def _decode_entry(value: Any) -> StoredEntry:
    item = _object(
        value,
        frozenset({"value", "expire_at_ms", "mutation_version"}),
        "stored entry",
    )
    return StoredEntry(
        value=_decode_value(item["value"]),
        expire_at_ms=_optional_integer(
            item["expire_at_ms"],
            "expire_at_ms",
        ),
        mutation_version=_integer(
            item["mutation_version"],
            "mutation_version",
        ),
    )


def encode_commit_payload(batch: CommitBatch) -> bytes:
    operations: list[dict[str, Any]] = []
    for operation in batch.operations:
        if isinstance(operation, PutEntry):
            operations.append(
                {
                    "op": "put",
                    "key": _bytes(operation.key),
                    "entry": _encode_entry(operation.entry),
                }
            )
        else:
            operations.append(
                {
                    "op": "delete",
                    "key": _bytes(operation.key),
                    "reason": operation.reason.value,
                }
            )
    return _dumps(
        {
            "version": PAYLOAD_VERSION,
            "seq": batch.seq,
            "trigger": batch.trigger.value,
            "operations": operations,
        }
    )


def decode_commit_payload(payload: bytes) -> CommitBatch:
    root = _object(
        _loads(payload),
        frozenset({"version", "seq", "trigger", "operations"}),
        "commit",
    )
    if _integer(root["version"], "version", minimum=1) != PAYLOAD_VERSION:
        _fail("unsupported commit payload version")
    try:
        trigger = CommitTrigger(_text(root["trigger"], "trigger"))
    except ValueError as exc:
        raise CodecError("unknown commit trigger") from exc

    raw_operations = _array(root["operations"], "operations")
    if not raw_operations:
        _fail("commit must contain an operation")
    operations = []
    for index, raw_operation in enumerate(raw_operations):
        if not isinstance(raw_operation, dict):
            _fail(f"operations[{index}] must be an object")
        kind = _text(raw_operation.get("op"), f"operations[{index}].op")
        if kind == "put":
            operation = _object(
                raw_operation,
                frozenset({"op", "key", "entry"}),
                f"operations[{index}]",
            )
            operations.append(
                PutEntry(
                    _decode_bytes(
                        operation["key"],
                        f"operations[{index}].key",
                    ),
                    _decode_entry(operation["entry"]),
                )
            )
        elif kind == "delete":
            operation = _object(
                raw_operation,
                frozenset({"op", "key", "reason"}),
                f"operations[{index}]",
            )
            try:
                reason = DeleteReason(
                    _text(
                        operation["reason"],
                        f"operations[{index}].reason",
                    )
                )
            except ValueError as exc:
                raise CodecError("unknown delete reason") from exc
            operations.append(
                DeleteKey(
                    _decode_bytes(
                        operation["key"],
                        f"operations[{index}].key",
                    ),
                    reason,
                )
            )
        else:
            _fail(f"unknown operation type: {kind}")
    try:
        return CommitBatch(
            seq=_integer(root["seq"], "seq", minimum=1),
            operations=tuple(operations),
            trigger=trigger,
        )
    except ValueError as exc:
        raise CodecError(str(exc)) from exc


def encode_snapshot_payload(image: SnapshotImage) -> bytes:
    return _dumps(
        {
            "version": PAYLOAD_VERSION,
            "checkpoint_seq": image.checkpoint_seq,
            "entries": [
                {"key": _bytes(key), "entry": _encode_entry(entry)}
                for key, entry in image.entries
            ],
        }
    )


def decode_snapshot_payload(payload: bytes) -> SnapshotImage:
    root = _object(
        _loads(payload),
        frozenset({"version", "checkpoint_seq", "entries"}),
        "snapshot",
    )
    if _integer(root["version"], "version", minimum=1) != PAYLOAD_VERSION:
        _fail("unsupported snapshot payload version")
    entries = []
    for index, raw_entry in enumerate(_array(root["entries"], "entries")):
        item = _object(
            raw_entry,
            frozenset({"key", "entry"}),
            f"entries[{index}]",
        )
        entries.append(
            (
                _decode_bytes(item["key"], f"entries[{index}].key"),
                _decode_entry(item["entry"]),
            )
        )
    try:
        return SnapshotImage(
            checkpoint_seq=_integer(
                root["checkpoint_seq"],
                "checkpoint_seq",
            ),
            entries=tuple(entries),
        )
    except ValueError as exc:
        raise CodecError(str(exc)) from exc
