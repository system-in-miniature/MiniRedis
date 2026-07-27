from __future__ import annotations

from collections.abc import Iterable

from miniredis.commands import model as cmd
from miniredis.commands.parser import (
    INT64_MAX,
    INT64_MIN,
    CommandParseError,
    parse_int64,
)
from miniredis.core.commit import (
    CommitOperation,
    DeleteKey,
    DeleteReason,
    PutEntry,
    StoredEntry,
)
from miniredis.core.database import Database, Entry, freeze_value
from miniredis.core.executor import ExecutionPlan
from miniredis.core.expiration import expiry_delete, is_expired
from miniredis.core.reply import Bytes, Failure, Number, Ok
from miniredis.core.values import (
    HashValue,
    ListValue,
    RedisValue,
    SetValue,
    StringValue,
    ZSetValue,
)


WRONGTYPE = Failure(
    "WRONGTYPE",
    "operation against a key holding the wrong kind of value",
)


def lookup(
    database: Database,
    key: bytes,
    now_ms: int,
) -> tuple[Entry | None, tuple[CommitOperation, ...]]:
    entry = database.entries.get(key)
    if entry is None:
        return None, ()
    if is_expired(entry, now_ms):
        return None, (expiry_delete(key),)
    return entry, ()


def dedupe_operations(
    operations: Iterable[CommitOperation],
) -> tuple[CommitOperation, ...]:
    result: list[CommitOperation] = []
    delete_indexes: dict[bytes, int] = {}
    put_indexes: dict[bytes, int] = {}
    for operation in operations:
        if isinstance(operation, DeleteKey):
            previous = delete_indexes.get(operation.key)
            if previous is None:
                delete_indexes[operation.key] = len(result)
                result.append(operation)
            else:
                result[previous] = operation
        else:
            previous = put_indexes.get(operation.key)
            if previous is None:
                put_indexes[operation.key] = len(result)
                result.append(operation)
            else:
                result[previous] = operation
    return tuple(result)


def make_put(
    key: bytes,
    value: RedisValue,
    previous: Entry | None,
    expire_at_ms: int | None,
) -> PutEntry:
    return PutEntry(
        key,
        StoredEntry(
            freeze_value(value),
            expire_at_ms,
            1 if previous is None else previous.mutation_version + 1,
        ),
    )


def type_name(entry: Entry | None) -> bytes:
    if entry is None:
        return b"none"
    match entry.value:
        case StringValue():
            return b"string"
        case HashValue():
            return b"hash"
        case ListValue():
            return b"list"
        case SetValue():
            return b"set"
        case ZSetValue():
            return b"zset"
    raise AssertionError(f"unhandled value: {entry.value!r}")


def _integer_failure() -> ExecutionPlan:
    return ExecutionPlan(Failure("ERR", "value is not an integer or out of range"))


def plan_general_and_strings(
    command: cmd.Command,
    database: Database,
    now_ms: int,
) -> ExecutionPlan | None:
    match command:
        case cmd.Ping(None):
            return ExecutionPlan(Ok(b"PONG"))
        case cmd.Ping(message):
            return ExecutionPlan(Bytes(message))
        case cmd.Echo(message):
            return ExecutionPlan(Bytes(message))
        case cmd.Delete(keys):
            operations: list[CommitOperation] = []
            removed = 0
            seen: set[bytes] = set()
            for key in keys:
                entry, expired = lookup(database, key, now_ms)
                operations.extend(expired)
                if key in seen:
                    continue
                seen.add(key)
                if entry is not None:
                    removed += 1
                    operations.append(DeleteKey(key, DeleteReason.CLIENT))
            return ExecutionPlan(Number(removed), dedupe_operations(operations))
        case cmd.Exists(keys):
            operations = []
            touches: list[bytes] = []
            count = 0
            for key in keys:
                entry, expired = lookup(database, key, now_ms)
                operations.extend(expired)
                if entry is not None:
                    count += 1
                    touches.append(key)
            return ExecutionPlan(
                Number(count),
                dedupe_operations(operations),
                tuple(touches),
            )
        case cmd.TypeOf(key):
            entry, expired = lookup(database, key, now_ms)
            return ExecutionPlan(
                Bytes(type_name(entry)),
                expired,
                (key,) if entry is not None else (),
            )
        case cmd.GetString(key):
            entry, expired = lookup(database, key, now_ms)
            if entry is None:
                return ExecutionPlan(Bytes(None), expired)
            if not isinstance(entry.value, StringValue):
                return ExecutionPlan(WRONGTYPE)
            return ExecutionPlan(Bytes(entry.value.data), expired, (key,))
        case cmd.SetString(key, value, only_if, expire_ms):
            previous, expired = lookup(database, key, now_ms)
            if only_if == "nx" and previous is not None:
                return ExecutionPlan(Bytes(None))
            if only_if == "xx" and previous is None:
                return ExecutionPlan(Bytes(None))
            expire_at_ms = None if expire_ms is None else now_ms + expire_ms
            put = make_put(
                key,
                StringValue(value),
                previous,
                expire_at_ms,
            )
            return ExecutionPlan(Ok(), expired + (put,))
        case cmd.Increment(key, amount):
            previous, expired = lookup(database, key, now_ms)
            if previous is None:
                old_value = 0
                old_expiry = None
            elif not isinstance(previous.value, StringValue):
                return ExecutionPlan(WRONGTYPE)
            else:
                old_expiry = previous.expire_at_ms
                try:
                    old_value = parse_int64(previous.value.data)
                except CommandParseError:
                    return _integer_failure()
            new_value = old_value + amount
            if not INT64_MIN <= new_value <= INT64_MAX:
                return _integer_failure()
            put = make_put(
                key,
                StringValue(str(new_value).encode("ascii")),
                previous,
                old_expiry,
            )
            return ExecutionPlan(Number(new_value), expired + (put,))
        case _:
            return None
