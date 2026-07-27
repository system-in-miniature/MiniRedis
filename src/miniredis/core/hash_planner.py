from miniredis.commands import model as cmd
from miniredis.commands.parser import (
    INT64_MAX,
    INT64_MIN,
    CommandParseError,
    parse_int64,
)
from miniredis.core.commit import DeleteKey, DeleteReason
from miniredis.core.database import Database
from miniredis.core.executor import ExecutionPlan
from miniredis.core.planning import WRONGTYPE, lookup, make_put
from miniredis.core.reply import Bytes, Failure, Items, Number
from miniredis.core.values import HashValue


def _integer_failure() -> ExecutionPlan:
    return ExecutionPlan(Failure("ERR", "value is not an integer or out of range"))


def plan_hash(
    command: cmd.Command,
    database: Database,
    now_ms: int,
) -> ExecutionPlan | None:
    match command:
        case cmd.HashSet(key, pairs):
            previous, expired = lookup(database, key, now_ms)
            if previous is not None and not isinstance(previous.value, HashValue):
                return ExecutionPlan(WRONGTYPE)
            items = {} if previous is None else dict(previous.value.items)
            added = 0
            for field, value in pairs:
                if field not in items:
                    added += 1
                items[field] = value
            put = make_put(
                key,
                HashValue(items),
                previous,
                None if previous is None else previous.expire_at_ms,
            )
            return ExecutionPlan(Number(added), expired + (put,))
        case cmd.HashGet(key, field):
            entry, expired = lookup(database, key, now_ms)
            if entry is None:
                return ExecutionPlan(Bytes(None), expired)
            if not isinstance(entry.value, HashValue):
                return ExecutionPlan(WRONGTYPE)
            return ExecutionPlan(
                Bytes(entry.value.items.get(field)),
                expired,
                (key,),
            )
        case cmd.HashDelete(key, fields):
            previous, expired = lookup(database, key, now_ms)
            if previous is None:
                return ExecutionPlan(Number(0), expired)
            if not isinstance(previous.value, HashValue):
                return ExecutionPlan(WRONGTYPE)
            items = dict(previous.value.items)
            removed = 0
            for field in dict.fromkeys(fields):
                if field in items:
                    removed += 1
                    del items[field]
            if removed == 0:
                return ExecutionPlan(Number(0), (), (key,))
            if not items:
                operation = DeleteKey(key, DeleteReason.CLIENT)
            else:
                operation = make_put(
                    key,
                    HashValue(items),
                    previous,
                    previous.expire_at_ms,
                )
            return ExecutionPlan(Number(removed), expired + (operation,))
        case cmd.HashGetAll(key):
            entry, expired = lookup(database, key, now_ms)
            if entry is None:
                return ExecutionPlan(Items(()), expired)
            if not isinstance(entry.value, HashValue):
                return ExecutionPlan(WRONGTYPE)
            values = tuple(
                item
                for field, value in sorted(entry.value.items.items())
                for item in (Bytes(field), Bytes(value))
            )
            return ExecutionPlan(Items(values), expired, (key,))
        case cmd.HashIncrement(key, field, amount):
            previous, expired = lookup(database, key, now_ms)
            if previous is not None and not isinstance(previous.value, HashValue):
                return ExecutionPlan(WRONGTYPE)
            items = {} if previous is None else dict(previous.value.items)
            raw_old = items.get(field, b"0")
            try:
                old_value = parse_int64(raw_old)
            except CommandParseError:
                return _integer_failure()
            new_value = old_value + amount
            if not INT64_MIN <= new_value <= INT64_MAX:
                return _integer_failure()
            items[field] = str(new_value).encode("ascii")
            put = make_put(
                key,
                HashValue(items),
                previous,
                None if previous is None else previous.expire_at_ms,
            )
            return ExecutionPlan(Number(new_value), expired + (put,))
        case _:
            return None
