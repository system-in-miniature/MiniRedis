from miniredis.commands import model as cmd
from miniredis.core.commit import (
    CommitOperation,
    DeleteKey,
    DeleteReason,
)
from miniredis.core.database import Database
from miniredis.core.executor import ExecutionPlan
from miniredis.core.planning import (
    WRONGTYPE,
    dedupe_operations,
    lookup,
    make_put,
)
from miniredis.core.reply import Bytes, Items, Number
from miniredis.core.values import SetValue


def plan_set(
    command: cmd.Command,
    database: Database,
    now_ms: int,
) -> ExecutionPlan | None:
    match command:
        case cmd.SetAdd(key, members):
            previous, expired = lookup(database, key, now_ms)
            if previous is not None and not isinstance(previous.value, SetValue):
                return ExecutionPlan(WRONGTYPE)
            items = set() if previous is None else set(previous.value.items)
            before = len(items)
            items.update(members)
            put = make_put(
                key,
                SetValue(items),
                previous,
                None if previous is None else previous.expire_at_ms,
            )
            return ExecutionPlan(
                Number(len(items) - before),
                expired + (put,),
            )
        case cmd.SetRemove(key, members):
            previous, expired = lookup(database, key, now_ms)
            if previous is None:
                return ExecutionPlan(Number(0), expired)
            if not isinstance(previous.value, SetValue):
                return ExecutionPlan(WRONGTYPE)
            items = set(previous.value.items)
            removed = 0
            for member in dict.fromkeys(members):
                if member in items:
                    items.remove(member)
                    removed += 1
            if removed == 0:
                return ExecutionPlan(Number(0), (), (key,))
            if items:
                operation = make_put(
                    key,
                    SetValue(items),
                    previous,
                    previous.expire_at_ms,
                )
            else:
                operation = DeleteKey(key, DeleteReason.CLIENT)
            return ExecutionPlan(Number(removed), expired + (operation,))
        case cmd.SetIsMember(key, member):
            entry, expired = lookup(database, key, now_ms)
            if entry is None:
                return ExecutionPlan(Number(0), expired)
            if not isinstance(entry.value, SetValue):
                return ExecutionPlan(WRONGTYPE)
            return ExecutionPlan(
                Number(int(member in entry.value.items)),
                expired,
                (key,),
            )
        case cmd.SetMembers(key):
            entry, expired = lookup(database, key, now_ms)
            if entry is None:
                return ExecutionPlan(Items(()), expired)
            if not isinstance(entry.value, SetValue):
                return ExecutionPlan(WRONGTYPE)
            return ExecutionPlan(
                Items(tuple(Bytes(member) for member in sorted(entry.value.items))),
                expired,
                (key,),
            )
        case cmd.SetIntersection(keys):
            operations: list[CommitOperation] = []
            sets: list[set[bytes] | None] = []
            touches: list[bytes] = []
            for key in keys:
                entry, expired = lookup(database, key, now_ms)
                operations.extend(expired)
                if entry is None:
                    sets.append(None)
                    continue
                if not isinstance(entry.value, SetValue):
                    return ExecutionPlan(WRONGTYPE)
                sets.append(set(entry.value.items))
                touches.append(key)
            if any(items is None for items in sets):
                intersection: set[bytes] = set()
            else:
                concrete = [items for items in sets if items is not None]
                intersection = set.intersection(*concrete)
            return ExecutionPlan(
                Items(tuple(Bytes(member) for member in sorted(intersection))),
                dedupe_operations(operations),
                tuple(touches),
            )
        case _:
            return None
