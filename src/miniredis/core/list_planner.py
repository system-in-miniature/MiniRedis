"""Plan non-blocking list commands; waiter registration stays in the executor."""

from collections import deque

from miniredis.commands import model as cmd
from miniredis.core.commit import DeleteKey, DeleteReason
from miniredis.core.database import Database
from miniredis.core.executor import ExecutionPlan
from miniredis.core.planning import WRONGTYPE, lookup, make_put
from miniredis.core.reply import Bytes, Items, Number
from miniredis.core.values import ListValue


def inclusive_slice(length: int, start: int, stop: int) -> tuple[int, int]:
    if start < 0:
        start += length
    if stop < 0:
        stop += length
    start = max(start, 0)
    stop = min(stop, length - 1)
    if start >= length or stop < 0 or start > stop:
        return 0, 0
    return start, stop + 1


def plan_list(
    command: cmd.Command,
    database: Database,
    now_ms: int,
) -> ExecutionPlan | None:
    match command:
        case cmd.ListPush(key, values, left):
            previous, expired = lookup(database, key, now_ms)
            if previous is not None and not isinstance(previous.value, ListValue):
                return ExecutionPlan(WRONGTYPE)
            items = deque() if previous is None else deque(previous.value.items)
            if left:
                for value in values:
                    items.appendleft(value)
            else:
                items.extend(values)
            put = make_put(
                key,
                ListValue(items),
                previous,
                None if previous is None else previous.expire_at_ms,
            )
            return ExecutionPlan(Number(len(items)), expired + (put,))
        case cmd.ListPop(key, left):
            previous, expired = lookup(database, key, now_ms)
            if previous is None:
                return ExecutionPlan(Bytes(None), expired)
            if not isinstance(previous.value, ListValue):
                return ExecutionPlan(WRONGTYPE)
            items = deque(previous.value.items)
            value = items.popleft() if left else items.pop()
            if items:
                operation = make_put(
                    key,
                    ListValue(items),
                    previous,
                    previous.expire_at_ms,
                )
            else:
                operation = DeleteKey(key, DeleteReason.CLIENT)
            return ExecutionPlan(Bytes(value), expired + (operation,))
        case cmd.ListRange(key, start, stop):
            entry, expired = lookup(database, key, now_ms)
            if entry is None:
                return ExecutionPlan(Items(()), expired)
            if not isinstance(entry.value, ListValue):
                return ExecutionPlan(WRONGTYPE)
            begin, end = inclusive_slice(
                len(entry.value.items),
                start,
                stop,
            )
            selected = tuple(entry.value.items)[begin:end]
            return ExecutionPlan(
                Items(tuple(Bytes(value) for value in selected)),
                expired,
                (key,),
            )
        case _:
            return None
