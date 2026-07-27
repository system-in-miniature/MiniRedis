from miniredis.commands import model as cmd
from miniredis.core.commit import DeleteKey, DeleteReason
from miniredis.core.database import Database
from miniredis.core.executor import ExecutionPlan
from miniredis.core.planning import lookup, make_put
from miniredis.core.reply import Number


def plan_ttl(
    command: cmd.Command,
    database: Database,
    now_ms: int,
) -> ExecutionPlan | None:
    match command:
        case cmd.Expire(key, seconds):
            previous, expired = lookup(database, key, now_ms)
            if previous is None:
                return ExecutionPlan(Number(0), expired)
            if seconds <= 0:
                return ExecutionPlan(
                    Number(1),
                    expired + (DeleteKey(key, DeleteReason.CLIENT),),
                )
            put = make_put(
                key,
                previous.value,
                previous,
                now_ms + seconds * 1_000,
            )
            return ExecutionPlan(Number(1), expired + (put,))
        case cmd.TimeToLive(key, milliseconds):
            entry, expired = lookup(database, key, now_ms)
            if entry is None:
                return ExecutionPlan(Number(-2), expired)
            if entry.expire_at_ms is None:
                return ExecutionPlan(Number(-1), (), (key,))
            remaining_ms = entry.expire_at_ms - now_ms
            value = remaining_ms if milliseconds else (remaining_ms + 500) // 1_000
            return ExecutionPlan(Number(value), expired, (key,))
        case cmd.Persist(key):
            previous, expired = lookup(database, key, now_ms)
            if previous is None:
                return ExecutionPlan(Number(0), expired)
            if previous.expire_at_ms is None:
                return ExecutionPlan(Number(0), (), (key,))
            put = make_put(key, previous.value, previous, None)
            return ExecutionPlan(Number(1), expired + (put,))
        case _:
            return None
