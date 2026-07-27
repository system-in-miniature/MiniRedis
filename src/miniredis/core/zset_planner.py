import math

from miniredis.commands import model as cmd
from miniredis.core.commit import DeleteKey, DeleteReason
from miniredis.core.database import Database
from miniredis.core.executor import ExecutionPlan
from miniredis.core.list_planner import inclusive_slice
from miniredis.core.planning import WRONGTYPE, lookup, make_put
from miniredis.core.reply import Bytes, Items, Number
from miniredis.core.values import ZSetValue


def _ordered(scores: dict[bytes, float]) -> list[tuple[bytes, float]]:
    return sorted(scores.items(), key=lambda item: (item[1], item[0]))


def _format_score(score: float) -> bytes:
    if math.isinf(score):
        return b"inf" if score > 0 else b"-inf"
    return repr(score).encode("ascii")


def _within(score: float, bound: cmd.ScoreBound, *, lower: bool) -> bool:
    if lower:
        return score >= bound.value if bound.inclusive else score > bound.value
    return score <= bound.value if bound.inclusive else score < bound.value


def plan_zset(
    command: cmd.Command,
    database: Database,
    now_ms: int,
) -> ExecutionPlan | None:
    match command:
        case cmd.ZAdd(key, pairs):
            previous, expired = lookup(database, key, now_ms)
            if previous is not None and not isinstance(previous.value, ZSetValue):
                return ExecutionPlan(WRONGTYPE)
            scores = {} if previous is None else dict(previous.value.scores)
            previously_present = set(scores)
            newly_added: set[bytes] = set()
            for score, member in pairs:
                if member not in previously_present:
                    newly_added.add(member)
                scores[member] = score
            put = make_put(
                key,
                ZSetValue(scores),
                previous,
                None if previous is None else previous.expire_at_ms,
            )
            return ExecutionPlan(
                Number(len(newly_added)),
                expired + (put,),
            )
        case cmd.ZRemove(key, members):
            previous, expired = lookup(database, key, now_ms)
            if previous is None:
                return ExecutionPlan(Number(0), expired)
            if not isinstance(previous.value, ZSetValue):
                return ExecutionPlan(WRONGTYPE)
            scores = dict(previous.value.scores)
            removed = 0
            for member in dict.fromkeys(members):
                if member in scores:
                    del scores[member]
                    removed += 1
            if removed == 0:
                return ExecutionPlan(Number(0), (), (key,))
            if scores:
                operation = make_put(
                    key,
                    ZSetValue(scores),
                    previous,
                    previous.expire_at_ms,
                )
            else:
                operation = DeleteKey(key, DeleteReason.CLIENT)
            return ExecutionPlan(Number(removed), expired + (operation,))
        case cmd.ZScore(key, member):
            entry, expired = lookup(database, key, now_ms)
            if entry is None:
                return ExecutionPlan(Bytes(None), expired)
            if not isinstance(entry.value, ZSetValue):
                return ExecutionPlan(WRONGTYPE)
            score = entry.value.scores.get(member)
            return ExecutionPlan(
                Bytes(None if score is None else _format_score(score)),
                expired,
                (key,),
            )
        case cmd.ZRank(key, member):
            entry, expired = lookup(database, key, now_ms)
            if entry is None:
                return ExecutionPlan(Bytes(None), expired)
            if not isinstance(entry.value, ZSetValue):
                return ExecutionPlan(WRONGTYPE)
            rank = next(
                (
                    index
                    for index, (candidate, _score) in enumerate(
                        _ordered(entry.value.scores)
                    )
                    if candidate == member
                ),
                None,
            )
            reply = Bytes(None) if rank is None else Number(rank)
            return ExecutionPlan(reply, expired, (key,))
        case cmd.ZRange(key, start, stop):
            entry, expired = lookup(database, key, now_ms)
            if entry is None:
                return ExecutionPlan(Items(()), expired)
            if not isinstance(entry.value, ZSetValue):
                return ExecutionPlan(WRONGTYPE)
            ordered = _ordered(entry.value.scores)
            begin, end = inclusive_slice(len(ordered), start, stop)
            return ExecutionPlan(
                Items(tuple(Bytes(member) for member, _score in ordered[begin:end])),
                expired,
                (key,),
            )
        case cmd.ZRangeByScore(key, minimum, maximum):
            entry, expired = lookup(database, key, now_ms)
            if entry is None:
                return ExecutionPlan(Items(()), expired)
            if not isinstance(entry.value, ZSetValue):
                return ExecutionPlan(WRONGTYPE)
            selected = tuple(
                Bytes(member)
                for member, score in _ordered(entry.value.scores)
                if _within(score, minimum, lower=True)
                and _within(score, maximum, lower=False)
            )
            return ExecutionPlan(Items(selected), expired, (key,))
        case _:
            return None
