"""Validate binary command requests and freeze options into typed commands."""

from __future__ import annotations

import math
import re
from decimal import (
    ROUND_CEILING,
    Decimal,
    InvalidOperation,
    Overflow as DecimalOverflow,
)
from typing import Literal

from miniredis.commands.model import (
    BlockingPop,
    CheckDecrement,
    Command,
    CompareDelete,
    Delete,
    Discard,
    Echo,
    Exists,
    Expire,
    Exec,
    GetString,
    HashDelete,
    HashGet,
    HashGetAll,
    HashIncrement,
    HashSet,
    Increment,
    ListPop,
    ListPush,
    ListRange,
    Multi,
    MultiGet,
    MultiSet,
    Persist,
    Ping,
    Publish,
    ScoreBound,
    SetAdd,
    SetIntersection,
    SetIsMember,
    SetMembers,
    SetRemove,
    SetString,
    Subscribe,
    TimeToLive,
    TypeOf,
    Unsubscribe,
    Unwatch,
    Watch,
    ZAdd,
    ZRange,
    ZRangeByScore,
    ZRank,
    ZRemove,
    ZScore,
)
from miniredis.commands.request import CommandRequest


class CommandParseError(ValueError):
    pass


INT64_MIN = -(2**63)
INT64_MAX = 2**63 - 1
_INTEGER = re.compile(rb"-?(?:0|[1-9][0-9]*)\Z")
_SCORE = re.compile(
    rb"-?(?:0|[1-9][0-9]*)(?:\.[0-9]+)?(?:[eE][+-]?(?:0|[1-9][0-9]*))?\Z"
)
_BLPOP_TIMEOUT = re.compile(
    rb"[+-]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)(?:[eE][+-]?[0-9]+)?"
)
_MAX_BLPOP_TIMEOUT_MS = (1 << 63) - 1


def parse_int64(value: bytes) -> int:
    if _INTEGER.fullmatch(value) is None or value == b"-0":
        raise CommandParseError("value is not an integer or out of range")
    if len(value.removeprefix(b"-")) > 19:
        raise CommandParseError("value is not an integer or out of range")
    try:
        number = int(value)
    except ValueError as error:
        raise CommandParseError("value is not an integer or out of range") from error
    if not INT64_MIN <= number <= INT64_MAX:
        raise CommandParseError("value is not an integer or out of range")
    return number


def parse_score(value: bytes) -> float:
    try:
        normalized = value.decode("ascii").lower()
    except UnicodeDecodeError as error:
        raise CommandParseError("value is not a valid score") from error
    if normalized in {"inf", "+inf", "-inf"}:
        return float(normalized)
    if _SCORE.fullmatch(value) is None:
        raise CommandParseError("value is not a valid score")
    number = float(normalized)
    if math.isnan(number) or math.isinf(number):
        raise CommandParseError("value is not a valid score")
    return number


def _parse_bound(value: bytes) -> ScoreBound:
    if not value:
        raise CommandParseError("value is not a valid score")
    exclusive = value.startswith(b"(")
    score = value[1:] if exclusive else value
    if not score:
        raise CommandParseError("value is not a valid score")
    return ScoreBound(parse_score(score), inclusive=not exclusive)


def _require_arity(name: bytes, args: tuple[bytes, ...], *allowed: int) -> None:
    if len(args) not in allowed:
        raise CommandParseError(
            f"wrong number of arguments for {name.decode('ascii', 'replace')}"
        )


def _require_min_arity(name: bytes, args: tuple[bytes, ...], minimum: int) -> None:
    if len(args) < minimum:
        raise CommandParseError(
            f"wrong number of arguments for {name.decode('ascii', 'replace')}"
        )


def _byte_pairs(values: tuple[bytes, ...]) -> tuple[tuple[bytes, bytes], ...]:
    return tuple(zip(values[::2], values[1::2], strict=True))


def _score_pairs(values: tuple[bytes, ...]) -> tuple[tuple[float, bytes], ...]:
    return tuple(
        (parse_score(score), member)
        for score, member in zip(values[::2], values[1::2], strict=True)
    )


def _parse_set(args: tuple[bytes, ...]) -> SetString:
    _require_min_arity(b"SET", args, 2)
    only_if: Literal["nx", "xx"] | None = None
    expire_ms: int | None = None
    index = 2
    while index < len(args):
        option = args[index].lower()
        if option in {b"nx", b"xx"}:
            requested: Literal["nx", "xx"] = "nx" if option == b"nx" else "xx"
            if only_if is not None:
                raise CommandParseError("conflicting SET condition")
            only_if = requested
            index += 1
        elif option in {b"ex", b"px"}:
            if expire_ms is not None or index + 1 >= len(args):
                raise CommandParseError("invalid SET expiration")
            duration = parse_int64(args[index + 1])
            if duration <= 0:
                raise CommandParseError("invalid SET expiration")
            if option == b"ex" and duration > INT64_MAX // 1000:
                raise CommandParseError("invalid SET expiration")
            expire_ms = duration * 1000 if option == b"ex" else duration
            index += 2
        else:
            raise CommandParseError("invalid SET option")
    return SetString(args[0], args[1], only_if=only_if, expire_ms=expire_ms)


def parse_request(request: CommandRequest) -> Command:
    name = request.name.upper()
    args = request.args
    match name:
        case b"PING":
            _require_arity(name, args, 0, 1)
            return Ping(args[0] if args else None)
        case b"ECHO":
            _require_arity(name, args, 1)
            return Echo(args[0])
        case b"MULTI":
            _require_arity(name, args, 0)
            return Multi()
        case b"EXEC":
            _require_arity(name, args, 0)
            return Exec()
        case b"DISCARD":
            _require_arity(name, args, 0)
            return Discard()
        case b"WATCH":
            _require_min_arity(name, args, 1)
            return Watch(args)
        case b"UNWATCH":
            _require_arity(name, args, 0)
            return Unwatch()
        case b"COMPAREDEL":
            _require_arity(name, args, 2)
            return CompareDelete(args[0], args[1])
        case b"CHECKDECR":
            _require_arity(name, args, 2)
            amount = parse_int64(args[1])
            if amount <= 0:
                raise CommandParseError("amount must be a positive integer")
            return CheckDecrement(args[0], amount)
        case b"DEL":
            _require_min_arity(name, args, 1)
            return Delete(args)
        case b"EXISTS":
            _require_min_arity(name, args, 1)
            return Exists(args)
        case b"TYPE":
            _require_arity(name, args, 1)
            return TypeOf(args[0])
        case b"GET":
            _require_arity(name, args, 1)
            return GetString(args[0])
        case b"MGET":
            _require_min_arity(name, args, 1)
            return MultiGet(args)
        case b"MSET":
            _require_min_arity(name, args, 2)
            if len(args) % 2 != 0:
                raise CommandParseError("wrong number of arguments for MSET")
            return MultiSet(_byte_pairs(args))
        case b"SET":
            return _parse_set(args)
        case b"INCR":
            _require_arity(name, args, 1)
            return Increment(args[0], 1)
        case b"DECR":
            _require_arity(name, args, 1)
            return Increment(args[0], -1)
        case b"INCRBY":
            _require_arity(name, args, 2)
            return Increment(args[0], parse_int64(args[1]))
        case b"HSET":
            _require_min_arity(name, args, 3)
            if len(args) % 2 == 0:
                raise CommandParseError("wrong number of arguments for HSET")
            return HashSet(args[0], _byte_pairs(args[1:]))
        case b"HGET":
            _require_arity(name, args, 2)
            return HashGet(args[0], args[1])
        case b"HDEL":
            _require_min_arity(name, args, 2)
            return HashDelete(args[0], args[1:])
        case b"HGETALL":
            _require_arity(name, args, 1)
            return HashGetAll(args[0])
        case b"HINCRBY":
            _require_arity(name, args, 3)
            return HashIncrement(args[0], args[1], parse_int64(args[2]))
        case b"LPUSH" | b"RPUSH":
            _require_min_arity(name, args, 2)
            return ListPush(args[0], args[1:], left=name == b"LPUSH")
        case b"LPOP" | b"RPOP":
            _require_arity(name, args, 1)
            return ListPop(args[0], left=name == b"LPOP")
        case b"LRANGE":
            _require_arity(name, args, 3)
            return ListRange(args[0], parse_int64(args[1]), parse_int64(args[2]))
        case b"BLPOP" | b"BRPOP":
            if len(args) < 2:
                raise CommandParseError("wrong number of arguments")
            raw_timeout = args[-1]
            if not _BLPOP_TIMEOUT.fullmatch(raw_timeout):
                raise CommandParseError("timeout is not a finite non-negative number")
            try:
                seconds = Decimal(raw_timeout.decode("ascii"))
            except (UnicodeDecodeError, InvalidOperation):
                raise CommandParseError(
                    "timeout is not a finite non-negative number"
                ) from None
            if not seconds.is_finite() or seconds < 0:
                raise CommandParseError("timeout is not a finite non-negative number")
            try:
                timeout_ms = seconds * Decimal(1000)
            except (InvalidOperation, DecimalOverflow):
                raise CommandParseError("timeout is out of range") from None
            if not timeout_ms.is_finite() or timeout_ms > _MAX_BLPOP_TIMEOUT_MS:
                raise CommandParseError("timeout is out of range")
            milliseconds = int(timeout_ms.to_integral_value(rounding=ROUND_CEILING))
            return BlockingPop(
                tuple(args[:-1]),
                milliseconds,
                left=name == b"BLPOP",
            )
        case b"SUBSCRIBE":
            if not args:
                raise CommandParseError("wrong number of arguments")
            return Subscribe(tuple(args))
        case b"UNSUBSCRIBE":
            return Unsubscribe(tuple(args))
        case b"PUBLISH":
            _require_arity(name, args, 2)
            return Publish(args[0], args[1])
        case b"SADD":
            _require_min_arity(name, args, 2)
            return SetAdd(args[0], args[1:])
        case b"SREM":
            _require_min_arity(name, args, 2)
            return SetRemove(args[0], args[1:])
        case b"SISMEMBER":
            _require_arity(name, args, 2)
            return SetIsMember(args[0], args[1])
        case b"SMEMBERS":
            _require_arity(name, args, 1)
            return SetMembers(args[0])
        case b"SINTER":
            _require_min_arity(name, args, 1)
            return SetIntersection(args)
        case b"ZADD":
            _require_min_arity(name, args, 3)
            if len(args) % 2 == 0:
                raise CommandParseError("wrong number of arguments for ZADD")
            return ZAdd(args[0], _score_pairs(args[1:]))
        case b"ZREM":
            _require_min_arity(name, args, 2)
            return ZRemove(args[0], args[1:])
        case b"ZSCORE":
            _require_arity(name, args, 2)
            return ZScore(args[0], args[1])
        case b"ZRANK":
            _require_arity(name, args, 2)
            return ZRank(args[0], args[1])
        case b"ZRANGE":
            _require_arity(name, args, 3)
            return ZRange(args[0], parse_int64(args[1]), parse_int64(args[2]))
        case b"ZRANGEBYSCORE":
            _require_arity(name, args, 3)
            return ZRangeByScore(args[0], _parse_bound(args[1]), _parse_bound(args[2]))
        case b"EXPIRE":
            _require_arity(name, args, 2)
            return Expire(args[0], parse_int64(args[1]))
        case b"TTL" | b"PTTL":
            _require_arity(name, args, 1)
            return TimeToLive(args[0], milliseconds=name == b"PTTL")
        case b"PERSIST":
            _require_arity(name, args, 1)
            return Persist(args[0])
        case _:
            raise CommandParseError("unknown command")


parse_command_request = parse_request
