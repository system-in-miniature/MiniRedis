from __future__ import annotations

import math

import pytest

from miniredis.commands.model import (
    Echo,
    Ping,
    SetString,
    TimeToLive,
    Exists,
    Increment,
    HashGetAll,
    ListPop,
    ListPush,
    SetMembers,
    ZRemove,
    ZRangeByScore,
)
from miniredis.commands.parser import CommandParseError, parse_command_request
from miniredis.commands.request import CommandRequest


def parse(name: bytes, *args: bytes):
    return parse_command_request(CommandRequest(name, args))


@pytest.mark.parametrize(
    ("command_request", "expected"),
    [
        (CommandRequest(b"PING"), Ping()),
        (CommandRequest(b"ping", (b"binary\x00message",)), Ping(b"binary\x00message")),
        (CommandRequest(b"ECHO", (b"\xff\x00",)), Echo(b"\xff\x00")),
    ],
)
def test_parse_ping_and_echo_binary_payloads(
    command_request: CommandRequest, expected: object
) -> None:
    assert parse_command_request(command_request) == expected


def test_parse_set_options_are_order_independent() -> None:
    assert parse(b"SET", b"k", b"v", b"PX", b"20", b"NX") == SetString(
        b"k", b"v", only_if="nx", expire_ms=20
    )


@pytest.mark.parametrize(
    "args",
    [
        (b"k", b"v", b"NX", b"XX"),
        (b"k", b"v", b"EX", b"1", b"PX", b"1"),
        (b"k", b"v", b"EX", b"0"),
        (b"k", b"v", b"PX", b"-1"),
        (b"k", b"v", b"UNKNOWN"),
    ],
)
def test_parse_set_rejects_invalid_entire_option_set(args: tuple[bytes, ...]) -> None:
    with pytest.raises(CommandParseError):
        parse(b"SET", *args)


@pytest.mark.parametrize(
    ("command_request", "expected"),
    [
        (CommandRequest(b"EXISTS", (b"a", b"a")), Exists((b"a", b"a"))),
        (CommandRequest(b"INCRBY", (b"a", b"2")), Increment(b"a", 2)),
        (CommandRequest(b"HGETALL", (b"h",)), HashGetAll(b"h")),
        (
            CommandRequest(b"LPUSH", (b"l", b"a", b"b")),
            ListPush(b"l", (b"a", b"b"), left=True),
        ),
        (CommandRequest(b"RPUSH", (b"l", b"a")), ListPush(b"l", (b"a",), left=False)),
        (CommandRequest(b"LPOP", (b"l",)), ListPop(b"l", left=True)),
        (CommandRequest(b"RPOP", (b"l",)), ListPop(b"l", left=False)),
        (CommandRequest(b"SMEMBERS", (b"s",)), SetMembers(b"s")),
        (CommandRequest(b"ZREM", (b"z", b"m")), ZRemove(b"z", (b"m",))),
        (CommandRequest(b"TTL", (b"key",)), TimeToLive(b"key", milliseconds=False)),
        (CommandRequest(b"PTTL", (b"key",)), TimeToLive(b"key", milliseconds=True)),
    ],
)
def test_parse_representative_commands_return_exact_typed_command(
    command_request: CommandRequest, expected: object
) -> None:
    assert parse_command_request(command_request) == expected


@pytest.mark.parametrize(
    "command_request",
    [
        CommandRequest(b"GET"),
        CommandRequest(b"HSET", (b"h", b"f")),
        CommandRequest(b"LRANGE", (b"l", b"0")),
        CommandRequest(b"SADD", (b"s",)),
        CommandRequest(b"ZADD", (b"z", b"1")),
        CommandRequest(b"ZADD", (b"z", b"1_0", b"m")),
        CommandRequest(b"ZRANGEBYSCORE", (b"z", b"Infinity", b"1")),
        CommandRequest(b"TTL", (b"key", b"extra")),
        CommandRequest(b"UNKNOWN"),
    ],
)
def test_parse_rejects_invalid_requests_before_planning(
    command_request: CommandRequest,
) -> None:
    with pytest.raises(CommandParseError):
        parse_command_request(command_request)


@pytest.mark.parametrize(
    "value",
    [
        b"0",
        b"-1",
        b"9223372036854775807",
        b"-9223372036854775808",
    ],
)
def test_parse_strict_integer_accepts_int64_extrema(value: bytes) -> None:
    assert parse(b"INCRBY", b"key", value) is not None


@pytest.mark.parametrize(
    "value",
    [
        b"-0",
        b"01",
        b"+1",
        b" 1",
        b"1 ",
        b"9223372036854775808",
        b"-9223372036854775809",
    ],
)
def test_parse_strict_integer_rejects_noncanonical_and_out_of_range(
    value: bytes,
) -> None:
    with pytest.raises(
        CommandParseError, match="value is not an integer or out of range"
    ):
        parse(b"INCRBY", b"key", value)


def test_parse_strict_integer_rejects_python_conversion_limit_before_int() -> None:
    with pytest.raises(
        CommandParseError, match="value is not an integer or out of range"
    ):
        parse(b"INCRBY", b"key", b"9" * 4301)


@pytest.mark.parametrize("value", [b"1", b"-1.5", b"1e2", b"inf", b"-inf", b"(1.5"])
def test_parse_score_and_score_bound_accept_canonical_forms(value: bytes) -> None:
    assert parse(b"ZRANGEBYSCORE", b"z", value, b"1") is not None


@pytest.mark.parametrize(
    ("bound", "inclusive"),
    [(b"+inf", True), (b"(+inf", False), (b"+INF", True)],
)
def test_parse_bound_accepts_positive_infinity(bound: bytes, inclusive: bool) -> None:
    command = parse(b"ZRANGEBYSCORE", b"z", bound, b"1")

    assert isinstance(command, ZRangeByScore)
    assert math.isinf(command.minimum.value) and command.minimum.value > 0
    assert command.minimum.inclusive is inclusive


@pytest.mark.parametrize("value", [b"inf", b"+inf", b"-inf"])
def test_parse_score_accepts_exact_infinite_forms(value: bytes) -> None:
    assert parse(b"ZADD", b"z", value, b"member") is not None


@pytest.mark.parametrize("value", [b"1e999", b"-1e999"])
def test_parse_score_rejects_finite_form_overflow(value: bytes) -> None:
    with pytest.raises(CommandParseError, match="value is not a valid score"):
        parse(b"ZADD", b"z", value, b"member")


def test_parse_set_ex_normalizes_only_to_int64_milliseconds() -> None:
    maximum_seconds = (2**63 - 1) // 1000
    assert parse(
        b"SET", b"key", b"value", b"EX", str(maximum_seconds).encode()
    ) == SetString(b"key", b"value", expire_ms=maximum_seconds * 1000)
    with pytest.raises(CommandParseError):
        parse(b"SET", b"key", b"value", b"EX", str(maximum_seconds + 1).encode())


def test_parse_set_px_accepts_int64_maximum() -> None:
    assert parse(b"SET", b"key", b"value", b"PX", b"9223372036854775807") == SetString(
        b"key", b"value", expire_ms=2**63 - 1
    )


@pytest.mark.parametrize(
    "value", [b"Infinity", b"NaN", b"1_0", b" 1", b"+Infinity", b"\xff"]
)
def test_parse_score_rejects_noncanonical_forms(value: bytes) -> None:
    with pytest.raises(CommandParseError, match="value is not a valid score"):
        parse(b"ZRANGEBYSCORE", b"z", value, b"1")
