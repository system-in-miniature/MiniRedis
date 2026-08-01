# Stage 02 · 类型化命令与严格解析

### 目标

完整校验二进制请求，并把它冻结成封闭的类型化命令词汇。

??? note "交付文件"
    - `src/miniredis/commands/model.py`
    - `src/miniredis/commands/parser.py`
    - `tests/unit/commands/test_parser.py`

### 当前遇到的问题

Stage 01 能表示请求，却不能判断 `SET k v NX EX 1`、畸形整数或未知命令是否合法。如果把原始参数数组继续向下传，每个 Planner 都会重复实现 Arity、选项冲突、数值边界与二进制归一化。

### 测试契约

#### 先看会坏在哪里

解析契约提交完整非法选项集合 `SET k v NX XX`。如果直到规划开始后才拒绝第二个选项，下游会收到一个部分接受的命令。数值用例还使用非规范 `-0`、过大的有限 Score 与 Python 巨整数转换限制，要求在转换前完成有界校验。

??? note "文件差异：tests/unit/commands/test_parser.py"
    ```diff
    diff --git a/tests/unit/commands/test_parser.py b/tests/unit/commands/test_parser.py
    new file mode 100644
    index 0000000000000000000000000000000000000000..d0214580f53a32ddd1efcd64df7d9ae1450560f7
    --- /dev/null
    +++ b/tests/unit/commands/test_parser.py
    @@ -0,0 +1,199 @@
    +from __future__ import annotations
    +
    +import math
    +
    +import pytest
    +
    +from miniredis.commands.model import (
    +    Echo,
    +    Ping,
    +    SetString,
    +    TimeToLive,
    +    Exists,
    +    Increment,
    +    HashGetAll,
    +    ListPop,
    +    ListPush,
    +    SetMembers,
    +    ZRemove,
    +    ZRangeByScore,
    +)
    +from miniredis.commands.parser import CommandParseError, parse_command_request
    +from miniredis.commands.request import CommandRequest
    +
    +
    +def parse(name: bytes, *args: bytes):
    +    return parse_command_request(CommandRequest(name, args))
    +
    +
    +@pytest.mark.parametrize(
    +    ("command_request", "expected"),
    +    [
    +        (CommandRequest(b"PING"), Ping()),
    +        (CommandRequest(b"ping", (b"binary\x00message",)), Ping(b"binary\x00message")),
    +        (CommandRequest(b"ECHO", (b"\xff\x00",)), Echo(b"\xff\x00")),
    +    ],
    +)
    +def test_parse_ping_and_echo_binary_payloads(
    +    command_request: CommandRequest, expected: object
    +) -> None:
    +    assert parse_command_request(command_request) == expected
    +
    +
    +def test_parse_set_options_are_order_independent() -> None:
    +    assert parse(b"SET", b"k", b"v", b"PX", b"20", b"NX") == SetString(
    +        b"k", b"v", only_if="nx", expire_ms=20
    +    )
    +
    +
    +@pytest.mark.parametrize(
    +    "args",
    +    [
    +        (b"k", b"v", b"NX", b"XX"),
    +        (b"k", b"v", b"EX", b"1", b"PX", b"1"),
    +        (b"k", b"v", b"EX", b"0"),
    +        (b"k", b"v", b"PX", b"-1"),
    +        (b"k", b"v", b"UNKNOWN"),
    +    ],
    +)
    +def test_parse_set_rejects_invalid_entire_option_set(args: tuple[bytes, ...]) -> None:
    +    with pytest.raises(CommandParseError):
    +        parse(b"SET", *args)
    +
    +
    +@pytest.mark.parametrize(
    +    ("command_request", "expected"),
    +    [
    +        (CommandRequest(b"EXISTS", (b"a", b"a")), Exists((b"a", b"a"))),
    +        (CommandRequest(b"INCRBY", (b"a", b"2")), Increment(b"a", 2)),
    +        (CommandRequest(b"HGETALL", (b"h",)), HashGetAll(b"h")),
    +        (
    +            CommandRequest(b"LPUSH", (b"l", b"a", b"b")),
    +            ListPush(b"l", (b"a", b"b"), left=True),
    +        ),
    +        (CommandRequest(b"RPUSH", (b"l", b"a")), ListPush(b"l", (b"a",), left=False)),
    +        (CommandRequest(b"LPOP", (b"l",)), ListPop(b"l", left=True)),
    +        (CommandRequest(b"RPOP", (b"l",)), ListPop(b"l", left=False)),
    +        (CommandRequest(b"SMEMBERS", (b"s",)), SetMembers(b"s")),
    +        (CommandRequest(b"ZREM", (b"z", b"m")), ZRemove(b"z", (b"m",))),
    +        (CommandRequest(b"TTL", (b"key",)), TimeToLive(b"key", milliseconds=False)),
    +        (CommandRequest(b"PTTL", (b"key",)), TimeToLive(b"key", milliseconds=True)),
    +    ],
    +)
    +def test_parse_representative_commands_return_exact_typed_command(
    +    command_request: CommandRequest, expected: object
    +) -> None:
    +    assert parse_command_request(command_request) == expected
    +
    +
    +@pytest.mark.parametrize(
    +    "command_request",
    +    [
    +        CommandRequest(b"GET"),
    +        CommandRequest(b"HSET", (b"h", b"f")),
    +        CommandRequest(b"LRANGE", (b"l", b"0")),
    +        CommandRequest(b"SADD", (b"s",)),
    +        CommandRequest(b"ZADD", (b"z", b"1")),
    +        CommandRequest(b"ZADD", (b"z", b"1_0", b"m")),
    +        CommandRequest(b"ZRANGEBYSCORE", (b"z", b"Infinity", b"1")),
    +        CommandRequest(b"TTL", (b"key", b"extra")),
    +        CommandRequest(b"UNKNOWN"),
    +    ],
    +)
    +def test_parse_rejects_invalid_requests_before_planning(
    +    command_request: CommandRequest,
    +) -> None:
    +    with pytest.raises(CommandParseError):
    +        parse_command_request(command_request)
    +
    +
    +@pytest.mark.parametrize(
    +    "value",
    +    [
    +        b"0",
    +        b"-1",
    +        b"9223372036854775807",
    +        b"-9223372036854775808",
    +    ],
    +)
    +def test_parse_strict_integer_accepts_int64_extrema(value: bytes) -> None:
    +    assert parse(b"INCRBY", b"key", value) is not None
    +
    +
    +@pytest.mark.parametrize(
    +    "value",
    +    [
    +        b"-0",
    +        b"01",
    +        b"+1",
    +        b" 1",
    +        b"1 ",
    +        b"9223372036854775808",
    +        b"-9223372036854775809",
    +    ],
    +)
    +def test_parse_strict_integer_rejects_noncanonical_and_out_of_range(
    +    value: bytes,
    +) -> None:
    +    with pytest.raises(
    +        CommandParseError, match="value is not an integer or out of range"
    +    ):
    +        parse(b"INCRBY", b"key", value)
    +
    +
    +def test_parse_strict_integer_rejects_python_conversion_limit_before_int() -> None:
    +    with pytest.raises(
    +        CommandParseError, match="value is not an integer or out of range"
    +    ):
    +        parse(b"INCRBY", b"key", b"9" * 4301)
    +
    +
    +@pytest.mark.parametrize("value", [b"1", b"-1.5", b"1e2", b"inf", b"-inf", b"(1.5"])
    +def test_parse_score_and_score_bound_accept_canonical_forms(value: bytes) -> None:
    +    assert parse(b"ZRANGEBYSCORE", b"z", value, b"1") is not None
    +
    +
    +@pytest.mark.parametrize(
    +    ("bound", "inclusive"),
    +    [(b"+inf", True), (b"(+inf", False), (b"+INF", True)],
    +)
    +def test_parse_bound_accepts_positive_infinity(bound: bytes, inclusive: bool) -> None:
    +    command = parse(b"ZRANGEBYSCORE", b"z", bound, b"1")
    +
    +    assert isinstance(command, ZRangeByScore)
    +    assert math.isinf(command.minimum.value) and command.minimum.value > 0
    +    assert command.minimum.inclusive is inclusive
    +
    +
    +@pytest.mark.parametrize("value", [b"inf", b"+inf", b"-inf"])
    +def test_parse_score_accepts_exact_infinite_forms(value: bytes) -> None:
    +    assert parse(b"ZADD", b"z", value, b"member") is not None
    +
    +
    +@pytest.mark.parametrize("value", [b"1e999", b"-1e999"])
    +def test_parse_score_rejects_finite_form_overflow(value: bytes) -> None:
    +    with pytest.raises(CommandParseError, match="value is not a valid score"):
    +        parse(b"ZADD", b"z", value, b"member")
    +
    +
    +def test_parse_set_ex_normalizes_only_to_int64_milliseconds() -> None:
    +    maximum_seconds = (2**63 - 1) // 1000
    +    assert parse(
    +        b"SET", b"key", b"value", b"EX", str(maximum_seconds).encode()
    +    ) == SetString(b"key", b"value", expire_ms=maximum_seconds * 1000)
    +    with pytest.raises(CommandParseError):
    +        parse(b"SET", b"key", b"value", b"EX", str(maximum_seconds + 1).encode())
    +
    +
    +def test_parse_set_px_accepts_int64_maximum() -> None:
    +    assert parse(b"SET", b"key", b"value", b"PX", b"9223372036854775807") == SetString(
    +        b"key", b"value", expire_ms=2**63 - 1
    +    )
    +
    +
    +@pytest.mark.parametrize(
    +    "value", [b"Infinity", b"NaN", b"1_0", b" 1", b"+Infinity", b"\xff"]
    +)
    +def test_parse_score_rejects_noncanonical_forms(value: bytes) -> None:
    +    with pytest.raises(CommandParseError, match="value is not a valid score"):
    +        parse(b"ZRANGEBYSCORE", b"z", value, b"1")
    ```

**测试锁定什么**

锁定精确 Arity、完整选项集合校验、二进制 Payload 保留、规范整数/Score 语法与每个支持命令族的具体类型结果。

**如何构造反例**

参数化非法请求一次改变一个语法边界，并要求在 Planner 或 Database 出现以前抛出 `CommandParseError`。

**关键测试语句**

```python
with pytest.raises(CommandParseError):
    parse_request(CommandRequest(b"SET", args))
```

**失败意味着什么**

失败表示畸形输入能以看似合理的命令越过语义边界，迫使后续层猜测哪些部分已被接受。

### 基本概念

解析不是命令执行。它在校验完整请求以后，把传输无关 `CommandRequest` 转成一个不可变 `Command` 数据类。封闭 Union 使下游 Planner 可以穷尽处理，并携带已归一化的 Expiration Milliseconds 等值。

规范数值语法为同一个值只接受一种表示，防止下划线、前后空白、`-0` 或无界整数转换等 Python 便利静默扩大公开协议。

### 为什么需要这个机制

如果原始 Bytes 与选项 Flag 进入规划，校验就会与状态查找和变更交织。完整请求解析让非法输入无副作用，让 Direct 与 RESP2 共用规则，并让命令 Traits 按类型而不是命令名字符串判断。

### 运行时心智模型

Adapter 构造 `CommandRequest(name, args)`。`parse_request` 只大写命令名，检查 Arity 与所有选项，转换有界数值字段，再返回一个冻结数据类；任何非法字节序列都在进入状态规划前抛错。

### 机制板块

#### 封闭命令词汇与严格解析器

先完整校验二进制请求，再冻结成一个类型值，让后续规划无需重新解析选项。

??? note "文件差异：src/miniredis/commands/model.py"
    ```diff
    diff --git a/src/miniredis/commands/model.py b/src/miniredis/commands/model.py
    new file mode 100644
    index 0000000000000000000000000000000000000000..db978c7fbdac19c1bbe18ae254149a434b6d5df5
    --- /dev/null
    +++ b/src/miniredis/commands/model.py
    @@ -0,0 +1,221 @@
    +from __future__ import annotations
    +
    +from dataclasses import dataclass
    +from typing import Literal, TypeAlias
    +
    +
    +@dataclass(frozen=True, slots=True)
    +class Ping:
    +    message: bytes | None = None
    +
    +
    +@dataclass(frozen=True, slots=True)
    +class Echo:
    +    message: bytes
    +
    +
    +@dataclass(frozen=True, slots=True)
    +class SetString:
    +    key: bytes
    +    value: bytes
    +    only_if: Literal["nx", "xx"] | None = None
    +    expire_ms: int | None = None
    +
    +
    +@dataclass(frozen=True, slots=True)
    +class GetString:
    +    key: bytes
    +
    +
    +@dataclass(frozen=True, slots=True)
    +class Delete:
    +    keys: tuple[bytes, ...]
    +
    +
    +@dataclass(frozen=True, slots=True)
    +class Exists:
    +    keys: tuple[bytes, ...]
    +
    +
    +@dataclass(frozen=True, slots=True)
    +class TypeOf:
    +    key: bytes
    +
    +
    +@dataclass(frozen=True, slots=True)
    +class Increment:
    +    key: bytes
    +    amount: int
    +
    +
    +@dataclass(frozen=True, slots=True)
    +class HashSet:
    +    key: bytes
    +    pairs: tuple[tuple[bytes, bytes], ...]
    +
    +
    +@dataclass(frozen=True, slots=True)
    +class HashGet:
    +    key: bytes
    +    field: bytes
    +
    +
    +@dataclass(frozen=True, slots=True)
    +class HashDelete:
    +    key: bytes
    +    fields: tuple[bytes, ...]
    +
    +
    +@dataclass(frozen=True, slots=True)
    +class HashGetAll:
    +    key: bytes
    +
    +
    +@dataclass(frozen=True, slots=True)
    +class HashIncrement:
    +    key: bytes
    +    field: bytes
    +    amount: int
    +
    +
    +@dataclass(frozen=True, slots=True)
    +class ListPush:
    +    key: bytes
    +    values: tuple[bytes, ...]
    +    left: bool
    +
    +
    +@dataclass(frozen=True, slots=True)
    +class ListPop:
    +    key: bytes
    +    left: bool
    +
    +
    +@dataclass(frozen=True, slots=True)
    +class ListRange:
    +    key: bytes
    +    start: int
    +    stop: int
    +
    +
    +@dataclass(frozen=True, slots=True)
    +class SetAdd:
    +    key: bytes
    +    members: tuple[bytes, ...]
    +
    +
    +@dataclass(frozen=True, slots=True)
    +class SetRemove:
    +    key: bytes
    +    members: tuple[bytes, ...]
    +
    +
    +@dataclass(frozen=True, slots=True)
    +class SetIsMember:
    +    key: bytes
    +    member: bytes
    +
    +
    +@dataclass(frozen=True, slots=True)
    +class SetMembers:
    +    key: bytes
    +
    +
    +@dataclass(frozen=True, slots=True)
    +class SetIntersection:
    +    keys: tuple[bytes, ...]
    +
    +
    +@dataclass(frozen=True, slots=True)
    +class ScoreBound:
    +    value: float
    +    inclusive: bool
    +
    +
    +@dataclass(frozen=True, slots=True)
    +class ZAdd:
    +    key: bytes
    +    pairs: tuple[tuple[float, bytes], ...]
    +
    +
    +@dataclass(frozen=True, slots=True)
    +class ZRemove:
    +    key: bytes
    +    members: tuple[bytes, ...]
    +
    +
    +@dataclass(frozen=True, slots=True)
    +class ZScore:
    +    key: bytes
    +    member: bytes
    +
    +
    +@dataclass(frozen=True, slots=True)
    +class ZRank:
    +    key: bytes
    +    member: bytes
    +
    +
    +@dataclass(frozen=True, slots=True)
    +class ZRange:
    +    key: bytes
    +    start: int
    +    stop: int
    +
    +
    +@dataclass(frozen=True, slots=True)
    +class ZRangeByScore:
    +    key: bytes
    +    minimum: ScoreBound
    +    maximum: ScoreBound
    +
    +
    +@dataclass(frozen=True, slots=True)
    +class Expire:
    +    key: bytes
    +    seconds: int
    +
    +
    +@dataclass(frozen=True, slots=True)
    +class TimeToLive:
    +    key: bytes
    +    milliseconds: bool
    +
    +
    +@dataclass(frozen=True, slots=True)
    +class Persist:
    +    key: bytes
    +
    +
    +Command: TypeAlias = (
    +    Ping
    +    | Echo
    +    | SetString
    +    | GetString
    +    | Delete
    +    | Exists
    +    | TypeOf
    +    | Increment
    +    | HashSet
    +    | HashGet
    +    | HashDelete
    +    | HashGetAll
    +    | HashIncrement
    +    | ListPush
    +    | ListPop
    +    | ListRange
    +    | SetAdd
    +    | SetRemove
    +    | SetIsMember
    +    | SetMembers
    +    | SetIntersection
    +    | ZAdd
    +    | ZRemove
    +    | ZScore
    +    | ZRank
    +    | ZRange
    +    | ZRangeByScore
    +    | Expire
    +    | TimeToLive
    +    | Persist
    +)
    ```

**是什么，为什么现在需要**

Model 是由 `SetString`、`HashSet`、`ZRangeByScore` 等不可变命令值组成的封闭词汇。

**在运行时做什么**

Planner 对这些类型做模式匹配，接收已归一化选项，不再查看原始参数位置。

**关键代码**

```python
class SetString:
    key: bytes
    value: bytes
    only_if: Literal["nx", "xx"] | None = None
    expire_ms: int | None = None
```

**关键语句理解**

类型只能表示一个条件与一个归一化时长；互相冲突的原始选项没有合法构造状态。

??? note "文件差异：src/miniredis/commands/parser.py"
    ```diff
    diff --git a/src/miniredis/commands/parser.py b/src/miniredis/commands/parser.py
    new file mode 100644
    index 0000000000000000000000000000000000000000..aaeb44009f85d5b6256d323d4c23a3ddd22072c7
    --- /dev/null
    +++ b/src/miniredis/commands/parser.py
    @@ -0,0 +1,249 @@
    +from __future__ import annotations
    +
    +import math
    +import re
    +from typing import Literal
    +
    +from miniredis.commands.model import (
    +    Command,
    +    Delete,
    +    Echo,
    +    Exists,
    +    Expire,
    +    GetString,
    +    HashDelete,
    +    HashGet,
    +    HashGetAll,
    +    HashIncrement,
    +    HashSet,
    +    Increment,
    +    ListPop,
    +    ListPush,
    +    ListRange,
    +    Persist,
    +    Ping,
    +    ScoreBound,
    +    SetAdd,
    +    SetIntersection,
    +    SetIsMember,
    +    SetMembers,
    +    SetRemove,
    +    SetString,
    +    TimeToLive,
    +    TypeOf,
    +    ZAdd,
    +    ZRange,
    +    ZRangeByScore,
    +    ZRank,
    +    ZRemove,
    +    ZScore,
    +)
    +from miniredis.commands.request import CommandRequest
    +
    +
    +class CommandParseError(ValueError):
    +    pass
    +
    +
    +INT64_MIN = -(2**63)
    +INT64_MAX = 2**63 - 1
    +_INTEGER = re.compile(rb"-?(?:0|[1-9][0-9]*)\Z")
    +_SCORE = re.compile(
    +    rb"-?(?:0|[1-9][0-9]*)(?:\.[0-9]+)?(?:[eE][+-]?(?:0|[1-9][0-9]*))?\Z"
    +)
    +
    +
    +def parse_int64(value: bytes) -> int:
    +    if _INTEGER.fullmatch(value) is None or value == b"-0":
    +        raise CommandParseError("value is not an integer or out of range")
    +    if len(value.removeprefix(b"-")) > 19:
    +        raise CommandParseError("value is not an integer or out of range")
    +    try:
    +        number = int(value)
    +    except ValueError as error:
    +        raise CommandParseError("value is not an integer or out of range") from error
    +    if not INT64_MIN <= number <= INT64_MAX:
    +        raise CommandParseError("value is not an integer or out of range")
    +    return number
    +
    +
    +def parse_score(value: bytes) -> float:
    +    try:
    +        normalized = value.decode("ascii").lower()
    +    except UnicodeDecodeError as error:
    +        raise CommandParseError("value is not a valid score") from error
    +    if normalized in {"inf", "+inf", "-inf"}:
    +        return float(normalized)
    +    if _SCORE.fullmatch(value) is None:
    +        raise CommandParseError("value is not a valid score")
    +    number = float(normalized)
    +    if math.isnan(number) or math.isinf(number):
    +        raise CommandParseError("value is not a valid score")
    +    return number
    +
    +
    +def _parse_bound(value: bytes) -> ScoreBound:
    +    if not value:
    +        raise CommandParseError("value is not a valid score")
    +    exclusive = value.startswith(b"(")
    +    score = value[1:] if exclusive else value
    +    if not score:
    +        raise CommandParseError("value is not a valid score")
    +    return ScoreBound(parse_score(score), inclusive=not exclusive)
    +
    +
    +def _require_arity(name: bytes, args: tuple[bytes, ...], *allowed: int) -> None:
    +    if len(args) not in allowed:
    +        raise CommandParseError(
    +            f"wrong number of arguments for {name.decode('ascii', 'replace')}"
    +        )
    +
    +
    +def _require_min_arity(name: bytes, args: tuple[bytes, ...], minimum: int) -> None:
    +    if len(args) < minimum:
    +        raise CommandParseError(
    +            f"wrong number of arguments for {name.decode('ascii', 'replace')}"
    +        )
    +
    +
    +def _byte_pairs(values: tuple[bytes, ...]) -> tuple[tuple[bytes, bytes], ...]:
    +    return tuple(zip(values[::2], values[1::2], strict=True))
    +
    +
    +def _score_pairs(values: tuple[bytes, ...]) -> tuple[tuple[float, bytes], ...]:
    +    return tuple(
    +        (parse_score(score), member)
    +        for score, member in zip(values[::2], values[1::2], strict=True)
    +    )
    +
    +
    +def _parse_set(args: tuple[bytes, ...]) -> SetString:
    +    _require_min_arity(b"SET", args, 2)
    +    only_if: Literal["nx", "xx"] | None = None
    +    expire_ms: int | None = None
    +    index = 2
    +    while index < len(args):
    +        option = args[index].lower()
    +        if option in {b"nx", b"xx"}:
    +            requested: Literal["nx", "xx"] = "nx" if option == b"nx" else "xx"
    +            if only_if is not None:
    +                raise CommandParseError("conflicting SET condition")
    +            only_if = requested
    +            index += 1
    +        elif option in {b"ex", b"px"}:
    +            if expire_ms is not None or index + 1 >= len(args):
    +                raise CommandParseError("invalid SET expiration")
    +            duration = parse_int64(args[index + 1])
    +            if duration <= 0:
    +                raise CommandParseError("invalid SET expiration")
    +            if option == b"ex" and duration > INT64_MAX // 1000:
    +                raise CommandParseError("invalid SET expiration")
    +            expire_ms = duration * 1000 if option == b"ex" else duration
    +            index += 2
    +        else:
    +            raise CommandParseError("invalid SET option")
    +    return SetString(args[0], args[1], only_if=only_if, expire_ms=expire_ms)
    +
    +
    +def parse_command_request(request: CommandRequest) -> Command:
    +    name = request.name.upper()
    +    args = request.args
    +    match name:
    +        case b"PING":
    +            _require_arity(name, args, 0, 1)
    +            return Ping(args[0] if args else None)
    +        case b"ECHO":
    +            _require_arity(name, args, 1)
    +            return Echo(args[0])
    +        case b"DEL":
    +            _require_min_arity(name, args, 1)
    +            return Delete(args)
    +        case b"EXISTS":
    +            _require_min_arity(name, args, 1)
    +            return Exists(args)
    +        case b"TYPE":
    +            _require_arity(name, args, 1)
    +            return TypeOf(args[0])
    +        case b"GET":
    +            _require_arity(name, args, 1)
    +            return GetString(args[0])
    +        case b"SET":
    +            return _parse_set(args)
    +        case b"INCR":
    +            _require_arity(name, args, 1)
    +            return Increment(args[0], 1)
    +        case b"INCRBY":
    +            _require_arity(name, args, 2)
    +            return Increment(args[0], parse_int64(args[1]))
    +        case b"HSET":
    +            _require_min_arity(name, args, 3)
    +            if len(args) % 2 == 0:
    +                raise CommandParseError("wrong number of arguments for HSET")
    +            return HashSet(args[0], _byte_pairs(args[1:]))
    +        case b"HGET":
    +            _require_arity(name, args, 2)
    +            return HashGet(args[0], args[1])
    +        case b"HDEL":
    +            _require_min_arity(name, args, 2)
    +            return HashDelete(args[0], args[1:])
    +        case b"HGETALL":
    +            _require_arity(name, args, 1)
    +            return HashGetAll(args[0])
    +        case b"HINCRBY":
    +            _require_arity(name, args, 3)
    +            return HashIncrement(args[0], args[1], parse_int64(args[2]))
    +        case b"LPUSH" | b"RPUSH":
    +            _require_min_arity(name, args, 2)
    +            return ListPush(args[0], args[1:], left=name == b"LPUSH")
    +        case b"LPOP" | b"RPOP":
    +            _require_arity(name, args, 1)
    +            return ListPop(args[0], left=name == b"LPOP")
    +        case b"LRANGE":
    +            _require_arity(name, args, 3)
    +            return ListRange(args[0], parse_int64(args[1]), parse_int64(args[2]))
    +        case b"SADD":
    +            _require_min_arity(name, args, 2)
    +            return SetAdd(args[0], args[1:])
    +        case b"SREM":
    +            _require_min_arity(name, args, 2)
    +            return SetRemove(args[0], args[1:])
    +        case b"SISMEMBER":
    +            _require_arity(name, args, 2)
    +            return SetIsMember(args[0], args[1])
    +        case b"SMEMBERS":
    +            _require_arity(name, args, 1)
    +            return SetMembers(args[0])
    +        case b"SINTER":
    +            _require_min_arity(name, args, 1)
    +            return SetIntersection(args)
    +        case b"ZADD":
    +            _require_min_arity(name, args, 3)
    +            if len(args) % 2 == 0:
    +                raise CommandParseError("wrong number of arguments for ZADD")
    +            return ZAdd(args[0], _score_pairs(args[1:]))
    +        case b"ZREM":
    +            _require_min_arity(name, args, 2)
    +            return ZRemove(args[0], args[1:])
    +        case b"ZSCORE":
    +            _require_arity(name, args, 2)
    +            return ZScore(args[0], args[1])
    +        case b"ZRANK":
    +            _require_arity(name, args, 2)
    +            return ZRank(args[0], args[1])
    +        case b"ZRANGE":
    +            _require_arity(name, args, 3)
    +            return ZRange(args[0], parse_int64(args[1]), parse_int64(args[2]))
    +        case b"ZRANGEBYSCORE":
    +            _require_arity(name, args, 3)
    +            return ZRangeByScore(args[0], _parse_bound(args[1]), _parse_bound(args[2]))
    +        case b"EXPIRE":
    +            _require_arity(name, args, 2)
    +            return Expire(args[0], parse_int64(args[1]))
    +        case b"TTL" | b"PTTL":
    +            _require_arity(name, args, 1)
    +            return TimeToLive(args[0], milliseconds=name == b"PTTL")
    +        case b"PERSIST":
    +            _require_arity(name, args, 1)
    +            return Persist(args[0])
    +        case _:
    +            raise CommandParseError("unknown command")
    ```

**是什么，为什么现在需要**

Parser 拥有公开语法、Arity、选项冲突与有界数值转换。

**在运行时做什么**

它把一个完整请求映射为一个类型化命令，或在不触碰 Runtime 状态时抛出 `CommandParseError`。

**关键代码**

```python
if not INT64_MIN <= number <= INT64_MAX:
    raise CommandParseError("value is not an integer or out of range")
```

**关键语句理解**

Python 转换成功并不够；协议的有符号 64 位边界仍是显式契约。

### 验证证据

运行 `uv run pytest -q $(cat journey/stages/02-typed-commands/tests.txt)`。它证明语法到类型的行为，不证明 Planner Reply 或串行执行。

### 需要真正记住的内容

请求保持二进制；解析在访问状态前校验完整请求；类型化命令携带归一化含义；协议数值边界显式定义，而不是继承 Python。

### 用自己的话讲清楚

Parser 是语义防火墙：Adapter 提供 Bytes，它要么产生一个完全合法的类型化命令，要么什么都不越过边界。后续每层无需重新解析公开语法即可推理命令含义。

### 教材

[第 2 章](https://github.com/system-in-miniature/mini-redis/blob/main/docs/zh/tutorial/02-command-life.md)

[在 GitHub 查看阶段差异](https://github.com/system-in-miniature/mini-redis/compare/f68b061...67f0d73)

完成后可运行 `python -m journey.tools.build_journey check 2` 验收学习工作区。

[Complete reference patch / 完整参考补丁](https://github.com/system-in-miniature/mini-redis/blob/main/journey/stages/02-typed-commands/stage.patch)
