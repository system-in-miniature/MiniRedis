# Stage 02 · Typed commands and strict parsing / 类型化命令与严格解析

<!-- journey: chapter=2 tests_added=31 -->

## English

### Goal

Validate complete binary requests and freeze them into a closed typed-command vocabulary.

### Deliverable files

- `src/miniredis/commands/model.py`
- `src/miniredis/commands/parser.py`
- `tests/unit/commands/test_parser.py`

### The problem at this point

Stage 01 can represent a request but cannot tell whether `SET k v NX EX 1`, a malformed integer, or an unknown command is legal. Passing raw argument arrays deeper would make every planner reimplement arity, option conflicts, numeric bounds, and binary normalization.

### Failure preview

The parser contract presents the complete invalid option set `SET k v NX XX`. Rejecting only the second option after planning starts would leave downstream code with a partially accepted command. Numeric cases also use non-canonical `-0`, oversized finite scores, and Python's huge-integer conversion limit to ensure validation is bounded before conversion.

### Test contract

<!-- journey-file: tests/unit/commands/test_parser.py -->
#### `tests/unit/commands/test_parser.py`

##### What this test locks

It locks exact arity, whole-option-set validation, binary payload preservation, canonical integer and score syntax, and one concrete typed result for each supported command family.

##### How it constructs the counterexample

Parameterized invalid requests vary one syntax boundary at a time and require `CommandParseError` before any planner or database exists.

##### Key test statement

```python
with pytest.raises(CommandParseError):
    parse_request(CommandRequest(b"SET", args))
```

##### What a failure means

A failure means malformed input can cross the semantic boundary as a plausible command, forcing later layers to guess which parts were accepted.

### Basic concepts

Parsing is not command execution. It converts a transport-neutral `CommandRequest` into one immutable `Command` dataclass after validating the entire request. A closed union makes the downstream planner exhaustive and carries normalized values such as expiration milliseconds.

Canonical numeric syntax accepts one representation for the same value. That prevents Python conveniences such as underscores, surrounding whitespace, `-0`, or unbounded integer conversion from silently widening the public protocol.

### Why this mechanism is necessary

If raw bytes and option flags survive into planning, validation becomes interleaved with state lookup and mutation. Whole-request parsing keeps invalid input side-effect free, lets Direct and RESP2 share the same rules, and makes command traits inspectable by type rather than by command-name strings.

### Runtime mental model

An adapter creates `CommandRequest(name, args)`. `parse_request` uppercases only the command name, checks arity and every option, converts bounded numeric fields, and returns one frozen dataclass. On any invalid byte sequence it raises before the request can enter state planning.

### Mechanism blocks

<!-- journey-file: src/miniredis/commands/model.py -->
#### `src/miniredis/commands/model.py`

##### What it is and why it appears

The model is a closed vocabulary of immutable command values such as `SetString`, `HashSet`, and `ZRangeByScore`.

##### Runtime role

Planners pattern-match on these types and receive already-normalized options instead of revisiting raw argument positions.

##### Key code

```python
class SetString:
    key: bytes
    value: bytes
    only_if: Literal["nx", "xx"] | None = None
    expire_ms: int | None = None
```

##### Statement understanding

The type can represent only one condition and one normalized duration; conflicting raw options have no valid constructed state.

<!-- journey-file: src/miniredis/commands/parser.py -->
#### `src/miniredis/commands/parser.py`

##### What it is and why it appears

The parser owns public syntax, arity, option conflicts, and bounded numeric conversion.

##### Runtime role

It maps one complete request to one typed command or raises `CommandParseError` without touching runtime state.

##### Key code

```python
if not INT64_MIN <= number <= INT64_MAX:
    raise CommandParseError("value is not an integer or out of range")
```

##### Statement understanding

Successful Python conversion is not enough; the protocol's signed 64-bit boundary remains an explicit contract.

### Verification evidence

Run `uv run pytest -q $(cat journey/stages/02-typed-commands/tests.txt)`. It proves syntax-to-type behavior, not planner replies or serialized execution.

### Durable takeaways

Requests stay binary. Parsing validates the whole request before state access. Typed commands carry normalized meaning, and protocol numeric bounds are explicit rather than inherited from Python.

### Explain it in your own words

The parser is a semantic firewall: adapters provide bytes, it either produces one fully valid typed command or nothing crosses the boundary. Every later layer can reason about command meaning without reparsing public syntax.

### Textbook

[Chapter 2](https://github.com/system-in-miniature/mini-redis/blob/main/docs/tutorial/02-command-life.md)

## 中文

### 目标

完整校验二进制请求，并把它冻结成封闭的类型化命令词汇。

### 交付文件

- `src/miniredis/commands/model.py`
- `src/miniredis/commands/parser.py`
- `tests/unit/commands/test_parser.py`

### 当前遇到的问题

Stage 01 能表示请求，却不能判断 `SET k v NX EX 1`、畸形整数或未知命令是否合法。如果把原始参数数组继续向下传，每个 Planner 都会重复实现 Arity、选项冲突、数值边界与二进制归一化。

### 先看会坏在哪里

解析契约提交完整非法选项集合 `SET k v NX XX`。如果直到规划开始后才拒绝第二个选项，下游会收到一个部分接受的命令。数值用例还使用非规范 `-0`、过大的有限 Score 与 Python 巨整数转换限制，要求在转换前完成有界校验。

### 测试契约

<!-- journey-file: tests/unit/commands/test_parser.py -->
#### `tests/unit/commands/test_parser.py`

##### 测试锁定什么

锁定精确 Arity、完整选项集合校验、二进制 Payload 保留、规范整数/Score 语法与每个支持命令族的具体类型结果。

##### 如何构造反例

参数化非法请求一次改变一个语法边界，并要求在 Planner 或 Database 出现以前抛出 `CommandParseError`。

##### 关键测试语句

```python
with pytest.raises(CommandParseError):
    parse_request(CommandRequest(b"SET", args))
```

##### 失败意味着什么

失败表示畸形输入能以看似合理的命令越过语义边界，迫使后续层猜测哪些部分已被接受。

### 基本概念

解析不是命令执行。它在校验完整请求以后，把传输无关 `CommandRequest` 转成一个不可变 `Command` 数据类。封闭 Union 使下游 Planner 可以穷尽处理，并携带已归一化的 Expiration Milliseconds 等值。

规范数值语法为同一个值只接受一种表示，防止下划线、前后空白、`-0` 或无界整数转换等 Python 便利静默扩大公开协议。

### 为什么需要这个机制

如果原始 Bytes 与选项 Flag 进入规划，校验就会与状态查找和变更交织。完整请求解析让非法输入无副作用，让 Direct 与 RESP2 共用规则，并让命令 Traits 按类型而不是命令名字符串判断。

### 运行时心智模型

Adapter 构造 `CommandRequest(name, args)`。`parse_request` 只大写命令名，检查 Arity 与所有选项，转换有界数值字段，再返回一个冻结数据类；任何非法字节序列都在进入状态规划前抛错。

### 机制板块

<!-- journey-file: src/miniredis/commands/model.py -->
#### `src/miniredis/commands/model.py`

##### 是什么，为什么现在需要

Model 是由 `SetString`、`HashSet`、`ZRangeByScore` 等不可变命令值组成的封闭词汇。

##### 在运行时做什么

Planner 对这些类型做模式匹配，接收已归一化选项，不再查看原始参数位置。

##### 关键代码

```python
class SetString:
    key: bytes
    value: bytes
    only_if: Literal["nx", "xx"] | None = None
    expire_ms: int | None = None
```

##### 关键语句理解

类型只能表示一个条件与一个归一化时长；互相冲突的原始选项没有合法构造状态。

<!-- journey-file: src/miniredis/commands/parser.py -->
#### `src/miniredis/commands/parser.py`

##### 是什么，为什么现在需要

Parser 拥有公开语法、Arity、选项冲突与有界数值转换。

##### 在运行时做什么

它把一个完整请求映射为一个类型化命令，或在不触碰 Runtime 状态时抛出 `CommandParseError`。

##### 关键代码

```python
if not INT64_MIN <= number <= INT64_MAX:
    raise CommandParseError("value is not an integer or out of range")
```

##### 关键语句理解

Python 转换成功并不够；协议的有符号 64 位边界仍是显式契约。

### 验证证据

运行 `uv run pytest -q $(cat journey/stages/02-typed-commands/tests.txt)`。它证明语法到类型的行为，不证明 Planner Reply 或串行执行。

### 需要真正记住的内容

请求保持二进制；解析在访问状态前校验完整请求；类型化命令携带归一化含义；协议数值边界显式定义，而不是继承 Python。

### 用自己的话讲清楚

Parser 是语义防火墙：Adapter 提供 Bytes，它要么产生一个完全合法的类型化命令，要么什么都不越过边界。后续每层无需重新解析公开语法即可推理命令含义。

### 教材

[第 2 章](https://github.com/system-in-miniature/mini-redis/blob/main/docs/zh/tutorial/02-command-life.md)
