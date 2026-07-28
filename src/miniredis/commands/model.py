from __future__ import annotations

from dataclasses import dataclass
from typing import Literal, TypeAlias


@dataclass(frozen=True, slots=True)
class Ping:
    message: bytes | None = None


@dataclass(frozen=True, slots=True)
class Echo:
    message: bytes


@dataclass(frozen=True, slots=True)
class Multi:
    pass


@dataclass(frozen=True, slots=True)
class Exec:
    pass


@dataclass(frozen=True, slots=True)
class Discard:
    pass


@dataclass(frozen=True, slots=True)
class Watch:
    keys: tuple[bytes, ...]


@dataclass(frozen=True, slots=True)
class Unwatch:
    pass


@dataclass(frozen=True, slots=True)
class CompareDelete:
    key: bytes
    expected: bytes


@dataclass(frozen=True, slots=True)
class CheckDecrement:
    key: bytes
    amount: int


@dataclass(frozen=True, slots=True)
class SetString:
    key: bytes
    value: bytes
    only_if: Literal["nx", "xx"] | None = None
    expire_ms: int | None = None


@dataclass(frozen=True, slots=True)
class GetString:
    key: bytes


@dataclass(frozen=True, slots=True)
class MultiGet:
    keys: tuple[bytes, ...]


@dataclass(frozen=True, slots=True)
class MultiSet:
    pairs: tuple[tuple[bytes, bytes], ...]


@dataclass(frozen=True, slots=True)
class Delete:
    keys: tuple[bytes, ...]


@dataclass(frozen=True, slots=True)
class Exists:
    keys: tuple[bytes, ...]


@dataclass(frozen=True, slots=True)
class TypeOf:
    key: bytes


@dataclass(frozen=True, slots=True)
class Increment:
    key: bytes
    amount: int


@dataclass(frozen=True, slots=True)
class HashSet:
    key: bytes
    pairs: tuple[tuple[bytes, bytes], ...]


@dataclass(frozen=True, slots=True)
class HashGet:
    key: bytes
    field: bytes


@dataclass(frozen=True, slots=True)
class HashDelete:
    key: bytes
    fields: tuple[bytes, ...]


@dataclass(frozen=True, slots=True)
class HashGetAll:
    key: bytes


@dataclass(frozen=True, slots=True)
class HashIncrement:
    key: bytes
    field: bytes
    amount: int


@dataclass(frozen=True, slots=True)
class ListPush:
    key: bytes
    values: tuple[bytes, ...]
    left: bool


@dataclass(frozen=True, slots=True)
class ListPop:
    key: bytes
    left: bool


@dataclass(frozen=True, slots=True)
class ListRange:
    key: bytes
    start: int
    stop: int


@dataclass(frozen=True, slots=True)
class BlockingPop:
    keys: tuple[bytes, ...]
    timeout_ms: int
    left: bool


@dataclass(frozen=True, slots=True)
class Subscribe:
    channels: tuple[bytes, ...]


@dataclass(frozen=True, slots=True)
class Unsubscribe:
    channels: tuple[bytes, ...]


@dataclass(frozen=True, slots=True)
class Publish:
    channel: bytes
    payload: bytes


@dataclass(frozen=True, slots=True)
class SetAdd:
    key: bytes
    members: tuple[bytes, ...]


@dataclass(frozen=True, slots=True)
class SetRemove:
    key: bytes
    members: tuple[bytes, ...]


@dataclass(frozen=True, slots=True)
class SetIsMember:
    key: bytes
    member: bytes


@dataclass(frozen=True, slots=True)
class SetMembers:
    key: bytes


@dataclass(frozen=True, slots=True)
class SetIntersection:
    keys: tuple[bytes, ...]


@dataclass(frozen=True, slots=True)
class ScoreBound:
    value: float
    inclusive: bool


@dataclass(frozen=True, slots=True)
class ZAdd:
    key: bytes
    pairs: tuple[tuple[float, bytes], ...]


@dataclass(frozen=True, slots=True)
class ZRemove:
    key: bytes
    members: tuple[bytes, ...]


@dataclass(frozen=True, slots=True)
class ZScore:
    key: bytes
    member: bytes


@dataclass(frozen=True, slots=True)
class ZRank:
    key: bytes
    member: bytes


@dataclass(frozen=True, slots=True)
class ZRange:
    key: bytes
    start: int
    stop: int


@dataclass(frozen=True, slots=True)
class ZRangeByScore:
    key: bytes
    minimum: ScoreBound
    maximum: ScoreBound


@dataclass(frozen=True, slots=True)
class Expire:
    key: bytes
    seconds: int


@dataclass(frozen=True, slots=True)
class TimeToLive:
    key: bytes
    milliseconds: bool


@dataclass(frozen=True, slots=True)
class Persist:
    key: bytes


Command: TypeAlias = (
    Ping
    | Echo
    | Multi
    | Exec
    | Discard
    | Watch
    | Unwatch
    | CompareDelete
    | CheckDecrement
    | SetString
    | GetString
    | MultiGet
    | MultiSet
    | Delete
    | Exists
    | TypeOf
    | Increment
    | HashSet
    | HashGet
    | HashDelete
    | HashGetAll
    | HashIncrement
    | ListPush
    | ListPop
    | ListRange
    | BlockingPop
    | Subscribe
    | Unsubscribe
    | Publish
    | SetAdd
    | SetRemove
    | SetIsMember
    | SetMembers
    | SetIntersection
    | ZAdd
    | ZRemove
    | ZScore
    | ZRank
    | ZRange
    | ZRangeByScore
    | Expire
    | TimeToLive
    | Persist
)


_DATASET_MUTATING_TYPES = frozenset(
    {
        SetString,
        CompareDelete,
        CheckDecrement,
        MultiSet,
        Delete,
        Increment,
        HashSet,
        HashDelete,
        HashIncrement,
        ListPush,
        ListPop,
        BlockingPop,
        SetAdd,
        SetRemove,
        ZAdd,
        ZRemove,
        Expire,
        Persist,
    }
)

_NON_DATASET_MUTATING_TYPES = frozenset(
    {
        Ping,
        Echo,
        Multi,
        Exec,
        Discard,
        Watch,
        Unwatch,
        GetString,
        MultiGet,
        Exists,
        TypeOf,
        HashGet,
        HashGetAll,
        ListRange,
        SetIsMember,
        SetMembers,
        SetIntersection,
        ZScore,
        ZRank,
        ZRange,
        ZRangeByScore,
        TimeToLive,
        Subscribe,
        Unsubscribe,
        Publish,
    }
)


def is_dataset_mutating(command: Command) -> bool:
    command_type = type(command)
    if command_type in _DATASET_MUTATING_TYPES:
        return True
    if command_type in _NON_DATASET_MUTATING_TYPES:
        return False
    raise AssertionError(f"unclassified command type: {command_type.__name__}")
