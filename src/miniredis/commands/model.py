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
class SetString:
    key: bytes
    value: bytes
    only_if: Literal["nx", "xx"] | None = None
    expire_ms: int | None = None


@dataclass(frozen=True, slots=True)
class GetString:
    key: bytes


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
class BlPop:
    keys: tuple[bytes, ...]
    timeout_ms: int


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
    | SetString
    | GetString
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
    | BlPop
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
