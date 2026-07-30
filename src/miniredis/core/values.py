"""Define mutable in-memory representations for the five supported value types."""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from typing import TypeAlias


@dataclass(frozen=True, slots=True)
class StringValue:
    data: bytes


@dataclass(slots=True)
class HashValue:
    items: dict[bytes, bytes]


@dataclass(slots=True)
class ListValue:
    items: deque[bytes]


@dataclass(slots=True)
class SetValue:
    items: set[bytes]


@dataclass(slots=True)
class ZSetValue:
    scores: dict[bytes, float]


RedisValue: TypeAlias = StringValue | HashValue | ListValue | SetValue | ZSetValue
