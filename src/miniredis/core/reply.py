"""Define transport-neutral command replies before RESP2 encoding."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TypeAlias


@dataclass(frozen=True, slots=True)
class Ok:
    message: bytes = b"OK"


@dataclass(frozen=True, slots=True)
class Bytes:
    value: bytes | None


@dataclass(frozen=True, slots=True)
class Number:
    value: int


@dataclass(frozen=True, slots=True)
class Items:
    values: tuple[Reply, ...]


@dataclass(frozen=True, slots=True)
class NullArray:
    pass


@dataclass(frozen=True, slots=True)
class Failure:
    code: str
    message: str


Reply: TypeAlias = Ok | Bytes | Number | Items | NullArray | Failure
