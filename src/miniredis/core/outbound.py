from __future__ import annotations

from dataclasses import dataclass
from typing import TypeAlias

from miniredis.core.reply import Reply


@dataclass(frozen=True, slots=True)
class RequestToken:
    value: int


@dataclass(frozen=True, slots=True)
class Replied:
    reply: Reply | None


@dataclass(frozen=True, slots=True)
class Abandoned:
    pass


@dataclass(frozen=True, slots=True)
class TransportClosed:
    pass


@dataclass(frozen=True, slots=True)
class RuntimeClosed:
    pass


@dataclass(frozen=True, slots=True)
class RuntimeFailed:
    reason: str


RequestOutcome: TypeAlias = (
    Replied | Abandoned | TransportClosed | RuntimeClosed | RuntimeFailed
)
