from __future__ import annotations

from dataclasses import dataclass
from typing import TypeAlias


class RespProtocolError(ValueError):
    pass


@dataclass(frozen=True, slots=True)
class RespSimple:
    data: bytes


@dataclass(frozen=True, slots=True)
class RespError:
    data: bytes


@dataclass(frozen=True, slots=True)
class RespInteger:
    value: int


@dataclass(frozen=True, slots=True)
class RespBulk:
    data: bytes | None


@dataclass(frozen=True, slots=True)
class RespArray:
    items: tuple[RespFrame, ...] | None


RespFrame: TypeAlias = (
    RespSimple | RespError | RespInteger | RespBulk | RespArray
)


def _line(prefix: bytes, data: bytes) -> bytes:
    if b"\r" in data or b"\n" in data:
        raise ValueError("RESP line values cannot contain CR or LF")
    return prefix + data + b"\r\n"


def encode_frame(frame: RespFrame) -> bytes:
    match frame:
        case RespSimple(data):
            return _line(b"+", data)
        case RespError(data):
            return _line(b"-", data)
        case RespInteger(value):
            return b":" + str(value).encode("ascii") + b"\r\n"
        case RespBulk(None):
            return b"$-1\r\n"
        case RespBulk(data):
            return (
                b"$"
                + str(len(data)).encode("ascii")
                + b"\r\n"
                + data
                + b"\r\n"
            )
        case RespArray(None):
            return b"*-1\r\n"
        case RespArray(items):
            return (
                b"*"
                + str(len(items)).encode("ascii")
                + b"\r\n"
                + b"".join(encode_frame(item) for item in items)
            )
    raise TypeError(f"unsupported RESP frame: {type(frame)!r}")
