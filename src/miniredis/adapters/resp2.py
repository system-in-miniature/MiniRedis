from __future__ import annotations

from dataclasses import dataclass
from typing import TypeAlias

from miniredis.commands.request import CommandRequest
from miniredis.core.outbound import (
    Outbound,
    PubSubMessage,
    PubSubPong,
    ReplyMessage,
    ServerClosed,
    SubscriptionAck,
)
from miniredis.core.reply import Bytes, Failure, Items, Number, Ok, Reply


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


RespFrame: TypeAlias = RespSimple | RespError | RespInteger | RespBulk | RespArray


@dataclass(frozen=True, slots=True)
class RespLimits:
    max_buffer: int = 1 << 20
    max_bulk: int = 1 << 20
    max_array: int = 1024
    max_depth: int = 8


class _NeedMore(Exception):
    pass


class RespDecoder:
    def __init__(self, limits: RespLimits | None = None) -> None:
        self._limits = limits or RespLimits()
        self._buffer = bytearray()

    def feed(self, data: bytes) -> tuple[RespFrame, ...]:
        self._buffer.extend(data)
        if len(self._buffer) > self._limits.max_buffer:
            raise RespProtocolError("RESP buffer limit exceeded")
        frames: list[RespFrame] = []
        offset = 0
        while offset < len(self._buffer):
            try:
                frame, next_offset = self._parse(offset, 0)
            except _NeedMore:
                break
            frames.append(frame)
            offset = next_offset
        if offset:
            del self._buffer[:offset]
        return tuple(frames)

    def finish(self) -> tuple[RespFrame, ...]:
        frames = self.feed(b"")
        if self._buffer:
            raise RespProtocolError("truncated RESP frame")
        return frames

    def _read_line(self, offset: int) -> tuple[bytes, int]:
        end = self._buffer.find(b"\r\n", offset)
        if end < 0:
            raise _NeedMore
        return bytes(self._buffer[offset:end]), end + 2

    @staticmethod
    def _decimal(data: bytes, label: str) -> int:
        try:
            text = data.decode("ascii")
            if not text or text in {"+", "-"}:
                raise ValueError
            if text[0] == "-":
                digits = text[1:]
            else:
                digits = text
            if not digits.isdecimal():
                raise ValueError
            return int(text)
        except (UnicodeDecodeError, ValueError) as exc:
            raise RespProtocolError(f"invalid RESP {label}") from exc

    def _parse(self, offset: int, depth: int) -> tuple[RespFrame, int]:
        if depth > self._limits.max_depth:
            raise RespProtocolError("RESP nesting limit exceeded")
        if offset >= len(self._buffer):
            raise _NeedMore
        prefix = self._buffer[offset]
        if prefix in (ord("+"), ord("-"), ord(":")):
            line, end = self._read_line(offset + 1)
            if prefix == ord("+"):
                return RespSimple(line), end
            if prefix == ord("-"):
                return RespError(line), end
            return RespInteger(self._decimal(line, "integer")), end
        if prefix == ord("$"):
            line, body = self._read_line(offset + 1)
            length = self._decimal(line, "bulk length")
            if length == -1:
                return RespBulk(None), body
            if length < 0 or length > self._limits.max_bulk:
                raise RespProtocolError("invalid RESP bulk length")
            end = body + length
            if end + 2 > len(self._buffer):
                raise _NeedMore
            if self._buffer[end : end + 2] != b"\r\n":
                raise RespProtocolError("invalid bulk terminator")
            return RespBulk(bytes(self._buffer[body:end])), end + 2
        if prefix == ord("*"):
            line, cursor = self._read_line(offset + 1)
            length = self._decimal(line, "array length")
            if length == -1:
                return RespArray(None), cursor
            if length < 0 or length > self._limits.max_array:
                raise RespProtocolError("invalid RESP array length")
            items: list[RespFrame] = []
            for _ in range(length):
                item, cursor = self._parse(cursor, depth + 1)
                items.append(item)
            return RespArray(tuple(items)), cursor
        raise RespProtocolError("unknown RESP type byte")


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
            return b"$" + str(len(data)).encode("ascii") + b"\r\n" + data + b"\r\n"
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


def frame_to_request(frame: RespFrame) -> CommandRequest:
    if not isinstance(frame, RespArray) or not frame.items:
        raise RespProtocolError("command must be a non-empty array")
    parts: list[bytes] = []
    for item in frame.items:
        if not isinstance(item, RespBulk) or item.data is None:
            raise RespProtocolError("command arguments must be bulk strings")
        parts.append(item.data)
    return CommandRequest(parts[0], tuple(parts[1:]))


def _reply_frame(reply: Reply) -> RespFrame:
    match reply:
        case Ok(message):
            return RespSimple(message)
        case Bytes(value):
            return RespBulk(value)
        case Number(value):
            return RespInteger(value)
        case Items(values):
            return RespArray(tuple(_reply_frame(value) for value in values))
        case Failure(code, message):
            return RespError(f"{code} {message}".encode())
    raise TypeError(f"unsupported reply: {type(reply)!r}")


def encode_outbound(outbound: Reply | Outbound) -> bytes:
    if isinstance(outbound, ReplyMessage):
        return encode_frame(_reply_frame(outbound.reply))
    if isinstance(outbound, (Ok, Bytes, Number, Items, Failure)):
        return encode_frame(_reply_frame(outbound))
    if isinstance(outbound, SubscriptionAck):
        channel = RespBulk(outbound.channel)
        return encode_frame(
            RespArray(
                (
                    RespBulk(outbound.kind.encode("ascii")),
                    channel,
                    RespInteger(outbound.subscription_count),
                )
            )
        )
    if isinstance(outbound, PubSubMessage):
        return encode_frame(
            RespArray(
                (
                    RespBulk(b"message"),
                    RespBulk(outbound.channel),
                    RespBulk(outbound.payload),
                )
            )
        )
    if isinstance(outbound, PubSubPong):
        return encode_frame(RespArray((RespBulk(b"pong"), RespBulk(outbound.payload))))
    if isinstance(outbound, ServerClosed):
        return encode_frame(RespError(f"CLOSED {outbound.reason}".encode()))
    raise TypeError(f"unsupported outbound: {type(outbound)!r}")
