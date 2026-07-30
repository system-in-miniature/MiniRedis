"""Represent one transport-neutral command name and binary argument tuple."""

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class CommandRequest:
    name: bytes
    args: tuple[bytes, ...] = ()
