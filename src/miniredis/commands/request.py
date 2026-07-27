from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class CommandRequest:
    name: bytes
    args: tuple[bytes, ...] = ()
