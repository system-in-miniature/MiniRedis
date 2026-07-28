from dataclasses import dataclass, field

from miniredis.commands.model import Command


@dataclass(slots=True)
class TransactionState:
    active: bool = False
    dirty: bool = False
    queued: list[Command] = field(default_factory=list)
    watched: dict[bytes, int] = field(default_factory=dict)

    def reset_transaction(self) -> None:
        self.active = False
        self.dirty = False
        self.queued.clear()

    def clear_all(self) -> None:
        self.reset_transaction()
        self.watched.clear()
