from dataclasses import dataclass, field

from miniredis.commands.model import Command
from miniredis.core.blocking import WaiterId, WaiterWakeup
from miniredis.core.commit import CommitOperation
from miniredis.core.database import Database
from miniredis.core.reply import Reply


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


@dataclass(slots=True)
class TransactionWorkspace:
    database: Database
    operations: list[CommitOperation] = field(default_factory=list)
    replies: list[Reply] = field(default_factory=list)
    touch_keys: list[bytes] = field(default_factory=list)
    wakeups: list[WaiterWakeup] = field(default_factory=list)
    reserved_waiters: set[WaiterId] = field(default_factory=set)
