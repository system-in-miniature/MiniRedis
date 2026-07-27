from __future__ import annotations

from miniredis.commands.model import Command, Echo, Ping
from miniredis.config import MiniRedisConfig
from miniredis.core.database import Database
from miniredis.core.executor import ExecutionPlan
from miniredis.core.reply import Bytes, Failure, Ok


class CommandPlanner:
    def __init__(self, config: MiniRedisConfig) -> None:
        self.config = config

    def plan(self, database: Database, command: Command, now_ms: int) -> ExecutionPlan:
        del database, now_ms
        match command:
            case Ping(message=None):
                return ExecutionPlan(Ok(b"PONG"))
            case Ping(message=message) | Echo(message=message):
                return ExecutionPlan(Bytes(message))
            case _:
                return ExecutionPlan(Failure("ERR", "unknown command"))
