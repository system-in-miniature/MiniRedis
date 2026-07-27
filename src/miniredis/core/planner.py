from miniredis.commands.model import Command
from miniredis.config import MiniRedisConfig
from miniredis.core.database import Database
from miniredis.core.executor import ExecutionPlan
from miniredis.core.hash_planner import plan_hash
from miniredis.core.planning import plan_general_and_strings
from miniredis.core.reply import Failure


class CommandPlanner:
    def __init__(self, config: MiniRedisConfig) -> None:
        self.config = config

    def plan(
        self,
        command: Command,
        database: Database,
        now_ms: int,
    ) -> ExecutionPlan:
        plan = plan_general_and_strings(command, database, now_ms)
        if plan is None:
            plan = plan_hash(command, database, now_ms)
        if plan is not None:
            return plan
        return ExecutionPlan(Failure("ERR", "unknown command"))
