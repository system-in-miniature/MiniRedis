"""Route typed commands to pure per-type planners and maxmemory enforcement.

The planner layer corresponds to Redis command implementations, but returns an
``ExecutionPlan`` rather than mutating the keyspace in place.
"""

from collections import deque

from miniredis.commands.model import BlockingPop
from miniredis.commands.model import Command
from miniredis.config import MiniRedisConfig
from miniredis.core.commit import (
    CommitOperation,
    DeleteKey,
    DeleteReason,
    PutEntry,
    StoredEntry,
    StoredList,
)
from miniredis.core.database import Database
from miniredis.core.eviction import enforce_memory
from miniredis.core.executor import ExecutionPlan
from miniredis.core.hash_planner import plan_hash
from miniredis.core.list_planner import plan_list
from miniredis.core.planning import plan_general_and_strings
from miniredis.core.reply import Bytes, Failure, Items
from miniredis.core.set_planner import plan_set
from miniredis.core.ttl_planner import plan_ttl
from miniredis.core.zset_planner import plan_zset
from miniredis.core.values import ListValue


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
        if plan is None:
            plan = plan_list(command, database, now_ms)
        if plan is None:
            plan = plan_set(command, database, now_ms)
        if plan is None:
            plan = plan_zset(command, database, now_ms)
        if plan is None:
            plan = plan_ttl(command, database, now_ms)
        if plan is not None:
            return enforce_memory(plan, database, self.config, now_ms)
        return ExecutionPlan(Failure("ERR", "unknown command"))

    def plan_blocking_pop_now(
        self,
        command: BlockingPop,
        database: Database,
        now_ms: int,
    ) -> ExecutionPlan | None:
        for key in command.keys:
            entry = database.entries.get(key)
            if entry is None:
                continue
            if entry.expire_at_ms is not None and entry.expire_at_ms <= now_ms:
                continue
            if not isinstance(entry.value, ListValue):
                return ExecutionPlan(
                    Failure(
                        "WRONGTYPE",
                        "operation against a key holding the wrong kind of value",
                    )
                )
            if not entry.value.items:
                continue
            items = deque(entry.value.items)
            item = items.popleft() if command.left else items.pop()
            if items:
                operation: CommitOperation = PutEntry(
                    key,
                    StoredEntry(
                        StoredList(tuple(items)),
                        entry.expire_at_ms,
                        entry.mutation_version + 1,
                    ),
                )
            else:
                operation = DeleteKey(key, DeleteReason.CLIENT)
            return ExecutionPlan(
                Items((Bytes(key), Bytes(item))),
                operations=(operation,),
            )
        return None
