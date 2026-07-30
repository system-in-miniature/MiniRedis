"""Plan maxmemory eviction in the same commit as the triggering write.

This corresponds to Redis ``evict.c`` while using logical bytes and exact,
deterministic victim ordering instead of allocator memory and sampled candidates.
"""

from __future__ import annotations

from collections.abc import Iterable

from miniredis.config import MiniRedisConfig
from miniredis.core.commit import (
    CommitOperation,
    DeleteKey,
    DeleteReason,
    PutEntry,
)
from miniredis.core.database import Database, logical_entry_size
from miniredis.core.executor import ExecutionPlan
from miniredis.core.expiration import expiry_delete, is_expired
from miniredis.core.frequency import project_frequency
from miniredis.core.planning import dedupe_operations
from miniredis.core.reply import Failure


OOM = Failure("OOM", "command exceeds maxmemory")


def projected_usage(
    database: Database,
    operations: Iterable[CommitOperation],
) -> int:
    sizes = {key: entry.logical_size for key, entry in database.entries.items()}
    for operation in operations:
        if isinstance(operation, DeleteKey):
            sizes.pop(operation.key, None)
        else:
            sizes[operation.key] = logical_entry_size(
                operation.key,
                operation.entry.value,
                operation.entry.expire_at_ms,
            )
    usage = sum(sizes.values())
    if usage < 0:
        raise AssertionError("projected usage cannot be negative")
    return usage


def _expired_operations(
    database: Database,
    now_ms: int,
) -> tuple[CommitOperation, ...]:
    return tuple(
        expiry_delete(key)
        for key, entry in sorted(database.entries.items())
        if is_expired(entry, now_ms)
    )


def _lfu_candidates(
    database: Database,
    *,
    now_ms: int,
    decay_interval_ms: int,
    excluded: set[bytes],
) -> list[tuple[int, int, bytes]]:
    return sorted(
        (
            project_frequency(
                entry.frequency,
                entry.last_frequency_decay_ms,
                now_ms,
                decay_interval_ms,
            )[0],
            entry.last_access_tick,
            key,
        )
        for key, entry in database.entries.items()
        if key not in excluded and not is_expired(entry, now_ms)
    )


def enforce_memory(
    plan: ExecutionPlan,
    database: Database,
    config: MiniRedisConfig,
    now_ms: int,
) -> ExecutionPlan:
    maxmemory = config.maxmemory
    if maxmemory is None or not plan.operations:
        return plan

    writes_data = any(
        isinstance(operation, PutEntry)
        or (
            isinstance(operation, DeleteKey) and operation.reason is DeleteReason.CLIENT
        )
        for operation in plan.operations
    )
    if not writes_data:
        return plan

    # Redis evict.c also reclaims logically expired data before sacrificing a
    # live victim. Keeping those deletes in this plan makes the triggering write
    # and all space reclamation one atomic propagation unit.
    expired = _expired_operations(database, now_ms)
    operations = dedupe_operations(expired + plan.operations)

    target_keys = {
        operation.key for operation in operations if isinstance(operation, PutEntry)
    }
    for operation in operations:
        if not isinstance(operation, PutEntry):
            continue
        target_size = logical_entry_size(
            operation.key,
            operation.entry.value,
            operation.entry.expire_at_ms,
        )
        if target_size > maxmemory:
            return ExecutionPlan(OOM)

    baseline = projected_usage(database, expired)
    usage = projected_usage(database, operations)
    if usage <= maxmemory or usage <= baseline:
        return ExecutionPlan(
            plan.reply,
            operations,
            plan.touch_keys,
            plan.trigger,
        )
    if config.eviction_policy == "noeviction":
        return ExecutionPlan(OOM)

    already_deleted = {
        operation.key for operation in operations if isinstance(operation, DeleteKey)
    }
    excluded = target_keys | already_deleted
    if config.eviction_policy == "allkeys-lfu":
        # Redis samples candidates and uses an approximate counter. Exact
        # projection plus stable tie-breaks is deliberate here: readers can
        # derive the victim without depending on randomness.
        candidate_keys = [
            key
            for _frequency, _tick, key in _lfu_candidates(
                database,
                now_ms=now_ms,
                decay_interval_ms=config.lfu_decay_interval_ms,
                excluded=excluded,
            )
        ]
    else:
        # This is the allkeys-LRU lesson from evict.c with a global exact order
        # instead of Redis's bounded candidate sampling.
        candidate_keys = [
            key
            for _tick, key in sorted(
                (entry.last_access_tick, key)
                for key, entry in database.entries.items()
                if key not in excluded and not is_expired(entry, now_ms)
            )
        ]
    victims: list[CommitOperation] = []
    for key in candidate_keys:
        victims.append(DeleteKey(key, DeleteReason.EVICTED))
        candidate_operations = dedupe_operations(
            expired + tuple(victims) + plan.operations
        )
        if projected_usage(database, candidate_operations) <= maxmemory:
            return ExecutionPlan(
                plan.reply,
                candidate_operations,
                plan.touch_keys,
                plan.trigger,
            )
    return ExecutionPlan(OOM)
