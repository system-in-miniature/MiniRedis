from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

EvictionPolicy = Literal["noeviction", "allkeys-lru"]


@dataclass(frozen=True, slots=True)
class MiniRedisConfig:
    max_pending_commands: int = 1024
    active_expire_sample_size: int = 20
    maxmemory: int | None = None
    eviction_policy: EvictionPolicy = "noeviction"
    outbox_limit: int = 64

    def __post_init__(self) -> None:
        if self.max_pending_commands <= 0:
            raise ValueError("max_pending_commands must be positive")
        if self.active_expire_sample_size <= 0:
            raise ValueError("active_expire_sample_size must be positive")
        if self.maxmemory is not None and self.maxmemory <= 0:
            raise ValueError("maxmemory must be positive")
        if self.eviction_policy not in {"noeviction", "allkeys-lru"}:
            raise ValueError("eviction_policy must be 'noeviction' or 'allkeys-lru'")
        if self.outbox_limit <= 0:
            raise ValueError("outbox_limit must be positive")
