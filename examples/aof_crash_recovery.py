"""Demonstrate that acknowledged AOF writes survive a simulated crash."""

from __future__ import annotations

import asyncio
from pathlib import Path
from tempfile import TemporaryDirectory

from miniredis import CommandRequest, MiniRedis, MiniRedisConfig
from miniredis.persistence.aof import AofPolicy


async def main() -> None:
    with TemporaryDirectory(prefix="miniredis-aof-") as directory:
        aof_path = Path(directory) / "appendonly.mraof"
        config = MiniRedisConfig(aof_path=aof_path, aof_policy=AofPolicy.ALWAYS)

        first = MiniRedis.open(config)
        await first.start()
        writer = first.direct_client()
        print("1. SET before crash:", await writer.execute(
            CommandRequest(b"SET", (b"lesson", b"durable"))
        ))
        print("2. Simulating a crash (no graceful AOF drain)...")
        await first.simulate_crash()

        recovered = MiniRedis.open(config)
        await recovered.start()
        reader = recovered.direct_client()
        value = await reader.execute(CommandRequest(b"GET", (b"lesson",)))
        print("3. GET after restart:", value)
        print("4. Recovery verified:", getattr(value, "value", None) == b"durable")
        await recovered.close()


if __name__ == "__main__":
    asyncio.run(main())
