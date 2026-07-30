"""Observe deterministic allkeys-LFU eviction through command replies only."""

from __future__ import annotations

import asyncio

from miniredis import CommandRequest, MiniRedis


async def main() -> None:
    async with MiniRedis.open(
        maxmemory=260,
        eviction_policy="allkeys-lfu",
    ) as redis:
        client = redis.direct_client()
        print("1. Create equally small 'hot' and 'cold' keys.")
        print("   hot:", await client.execute(
            CommandRequest(b"SET", (b"hot", b"x"))
        ))
        print("   cold:", await client.execute(
            CommandRequest(b"SET", (b"cold", b"x"))
        ))

        print("2. Read 'hot' four times to raise its frequency.")
        for attempt in range(1, 5):
            reply = await client.execute(CommandRequest(b"GET", (b"hot",)))
            print(f"   read {attempt}: {reply!r}")

        print("3. Add a larger key so maxmemory requires one victim.")
        print("   new:", await client.execute(
            CommandRequest(b"SET", (b"new", b"x" * 60))
        ))

        cold = await client.execute(CommandRequest(b"GET", (b"cold",)))
        hot = await client.execute(CommandRequest(b"GET", (b"hot",)))
        new = await client.execute(CommandRequest(b"GET", (b"new",)))
        print("4. Public GET observations:")
        print("   cold (least frequent, expected missing):", cold)
        print("   hot  (expected retained):", hot)
        print("   new  (expected retained):", new)


if __name__ == "__main__":
    asyncio.run(main())
