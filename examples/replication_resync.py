"""Compare partial replica resume with full-sync fallback after a backlog gap."""

from __future__ import annotations

import asyncio

from miniredis import CommandRequest, MiniRedis, MiniRedisConfig
from miniredis.replication.sink import ReplicaSink, ReplicaSyncMode


async def set_value(runtime: MiniRedis, key: bytes, value: bytes) -> None:
    reply = await runtime.direct_client().execute(
        CommandRequest(b"SET", (key, value))
    )
    print(f"   SET {key!r} -> {reply!r}")


async def short_disconnect() -> None:
    primary = MiniRedis.open(MiniRedisConfig(replication_backlog_batches=2))
    replica = MiniRedis.open()
    await primary.start()
    await replica.start()
    sink = ReplicaSink(replica, queue_limit=4)

    initial = await primary.attach_replica(sink)
    print("1. Initial attachment:", initial.sync_mode)
    await set_value(primary, b"a", b"1")
    await sink.wait_until_applied(initial.primary_seq + 1)
    await sink.disconnect()
    await set_value(primary, b"b", b"2")

    resumed = await primary.attach_replica(sink)
    await sink.wait_until_applied(resumed.primary_seq)
    print("2. Short disconnect resumed with:", resumed.sync_mode)
    print("   Expected partial:", resumed.sync_mode is ReplicaSyncMode.PARTIAL)
    print("   Replica GET b:", await replica.direct_client().execute(
        CommandRequest(b"GET", (b"b",))
    ))
    await primary.close()
    await replica.close()


async def backlog_gap() -> None:
    primary = MiniRedis.open(MiniRedisConfig(replication_backlog_batches=2))
    replica = MiniRedis.open()
    await primary.start()
    await replica.start()
    sink = ReplicaSink(replica, queue_limit=4)

    initial = await primary.attach_replica(sink)
    await set_value(primary, b"old", b"present")
    await sink.wait_until_applied(initial.primary_seq + 1)
    await sink.disconnect()
    await set_value(primary, b"k2", b"2")
    await set_value(primary, b"k3", b"3")
    await set_value(primary, b"k4", b"4")

    resumed = await primary.attach_replica(sink)
    print("3. Cursor older than backlog resumed with:", resumed.sync_mode)
    print("   Expected full:", resumed.sync_mode is ReplicaSyncMode.FULL)
    print("   Replica GET k4:", await replica.direct_client().execute(
        CommandRequest(b"GET", (b"k4",))
    ))
    await primary.close()
    await replica.close()


async def main() -> None:
    print("Short disconnect: complete history is still in the backlog.")
    await short_disconnect()
    print("\nLong disconnect: a required batch has fallen out of the backlog.")
    await backlog_gap()


if __name__ == "__main__":
    asyncio.run(main())
