# MiniRedis → Redis Mapping

The canonical mapping is embedded in the
[architecture guide](architecture.md#mapping-to-real-redis), next to the data
flow it explains. It grades each relationship as **Equivalent**,
**Intentional simplification**, or **Semantically opposite**.

Use it to follow one mechanism at a time:

1. command parsing and per-type planners;
2. the serialized executor and immutable commit batches;
3. expiration, eviction, transactions, and Pub/Sub;
4. AOF, snapshots, recovery, and online rewrite;
5. replication backlog, partial/full synchronization, and RESP2/TCP.

Keep the [behavior matrix](behavior-matrix.md) open beside the mapping. The
mapping explains what carries across to Redis; the matrix identifies the
observable contract and its executable test evidence.
