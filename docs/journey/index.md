# Self-Guided Rebuild

Each Stage is a complete independent-browser lesson: understand the current problem, concepts, and necessity; connect related files and critical statements through mechanism blocks; then close with evidence and your own explanation.

This is the browser-based path among MiniRedis's three learning modes. Use the [Mechanism Tutorial](../tutorial/index.md) for topic-oriented study, or the [Agent-Guided usage guide](../agent-guided.md) for interactive CLI teaching.

For an editor-focused diff, run `python -m journey.tools.build_journey study N` and open `../MiniRedis-journey-workspace`.

| Stage | Topic | New tests | Book chapter |
|---:|---|---:|---:|
| [01](stage-01.md) | Domain state and commit vocabulary | 13 | [2](../tutorial/02-life-of-a-command.md) |
| [02](stage-02.md) | Typed commands and strict parsing | 31 | [2](../tutorial/02-life-of-a-command.md) |
| [03](stage-03.md) | Serialized Direct executor | 13 | [2](../tutorial/02-life-of-a-command.md) |
| [04](stage-04.md) | Atomic String planning | 6 | [3](../tutorial/03-data-types.md) |
| [05](stage-05.md) | Hash and List planning | 7 | [3](../tutorial/03-data-types.md) |
| [06](stage-06.md) | Set and Sorted Set projections | 6 | [3](../tutorial/03-data-types.md) |
| [07](stage-07.md) | Absolute TTL and bounded expiry | 4 | [4](../tutorial/04-expiration.md) |
| [08](stage-08.md) | Deterministic eviction | 5 | [5](../tutorial/05-eviction.md) |
| [09](stage-09.md) | Request ownership and terminal outcomes | 2 | [2](../tutorial/02-life-of-a-command.md) |
| [10](stage-10.md) | Ordered outbox and slow sessions | 3 | [2](../tutorial/02-life-of-a-command.md) |
| [11](stage-11.md) | Blocking pop race ownership | 12 | [9](../tutorial/09-transactions-blocking.md) |
| [12](stage-12.md) | Pub/Sub and supervised shutdown | 13 | [9](../tutorial/09-transactions-blocking.md) |
| [13](stage-13.md) | Stable commit batches | 4 | [6](../tutorial/06-persistence-aof.md) |
| [14](stage-14.md) | Canonical persistence frames | 16 | [6](../tutorial/06-persistence-aof.md) |
| [15](stage-15.md) | AOF commit barrier | 11 | [6](../tutorial/06-persistence-aof.md) |
| [16](stage-16.md) | Snapshot capture and recovery | 20 | [7](../tutorial/07-rewrite-snapshot.md) |
| [17](stage-17.md) | Asynchronous replication | 5 | [8](../tutorial/08-replication.md) |
| [18](stage-18.md) | Promotion and supervised lifecycle | 7 | [8](../tutorial/08-replication.md) |
| [19](stage-19.md) | RESP2 protocol boundary | 3 | [10](../tutorial/10-protocol.md) |
| [20](stage-20.md) | TCP runtime parity | 4 | [10](../tutorial/10-protocol.md) |
| [21](stage-21.md) | Bulk strings and directional blocking pop | 5 | [3](../tutorial/03-data-types.md) |
| [22](stage-22.md) | Ordered pipelines | 2 | [10](../tutorial/10-protocol.md) |
| [23](stage-23.md) | Transactions and atomic functions | 7 | [9](../tutorial/09-transactions-blocking.md) |
| [24](stage-24.md) | Decaying LFU eviction | 6 | [5](../tutorial/05-eviction.md) |
| [25](stage-25.md) | AOF state base | 8 | [7](../tutorial/07-rewrite-snapshot.md) |
| [26](stage-26.md) | Online AOF rewrite | 2 | [7](../tutorial/07-rewrite-snapshot.md) |
| [27](stage-27.md) | Replication backlog | 2 | [8](../tutorial/08-replication.md) |
| [28](stage-28.md) | Partial resynchronization | 4 | [8](../tutorial/08-replication.md) |
| [29](stage-29.md) | Primary-owned expiry | 8 | [8](../tutorial/08-replication.md) |
| [30](stage-30.md) | Public parity and reading map | 0 | [11](../tutorial/index.md) |
