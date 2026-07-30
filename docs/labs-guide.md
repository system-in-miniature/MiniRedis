# Hands-on Examples

> English · [中文版](zh/labs-guide.md)

Install from the repository root:

```bash
uv sync
```

The examples call the Direct API and create only temporary state.

## 1. AOF crash recovery

```bash
uv run python examples/aof_crash_recovery.py
```

Expected landmarks:

```text
1. SET before crash: Ok(message=b'OK')
2. Simulating a crash (no graceful AOF drain)...
3. GET after restart: Bytes(value=b'durable')
4. Recovery verified: True
```

Watch the `AofPolicy.ALWAYS` boundary: the successful `SET` crosses the AOF
durability barrier before the reply is released. `simulate_crash()` skips a
graceful drain; reopening and replay still recover the value.

## 2. Deterministic LFU eviction

```bash
uv run python examples/lfu_eviction.py
```

Expected landmarks:

```text
cold (least frequent, expected missing): Bytes(value=None)
hot  (expected retained): Bytes(value=b'x')
new  (expected retained): Bytes(value=b'xxx...')
```

Watch public replies rather than internal counters. Four reads make `hot` more
frequent than `cold`; inserting `new` exceeds the configured logical-memory
budget, so deterministic allkeys-LFU removes `cold`.

## 3. Partial resynchronization and full-sync fallback

```bash
uv run python examples/replication_resync.py
```

Expected landmarks:

```text
Initial attachment: full
Short disconnect resumed with: partial
Cursor older than backlog resumed with: full
```

The first disconnect misses only one retained batch, so the replication cursor
can resume from the backlog. The second misses more history than the two-batch
backlog retains, forcing a complete state transfer. This is an in-process
logical model of the PSYNC decision, not Redis wire replication.

## Continue with executable evidence

Use the [behavior matrix](behavior-matrix.md) to find the focused pytest node
for a mechanism, or run the entire suite:

```bash
uv run pytest -q
```
