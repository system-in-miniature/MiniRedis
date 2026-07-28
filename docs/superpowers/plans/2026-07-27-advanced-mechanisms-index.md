# Advanced MiniRedis Mechanisms Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use
> `superpowers:executing-plans` to implement these plans task-by-task. Steps use
> checkbox (`- [ ]`) syntax for tracking. The user selected inline execution;
> do not dispatch subagents.

**Goal:** Execute the approved advanced-mechanisms design as five independently
green, reviewable capability phases.

**Architecture:** Preserve CommandExecutor as the only state owner and
CommitBatch as the shared durability/replication unit. Complete adapter-facing
commands first, then transaction semantics, volatile LFU, online AOF rewrite,
and finally logical partial replication.

**Tech Stack:** Python 3.13, asyncio, pytest, pytest-asyncio, Ruff, POSIX file
operations, RESP2 adapter.

---

## Approved design

Read before execution:

- `docs/superpowers/specs/2026-07-27-miniredis-advanced-mechanisms-design.md`

## Fixed execution order

1. `2026-07-27-phase-a-commands-pipeline.md`
2. `2026-07-27-phase-b-transactions-atomics.md`
3. `2026-07-27-phase-c-deterministic-lfu.md`
4. `2026-07-27-phase-d-online-aof-rewrite.md`
5. `2026-07-27-phase-e-partial-resync.md`

Do not begin a later phase until the earlier phase's full-suite verification
and acceptance commit are complete.

## Coverage ledger

| Approved requirement | Owning plan |
|---|---|
| MGET, MSET, DECR | Phase A, Task 1 |
| BRPOP and direction-aware waiters | Phase A, Task 2 |
| DirectPipeline | Phase A, Task 3 |
| Ordered RESP2 pipelining and bounds | Phase A, Task 4 |
| Null-array reply | Phase B, Task 1 |
| Per-key revision ledger | Phase B, Task 2 |
| Session transaction lifecycle | Phase B, Task 3 |
| EXEC workspace and one CommitBatch | Phase B, Task 4 |
| WATCH conflict and create-delete detection | Phase B, Tasks 2 and 4 |
| COMPAREDEL and CHECKDECR | Phase B, Task 5 |
| Deterministic decaying LFU | Phase C, Tasks 1–3 |
| LFU metadata excluded from persistence/replication | Phase C, Task 4 |
| AOF state-base format and recovery | Phase D, Tasks 1–2 |
| Bounded concurrent rewrite delta | Phase D, Task 3 |
| Ordered atomic AOF replacement | Phase D, Task 4 |
| Race-free rewrite API and lifecycle | Phase D, Task 5 |
| Logical replication ID and backlog | Phase E, Tasks 1–2 |
| Partial catch-up and live ordering | Phase E, Task 3 |
| Full-sync fallback and restart identity | Phase E, Task 4 |
| Promotion fencing and acknowledged loss | Phase E, Task 5 |
| Final stats, docs, cleanup, and acceptance | Final task of every phase |

## Inline execution contract

For every task:

```text
write one focused failing test
→ run the exact focused command and observe RED
→ make the smallest implementation change
→ rerun focused tests and observe GREEN
→ run the listed regression slice
→ inspect the diff
→ commit only that task
```

Use `apply_patch` for source edits. Preserve unrelated user changes. Do not
create a worktree: the user explicitly selected inline development in the
current repository. Do not use subagents.

If a planned signature proves inconsistent with current code during execution,
stop only for a genuinely architecture-changing conflict. For a local naming
or typing correction that preserves the approved behavior, update the active
plan inline, note the correction in the task commit, and continue.

## Phase boundary verification

At the end of every phase:

```bash
uv run ruff check .
uv run pytest -q
git diff --check
git status --short
```

Expected:

- Ruff reports no errors.
- The complete test suite passes.
- Diff check prints nothing.
- Before the next phase begins, the worktree is clean.

## Final excluded scope

Completion of all plans must not introduce RESP3, Lua, Redis RDB/AOF
compatibility, TCP replication, automatic reconnect, heartbeat, election,
Sentinel, Cluster, WAIT, quorum writes, or application-level cache-pattern
projects. Course design remains a separate later task.
