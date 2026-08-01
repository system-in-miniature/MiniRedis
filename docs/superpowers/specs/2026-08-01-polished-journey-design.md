# MiniRedis Polished Learning Surfaces Design

## Status and objective

MiniRedis already has a polished bilingual mechanism tutorial and a finished,
well-tested reference implementation. It is not polished under the newer
system-in-miniature learning contract because it has no self-guided rebuild
Journey and no direct CLI Agent-guided entry.

This design adds the same three learning modes established by MiniS3 while
preserving MiniRedis's own architecture:

1. **Mechanism tutorial** — the existing topic-oriented textbook.
2. **Self-guided rebuild** — bilingual browser lessons backed by an exact,
   cumulative patch chain.
3. **Agent-guided rebuild** — open Codex in the canonical repository and ask
   `开始 Agent 带教 Stage NN`; the root `AGENTS.md` routes the session.

The repository stays Direct-first. The Journey must not accidentally teach
RESP2/TCP as the owner of command semantics.

## Evidence used for decomposition

The design is based on a complete read of all 8,898 lines under `src/` and
`examples/`, the 417 collected tests, all ten bilingual tutorial chapters, and
the source-bearing Git history from `f68b061` through `8151fae`.

The central ownership chain is:

```text
CommandRequest
  -> strict parser and closed Command vocabulary
  -> pure CommandPlanner
  -> one CommandExecutor mailbox owner
  -> optional AOF durability barrier
  -> atomic CommitBatch apply
  -> replication and ordered session completion
```

TTL, eviction, blocking waiters, transactions, Pub/Sub, snapshots, online AOF
rewrite, replication resume, and TCP sessions are extensions of that chain.
They are not independent mini-applications.

## Why the Stage count differs from MiniS3

MiniS3 uses 15 stages because its production and test surface is much smaller.
MiniRedis has 8,898 production/example lines and 7,461 test lines. Forcing it
into 15 stages would combine unrelated state machines and produce several
1,500-line lessons.

MiniRedis therefore uses **30 stages**. The stable contract is not the number
15; it is one causally coherent increment, executable evidence, exact
reconstruction, and a browser lesson that explains the problem before the
files. Stage count is project-sized.

Historical feature endpoints provide a known-good dependency order. The
checked-in patches are self-contained and do not require Git history at study
time.

## Stage map

| Stage | Endpoint | Mechanism increment |
|---:|---|---|
| 01 | `f68b061` | Binary-safe values, replies, database state, and project scaffold |
| 02 | `67f0d73` | Closed typed commands and strict request parsing |
| 03 | `a5f7a27` | Bounded mailbox, serialized executor, and Direct runtime |
| 04 | `be7969d` | Atomic String planning and commit application |
| 05 | `eb41b6e` | Hash and non-blocking List semantics |
| 06 | `79fc734` | Set and Sorted Set semantics |
| 07 | `ddfd69e` | Absolute TTL, lazy expiry, and bounded active expiry |
| 08 | `7628635` | Deterministic maxmemory eviction and domain closure |
| 09 | `319d14a` | Accepted-request outcome ownership |
| 10 | `5436512` | Ordered bounded outboxes and close semantics |
| 11 | `bb842dd` | Blocking-pop registration, terminal races, and push wakeups |
| 12 | `6ff1e5f` | Pub/Sub, slow consumers, and cancellation-safe shutdown |
| 13 | `5a40b5f` | Stable `CommitBatch` propagation unit |
| 14 | `633bbe6` | Canonical stored-state codec and framed records |
| 15 | `b03560a` | Owned AOF writer and durability-before-apply barrier |
| 16 | `b267f92` | Atomic snapshots and contiguous recovery |
| 17 | `e18be82` | Laggable asynchronous replica attachment |
| 18 | `0fbaeee` | Promotion fencing and reliability lifecycle ordering |
| 19 | `c088652` | Bounded RESP2 framing and domain mapping |
| 20 | `5419f99` | Thin TCP sessions, Direct parity, and redis-py smoke |
| 21 | `40d00de` | Bulk String commands and direction-aware blocking pop |
| 22 | `0016059` | Ordered Direct and RESP2 pipelines |
| 23 | `b195a43` | Transactions, WATCH revisions, and atomic functions |
| 24 | `b25b473` | Deterministic decaying LFU eviction |
| 25 | `b9b363e` | AOF state bases and checkpoint selection |
| 26 | `8cd6d5e` | Race-free online AOF rewrite |
| 27 | `e65b568` | Bounded logical replication backlog and attachment choice |
| 28 | `c07182f` | Partial resume, full fallback, and history fencing |
| 29 | `94109b0` | Primary-owned expiry and bounded observability |
| 30 | `8151fae` | Runnable examples, module intent, and final public parity |

## Canonical stage artifacts

Each `journey/stages/NN-slug/` directory contains:

- `goal.md` — bilingual authored lesson facts and per-file explanations;
- `stage.patch` — the exact delta from Stage N-1;
- `tests.txt` — focused nodes that pass at that cumulative state;
- `layout.toml` — `failure_files` plus implementation/supporting mechanism
  blocks.

Test files belong only to `failure_files`. They render under Failure preview /
先看会坏在哪里 with test-specific labels. They must never be owned by a
mechanism block. Pure evidence stages omit the mechanism section rather than
displaying an empty or test-shaped mechanism block.

## Browser lesson contract

Every localized page follows this order:

1. current problem;
2. failure preview and collapsed test diffs;
3. basic concepts;
4. why the mechanism is necessary;
5. runtime mental model;
6. mechanism blocks with collapsed implementation diffs and key statements;
7. verification evidence;
8. durable takeaways and a learner explanation;
9. link to the relevant tutorial chapter.

Routine package exports, lockfiles, README changes, and project configuration
are grouped into one supporting drawer. They are not promoted into mechanism
lessons.

## Patch and parity boundaries

The learner chain owns:

- `src/miniredis/**`;
- behavior, unit, reliability, concurrency, replication, and adapter tests;
- `tests/helpers/**`;
- `examples/**`;
- the minimal project files needed to install and run those tests.

The chain does not reconstruct the existing mechanism tutorial, public design
records, website assets, or documentation-only contract tests. Those belong to
learning mode one and remain available in the canonical repository.

The final guard compares all owned production/example files and Journey-owned
tests byte-for-byte with `main`. Every cumulative Stage must apply cleanly and
its declared tests must pass.

## Agent-guided contract

The root `AGENTS.md` is a router, not a second textbook. On
`开始 Agent 带教 Stage NN`, the agent:

1. reads the canonical Stage artifacts;
2. prepares or resumes the internal Stage workspace;
3. establishes the Stage N-1 baseline without asking the learner to switch
   branches;
4. teaches problem and concepts before implementation;
5. uses tests as visible evidence, not as the mechanism explanation;
6. checks the learner result against the canonical Stage tree.

`docs/agent-guided.md` and its Chinese counterpart remain short usage guides.

## Localization and navigation

English and Chinese Journey pages have one-to-one Stage parity. The MkDocs
language switch preserves the current Journey path. Both language nav trees
expose the three modes with content-derived names:

- Mechanism Tutorial / 机制教程
- Self-Guided Rebuild / 自主重建
- Agent-Guided / Agent 带教

## Acceptance gates

The migration is polished only when all of the following are fresh and green:

- the existing full MiniRedis test suite;
- renderer/tool unit tests;
- all 30 cumulative Stage checks;
- final owned-file parity with `main`;
- bilingual page generation with exact source/diff coverage;
- `mkdocs build --strict`;
- browser checks for language switching, collapsed directories, test-before-
  concept order, mechanism-only implementation ownership, and Agent guide
  routing.

