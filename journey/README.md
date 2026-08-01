# MiniRedis Journey

The Journey is the exact incremental source for MiniRedis's self-guided and
Agent-guided rebuild modes. It complements the topic-oriented mechanism
tutorial; it does not replace it.

## Why 30 Stages

MiniRedis has several independently meaningful state machines: serialized
request ownership, blocking waiters, durability barriers, snapshot recovery,
replication, promotion, transactions, online AOF rewrite, partial resync, and
TCP lifecycle. Thirty Stages keep those causal boundaries visible. The Stage
count is project-sized rather than copied from MiniS3's smaller codebase.

## Canonical artifacts

Every `stages/NN-slug/` directory contains:

- `goal.md`: bilingual authored lesson facts;
- `stage.patch`: exact delta from the preceding Stage;
- `tests.txt`: focused executable evidence;
- `layout.toml`: failure-preview ownership and mechanism blocks.

`manifest.toml` freezes the historical endpoint, tutorial chapter, slug, and
focused tests for all 30 Stages. `tools/extract_history.py` reproducibly
regenerates the patches from those endpoints. Learners never need the history:
the checked-in patches are authoritative and self-contained.

Tests belong to `failure_files` and render before concepts. Only production
files belong to mechanism blocks. Supporting package, dependency, README, and
export changes are collapsed together.

## Verify the complete chain

```bash
uv run python journey/tools/build_journey.py --check
```

The guard starts from an empty owned tree, applies every patch, runs each
Stage's focused tests, and requires final byte parity with the canonical
production/example and Journey-owned test roots.

## Learner workspaces

Study one completed increment as uncommitted editor changes:

```bash
uv run python journey/tools/build_journey.py study 11
```

Prepare the clean Stage 10 baseline and implement Stage 11 yourself:

```bash
uv run python journey/tools/build_journey.py attempt 11
```

Check the focused tests in an existing learner workspace:

```bash
uv run python journey/tools/build_journey.py check 11
```

For interactive teaching, open Codex in the canonical MiniRedis repository and
ask `开始 Agent 带教 Stage 11`. The root `AGENTS.md` prepares or resumes the
private Stage workspace; no learner-facing branch switch is required.
