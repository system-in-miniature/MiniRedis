# MiniRedis Polished Journey Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a verified 30-stage bilingual self-guided Journey and direct CLI Agent-guided mode without changing MiniRedis runtime behavior.

**Architecture:** Historical known-good feature endpoints define the cumulative source/test snapshots. A checked-in patch chain and manifest-driven tools reconstruct those snapshots without requiring history, while authored bilingual cards render test evidence before concepts and implementation-only mechanism blocks afterward.

**Tech Stack:** Python 3.12, `pytest`, Git patches/worktrees, TOML manifests, MkDocs Material.

---

### Task 1: Freeze the Journey manifest and historical snapshot generator

**Files:**
- Create: `journey/manifest.toml`
- Create: `journey/tools/extract_history.py`
- Test: `journey/tools/tests/test_extract_history.py`

- [ ] **Step 1: Write a failing manifest contract test**

Assert that all 30 entries have contiguous numbers, unique slugs, existing Git
endpoints, and the exact endpoint sequence from the design spec. Assert that
the final endpoint's owned `src/miniredis` and `examples` tree matches `main`.

- [ ] **Step 2: Run the focused test and observe the missing manifest failure**

Run: `uv run pytest -q journey/tools/tests/test_extract_history.py`

Expected: FAIL because `journey/manifest.toml` and its loader do not exist.

- [ ] **Step 3: Implement the manifest and snapshot extraction**

The manifest records `number`, `slug`, `endpoint`, tutorial `chapter`, and
focused test files. `extract_history.py` materializes only owned roots from each
endpoint, diffs adjacent snapshots, and writes deterministic `stage.patch`
files. Stage 01 is a diff from an empty owned tree.

- [ ] **Step 4: Verify deterministic extraction**

Run the extractor twice and assert the second run produces no Git diff.

- [ ] **Step 5: Commit**

```bash
git add journey/manifest.toml journey/tools/extract_history.py journey/tools/tests/test_extract_history.py
git commit -m "feat: define MiniRedis Journey history manifest"
```

### Task 2: Generalize the MiniS3 Journey chain builder for MiniRedis

**Files:**
- Create: `journey/tools/build_journey.py`
- Create: `journey/tools/tests/test_build_journey.py`
- Create: `journey/README.md`

- [ ] **Step 1: Write failing tests for stage discovery, patch coverage, and final parity**

Cover a clean cumulative build, an undeclared patch path, a missing focused
test, a non-contiguous Stage number, and a final source mismatch.

- [ ] **Step 2: Verify the tests fail before the builder exists**

Run: `uv run pytest -q journey/tools/tests/test_build_journey.py`

- [ ] **Step 3: Implement project-configured build, study, attempt, agent, and check modes**

The builder must use `src/miniredis`, `examples`, and the declared behavior test
roots rather than MiniS3 hard-coded paths. It must never require the learner to
switch branches.

- [ ] **Step 4: Verify all 30 stages in `--check` mode**

Run: `uv run python journey/tools/build_journey.py --check`

Expected: 30 Stage PASS lines followed by guard-chain and goal-parity PASS.

- [ ] **Step 5: Commit**

```bash
git add journey/tools/build_journey.py journey/tools/tests/test_build_journey.py journey/README.md
git commit -m "feat: add MiniRedis Journey chain builder"
```

### Task 3: Author bilingual Stage facts and explicit layout ownership

**Files:**
- Create: `journey/stages/01-*/goal.md` through `journey/stages/30-*/goal.md`
- Create: `journey/stages/01-*/tests.txt` through `journey/stages/30-*/tests.txt`
- Create: `journey/stages/01-*/layout.toml` through `journey/stages/30-*/layout.toml`
- Test: `journey/tools/tests/test_stage_contracts.py`

- [ ] **Step 1: Write failing content-contract tests**

Require every localized card heading, at least one concrete failure preview,
test-only `failure_files`, exact patch ownership, implementation key slices of
at most 15 nonblank lines, and no generic boilerplate or interview framing.

- [ ] **Step 2: Author cards in Stage batches 01–08, 09–16, 17–24, and 25–30**

For each batch, return to the endpoint source and focused tests. Explain the
current problem, concepts, necessity, runtime flow, and critical statements.
Group only package/config/docs files as supporting.

- [ ] **Step 3: Run the content test after each batch**

Run: `uv run pytest -q journey/tools/tests/test_stage_contracts.py`

- [ ] **Step 4: Run each batch's cumulative Stage tests**

Run `build_journey.py check N` for every newly authored Stage before moving to
the next batch.

- [ ] **Step 5: Commit each authored batch**

Use `docs: author MiniRedis Journey stages NN-NN` commit messages.

### Task 4: Render bilingual browser lessons

**Files:**
- Create: `journey/tools/render_pages.py`
- Create: `journey/tools/tests/test_render_pages.py`
- Create: `docs/journey/index.md` and `docs/journey/stage-01.md` through `stage-30.md`
- Create: `docs/zh/journey/index.md` and `docs/zh/journey/stage-01.md` through `stage-30.md`

- [ ] **Step 1: Port the MiniS3 renderer contracts as failing MiniRedis tests**

Require collapsed deliverables, test evidence before concepts, test-specific
labels, implementation diffs as mechanism separators, combined supporting
drawers, no empty mechanism section, and lossless exact patch coverage.

- [ ] **Step 2: Implement the manifest-driven renderer**

Replace MiniS3 package/path/tutorial mappings with MiniRedis manifest data.

- [ ] **Step 3: Generate both languages and verify idempotence**

Run the renderer twice; the second run must leave no diff.

- [ ] **Step 4: Commit**

```bash
git add journey/tools/render_pages.py journey/tools/tests/test_render_pages.py docs/journey docs/zh/journey
git commit -m "docs: render MiniRedis self-guided Journey"
```

### Task 5: Add direct Agent-guided entry

**Files:**
- Create: `AGENTS.md`
- Create: `docs/agent-guided.md`
- Create: `docs/zh/agent-guided.md`
- Test: `journey/tools/tests/test_agent_mode.py`

- [ ] **Step 1: Write failing tests for the direct prompt route**

Require root routing for `开始 Agent 带教 Stage NN`, Stage-specific workspace
preparation, canonical artifact loading, resume behavior, and no branch-switch
instruction.

- [ ] **Step 2: Implement the root router and short bilingual usage guides**

Keep lesson content in Stage artifacts; the public Agent pages only explain how
to start and what the agent will do.

- [ ] **Step 3: Verify two representative stages**

Prepare Stage 01 and Stage 26 Agent workspaces, rerun without `--yes`, and
confirm learner files are preserved while canonical `.journey/` facts refresh.

- [ ] **Step 4: Commit**

```bash
git add AGENTS.md docs/agent-guided.md docs/zh/agent-guided.md journey/tools/tests/test_agent_mode.py
git commit -m "docs: add direct MiniRedis Agent guidance"
```

### Task 6: Wire navigation, localization, CI, and public repository language

**Files:**
- Modify: `mkdocs.yml`
- Modify: `README.md`
- Modify: `README.zh-CN.md`
- Create: `.github/workflows/journey.yml`
- Test: `journey/tools/tests/test_docs_navigation.py`

- [ ] **Step 1: Write failing navigation and wording tests**

Require path-preserving EN/ZH switches for all 30 Journey pages, all three
mode links in both nav trees, and removal of the obsolete README claim that
course material is absent.

- [ ] **Step 2: Add nav entries and path-aware alternate links**

Follow the existing MiniS3 override approach, adapted to the `mini-redis` site
prefix.

- [ ] **Step 3: Add Journey chain CI**

Run `python journey/tools/build_journey.py --check` for changes under
`journey/**`, `src/**`, `tests/**`, `examples/**`, or the workflow.

- [ ] **Step 4: Commit**

```bash
git add mkdocs.yml README.md README.zh-CN.md .github/workflows/journey.yml journey/tools/tests/test_docs_navigation.py
git commit -m "docs: expose MiniRedis learning modes"
```

### Task 7: Complete the polished acceptance gate

**Files:**
- Modify only defects found by the checks above.

- [ ] **Step 1: Run runtime and tool tests**

```bash
uv run pytest -q
uv run python -m compileall -q src tests journey/tools
```

- [ ] **Step 2: Run the complete Stage chain**

```bash
uv run python journey/tools/build_journey.py --check
```

- [ ] **Step 3: Build documentation strictly**

```bash
uv run mkdocs build --strict
```

- [ ] **Step 4: Run local browser acceptance**

Verify Stage 01, 03, 11, 15, 20, 23, 26, and 30 in both languages, language
switch preservation, test-before-concept ordering, collapsed diffs, and the
Agent usage route.

- [ ] **Step 5: Review final parity and worktree state**

Require exact owned-file parity, no generated drift, `git diff --check`, and a
clean worktree after the final commit. Do not push without explicit user
authorization.

