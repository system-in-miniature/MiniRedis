# MiniRedis README Learning Entry Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the hosted learning site and its three learning modes the main content immediately after a concise MiniRedis repository introduction in both root READMEs.

**Architecture:** Keep the English and Chinese READMEs structurally parallel. Each starts with repository identity, presents one hosted-site call to action and three direct mode links, then retains the existing technical reference sections below; the old bottom learning-mode section is removed to avoid duplication.

**Tech Stack:** GitHub-Flavored Markdown, GitHub Pages, MkDocs Material

---

### Task 1: Add the English learning entry

**Files:**
- Modify: `README.md`

- [x] **Step 1: Record the failing structural check**

Run:

```bash
python - <<'PY'
from pathlib import Path

body = Path("README.md").read_text()
intro = body.index("MiniRedis is a compact")
learning = body.index("## Learning modes")
technical = body.index("## Why Direct-first")
assert intro < learning < technical
assert "https://system-in-miniature.github.io/mini-redis/tutorial/" in body
assert "https://system-in-miniature.github.io/mini-redis/journey/" in body
assert "https://system-in-miniature.github.io/mini-redis/agent-guided/" in body
PY
```

Expected: FAIL because `## Learning modes` currently appears after the technical sections and uses repository-relative links.

- [x] **Step 2: Replace the late English learning section with the first-screen entry**

Immediately after the compact introductory paragraph, add:

```markdown
## Learn MiniRedis

**[Open the online learning site →](https://system-in-miniature.github.io/mini-redis/)**

MiniRedis is designed to be learned as well as read. Choose the path that
matches how you want to understand or rebuild the system:

| Learning mode | What it gives you | Start here |
|---|---|---|
| Mechanism Tutorial | Understand the completed system through Redis mechanisms, runtime flow, and ownership boundaries. | [Start the tutorial](https://system-in-miniature.github.io/mini-redis/tutorial/) |
| Self-Guided Rebuild | Rebuild MiniRedis through 30 browser-native Stages with test contracts, grouped diffs, critical statements, and cumulative evidence. | [Start Stage 01](https://system-in-miniature.github.io/mini-redis/journey/) |
| Agent-Guided Rebuild | Ask Codex to prepare or resume a Stage and guide the implementation interactively. | [Open the usage guide](https://system-in-miniature.github.io/mini-redis/agent-guided/) |

The three modes use the same implementation and mechanism boundaries. Tests
make failure motivation and completion evidence executable without forcing
every lesson into a test-first narrative.
```

Remove the old bottom `## Learning modes` section, including its repeated mode descriptions and explanatory paragraphs.

- [x] **Step 3: Run the English structural check**

Run:

```bash
python - <<'PY'
from pathlib import Path

body = Path("README.md").read_text()
intro = body.index("MiniRedis is a compact")
learning = body.index("## Learn MiniRedis")
technical = body.index("## Why Direct-first")
assert intro < learning < technical
assert "https://system-in-miniature.github.io/mini-redis/tutorial/" in body
assert "https://system-in-miniature.github.io/mini-redis/journey/" in body
assert "https://system-in-miniature.github.io/mini-redis/agent-guided/" in body
PY
```

Expected: exit code 0.

### Task 2: Add the Chinese learning entry

**Files:**
- Modify: `README.zh-CN.md`

- [x] **Step 1: Record the failing structural check**

Run:

```bash
python - <<'PY'
from pathlib import Path

body = Path("README.zh-CN.md").read_text()
intro = body.index("MiniRedis 是一个紧凑的")
learning = body.index("## 学习模式")
technical = body.index("## 为什么采用 Direct-first")
assert intro < learning < technical
assert "https://system-in-miniature.github.io/mini-redis/zh/tutorial/" in body
assert "https://system-in-miniature.github.io/mini-redis/zh/journey/" in body
assert "https://system-in-miniature.github.io/mini-redis/zh/agent-guided/" in body
PY
```

Expected: FAIL because `## 学习模式` currently appears after the technical sections and uses repository-relative links.

- [x] **Step 2: Replace the late Chinese learning section with the first-screen entry**

Immediately after the compact introductory paragraph, add:

```markdown
## 学习 MiniRedis

**[进入在线学习站点 →](https://system-in-miniature.github.io/mini-redis/zh/)**

MiniRedis 不只是供人阅读的实现，也是一套可以真正走完的学习模型。你可以按自己的目标选择路径：

| 学习模式 | 你会获得什么 | 从这里开始 |
|---|---|---|
| 机制教程 | 按 Redis 机制、运行链路与所有权边界理解完成后的系统。 | [开始教程](https://system-in-miniature.github.io/mini-redis/zh/tutorial/) |
| 自主重建 | 通过 30 个浏览器原生 Stage 重建 MiniRedis，逐步理解测试契约、机制分组、关键语句与累计证据。 | [从 Stage 01 开始](https://system-in-miniature.github.io/mini-redis/zh/journey/) |
| Agent 带教 | 让 Codex 准备或续接指定 Stage，并互动带你完成实现。 | [查看使用教程](https://system-in-miniature.github.io/mini-redis/zh/agent-guided/) |

三种模式使用同一套实现与机制边界。测试负责把错误动机和完成证据变成可执行契约，但不会强制每一课采用测试优先叙事。
```

Remove the old bottom `## 学习模式` section and its repeated explanatory paragraphs.

- [x] **Step 3: Run the Chinese structural check**

Run:

```bash
python - <<'PY'
from pathlib import Path

body = Path("README.zh-CN.md").read_text()
intro = body.index("MiniRedis 是一个紧凑的")
learning = body.index("## 学习 MiniRedis")
technical = body.index("## 为什么采用 Direct-first")
assert intro < learning < technical
assert "https://system-in-miniature.github.io/mini-redis/zh/tutorial/" in body
assert "https://system-in-miniature.github.io/mini-redis/zh/journey/" in body
assert "https://system-in-miniature.github.io/mini-redis/zh/agent-guided/" in body
PY
```

Expected: exit code 0.

### Task 3: Verify and publish the README entry

**Files:**
- Verify: `README.md`
- Verify: `README.zh-CN.md`

- [x] **Step 1: Check formatting and exact link inventory**

Run:

```bash
git diff --check
rg -n "system-in-miniature.github.io/mini-redis" README.md README.zh-CN.md
```

Expected: no whitespace errors and four matching-language hosted routes in each README.

- [x] **Step 2: Check the hosted routes**

Run:

```bash
for url in \
  https://system-in-miniature.github.io/mini-redis/ \
  https://system-in-miniature.github.io/mini-redis/tutorial/ \
  https://system-in-miniature.github.io/mini-redis/journey/ \
  https://system-in-miniature.github.io/mini-redis/agent-guided/ \
  https://system-in-miniature.github.io/mini-redis/zh/ \
  https://system-in-miniature.github.io/mini-redis/zh/tutorial/ \
  https://system-in-miniature.github.io/mini-redis/zh/journey/ \
  https://system-in-miniature.github.io/mini-redis/zh/agent-guided/
do
  curl -fsS -o /dev/null -w '%{http_code} %{url_effective}\n' "$url"
done
```

Expected: each route returns HTTP 200.

- [x] **Step 3: Run documentation regression tests**

Run:

```bash
uv run pytest -q journey/tools/tests
uvx --from mkdocs-material mkdocs build --strict --site-dir /tmp/miniredis-readme-site
```

Expected: Journey tests pass and the strict documentation build exits 0.

- [ ] **Step 4: Commit the implementation**

Run:

```bash
git add -- README.md README.zh-CN.md docs/superpowers/plans/2026-08-01-readme-learning-entry.md
git commit -m "docs: make learning modes the README entry"
```

- [ ] **Step 5: Push and verify**

Run:

```bash
git push origin main
test "$(git rev-parse HEAD)" = "$(git rev-parse origin/main)"
test -z "$(git status --porcelain)"
```

Expected: push succeeds, local and remote `main` match, and the worktree is clean.
