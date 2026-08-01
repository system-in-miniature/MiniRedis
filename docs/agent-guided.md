# Agent-Guided Rebuild

Use this mode when you want Codex to guide one MiniRedis Stage interactively in
the terminal.

## 1. Open Codex in MiniRedis

```bash
cd MiniRedis
codex
```

## 2. Ask to start a Stage

Send this directly to Codex:

```text
开始 Agent 带教 Stage 03
```

Replace `03` with any Stage from `01` through `30`. Codex prepares the correct
starting state automatically and begins with a short understanding check before
teaching and implementing the Stage.

## 3. Continue or return later

Answer Codex's question, ask for a direct explanation, or say `继续`. If you
leave and later send the same Stage request again, Codex resumes your existing
progress automatically.

When the Stage is complete, Codex runs its cumulative tests and checks the
result against the canonical Stage boundary. Ask to reset the Stage only when
you intentionally want to discard that Stage's current progress.

