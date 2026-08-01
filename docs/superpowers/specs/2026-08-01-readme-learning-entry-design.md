# MiniRedis README Learning Entry Design

## Goal

Make the hosted learning experience—and its three complementary learning
modes—a primary MiniRedis selling point visible in the first screen of both
root READMEs. Preserve the repository's technical depth below that entry.

## Audience decision

The README serves three reader needs in order:

1. Any visitor quickly understanding what the MiniRedis repository contains.
2. A learner choosing how to learn the project.
3. An engineer evaluating what MiniRedis implements and how it differs from
   production Redis.

The repository introduction must be short: identify MiniRedis as a compact,
Redis-inspired implementation and name only its broad mechanism coverage. The
learning experience then becomes the main README content before detailed
architecture and command documentation. Technical detail remains intact below
as evidence that the course rebuilds a substantive system rather than a toy.

## First-screen structure

Use this top-level reading order:

1. Title, badges, and a compact repository introduction.
2. A substantial learning-site section presenting the core product experience.
3. The existing detailed technical reference content.

The learning-site section contains:

- one primary call to action to open the hosted MiniRedis learning site;
- one sentence explaining that the same project can be learned in three ways;
- three concise mode entries with direct hosted-page links:
  - Mechanism Tutorial: understand the completed system by mechanism and
    ownership boundary;
  - Self-Guided Rebuild: rebuild it through 30 browser-native Stages;
  - Agent-Guided Rebuild: learn interactively with Codex from a prepared Stage
    baseline.

`README.md` links to the English pages. `README.zh-CN.md` uses equivalent
Chinese copy and links directly to the Chinese pages. Repository-relative
`docs/` links are not used as the primary calls to action.

## Content treatment

- Keep the existing technical overview, diagrams, command matrix, examples,
  limitations, and verification commands, but place them after the learning
  experience.
- Keep the initial repository introduction concise; do not front-load command,
  architecture, persistence, or replication details before the learning modes.
- Remove or reduce the current bottom learning-mode section after moving its
  useful meaning to the first screen, so the README does not repeat itself.
- Do not describe the learning model as interview preparation.
- Keep Agent-Guided Rebuild framed as an interactive mode; its web page remains
  a usage guide, while `AGENTS.md` remains the teaching contract.

## Hosted routes

English:

- Site: `https://system-in-miniature.github.io/mini-redis/`
- Mechanism Tutorial: `https://system-in-miniature.github.io/mini-redis/tutorial/`
- Self-Guided Rebuild: `https://system-in-miniature.github.io/mini-redis/journey/`
- Agent-Guided Rebuild: `https://system-in-miniature.github.io/mini-redis/agent-guided/`

Chinese:

- Site: `https://system-in-miniature.github.io/mini-redis/zh/`
- Mechanism Tutorial: `https://system-in-miniature.github.io/mini-redis/zh/tutorial/`
- Self-Guided Rebuild: `https://system-in-miniature.github.io/mini-redis/zh/journey/`
- Agent-Guided Rebuild: `https://system-in-miniature.github.io/mini-redis/zh/agent-guided/`

## Acceptance checks

- Both READMEs begin with a concise repository introduction, then expose the
  hosted site and all three modes before the first detailed technical section.
- English and Chinese links target their matching language routes.
- No learning mode is presented as secondary or optional.
- Existing technical detail is retained without duplicated learning-mode prose.
- Markdown links and the four hosted routes per language resolve successfully.
