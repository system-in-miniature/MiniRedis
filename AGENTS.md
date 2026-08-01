# MiniRedis Agent-Guided Rebuild Contract

This repository supports normal development and an Agent-Guided Rebuild mode.
Enter teaching mode when the learner says `开始 Agent 带教 Stage NN`, asks to
continue that Stage, or clearly requests the Agent-Guided Rebuild mode. Without
such a request, follow the user's ordinary repository request.

## Direct Session Startup

When the learner starts Stage NN from the canonical repository:

1. Parse NN as an integer from 1 through 30.
2. Run `python -m journey.tools.build_journey agent NN` from this canonical
   repository. Do not add `--yes` during ordinary startup.
3. Read the `WORKSPACE:` and `CHECK:` lines printed by the command.
4. Treat the returned path as the learner workspace for all edits, Git checks,
   tests, and code-reading line references. The learner does not need to change
   directories or understand the internal workspace layout.
5. A `READY` result starts a clean Stage N-1 baseline. A `RESUME` result keeps
   the learner's current changes and continues from them.

Only reset a Stage when the learner explicitly asks to discard its progress;
then rerun the same command with `--yes` after stating what will be replaced.
Never create or switch a teaching branch.

## Current Stage Sources

Discover the unique `journey/stages/NN-*` directory and read these canonical
files before teaching:

1. `goal.md` — authored test contract with its failure preview, concepts, file
   responsibilities, critical statements, and completion explanation.
2. `stage.patch` — the canonical implementation reference.
3. `tests.txt` — the cumulative verification nodes for this Stage.
4. `layout.toml` — the authored mechanism blocks, file ownership, localized
   summaries, and supporting-file classification.

The canonical patch is an Agent reference: do not ask the learner to read the
complete patch and do not quiz them on a symbol that has not appeared in the
learner workspace or in an excerpt you already explained.

## Session Startup

After preparing or resuming the requested Stage:

1. Give a short outcome, current limitation, deliverables, and first action.
2. Use 2-4 low-burden questions for quick misconception screening and
   metacognitive calibration. Prefer multiple choice with plausible competing
   mental models; do not reveal answers before the learner responds.
3. Explain only the concepts the learner does not already understand, then
   connect the failure preview to the mechanism being built.

The questions are an interactive advantage of this mode. Do not copy the
Self-Guided Rebuild page verbatim, and do not turn every paragraph into a quiz.
If the learner asks for a direct answer, says the questions are burdensome, or
asks to continue, answer concisely and move forward.

## Teaching And Implementation Flow

Use this order unless the learner explicitly changes it:

1. Orient around the current problem and the failure preview inside its test
   contract.
2. Run quick misconception screening and establish the minimum mental model.
3. Explain why the mechanism and ownership boundaries are necessary.
4. Implement the Stage in small coherent slices in the learner workspace.
5. Run focused checks after meaningful slices; tests are evidence, not a
   mandatory test-first lesson narrative.
6. Run the exact cumulative command printed as `CHECK:` during startup.
7. Walk through the authored mechanism blocks and account for every changed
   file within its owning block.
8. Ask the learner to explain the mechanism in their own words.

The agent owns implementation order and may edit the learner project directly.
Do not repeatedly ask for routine approval. Stop and ask only when scope or an
architecture choice is genuinely ambiguous.

## Concept And Code Understanding

Keep concept checks and code-understanding checks distinct.

- Concept checks cover why the boundary exists, state ownership, visibility,
  durability, failure recovery, and trade-offs.
- Code checks cover the actual current file, inputs, outputs, state changes,
  branches, and failure points.

For code reading:

1. Give the exact current workspace file and line range.
2. Select one function, one branch, or 5-15 lines.
3. Explain what the slice is and why it exists before asking a small question.
4. Explain critical statements causally: what would break if the line were
   removed, reordered, or changed.
5. If the learner says they do not understand, shrink to the current statement,
   variable, or call and explain it directly.

Never require the learner to scan an entire file to answer the first question.

## Mechanism Block Walkthrough

Use `layout.toml` as the teaching order. Start each block with its problem and
runtime relationship, then connect the files that jointly implement it. For
each substantive file within a core block cover:

- what the file/component is;
- why this Stage needs it;
- who calls or consumes it;
- relevant input, output, or state effect;
- at least one important 5-15 line slice;
- why its critical statements exist;
- which test or parity check covers it.

For a block marked `supporting = true`, give only its authored summary and a
short account of why the wiring is required. Do not invent per-file conceptual
questions for routine exports, documentation, lockfiles, or configuration.

## Completion

A Stage is complete only when:

- the implementation matches the Stage boundary;
- the exact `CHECK:` command passes tests and reference parity;
- every modified file has been explained;
- important code has been read through small anchored slices;
- material misconceptions have been corrected;
- the learner can connect problem, mechanism, key constraint, and failure
  consequence in their own words.

Passing tests alone is not completion. This is a learning model, not an
interview-preparation workflow; do not label the closing explanation as an
interview answer.
