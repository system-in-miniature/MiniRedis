"""Contracts for direct, resumable Agent-guided Stage startup."""

from __future__ import annotations

from pathlib import Path
import subprocess
import sys

from journey.tools import build_journey


def run_cli(*arguments: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, str(build_journey.__file__), *arguments],
        cwd=build_journey.ROOT,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        check=False,
    )


def test_agent_prepares_clean_stage_baseline_then_resumes(tmp_path: Path) -> None:
    workspace = tmp_path / "stage-03"
    ready = run_cli("agent", "3", "--workspace", str(workspace), "--yes")

    assert ready.returncode == 0, ready.stdout
    assert "[agent stage-03] READY" in ready.stdout
    assert f"WORKSPACE: {workspace.resolve()}" in ready.stdout
    assert "CHECK:" in ready.stdout
    assert not (workspace / "AGENTS.md").exists()
    assert not (workspace / ".journey").exists()
    assert subprocess.run(
        ["git", "status", "--short"],
        cwd=workspace,
        text=True,
        capture_output=True,
        check=True,
    ).stdout == ""
    assert subprocess.run(
        ["git", "config", "--local", "--get", "journey.agentStage"],
        cwd=workspace,
        text=True,
        capture_output=True,
        check=True,
    ).stdout.strip() == "03"

    incomplete = run_cli("check", "3", "--workspace", str(workspace))
    assert incomplete.returncode != 0
    build_journey._apply_stage(build_journey.discover_stages()[2], workspace)
    complete = run_cli("check", "3", "--workspace", str(workspace))
    assert complete.returncode == 0, complete.stdout
    assert "[check stage-03] PASS" in complete.stdout

    note = workspace / "learner-note.txt"
    note.write_text("keep progress\n")
    resumed = run_cli("agent", "3", "--workspace", str(workspace))

    assert resumed.returncode == 0, resumed.stdout
    assert "[agent stage-03] RESUME" in resumed.stdout
    assert note.read_text() == "keep progress\n"


def test_agent_default_workspace_is_scoped_by_stage() -> None:
    stages = build_journey.discover_stages()
    assert build_journey.default_agent_workspace(stages[2]) == (
        build_journey.ROOT / ".journey-workspaces" / "stage-03"
    )
    assert build_journey.default_agent_workspace(stages[3]) == (
        build_journey.ROOT / ".journey-workspaces" / "stage-04"
    )


def test_agent_refuses_an_unmarked_repository_even_with_reset(tmp_path: Path) -> None:
    workspace = tmp_path / "unrelated"
    workspace.mkdir()
    subprocess.run(["git", "init", "-q"], cwd=workspace, check=True)

    result = run_cli("agent", "3", "--workspace", str(workspace), "--yes")

    assert result.returncode != 0
    assert "unmarked Git repository" in result.stdout


def test_agent_rejects_a_different_stage_without_explicit_reset(tmp_path: Path) -> None:
    workspace = tmp_path / "learner"
    assert run_cli("agent", "3", "--workspace", str(workspace), "--yes").returncode == 0

    result = run_cli("agent", "4", "--workspace", str(workspace))

    assert result.returncode != 0
    assert "prepared for Stage 03" in result.stdout
