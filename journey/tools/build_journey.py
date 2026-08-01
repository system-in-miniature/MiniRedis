#!/usr/bin/env python3
"""Build, verify, and prepare MiniRedis Journey workspaces."""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tempfile

from journey.tools.extract_history import HistoryManifest, load_manifest


ROOT = Path(__file__).resolve().parents[2]
STAGES_ROOT = ROOT / "journey" / "stages"
MANIFEST_PATH = ROOT / "journey" / "manifest.toml"
PATCH_HEADER = re.compile(r"^diff --git a/(.+?) b/(.+?)$", re.MULTILINE)
DELIVERABLE = re.compile(r"^- `([^`]+)`$", re.MULTILINE)


class JourneyError(RuntimeError):
    """A canonical Stage or reconstructed workspace violated its contract."""


@dataclass(frozen=True, slots=True)
class Stage:
    number: int
    slug: str
    directory: Path
    goal: Path
    patch: Path
    tests: Path
    layout: Path

    @property
    def label(self) -> str:
        return f"stage-{self.number:02d}"


@dataclass(frozen=True, slots=True)
class StageCheck:
    stage_number: int
    passed: bool
    test_output: str


def _run(
    command: list[str],
    *,
    cwd: Path,
    capture: bool = True,
) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(
        command,
        cwd=cwd,
        text=True,
        stdout=subprocess.PIPE if capture else None,
        stderr=subprocess.STDOUT if capture else None,
    )
    if result.returncode != 0:
        output = result.stdout or ""
        raise JourneyError(
            f"command failed ({' '.join(command)}):\n{output.rstrip()}"
        )
    return result


def discover_stages(stages_root: Path = STAGES_ROOT) -> tuple[Stage, ...]:
    if not stages_root.is_dir():
        raise JourneyError(f"missing Journey stages directory: {stages_root}")
    stages: list[Stage] = []
    for directory in sorted(path for path in stages_root.iterdir() if path.is_dir()):
        match = re.fullmatch(r"(\d{2})-([a-z0-9-]+)", directory.name)
        if match is None:
            raise JourneyError(f"invalid Stage directory name: {directory.name}")
        stage = Stage(
            number=int(match.group(1)),
            slug=match.group(2),
            directory=directory,
            goal=directory / "goal.md",
            patch=directory / "stage.patch",
            tests=directory / "tests.txt",
            layout=directory / "layout.toml",
        )
        missing = [
            path.name
            for path in (stage.goal, stage.patch, stage.tests, stage.layout)
            if not path.is_file()
        ]
        if missing:
            raise JourneyError(f"{stage.label} missing artifacts: {missing}")
        if not focused_tests(stage):
            raise JourneyError(f"{stage.label} requires at least one focused test")
        stages.append(stage)
    numbers = [stage.number for stage in stages]
    if numbers != list(range(1, len(stages) + 1)):
        raise JourneyError("Journey Stage numbers must be contiguous from 1")
    if not stages:
        raise JourneyError("Journey requires at least one Stage")
    return tuple(stages)


def focused_tests(stage: Stage) -> tuple[str, ...]:
    return tuple(
        line.strip()
        for line in stage.tests.read_text().splitlines()
        if line.strip() and not line.lstrip().startswith("#")
    )


def patch_files(stage: Stage) -> tuple[str, ...]:
    paths = tuple(match.group(2) for match in PATCH_HEADER.finditer(stage.patch.read_text()))
    if not paths and stage.patch.read_text():
        raise JourneyError(f"{stage.label} patch has no diff headers")
    if len(paths) != len(set(paths)):
        raise JourneyError(f"{stage.label} patch contains duplicate file sections")
    return paths


def goal_files(stage: Stage) -> set[str]:
    return set(DELIVERABLE.findall(stage.goal.read_text()))


def validate_stage(stage: Stage) -> None:
    changed = set(patch_files(stage))
    declared = goal_files(stage)
    if changed != declared:
        missing = sorted(changed - declared)
        extra = sorted(declared - changed)
        raise JourneyError(
            f"{stage.label} goal coverage mismatch; missing={missing}, extra={extra}"
        )


def _tree_files(root: Path, paths: tuple[str, ...]) -> dict[str, bytes]:
    result: dict[str, bytes] = {}
    for relative in paths:
        candidate = root / relative
        if candidate.is_file():
            result[relative] = candidate.read_bytes()
            continue
        if not candidate.exists():
            continue
        for path in sorted(item for item in candidate.rglob("*") if item.is_file()):
            if "__pycache__" in path.parts or path.suffix in {".pyc", ".pyo"}:
                continue
            result[path.relative_to(root).as_posix()] = path.read_bytes()
    return result


def assert_tree_parity(
    rebuilt: Path,
    reference: Path,
    *,
    roots: tuple[str, ...],
) -> None:
    rebuilt_files = _tree_files(rebuilt, roots)
    reference_files = _tree_files(reference, roots)
    if rebuilt_files == reference_files:
        return
    rebuilt_paths = set(rebuilt_files)
    reference_paths = set(reference_files)
    missing = sorted(reference_paths - rebuilt_paths)
    extra = sorted(rebuilt_paths - reference_paths)
    changed = sorted(
        path
        for path in rebuilt_paths & reference_paths
        if rebuilt_files[path] != reference_files[path]
    )
    raise JourneyError(
        f"final parity mismatch; missing={missing}, extra={extra}, changed={changed}"
    )


def _apply_stage(stage: Stage, workspace: Path) -> None:
    # Consecutive historical patches can rewrite a same-sized module within one
    # filesystem timestamp tick. Timestamp-based pyc validation may then load
    # the preceding Stage even though the source patch applied successfully.
    for cache in sorted(workspace.rglob("__pycache__"), reverse=True):
        if cache.is_dir():
            shutil.rmtree(cache)
    for bytecode in (*workspace.rglob("*.pyc"), *workspace.rglob("*.pyo")):
        bytecode.unlink()
    _run(["git", "apply", "--check", str(stage.patch)], cwd=workspace)
    _run(["git", "apply", str(stage.patch)], cwd=workspace)


def _run_stage_tests(stage: Stage, workspace: Path) -> str:
    nodes = focused_tests(stage)
    for node in nodes:
        if not (workspace / node.split("::", 1)[0]).exists():
            raise JourneyError(f"{stage.label} focused test is absent: {node}")
    result = _run(
        [sys.executable, "-m", "pytest", "-q", *nodes],
        cwd=workspace,
    )
    return result.stdout or ""


def check_chain(
    stages: tuple[Stage, ...],
    *,
    reference_root: Path,
    parity_roots: tuple[str, ...],
) -> tuple[StageCheck, ...]:
    for stage in stages:
        validate_stage(stage)
    results: list[StageCheck] = []
    with tempfile.TemporaryDirectory(prefix="miniredis-journey-chain-") as raw:
        workspace = Path(raw)
        _run(["git", "init", "-q"], cwd=workspace)
        for stage in stages:
            _apply_stage(stage, workspace)
            output = _run_stage_tests(stage, workspace)
            results.append(StageCheck(stage.number, True, output))
        assert_tree_parity(
            workspace,
            reference_root,
            roots=parity_roots,
        )
    return tuple(results)


def parity_roots(manifest: HistoryManifest) -> tuple[str, ...]:
    return (*manifest.owned_roots, *manifest.owned_files)


def prepare_workspace(
    stages: tuple[Stage, ...],
    count: int,
    destination: Path,
    *,
    apply_current: bool,
) -> None:
    resolved = destination.resolve()
    forbidden = {Path("/"), Path.home().resolve(), ROOT.resolve(), ROOT.parent.resolve()}
    if resolved in forbidden:
        raise JourneyError(f"refusing to reset unsafe workspace path: {resolved}")
    if destination.exists():
        shutil.rmtree(destination)
    destination.mkdir(parents=True)
    _run(["git", "init", "-q"], cwd=destination)
    for stage in stages[:count]:
        _apply_stage(stage, destination)
        _run(["git", "add", "-A"], cwd=destination)
        _run(
            [
                "git",
                "-c",
                "user.name=MiniRedis Journey",
                "-c",
                "user.email=journey@example.invalid",
                "commit",
                "-q",
                "-m",
                stage.label,
            ],
            cwd=destination,
        )
    if apply_current:
        _apply_stage(stages[count], destination)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true", dest="check_chain")
    subparsers = parser.add_subparsers(dest="command")
    for command in ("study", "attempt", "agent", "check"):
        child = subparsers.add_parser(command)
        child.add_argument("stage", type=int)
        child.add_argument("--workspace", type=Path)
        if command in {"study", "attempt", "agent"}:
            child.add_argument("--yes", action="store_true")
    arguments = parser.parse_args()

    manifest = load_manifest(MANIFEST_PATH)
    stages = discover_stages()
    if len(stages) != len(manifest.stages):
        raise JourneyError(
            f"manifest has {len(manifest.stages)} stages but artifacts have {len(stages)}"
        )
    if arguments.command is None:
        if not arguments.check_chain:
            parser.error("use --check or a learner command")
        results = check_chain(
            stages,
            reference_root=ROOT,
            parity_roots=parity_roots(manifest),
        )
        for result in results:
            print(f"[stage-{result.stage_number:02d}] PASS")
        print("[guard-chain] PASS Journey-owned files match main")
        print("[goal-parity] PASS every patch file is declared by its goal")
        return 0

    if not 1 <= arguments.stage <= len(stages):
        raise JourneyError(f"Stage must be between 1 and {len(stages)}")
    selected = stages[arguments.stage - 1]
    default = ROOT.parent / "MiniRedis-journey-workspace"
    destination = arguments.workspace or default
    if arguments.command == "check":
        output = _run_stage_tests(selected, destination)
        print(output.rstrip())
        return 0
    if destination.exists() and not arguments.yes:
        raise JourneyError(f"workspace already exists: {destination}; pass --yes to reset")
    baseline_count = selected.number - 1
    prepare_workspace(
        stages,
        baseline_count,
        destination,
        apply_current=arguments.command == "study",
    )
    print(f"prepared {arguments.command} Stage {selected.number:02d} at {destination}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except JourneyError as error:
        raise SystemExit(f"journey failed: {error}") from error
