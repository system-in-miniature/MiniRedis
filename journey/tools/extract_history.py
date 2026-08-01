#!/usr/bin/env python3
"""Extract deterministic Journey patches from frozen historical endpoints."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import shutil
import subprocess
import tempfile
import tomllib


ROOT = Path(__file__).resolve().parents[2]


@dataclass(frozen=True, slots=True)
class HistoryStage:
    number: int
    slug: str
    endpoint: str
    chapter: int
    tests: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class HistoryManifest:
    name: str
    package: str
    repository_url: str
    owned_roots: tuple[str, ...]
    owned_files: tuple[str, ...]
    stages: tuple[HistoryStage, ...]


def _run_git(
    repo_root: Path,
    *arguments: str,
    text: bool = False,
) -> subprocess.CompletedProcess[str] | subprocess.CompletedProcess[bytes]:
    return subprocess.run(
        ["git", *arguments],
        cwd=repo_root,
        check=True,
        text=text,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )


def load_manifest(path: Path) -> HistoryManifest:
    data = tomllib.loads(path.read_text())
    project = data["project"]
    raw_stages = data["stages"]
    stages = tuple(
        HistoryStage(
            number=item["number"],
            slug=item["slug"],
            endpoint=item["endpoint"],
            chapter=item["chapter"],
            tests=tuple(item["tests"]),
        )
        for item in raw_stages
    )
    numbers = [stage.number for stage in stages]
    if numbers != list(range(1, len(stages) + 1)):
        raise ValueError("Journey stage numbers must be contiguous from 1")
    slugs = [stage.slug for stage in stages]
    endpoints = [stage.endpoint for stage in stages]
    if len(slugs) != len(set(slugs)):
        raise ValueError("Journey stage slugs must be unique")
    if len(endpoints) != len(set(endpoints)):
        raise ValueError("Journey endpoints must be unique")
    return HistoryManifest(
        name=project["name"],
        package=project["package"],
        repository_url=project["repository_url"],
        owned_roots=tuple(project["owned_roots"]),
        owned_files=tuple(project["owned_files"]),
        stages=stages,
    )


def snapshot_files(
    endpoint: str,
    manifest: HistoryManifest,
    *,
    repo_root: Path = ROOT,
) -> dict[str, bytes]:
    requested = (*manifest.owned_roots, *manifest.owned_files)
    listing = _run_git(
        repo_root,
        "ls-tree",
        "-r",
        "--name-only",
        endpoint,
        "--",
        *requested,
        text=True,
    ).stdout
    assert isinstance(listing, str)
    paths = tuple(line for line in listing.splitlines() if line)
    return {
        path: _run_git(repo_root, "show", f"{endpoint}:{path}").stdout
        for path in paths
    }


def _replace_snapshot(workspace: Path, files: dict[str, bytes]) -> None:
    for child in workspace.iterdir():
        if child.name == ".git":
            continue
        if child.is_dir():
            shutil.rmtree(child)
        else:
            child.unlink()
    for relative, content in files.items():
        destination = workspace / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(content)


def _git_in(workspace: Path, *arguments: str) -> subprocess.CompletedProcess[bytes]:
    result = subprocess.run(
        ["git", *arguments],
        cwd=workspace,
        check=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    return result


def extract_patches(
    manifest: HistoryManifest,
    output: Path,
    *,
    repo_root: Path = ROOT,
) -> None:
    output.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="miniredis-journey-history-") as raw:
        workspace = Path(raw)
        _git_in(workspace, "init", "-q")
        _git_in(workspace, "config", "user.name", "MiniRedis Journey")
        _git_in(workspace, "config", "user.email", "journey@example.invalid")
        _git_in(workspace, "commit", "--allow-empty", "-q", "-m", "stage-00")

        for stage in manifest.stages:
            files = snapshot_files(stage.endpoint, manifest, repo_root=repo_root)
            _replace_snapshot(workspace, files)
            _git_in(workspace, "add", "-A")
            patch = _git_in(
                workspace,
                "diff",
                "--cached",
                "--binary",
                "--full-index",
                "--no-ext-diff",
                "HEAD",
                "--",
            ).stdout
            (output / f"stage-{stage.number:02d}.patch").write_bytes(patch)
            _git_in(
                workspace,
                "commit",
                "-q",
                "-m",
                f"stage-{stage.number:02d}",
            )


def install_stage_artifacts(
    manifest: HistoryManifest,
    stages_root: Path,
    *,
    repo_root: Path = ROOT,
) -> None:
    """Write canonical patches and focused-test lists without touching lessons."""

    stages_root.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="miniredis-journey-patches-") as raw:
        extracted = Path(raw)
        extract_patches(manifest, extracted, repo_root=repo_root)
        for stage in manifest.stages:
            directory = stages_root / f"{stage.number:02d}-{stage.slug}"
            directory.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(
                extracted / f"stage-{stage.number:02d}.patch",
                directory / "stage.patch",
            )
            directory.joinpath("tests.txt").write_text(
                "".join(f"{node}\n" for node in stage.tests)
            )


def main() -> int:
    manifest = load_manifest(ROOT / "journey" / "manifest.toml")
    install_stage_artifacts(manifest, ROOT / "journey" / "stages")
    print(f"installed {len(manifest.stages)} deterministic Journey stage artifacts")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
