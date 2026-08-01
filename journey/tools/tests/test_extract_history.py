"""Contracts for the historical source of the MiniRedis Journey chain."""

from __future__ import annotations

from pathlib import Path
import subprocess

from journey.tools import extract_history


ROOT = Path(__file__).resolve().parents[3]
MANIFEST = ROOT / "journey" / "manifest.toml"


def git(*arguments: str) -> str:
    return subprocess.run(
        ["git", *arguments],
        cwd=ROOT,
        check=True,
        text=True,
        stdout=subprocess.PIPE,
    ).stdout


def test_manifest_freezes_thirty_contiguous_unique_history_endpoints() -> None:
    manifest = extract_history.load_manifest(MANIFEST)

    assert [stage.number for stage in manifest.stages] == list(range(1, 31))
    assert len({stage.slug for stage in manifest.stages}) == 30
    assert len({stage.endpoint for stage in manifest.stages}) == 30
    assert manifest.stages[0].endpoint == "f68b061"
    assert manifest.stages[-1].endpoint == "8151fae"
    for stage in manifest.stages:
        git("cat-file", "-e", f"{stage.endpoint}^{{commit}}")


def test_final_endpoint_matches_main_for_production_and_examples() -> None:
    manifest = extract_history.load_manifest(MANIFEST)
    endpoint = manifest.stages[-1].endpoint

    assert git("diff", "--quiet", endpoint, "HEAD", "--", "src/miniredis", "examples") == ""


def test_patch_extraction_is_deterministic_and_reconstructs_owned_tree(
    tmp_path: Path,
) -> None:
    manifest = extract_history.load_manifest(MANIFEST)
    first = tmp_path / "first"
    second = tmp_path / "second"

    extract_history.extract_patches(manifest, first, repo_root=ROOT)
    extract_history.extract_patches(manifest, second, repo_root=ROOT)

    first_files = sorted(path.relative_to(first) for path in first.rglob("*.patch"))
    second_files = sorted(path.relative_to(second) for path in second.rglob("*.patch"))
    assert first_files == second_files
    assert first_files == [Path(f"stage-{number:02d}.patch") for number in range(1, 31)]
    for relative in first_files:
        assert (first / relative).read_bytes() == (second / relative).read_bytes()

    reconstructed = tmp_path / "reconstructed"
    reconstructed.mkdir()
    git("init", "-q", str(reconstructed))
    for relative in first_files:
        subprocess.run(
            ["git", "apply", str(first / relative)],
            cwd=reconstructed,
            check=True,
        )

    expected = extract_history.snapshot_files(
        manifest.stages[-1].endpoint,
        manifest,
        repo_root=ROOT,
    )
    actual = {
        path.relative_to(reconstructed).as_posix(): path.read_bytes()
        for path in reconstructed.rglob("*")
        if path.is_file() and ".git" not in path.parts
    }
    assert actual == expected
