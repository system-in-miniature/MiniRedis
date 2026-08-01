"""Contracts for reconstructing and checking cumulative Journey stages."""

from __future__ import annotations

from pathlib import Path
import subprocess

import pytest

from journey.tools import build_journey


def write_stage(
    stages_root: Path,
    number: int,
    slug: str,
    *,
    patch: str,
    tests: str,
    deliverables: tuple[str, ...],
) -> None:
    directory = stages_root / f"{number:02d}-{slug}"
    directory.mkdir(parents=True)
    listed = "\n".join(f"- `{path}`" for path in deliverables)
    directory.joinpath("goal.md").write_text(
        f"# Stage {number:02d} · Example / 示例\n\n"
        "## English\n\n### Goal\n\nExample.\n\n"
        f"### Deliverable files\n\n{listed}\n\n"
        "## 中文\n\n### 目标\n\n示例。\n\n"
        f"### 交付文件\n\n{listed}\n"
    )
    directory.joinpath("stage.patch").write_text(patch)
    directory.joinpath("tests.txt").write_text(tests)
    directory.joinpath("layout.toml").write_text("failure_files = []\n")


def test_patch_files_and_goal_coverage_are_exact(tmp_path: Path) -> None:
    patch = (
        "diff --git a/src/example.py b/src/example.py\n"
        "new file mode 100644\n"
        "--- /dev/null\n"
        "+++ b/src/example.py\n"
        "@@ -0,0 +1 @@\n"
        "+VALUE = 1\n"
    )
    stages_root = tmp_path / "stages"
    write_stage(
        stages_root,
        1,
        "example",
        patch=patch,
        tests="tests/test_example.py\n",
        deliverables=("src/example.py",),
    )
    stage = build_journey.discover_stages(stages_root)[0]

    assert build_journey.patch_files(stage) == ("src/example.py",)
    assert build_journey.goal_files(stage) == {"src/example.py"}

    stage.goal.write_text(stage.goal.read_text().replace("src/example.py", "src/wrong.py"))
    with pytest.raises(build_journey.JourneyError, match="goal coverage"):
        build_journey.validate_stage(stage)


def test_discovery_rejects_non_contiguous_numbers_and_missing_tests(tmp_path: Path) -> None:
    stages_root = tmp_path / "stages"
    write_stage(
        stages_root,
        2,
        "late",
        patch="",
        tests="tests/test_late.py\n",
        deliverables=(),
    )
    with pytest.raises(build_journey.JourneyError, match="contiguous"):
        build_journey.discover_stages(stages_root)

    directory = stages_root / "02-late"
    directory.rename(stages_root / "01-late")
    (stages_root / "01-late" / "tests.txt").write_text("")
    with pytest.raises(build_journey.JourneyError, match="focused test"):
        build_journey.discover_stages(stages_root)


def test_final_parity_reports_owned_file_drift(tmp_path: Path) -> None:
    reference = tmp_path / "reference"
    rebuilt = tmp_path / "rebuilt"
    for root, value in ((reference, "one"), (rebuilt, "two")):
        path = root / "src" / "example.py"
        path.parent.mkdir(parents=True)
        path.write_text(f"VALUE = {value!r}\n")

    with pytest.raises(build_journey.JourneyError, match="final parity"):
        build_journey.assert_tree_parity(
            rebuilt,
            reference,
            roots=("src",),
        )


def test_workspace_preparation_rejects_broad_destructive_targets() -> None:
    for destination in (Path("/"), Path.home(), build_journey.ROOT):
        with pytest.raises(build_journey.JourneyError, match="unsafe workspace"):
            build_journey.prepare_workspace(
                (),
                0,
                destination,
                apply_current=False,
            )


def test_two_stage_chain_runs_focused_tests_and_reaches_final_parity(
    tmp_path: Path,
) -> None:
    reference = tmp_path / "reference"
    stages_root = reference / "journey" / "stages"
    source = reference / "src" / "example.py"
    test_file = reference / "tests" / "test_example.py"
    source.parent.mkdir(parents=True)
    test_file.parent.mkdir(parents=True)
    reference.joinpath("pyproject.toml").write_text(
        "[tool.pytest.ini_options]\npythonpath = ['src']\n"
    )
    source.write_text("VALUE = 2\n")
    test_file.write_text(
        "from example import VALUE\n\ndef test_value():\n    assert VALUE == 2\n"
    )

    patch_one = """diff --git a/pyproject.toml b/pyproject.toml
new file mode 100644
--- /dev/null
+++ b/pyproject.toml
@@ -0,0 +1,2 @@
+[tool.pytest.ini_options]
+pythonpath = ['src']
diff --git a/src/example.py b/src/example.py
new file mode 100644
--- /dev/null
+++ b/src/example.py
@@ -0,0 +1 @@
+VALUE = 1
diff --git a/tests/test_example.py b/tests/test_example.py
new file mode 100644
--- /dev/null
+++ b/tests/test_example.py
@@ -0,0 +1,4 @@
+from example import VALUE
+
+def test_value():
+    assert VALUE == 1
"""
    patch_two = """diff --git a/src/example.py b/src/example.py
index 6257cd3..85de9df 100644
--- a/src/example.py
+++ b/src/example.py
@@ -1 +1 @@
-VALUE = 1
+VALUE = 2
diff --git a/tests/test_example.py b/tests/test_example.py
index 7408b39..a8b5801 100644
--- a/tests/test_example.py
+++ b/tests/test_example.py
@@ -1,4 +1,4 @@
 from example import VALUE

 def test_value():
-    assert VALUE == 1
+    assert VALUE == 2
"""
    write_stage(
        stages_root,
        1,
        "one",
        patch=patch_one,
        tests="tests/test_example.py\n",
        deliverables=("pyproject.toml", "src/example.py", "tests/test_example.py"),
    )
    write_stage(
        stages_root,
        2,
        "two",
        patch=patch_two,
        tests="tests/test_example.py\n",
        deliverables=("src/example.py", "tests/test_example.py"),
    )

    stages = build_journey.discover_stages(stages_root)
    results = build_journey.check_chain(
        stages,
        reference_root=reference,
        parity_roots=("pyproject.toml", "src", "tests"),
    )

    assert [result.stage_number for result in results] == [1, 2]
    assert all(result.passed for result in results)
    subprocess.run(
        ["git", "apply", "--check", str(stages[0].patch)],
        cwd=tmp_path,
        check=True,
    )
