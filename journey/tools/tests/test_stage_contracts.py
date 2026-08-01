"""Authored-content and ownership contracts for every MiniRedis Stage."""

from __future__ import annotations

from pathlib import Path
import re
import tomllib

from journey.tools import build_journey
from journey.tools.extract_history import load_manifest


ROOT = Path(__file__).resolve().parents[3]
STAGES_ROOT = ROOT / "journey" / "stages"

HEADINGS = {
    "en": (
        "### Goal",
        "### Deliverable files",
        "### The problem at this point",
        "### Failure preview",
        "### Test contract",
        "### Basic concepts",
        "### Why this mechanism is necessary",
        "### Runtime mental model",
        "### Mechanism blocks",
        "### Verification evidence",
        "### Durable takeaways",
        "### Explain it in your own words",
        "### Textbook",
    ),
    "zh": (
        "### 目标",
        "### 交付文件",
        "### 当前遇到的问题",
        "### 先看会坏在哪里",
        "### 测试契约",
        "### 基本概念",
        "### 为什么需要这个机制",
        "### 运行时心智模型",
        "### 机制板块",
        "### 验证证据",
        "### 需要真正记住的内容",
        "### 用自己的话讲清楚",
        "### 教材",
    ),
}


def localized(goal: str, language: str) -> str:
    if language == "en":
        return goal[goal.index("## English"):goal.index("## 中文")]
    return goal[goal.index("## 中文"):]


def authored_stages() -> tuple[build_journey.Stage, ...]:
    """Return the complete authored prefix while rejecting half-written stages."""

    manifest = load_manifest(ROOT / "journey" / "manifest.toml")
    result: list[build_journey.Stage] = []
    gap_seen = False
    for item in manifest.stages:
        directory = STAGES_ROOT / f"{item.number:02d}-{item.slug}"
        goal = directory / "goal.md"
        layout = directory / "layout.toml"
        authored = goal.is_file() or layout.is_file()
        assert goal.is_file() == layout.is_file(), f"stage-{item.number:02d} is half-authored"
        if not authored:
            gap_seen = True
            continue
        assert not gap_seen, f"stage-{item.number:02d} appears after an authored gap"
        result.append(
            build_journey.Stage(
                number=item.number,
                slug=item.slug,
                directory=directory,
                goal=goal,
                patch=directory / "stage.patch",
                tests=directory / "tests.txt",
                layout=layout,
            )
        )
    assert result, "at least one Stage must be authored"
    return tuple(result)


def test_manifest_and_stage_directories_have_one_to_one_identity() -> None:
    manifest = load_manifest(ROOT / "journey" / "manifest.toml")
    expected = [f"{stage.number:02d}-{stage.slug}" for stage in manifest.stages]
    actual = sorted(path.name for path in STAGES_ROOT.iterdir() if path.is_dir())
    assert actual == expected


def test_every_stage_has_complete_bilingual_lesson_in_teaching_order() -> None:
    for stage in authored_stages():
        goal = stage.goal.read_text()
        for language, headings in HEADINGS.items():
            body = localized(goal, language)
            positions = [body.index(heading) for heading in headings]
            assert positions == sorted(positions), (stage.label, language)
            for first, second in zip(headings, headings[1:]):
                section = body[body.index(first) + len(first):body.index(second)]
                assert section.strip(), (stage.label, language, first)


def test_layout_separates_test_evidence_from_mechanism_files() -> None:
    for stage in authored_stages():
        data = tomllib.loads(stage.layout.read_text())
        failure_files = data.get("failure_files", [])
        blocks = data.get("blocks", [])
        block_files = [path for block in blocks for path in block["files"]]
        changed = set(build_journey.patch_files(stage))
        expected_tests = {path for path in changed if path.startswith("tests/")}

        assert set(failure_files) == expected_tests, stage.label
        assert not expected_tests & set(block_files), stage.label
        assert set(failure_files) | set(block_files) == changed, stage.label
        assert len(failure_files) + len(block_files) == len(changed), stage.label


def test_file_lessons_cover_each_changed_file_once_without_boilerplate() -> None:
    forbidden = (
        "TODO",
        "TBD",
        "Supporting project wiring for this stage.",
        "本阶段所需的项目支撑接线。",
        "interview",
        "面试",
    )
    marker = re.compile(r"<!-- journey-file: ([^\n]+) -->")
    for stage in authored_stages():
        goal = stage.goal.read_text()
        expected = set(build_journey.patch_files(stage))
        for language in ("en", "zh"):
            body = localized(goal, language)
            paths = marker.findall(body)
            assert set(paths) == expected, (stage.label, language)
            assert len(paths) == len(set(paths)), (stage.label, language)
            for phrase in forbidden:
                assert phrase not in body, (stage.label, language, phrase)
