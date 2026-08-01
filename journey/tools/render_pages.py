#!/usr/bin/env python3
"""Render Journey task cards and indexes from stage facts."""

from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
import re
import subprocess
import tomllib


ROOT = Path(__file__).resolve().parents[2]
STAGES_ROOT = ROOT / "journey" / "stages"
MANIFEST = tomllib.loads((ROOT / "journey" / "manifest.toml").read_text())
ENDPOINT_BY_STAGE = {
    item["number"]: item["endpoint"] for item in MANIFEST["stages"]
}
REPO_URL = "https://github.com/system-in-miniature/mini-redis"
META = re.compile(
    r"<!-- journey: chapter=(?P<chapter>\d+) tests_added=(?P<tests>\d+) -->"
)


@dataclass(frozen=True)
class Card:
    number: int
    slug: str
    english_title: str
    chinese_title: str
    chapter: int
    tests_added: int
    english: str
    chinese: str
    patch: str
    failure_files: tuple[str, ...]
    blocks: tuple["BlockLayout", ...]


@dataclass(frozen=True)
class BlockLayout:
    id: str
    title_en: str
    title_zh: str
    summary_en: str
    summary_zh: str
    files: tuple[str, ...]
    supporting: bool = False


@dataclass(frozen=True)
class FilePatch:
    path: str
    patch: str
    role_rank: int
    changed_symbols: tuple[str, ...]


@dataclass(frozen=True)
class FileLesson:
    path: str
    body: str
    code_slices: tuple[str, ...]


@dataclass(frozen=True)
class LocalizedLesson:
    pre_walkthrough: str
    files: tuple[FileLesson, ...]
    post_walkthrough: str


PATCH_HEADER = re.compile(r"^diff --git a/(.+?) b/(.+?)$", re.MULTILINE)
PYTHON_SYMBOL = re.compile(r"^\+\s*(?:async\s+)?(?:def|class)\s+(\w+)", re.MULTILINE)
TEST_SYMBOL = re.compile(r"^\+def\s+(test_\w+)", re.MULTILINE)
FILE_MARKER = re.compile(r"<!-- journey-file: ([^\n]+) -->")
CODE_FENCE = re.compile(
    r"^##### (?:Key code|关键代码)\s*\n+```[^\n]*\n(.*?)\n```",
    re.MULTILINE | re.DOTALL,
)

REQUIRED_HEADINGS = {
    False: (
        "### Goal",
        "### Deliverable files",
        "### The problem at this point",
        "### Failure preview",
        "### Basic concepts",
        "### Why this mechanism is necessary",
        "### Runtime mental model",
        "### Mechanism blocks",
        "### Verification evidence",
        "### Durable takeaways",
        "### Explain it in your own words",
        "### Textbook",
    ),
    True: (
        "### 目标",
        "### 交付文件",
        "### 当前遇到的问题",
        "### 先看会坏在哪里",
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


def role_rank(path: str) -> int:
    if path in {
        "src/miniredis/errors.py",
        "src/miniredis/model.py",
        "src/miniredis/multipart.py",
        "src/miniredis/conditional.py",
        "src/miniredis/lifecycle.py",
    }:
        return 0
    if path == "src/miniredis/bucket.py":
        return 1
    if path.startswith("src/miniredis/storage/"):
        return 2
    if path == "src/miniredis/listing.py":
        return 3
    if path == "src/miniredis/store.py":
        return 4
    if path == "src/miniredis/__init__.py":
        return 5
    if path.startswith("tests/"):
        return 6
    return 7


def split_file_patches(patch: str) -> list[FilePatch]:
    matches = list(PATCH_HEADER.finditer(patch))
    if not matches and patch:
        raise ValueError("stage patch has no diff --git headers")
    result: list[FilePatch] = []
    for index, match in enumerate(matches):
        end = matches[index + 1].start() if index + 1 < len(matches) else len(patch)
        file_patch = patch[match.start():end]
        path = match.group(2)
        result.append(
            FilePatch(
                path=path,
                patch=file_patch,
                role_rank=role_rank(path),
                changed_symbols=tuple(dict.fromkeys(PYTHON_SYMBOL.findall(file_patch))),
            )
        )
    return result


def order_file_patches(file_patches: list[FilePatch]) -> list[FilePatch]:
    return sorted(file_patches, key=lambda item: item.role_rank)


def section(text: str, heading: str, next_heading: str | None) -> str:
    start = text.index(heading) + len(heading)
    end = len(text) if next_heading is None else text.index(next_heading, start)
    return text[start:end].strip()


def load_block_layouts(
    path: Path,
    *,
    stage_label: str,
    patch_paths: set[str],
) -> tuple[tuple[str, ...], tuple[BlockLayout, ...]]:
    data = tomllib.loads(path.read_text())
    raw_failure_files = data.get("failure_files", [])
    if (
        not isinstance(raw_failure_files, list)
        or any(not isinstance(item, str) or not item for item in raw_failure_files)
    ):
        raise ValueError(f"{stage_label}: failure_files must be a list of paths")
    failure_files = tuple(raw_failure_files)
    expected_failure_files = tuple(sorted(path for path in patch_paths if path.startswith("tests/")))
    if tuple(sorted(failure_files)) != expected_failure_files:
        raise ValueError(
            f"{stage_label}: failure_files must own every tests/ path exactly once; "
            f"expected={list(expected_failure_files)}, actual={list(failure_files)}"
        )

    raw_blocks = data.get("blocks", [])
    if not isinstance(raw_blocks, list):
        raise ValueError(f"{stage_label}: blocks must be a list")

    blocks: list[BlockLayout] = []
    for index, raw in enumerate(raw_blocks, start=1):
        if not isinstance(raw, dict):
            raise ValueError(f"{stage_label}: block {index} must be a table")
        required = ("id", "title_en", "title_zh", "summary_en", "summary_zh", "files")
        missing = [field for field in required if field not in raw]
        if missing:
            raise ValueError(f"{stage_label}: block {index} missing {missing}")
        block_id = raw["id"]
        if not isinstance(block_id, str) or not re.fullmatch(r"[a-z0-9-]+", block_id):
            raise ValueError(f"{stage_label}: block {index} has invalid id")
        strings = {field: raw[field] for field in required[1:-1]}
        if any(not isinstance(value, str) or not value.strip() for value in strings.values()):
            raise ValueError(f"{stage_label}: block {block_id} has an empty localized field")
        files = raw["files"]
        if (
            not isinstance(files, list)
            or not files
            or any(not isinstance(item, str) or not item for item in files)
        ):
            raise ValueError(f"{stage_label}: block {block_id} requires a non-empty file list")
        test_paths = [item for item in files if item.startswith("tests/")]
        if test_paths:
            raise ValueError(
                f"{stage_label}: tests belong to failure_files, not mechanism block "
                f"{block_id}: {test_paths}"
            )
        supporting = raw.get("supporting", False)
        if not isinstance(supporting, bool):
            raise ValueError(f"{stage_label}: block {block_id} supporting must be boolean")
        blocks.append(
            BlockLayout(
                id=block_id,
                title_en=raw["title_en"].strip(),
                title_zh=raw["title_zh"].strip(),
                summary_en=raw["summary_en"].strip(),
                summary_zh=raw["summary_zh"].strip(),
                files=tuple(files),
                supporting=supporting,
            )
        )

    ids = [block.id for block in blocks]
    if len(ids) != len(set(ids)):
        raise ValueError(f"{stage_label}: duplicate block id")
    owned = [*failure_files, *(file for block in blocks for file in block.files)]
    duplicates = sorted({file for file in owned if owned.count(file) > 1})
    missing_paths = sorted(patch_paths - set(owned))
    extra_paths = sorted(set(owned) - patch_paths)
    if duplicates or missing_paths or extra_paths:
        raise ValueError(
            f"{stage_label}: block coverage mismatch; duplicates={duplicates}, "
            f"missing={missing_paths}, extra={extra_paths}"
        )
    return failure_files, tuple(blocks)


def load_cards() -> list[Card]:
    cards: list[Card] = []
    for directory in sorted(STAGES_ROOT.iterdir()):
        if not directory.is_dir():
            continue
        number_text, slug = directory.name.split("-", 1)
        goal = (directory / "goal.md").read_text()
        title_line = goal.splitlines()[0]
        title_match = re.fullmatch(r"# Stage \d{2} · (.+) / (.+)", title_line)
        meta_match = META.search(goal)
        if title_match is None or meta_match is None:
            raise ValueError(f"invalid goal metadata: {directory}")
        patch = (directory / "stage.patch").read_text()
        patch_paths = {item.path for item in split_file_patches(patch)}
        failure_files, blocks = load_block_layouts(
            directory / "layout.toml",
            stage_label=f"stage-{number_text}",
            patch_paths=patch_paths,
        )
        cards.append(
            Card(
                number=int(number_text),
                slug=slug,
                english_title=title_match.group(1),
                chinese_title=title_match.group(2),
                chapter=int(meta_match["chapter"]),
                tests_added=int(meta_match["tests"]),
                english=section(goal, "## English", "## 中文"),
                chinese=section(goal, "## 中文", None),
                patch=patch,
                failure_files=failure_files,
                blocks=blocks,
            )
        )
    return cards


def compare_link(number: int) -> str:
    endpoint = ENDPOINT_BY_STAGE[number]
    if number == 1:
        return f"{REPO_URL}/tree/{endpoint}"
    return f"{REPO_URL}/compare/{ENDPOINT_BY_STAGE[number - 1]}...{endpoint}"


def _heading_positions(body: str, headings: tuple[str, ...], *, label: str) -> None:
    positions: list[int] = []
    for heading in headings:
        occurrences = [match.start() for match in re.finditer(rf"^{re.escape(heading)}$", body, re.MULTILINE)]
        if len(occurrences) != 1:
            raise ValueError(f"{label}: expected one {heading!r}, found {len(occurrences)}")
        positions.append(occurrences[0])
    if positions != sorted(positions):
        raise ValueError(f"{label}: required lesson headings are out of order")


def _post_patch_text(file_patch: FilePatch) -> str:
    """Return changed-hunk after-state text for authored slice validation."""

    lines: list[str] = []
    inside_hunk = False
    for line in file_patch.patch.splitlines():
        if line.startswith("@@"):
            inside_hunk = True
            continue
        if not inside_hunk or line.startswith("\\ No newline"):
            continue
        if line.startswith("+") and not line.startswith("+++"):
            lines.append(line[1:].rstrip())
        elif line.startswith(" "):
            lines.append(line[1:].rstrip())
    return "\n".join(lines)


def _normalize_slice(code: str) -> str:
    lines = [line.rstrip() for line in code.strip("\n").splitlines()]
    if not lines:
        return ""
    indent = min((len(line) - len(line.lstrip()) for line in lines if line.strip()), default=0)
    return "\n".join(line[indent:] if line.strip() else "" for line in lines)


def _slice_in_patch(code: str, after_text: str) -> bool:
    wanted = _normalize_slice(code)
    wanted_lines = wanted.splitlines()
    source_lines = after_text.splitlines()
    for start in range(len(source_lines) - len(wanted_lines) + 1):
        window = "\n".join(source_lines[start:start + len(wanted_lines)])
        if _normalize_slice(window) == wanted:
            return True
    return False


@lru_cache(maxsize=None)
def _stage_file_text(card_number: int, path: str) -> str:
    endpoint = ENDPOINT_BY_STAGE[card_number]
    result = subprocess.run(
        ["git", "show", f"{endpoint}:{path}"],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise ValueError(
            f"stage-{card_number:02d} {path}: cannot read endpoint {endpoint}"
        )
    return result.stdout


def _parse_region_file_lessons(
    region: str,
    *,
    region_name: str,
    label: str,
    card_number: int,
    patch_by_path: dict[str, FilePatch],
) -> list[FileLesson]:
    """Bind single-file and consecutive-marker group explanations."""

    markers = list(FILE_MARKER.finditer(region))
    files: list[FileLesson] = []
    index = 0
    while index < len(markers):
        last = index
        while (
            last + 1 < len(markers)
            and not region[markers[last].end():markers[last + 1].start()].strip()
        ):
            last += 1
        end = markers[last + 1].start() if last + 1 < len(markers) else len(region)
        file_body = region[markers[last].end():end].strip()
        if not file_body.startswith("#### "):
            paths = [markers[item].group(1).strip() for item in range(index, last + 1)]
            raise ValueError(f"{label} {paths}: marker group requires one #### explanation")

        paths = [markers[item].group(1).strip() for item in range(index, last + 1)]
        for path in paths:
            if path not in patch_by_path:
                raise ValueError(f"{label} {path}: file is absent from stage.patch")
            if (path.startswith("tests/")) != (region_name == "test contract"):
                raise ValueError(
                    f"{label} {path}: tests belong in the test contract and production "
                    "files belong in mechanism blocks"
                )

        slices = tuple(_normalize_slice(match) for match in CODE_FENCE.findall(file_body))
        for code in slices:
            if len([line for line in code.splitlines() if line.strip()]) > 15:
                raise ValueError(
                    f"{label} {paths}: key code must contain at most 15 nonblank lines"
                )
            if code and not any(
                _slice_in_patch(code, _post_patch_text(patch_by_path[path]))
                or _slice_in_patch(code, _stage_file_text(card_number, path))
                for path in paths
            ):
                raise ValueError(
                    f"{label} {paths}: key code does not match the Stage endpoint"
                )
        files.extend(
            FileLesson(path=path, body=file_body, code_slices=slices) for path in paths
        )
        index = last + 1
    return files


def parse_localized_lesson(
    body: str,
    *,
    card_number: int,
    chinese: bool,
    file_patches: list[FilePatch],
) -> LocalizedLesson:
    language = "zh" if chinese else "en"
    label = f"stage-{card_number:02d} {language}"
    headings = REQUIRED_HEADINGS[chinese]
    _heading_positions(body, headings, label=label)

    walkthrough_heading = "### 机制板块" if chinese else "### Mechanism blocks"
    verification_heading = "### 验证证据" if chinese else "### Verification evidence"
    test_heading = "### 测试契约" if chinese else "### Test contract"
    failure_heading = "### 先看会坏在哪里" if chinese else "### Failure preview"
    nested_failure_heading = "#### 先看会坏在哪里" if chinese else "#### See the failure first"
    concepts_heading = "### 基本概念" if chinese else "### Basic concepts"
    walkthrough_start = body.index(walkthrough_heading) + len(walkthrough_heading)
    verification_start = body.index(verification_heading, walkthrough_start)
    walkthrough = body[walkthrough_start:verification_start].strip()
    expected_test_paths = {item.path for item in file_patches if item.path.startswith("tests/")}
    regions: list[tuple[str, str]] = []
    if test_heading not in body:
        raise ValueError(f"{label}: missing {test_heading}")
    test_start = body.index(test_heading) + len(test_heading)
    concepts_start = body.index(concepts_heading, test_start)
    if concepts_start > body.index(walkthrough_heading):
        raise ValueError(f"{label}: test contract must precede basic concepts")
    failure_start = body.index(failure_heading)
    if failure_start > body.index(test_heading):
        raise ValueError(f"{label}: failure preview must precede the authored test contract")
    failure_text = body[
        failure_start + len(failure_heading):body.index(test_heading)
    ].strip()
    authored_test_contract = body[test_start:concepts_start].strip()
    first_test_marker = FILE_MARKER.search(authored_test_contract)
    test_intro = (
        authored_test_contract[:first_test_marker.start()].strip()
        if first_test_marker is not None
        else authored_test_contract
    )
    if expected_test_paths:
        regions.append(("test contract", body[test_start:concepts_start].strip()))
    prelude_parts = [
        body[:failure_start].rstrip(),
        test_heading,
        nested_failure_heading,
        failure_text,
    ]
    if test_intro:
        prelude_parts.append(test_intro)
    prelude_parts.append(body[concepts_start:walkthrough_start].strip())
    pre_walkthrough = "\n\n".join(prelude_parts)
    regions.append(("mechanism blocks", walkthrough))

    if not any(FILE_MARKER.search(region) for _, region in regions):
        raise ValueError(f"{label}: no journey-file sections")

    patch_by_path = {item.path: item for item in file_patches}
    files: list[FileLesson] = []
    for region_name, region in regions:
        files.extend(
            _parse_region_file_lessons(
                region,
                region_name=region_name,
                label=label,
                card_number=card_number,
                patch_by_path=patch_by_path,
            )
        )

    paths = [item.path for item in files]
    if len(paths) != len(set(paths)):
        raise ValueError(f"{label}: duplicate journey-file section")
    expected_paths = {item.path for item in file_patches}
    actual_paths = set(paths)
    if actual_paths != expected_paths:
        missing = sorted(expected_paths - actual_paths)
        extra = sorted(actual_paths - expected_paths)
        raise ValueError(f"{label}: walkthrough coverage mismatch; missing={missing}, extra={extra}")

    return LocalizedLesson(
        pre_walkthrough=pre_walkthrough,
        files=tuple(files),
        post_walkthrough=body[verification_start:].strip(),
    )


def _render_file_diff(file_patch: FilePatch, *, chinese: bool) -> str:
    label = "文件差异：" if chinese else "File diff: "
    lines = [f'??? note "{label}{file_patch.path}"', "    ```diff"]
    lines.extend(
        f"    {clean}" if (clean := line.rstrip()) else ""
        for line in file_patch.patch.splitlines()
    )
    lines.append("    ```")
    return "\n".join(lines)


def _render_supporting_diff(
    block: BlockLayout,
    patch_by_path: dict[str, FilePatch],
    *,
    chinese: bool,
) -> str:
    count = len(block.files)
    if chinese:
        label = f"支撑文件差异（{count} 个文件）"
    else:
        noun = "file" if count == 1 else "files"
        label = f"Supporting file diffs ({count} {noun})"
    lines = [f'??? note "{label}"']
    for path in block.files:
        lines.extend([f"    **`{path}`**", "", "    ```diff"])
        lines.extend(
            f"    {clean}" if (clean := line.rstrip()) else ""
            for line in patch_by_path[path].patch.splitlines()
        )
        lines.extend(["    ```", ""])
    return "\n".join(lines)


def _collapse_deliverables(prelude: str, *, chinese: bool) -> str:
    heading = "### 交付文件" if chinese else "### Deliverable files"
    label = "交付文件" if chinese else "Deliverable files"
    heading_start = prelude.index(heading)
    content_start = heading_start + len(heading)
    next_heading = prelude.index("\n### ", content_start)
    items = prelude[content_start:next_heading].strip()
    indented = "\n".join(f"    {line}" if line else "" for line in items.splitlines())
    collapsed = f'??? note "{label}"\n{indented}\n\n'
    return prelude[:heading_start] + collapsed + prelude[next_heading + 1:]


def _insert_test_walkthrough(
    prelude: str,
    test_sections: list[str],
    *,
    chinese: bool,
) -> str:
    if not test_sections:
        return prelude
    concepts_heading = "### 基本概念" if chinese else "### Basic concepts"
    position = prelude.index(concepts_heading)
    tests = "\n\n".join(test_sections)
    return f"{prelude[:position].rstrip()}\n\n{tests}\n\n{prelude[position:]}"


def _render_file_explanation(
    lesson: FileLesson,
    *,
    test_evidence: bool = False,
    chinese: bool = False,
) -> str:
    _, separator, explanation = lesson.body.partition("\n")
    if not separator:
        raise ValueError(f"{lesson.path}: file lesson has no explanation after its heading")
    explanation = re.sub(
        r"^##### (.+)$",
        lambda match: f"**{match.group(1)}**",
        explanation.lstrip(),
        flags=re.MULTILINE,
    )
    if test_evidence:
        labels = (
            {
                "是什么，为什么现在需要": "测试锁定什么",
                "在运行时做什么": "如何构造反例",
                "关键代码": "关键测试语句",
                "关键语句理解": "失败意味着什么",
            }
            if chinese
            else {
                "What it is and why it appears": "What this test locks",
                "Runtime role": "How it constructs the counterexample",
                "Key code": "Key test statement",
                "Statement understanding": "What a failure means",
            }
        )
        for original, replacement in labels.items():
            explanation = explanation.replace(f"**{original}**", f"**{replacement}**")
    return explanation


def _render_mechanism_block(
    block: BlockLayout,
    lesson_by_path: dict[str, FileLesson],
    patch_by_path: dict[str, FilePatch],
    *,
    chinese: bool,
) -> str:
    title = block.title_zh if chinese else block.title_en
    summary = block.summary_zh if chinese else block.summary_en
    parts = [f"#### {title}", summary]
    if block.supporting:
        parts.append(_render_supporting_diff(block, patch_by_path, chinese=chinese))
        return "\n\n".join(parts)
    lessons = [lesson_by_path[path] for path in block.files]
    shared_explanation = bool(lessons) and all(
        lesson.body == lessons[0].body for lesson in lessons
    )
    if shared_explanation:
        for path in block.files:
            parts.append(_render_file_diff(patch_by_path[path], chinese=chinese))
        parts.append(_render_file_explanation(lessons[0]))
        return "\n\n".join(parts)
    for path in block.files:
        if path.startswith("tests/"):
            continue
        parts.append(_render_file_diff(patch_by_path[path], chinese=chinese))
        parts.append(_render_file_explanation(lesson_by_path[path]))
    return "\n\n".join(parts)


def render_card(card: Card, *, chinese: bool) -> str:
    title = card.chinese_title if chinese else card.english_title
    body = card.chinese if chinese else card.english
    file_patches = split_file_patches(card.patch)
    lesson = parse_localized_lesson(
        body,
        card_number=card.number,
        chinese=chinese,
        file_patches=file_patches,
    )
    patch_by_path = {item.path: item for item in file_patches}
    lesson_by_path = {item.path: item for item in lesson.files}
    test_sections = [
        "\n\n".join(
            (
                _render_file_diff(patch_by_path[item.path], chinese=chinese),
                _render_file_explanation(item, test_evidence=True, chinese=chinese),
            )
        )
        for path in card.failure_files
        for item in (lesson_by_path[path],)
    ]
    prelude = _insert_test_walkthrough(
        _collapse_deliverables(lesson.pre_walkthrough, chinese=chinese),
        test_sections,
        chinese=chinese,
    )
    if not card.blocks:
        mechanism_heading = "### 机制板块" if chinese else "### Mechanism blocks"
        prelude = prelude.removesuffix(mechanism_heading).rstrip()
    block_sections = [
        _render_mechanism_block(
            block,
            lesson_by_path,
            patch_by_path,
            chinese=chinese,
        )
        for block in card.blocks
    ]
    link_label = "在 GitHub 查看阶段差异" if chinese else "Compare this stage on GitHub"
    checkout = (
        f"完成后可运行 `python -m journey.tools.build_journey check {card.number}` "
        "验收学习工作区。"
        if chinese
        else f"After finishing, run `python -m journey.tools.build_journey check "
        f"{card.number}` to verify the learner workspace."
    )
    patch_link = f"{REPO_URL}/blob/main/journey/stages/{card.number:02d}-{card.slug}/stage.patch"
    return (
        f"# Stage {card.number:02d} · {title}\n\n"
        f"{prelude}\n\n"
        + "\n\n".join(block_sections)
        + f"\n\n{lesson.post_walkthrough}\n\n"
        f"[{link_label}]({compare_link(card.number)})\n\n"
        f"{checkout}\n\n"
        f"[Complete reference patch / 完整参考补丁]({patch_link})\n"
    )


def render_index(cards: list[Card], *, chinese: bool) -> str:
    if chinese:
        lines = [
            "# 自主重建",
            "",
            "每个 Stage 都是一节可独立浏览的完整课：先理解当前问题、基本概念与必要性，再按机制板块连接相关文件和关键语句，最后用验证证据和自己的话完成理解闭环。",
            "",
            "这是三种学习模式中的浏览器自主学习路径。按主题学习请进入[机制教程](../tutorial/index.md)；需要 CLI 互动请查看 [Agent 带教使用教程](../agent-guided.md)。",
            "",
            "如果希望在编辑器里聚焦当前增量，运行 `python -m journey.tools.build_journey study N`，再打开 `../MiniRedis-journey-workspace`。",
            "",
            "| Stage | 主题 | 新增测试 | 教材章节 |",
            "|---:|---|---:|---:|",
        ]
    else:
        lines = [
            "# Self-Guided Rebuild",
            "",
            "Each Stage is a complete independent-browser lesson: understand the current problem, concepts, and necessity; connect related files and critical statements through mechanism blocks; then close with evidence and your own explanation.",
            "",
            "This is the browser-based path among MiniRedis's three learning modes. Use the [Mechanism Tutorial](../tutorial/index.md) for topic-oriented study, or the [Agent-Guided usage guide](../agent-guided.md) for interactive CLI teaching.",
            "",
            "For an editor-focused diff, run `python -m journey.tools.build_journey study N` and open `../MiniRedis-journey-workspace`.",
            "",
            "| Stage | Topic | New tests | Book chapter |",
            "|---:|---|---:|---:|",
        ]
    for card in cards:
        title = card.chinese_title if chinese else card.english_title
        chapter_name = {
            1: "getting-started",
            2: "life-of-a-command",
            3: "data-types",
            4: "expiration",
            5: "eviction",
            6: "persistence-aof",
            7: "rewrite-snapshot",
            8: "replication",
            9: "transactions-blocking",
            10: "protocol",
        }.get(card.chapter)
        chapter_link = (
            f"../tutorial/{card.chapter:02d}-{chapter_name}.md"
            if chapter_name is not None
            else "../tutorial/index.md"
        )
        lines.append(
            f"| [{card.number:02d}](stage-{card.number:02d}.md) | {title} | "
            f"{card.tests_added} | [{card.chapter}]({chapter_link}) |"
        )
    return "\n".join(lines) + "\n"


def main() -> int:
    cards = load_cards()
    for chinese, output in (
        (False, ROOT / "docs" / "journey"),
        (True, ROOT / "docs" / "zh" / "journey"),
    ):
        output.mkdir(parents=True, exist_ok=True)
        expected = {"index.md"}
        for card in cards:
            name = f"stage-{card.number:02d}.md"
            expected.add(name)
            (output / name).write_text(render_card(card, chinese=chinese))
        (output / "index.md").write_text(render_index(cards, chinese=chinese))
        for stale in output.glob("stage-*.md"):
            if stale.name not in expected:
                stale.unlink()
    print(f"rendered {len(cards)} bilingual Journey stages")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
