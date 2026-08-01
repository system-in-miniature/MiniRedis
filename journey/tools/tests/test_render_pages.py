#!/usr/bin/env python3
"""Contracts for MiniRedis browser-native Journey lessons."""

from __future__ import annotations

from pathlib import Path
import unittest

from journey.tools import render_pages


class RenderPagesTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.cards = render_pages.load_cards()

    def test_all_thirty_cards_load_with_lossless_patch_coverage(self) -> None:
        self.assertEqual([card.number for card in self.cards], list(range(1, 31)))
        for card in self.cards:
            parsed = render_pages.split_file_patches(card.patch)
            with self.subTest(stage=card.number):
                self.assertEqual("".join(item.patch for item in parsed), card.patch)
                expected = [item.path for item in parsed]
                actual = [
                    *card.failure_files,
                    *(path for block in card.blocks for path in block.files),
                ]
                self.assertEqual(set(actual), set(expected))
                self.assertEqual(len(actual), len(set(actual)))

    def test_every_authored_file_is_bound_once_in_both_languages(self) -> None:
        for card in self.cards:
            patches = render_pages.split_file_patches(card.patch)
            expected = {item.path for item in patches}
            for chinese, body in ((False, card.english), (True, card.chinese)):
                with self.subTest(stage=card.number, chinese=chinese):
                    lesson = render_pages.parse_localized_lesson(
                        body,
                        card_number=card.number,
                        chinese=chinese,
                        file_patches=patches,
                    )
                    paths = [item.path for item in lesson.files]
                    self.assertEqual(set(paths), expected)
                    self.assertEqual(len(paths), len(set(paths)))

    def test_test_contract_owns_failure_preview_and_test_diffs(self) -> None:
        for card in self.cards:
            for chinese in (False, True):
                with self.subTest(stage=card.number, chinese=chinese):
                    page = render_pages.render_card(card, chinese=chinese)
                    contract = "### 测试契约" if chinese else "### Test contract"
                    failure = "#### 先看会坏在哪里" if chinese else "#### See the failure first"
                    concepts = "### 基本概念" if chinese else "### Basic concepts"
                    mechanisms = "### 机制板块" if chinese else "### Mechanism blocks"
                    self.assertLess(page.index(contract), page.index(failure))
                    self.assertLess(page.index(failure), page.index(concepts))
                    for path in card.failure_files:
                        label = "文件差异：" if chinese else "File diff: "
                        drawer = f'{label}{path}"'
                        self.assertEqual(page.count(drawer), 1)
                        self.assertLess(page.index(drawer), page.index(concepts))
                        if mechanisms in page:
                            self.assertNotIn(drawer, page[page.index(mechanisms):])

    def test_stage_thirty_keeps_reused_test_evidence_without_empty_diff(self) -> None:
        stage = self.cards[29]
        self.assertEqual(stage.failure_files, ())
        for chinese, phrase in (
            (False, "No test file changes in this stage."),
            (True, "本阶段没有修改 Test File。"),
        ):
            with self.subTest(chinese=chinese):
                page = render_pages.render_card(stage, chinese=chinese)
                self.assertIn(phrase, page)
                self.assertIn("### 测试契约" if chinese else "### Test contract", page)

    def test_grouped_mechanism_files_share_one_explanation(self) -> None:
        stage_21 = self.cards[20]
        stage_30 = self.cards[29]
        for chinese, phrase_21, phrase_30 in (
            (
                False,
                "Direct, AOF, replica sink, and runtime surfaces expose",
                "The three examples demonstrate acknowledged AOF recovery",
            ),
            (
                True,
                "Direct、AOF、Replica Sink 与 Runtime 暴露",
                "三个 Example 分别演示 Simulated Crash 后 Acknowledged AOF Recovery",
            ),
        ):
            with self.subTest(chinese=chinese):
                self.assertEqual(render_pages.render_card(stage_21, chinese=chinese).count(phrase_21), 1)
                self.assertEqual(render_pages.render_card(stage_30, chinese=chinese).count(phrase_30), 1)

    def test_supporting_scaffold_is_one_collapsed_group(self) -> None:
        stage = self.cards[0]
        for chinese, drawer in (
            (False, '??? note "Supporting file diffs (8 files)"'),
            (True, '??? note "支撑文件差异（8 个文件）"'),
        ):
            with self.subTest(chinese=chinese):
                page = render_pages.render_card(stage, chinese=chinese)
                self.assertIn(drawer, page)
                self.assertNotIn('File diff: pyproject.toml"', page)
                self.assertNotIn('文件差异：pyproject.toml"', page)

    def test_deliverables_are_collapsed_and_every_diff_is_rendered_once(self) -> None:
        for card in self.cards:
            expected_count = card.patch.count("diff --git ")
            for chinese in (False, True):
                with self.subTest(stage=card.number, chinese=chinese):
                    page = render_pages.render_card(card, chinese=chinese)
                    self.assertIn(
                        '??? note "交付文件"' if chinese else '??? note "Deliverable files"',
                        page,
                    )
                    self.assertNotIn("### 交付文件" if chinese else "### Deliverable files", page)
                    self.assertEqual(page.count("diff --git "), expected_count)

    def test_pages_close_with_evidence_takeaways_and_explanation(self) -> None:
        for card in self.cards:
            for chinese, headings in (
                (False, ("### Verification evidence", "### Durable takeaways", "### Explain it in your own words")),
                (True, ("### 验证证据", "### 需要真正记住的内容", "### 用自己的话讲清楚")),
            ):
                with self.subTest(stage=card.number, chinese=chinese):
                    page = render_pages.render_card(card, chinese=chinese)
                    positions = [page.index(heading) for heading in headings]
                    self.assertEqual(positions, sorted(positions))

    def test_main_generates_bilingual_indexes_and_thirty_pages(self) -> None:
        self.assertEqual(render_pages.main(), 0)
        for root in (Path("docs/journey"), Path("docs/zh/journey")):
            self.assertTrue((root / "index.md").is_file())
            self.assertEqual(len(list(root.glob("stage-*.md"))), 30)


if __name__ == "__main__":
    unittest.main()
