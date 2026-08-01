"""Contracts for the three polished MiniRedis learning modes."""

from __future__ import annotations

from pathlib import Path


def test_homepage_readmes_and_navigation_expose_three_learning_modes() -> None:
    homepage = Path("docs/index.md").read_text()
    english = Path("README.md").read_text()
    chinese = Path("README.zh-CN.md").read_text()
    navigation = Path("mkdocs.yml").read_text()

    for name in (
        "Mechanism Tutorial",
        "Self-Guided Rebuild",
        "Agent-Guided Rebuild",
    ):
        assert name in english
        assert name in navigation
    for name in ("机制教程", "自主重建", "Agent 带教"):
        assert name in chinese
        assert name in navigation
    assert "Mechanism Tutorial / 机制教程" in homepage
    assert "Self-Guided Rebuild / 自主重建" in homepage
    assert "Agent-Guided Rebuild / Agent 带教" in homepage


def test_agent_pages_are_short_usage_guides_without_internal_workspace_details() -> None:
    english = Path("docs/agent-guided.md").read_text()
    chinese = Path("docs/zh/agent-guided.md").read_text()
    for page in (english, chinese):
        assert "开始 Agent 带教 Stage 03" in page
        for internal in ("build_journey.py agent", ".journey/", "agent-only", "branch"):
            assert internal not in page
    assert "### Basic concepts" not in english
    assert "### 基本概念" not in chinese


def test_root_agents_contract_routes_direct_agent_teaching_without_branch_switches() -> None:
    contract = Path("AGENTS.md").read_text()
    assert "开始 Agent 带教 Stage NN" in contract
    assert "1 through 30" in contract
    assert "python -m journey.tools.build_journey agent NN" in contract
    assert "WORKSPACE:" in contract
    assert "CHECK:" in contract
    assert "layout.toml" in contract
    assert "test contract" in contract.lower()
    assert "Never create or switch a teaching branch" in contract


def test_language_switch_preserves_journey_paths() -> None:
    navigation = Path("mkdocs.yml").read_text()
    script = Path("docs/assets/javascripts/language-switch.js")
    assert "assets/javascripts/language-switch.js" in navigation
    assert script.is_file()
    source = script.read_text()
    assert '"journey/"' in source
    assert '"tutorial/"' in source
    assert '"zh/"' in source


def test_generated_journey_uses_collapsed_deliverables() -> None:
    for root, label, heading in (
        (Path("docs/journey"), '??? note "Deliverable files"', "### Deliverable files"),
        (Path("docs/zh/journey"), '??? note "交付文件"', "### 交付文件"),
    ):
        stages = sorted(root.glob("stage-*.md"))
        assert len(stages) == 30
        for stage in stages:
            lesson = stage.read_text()
            assert label in lesson
            assert heading not in lesson


def test_journey_ci_rebuilds_pages_chain_and_strict_docs() -> None:
    workflow = Path(".github/workflows/journey.yml").read_text()
    assert "python -m journey.tools.render_pages" in workflow
    assert "git diff --exit-code -- docs/journey docs/zh/journey" in workflow
    assert "python -m journey.tools.build_journey --check" in workflow
    assert "mkdocs build --strict" in workflow
