from collections.abc import Iterable
from pathlib import Path


def physical_lines(path: Path) -> int:
    data = path.read_bytes()
    if not data:
        return 0
    return data.count(b"\n") + int(not data.endswith(b"\n"))


def total(paths: Iterable[Path]) -> int:
    return sum(physical_lines(path) for path in paths)


def report(root: Path = Path(".")) -> dict[str, int]:
    production = sorted((root / "src" / "miniredis").rglob("*.py"))
    tests = sorted((root / "tests").rglob("*.py"))
    documentation = [
        root / "README.md",
        *sorted((root / "docs").rglob("*.md")),
    ]
    return {
        "production_python_lines": total(production),
        "test_python_lines": total(tests),
        "documentation_markdown_lines": total(documentation),
    }


if __name__ == "__main__":
    print(report())
