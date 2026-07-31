import re
import unittest
from pathlib import Path


class DocumentationHomepageTest(unittest.TestCase):
    def test_homepage_is_fully_bilingual(self) -> None:
        homepage = Path("docs/index.md").read_text(encoding="utf-8")
        headings = [line for line in homepage.splitlines() if line.startswith("#")]

        self.assertTrue(headings)
        self.assertTrue(all(" / " in heading for heading in headings))
        self.assertIn("[Chinese edition / 中文版]", homepage)
        self.assertGreaterEqual(len(re.findall(r"[\u4e00-\u9fff]", homepage)), 120)


if __name__ == "__main__":
    unittest.main()
