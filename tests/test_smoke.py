from __future__ import annotations

import subprocess
import sys
import unittest
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]


class SmokeTests(unittest.TestCase):
    def run_command(self, *args: str) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [sys.executable, *args],
            cwd=PROJECT_ROOT,
            text=True,
            capture_output=True,
            check=False,
        )

    def assert_help_command(self, *args: str, expected: str) -> None:
        result = self.run_command(*args)
        self.assertEqual(result.returncode, 0, msg=result.stderr)
        self.assertIn(expected, result.stdout)

    def test_pipeline_help(self) -> None:
        self.assert_help_command("run_pipeline.py", "--help", expected="pipeline")

    def test_merge_help(self) -> None:
        self.assert_help_command("merge_all_carriers.py", "--help", expected="merge")

    def test_sync_help(self) -> None:
        self.assert_help_command("sync_to_rds.py", "--help", expected="sync")

    def test_csl_help(self) -> None:
        self.assert_help_command(str(Path("CSL FETCH") / "CSL_FETCH.py"), "--help", expected="fetch csl")

    def test_msc_help(self) -> None:
        self.assert_help_command(str(Path("MSC FETCH") / "MSC_FETCH.py"), "--help", expected="fetch msc")

    def test_msk_help(self) -> None:
        self.assert_help_command(str(Path("MSK FETCH") / "MSK_FETCH.py"), "--help", expected="fetch msk")


if __name__ == "__main__":
    unittest.main()
