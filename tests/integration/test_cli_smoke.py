import os
import subprocess
import sys
from pathlib import Path
import unittest


class CliSmokeTest(unittest.TestCase):
    def test_cli_help_exits_zero(self) -> None:
        repo_root = Path(__file__).resolve().parents[2]
        env = os.environ.copy()
        pythonpath = str(repo_root / "src")
        if env.get("PYTHONPATH"):
            env["PYTHONPATH"] = pythonpath + os.pathsep + env["PYTHONPATH"]
        else:
            env["PYTHONPATH"] = pythonpath

        result = subprocess.run(
            [sys.executable, "-m", "graph_pipeline.cli", "--help"],
            cwd=repo_root,
            env=env,
            capture_output=True,
            text=True,
        )

        self.assertEqual(0, result.returncode, msg=result.stderr)
        self.assertIn("run-case", result.stdout)
        self.assertIn("run-batch", result.stdout)


if __name__ == "__main__":
    unittest.main()
