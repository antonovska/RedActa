import sys
import types
import unittest
from pathlib import Path


sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))
sys.modules.setdefault("openai", types.SimpleNamespace(OpenAI=object))

from graph_pipeline.base_agent import load_prompt


class BasePromptLoadingTest(unittest.TestCase):
    def test_base_repair_json_prompt_is_available(self) -> None:
        system_prompt, user_template = load_prompt("base_repair_json")

        self.assertTrue(system_prompt.strip())
        self.assertTrue(user_template.strip())
        self.assertIn("{raw_text}", user_template)


if __name__ == "__main__":
    unittest.main()
