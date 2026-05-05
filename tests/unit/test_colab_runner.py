import sys
import types
import unittest
from pathlib import Path
from unittest.mock import patch


sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))
sys.modules.setdefault("openai", types.SimpleNamespace(OpenAI=object))

from graph_pipeline.colab_runner import run_uploaded_pair


class ColabRunnerTest(unittest.TestCase):
    def test_uploaded_names_are_preserved(self) -> None:
        root = Path(__file__).resolve().parents[2]
        base_docx = Path("base_Распоряжение.docx")
        amendment_docx = Path("изм_Распоряжение.docx")
        models_config = root / "config" / "models.example.json"

        with patch("graph_pipeline.colab_runner.shutil.copy2") as mocked_copy2:
            with patch("graph_pipeline.colab_runner.run_case") as mocked_run_case:
                mocked_run_case.return_value = {"ok": True}
                result = run_uploaded_pair(
                    base_docx=base_docx,
                    amendment_docx=amendment_docx,
                    workspace_root=root,
                    models_config=models_config,
                    case_id="colab",
                )

        self.assertEqual({"ok": True}, result)
        self.assertEqual(2, mocked_copy2.call_count)
        case = mocked_run_case.call_args.args[0]
        self.assertEqual("base_Распоряжение.docx", Path(case["base_doc"]).name)
        self.assertEqual("изм_Распоряжение.docx", Path(case["amendment_docs"][0]).name)


if __name__ == "__main__":
    unittest.main()
