import sys
import types
import unittest
from pathlib import Path


sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))
sys.modules.setdefault("openai", types.SimpleNamespace(OpenAI=object))

from graph_pipeline.utils import build_source_document_label


class SourceDocumentLabelTest(unittest.TestCase):
    def test_strips_single_izm_prefix(self) -> None:
        path = Path("изм_Распоряжение Росавтодора от 24_10_2025 N 1971-р.docx")
        self.assertEqual(
            "Распоряжение Росавтодора от 24.10.2025 N 1971-р",
            build_source_document_label(path),
        )

    def test_strips_repeated_izm_prefixes(self) -> None:
        path = Path("изм_изм_Распоряжение Росавтодора от 24_10_2025 N 1971-р.docx")
        self.assertEqual(
            "Распоряжение Росавтодора от 24.10.2025 N 1971-р",
            build_source_document_label(path),
        )


if __name__ == "__main__":
    unittest.main()
