import sys
import unittest
from io import BytesIO
from pathlib import Path

from docx import Document


sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

from graph_pipeline.post_operation_validation import validate_operation_materialized
from graph_pipeline.schema import ResolvedOperation


def _make_docx(*paragraphs: str) -> BytesIO:
    doc = Document()
    for paragraph in paragraphs:
        doc.add_paragraph(paragraph)
    stream = BytesIO()
    doc.save(stream)
    stream.seek(0)
    return stream


class PostOperationValidationTest(unittest.TestCase):
    def test_new_text_is_materialized(self) -> None:
        operation = ResolvedOperation(
            operation_id="op1",
            operation_kind="replace_text",
            status="resolved",
            source_document_label="doc",
            new_text="New paragraph text",
        )

        result = validate_operation_materialized(_make_docx("New paragraph text"), operation)

        self.assertTrue(result["ok"])
        self.assertEqual("materialized", result["reason"])

    def test_missing_new_text_is_not_materialized(self) -> None:
        operation = ResolvedOperation(
            operation_id="op1",
            operation_kind="replace_text",
            status="resolved",
            source_document_label="doc",
            new_text="Expected text",
        )

        result = validate_operation_materialized(_make_docx("Different text"), operation)

        self.assertFalse(result["ok"])
        self.assertEqual("new_text not materialized", result["reason"])


if __name__ == "__main__":
    unittest.main()
