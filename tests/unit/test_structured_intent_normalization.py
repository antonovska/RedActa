import sys
from pathlib import Path
import unittest


sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

from graph_pipeline.amendment_analyzer import normalize_structured_replacement_intent
from graph_pipeline.schema import ChangeIntent


class StructuredIntentNormalizationTest(unittest.TestCase):
    def test_normalizes_table_row_replacement_directive(self) -> None:
        intent = ChangeIntent(
            change_id="c1",
            operation_kind="replace_phrase_globally",
            source_document_label="doc",
            old_text="строку 4 изложить в следующей редакции",
            new_block_lines=["4 | Новая редакция строки"],
        )

        normalized = normalize_structured_replacement_intent(intent)

        self.assertEqual("replace_table_row", normalized.operation_kind)
        self.assertEqual("4", normalized.table_row_ref)

    def test_normalizes_structured_entry_replacement_directive(self) -> None:
        intent = ChangeIntent(
            change_id="c2",
            operation_kind="replace_phrase_globally",
            source_document_label="doc",
            old_text='позицию "010" изложить в следующей редакции',
            new_block_lines=['010 | Новая редакция позиции'],
        )

        normalized = normalize_structured_replacement_intent(intent)

        self.assertEqual("replace_structured_entry", normalized.operation_kind)
        self.assertEqual("010", normalized.structured_entry_ref)


if __name__ == "__main__":
    unittest.main()
