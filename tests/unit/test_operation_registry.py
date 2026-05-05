import sys
from pathlib import Path
import unittest


sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

from graph_pipeline.operation_registry import (
    OPERATION_REGISTRY,
    OperationSupport,
    validate_intent_fields,
)
from graph_pipeline.schema import ChangeIntent


class OperationRegistryTest(unittest.TestCase):
    def test_registry_contains_structured_operations(self) -> None:
        self.assertEqual(OperationSupport.SUPPORTED, OPERATION_REGISTRY["replace_structured_entry"].support)
        self.assertEqual(OperationSupport.SUPPORTED, OPERATION_REGISTRY["replace_table_row"].support)

    def test_unknown_operation_is_rejected(self) -> None:
        intent = ChangeIntent(change_id="c1", operation_kind="made_up", source_document_label="doc")
        self.assertEqual(["unsupported operation_kind: made_up"], validate_intent_fields(intent))

    def test_replace_table_row_requires_locator_and_new_block(self) -> None:
        intent = ChangeIntent(change_id="c1", operation_kind="replace_table_row", source_document_label="doc")
        errors = validate_intent_fields(intent)
        self.assertIn("missing one of: table_row_ref, structured_entry_ref, point_ref, point_number", errors)
        self.assertIn("missing required field: new_block_lines", errors)

    def test_replace_table_row_accepts_locator_and_new_block(self) -> None:
        intent = ChangeIntent(
            change_id="c1",
            operation_kind="replace_table_row",
            source_document_label="doc",
            table_row_ref="4",
            new_block_lines=["4 | Новая редакция строки"],
        )
        self.assertEqual([], validate_intent_fields(intent))


if __name__ == "__main__":
    unittest.main()
