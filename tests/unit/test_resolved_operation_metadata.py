import sys
import unittest
from pathlib import Path


sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

from graph_pipeline.schema import ResolvedOperation


class ResolvedOperationMetadataTest(unittest.TestCase):
    def test_to_dict_includes_table_position_metadata(self) -> None:
        operation = ResolvedOperation(
            operation_id="op1",
            operation_kind="replace_table_row",
            status="resolved",
            source_document_label="doc",
            metadata={"table_position": {"table_index": 0, "row_index": 1}},
        )

        self.assertEqual(
            {"table_position": {"table_index": 0, "row_index": 1}},
            operation.to_dict()["metadata"],
        )


if __name__ == "__main__":
    unittest.main()
