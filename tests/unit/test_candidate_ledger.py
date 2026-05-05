import sys
from pathlib import Path
import unittest


sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

from graph_pipeline.candidate_ledger import CandidateLedger


class CandidateLedgerTest(unittest.TestCase):
    def test_records_candidates_rejections_and_selection(self) -> None:
        ledger = CandidateLedger(change_id="c1", operation_kind="replace_table_row")
        ledger.add_candidate("row-1", "table row 1", score=0.8, evidence="first cell matched")
        ledger.reject("row-1", reason="wrong scope")
        ledger.add_candidate("row-2", "table row 2", score=0.95, evidence="scope and row matched")
        ledger.select("row-2", reason="unique exact row ref")

        payload = ledger.to_dict()
        self.assertEqual("row-2", payload["selected_candidate_id"])
        self.assertEqual("unique exact row ref", payload["selected_reason"])
        self.assertEqual("wrong scope", payload["candidates"][0]["reject_reason"])

    def test_marks_ambiguity_without_selection(self) -> None:
        ledger = CandidateLedger(change_id="c1", operation_kind="replace_table_row")
        ledger.add_candidate("row-1", "row")
        ledger.add_candidate("row-2", "row")
        ledger.mark_ambiguous("two equal row references")

        payload = ledger.to_dict()
        self.assertTrue(payload["ambiguous"])
        self.assertEqual("two equal row references", payload["ambiguity_reason"])
        self.assertIsNone(payload["selected_candidate_id"])


if __name__ == "__main__":
    unittest.main()
