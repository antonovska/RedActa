import sys
from pathlib import Path
import unittest


sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

from graph_pipeline.deterministic_intent_extractor import DeterministicIntentExtractor
from graph_pipeline.amendment_analyzer import AmendmentAnalyzer
from graph_pipeline.schema import ChangeIntent


class DeterministicIntentExtractorTest(unittest.TestCase):
    def test_case_04_extracts_only_safe_intents(self) -> None:
        lines = [
            '1. \u0412 \u043f\u0440\u0435\u0430\u043c\u0431\u0443\u043b\u0435 \u0441\u043b\u043e\u0432\u0430 "old citation 1", "old citation 2" \u0438\u0441\u043a\u043b\u044e\u0447\u0438\u0442\u044c.',
            '2. \u041f\u0443\u043d\u043a\u0442 3 \u043f\u0440\u0438\u0437\u043d\u0430\u0442\u044c \u0443\u0442\u0440\u0430\u0442\u0438\u0432\u0448\u0438\u043c \u0441\u0438\u043b\u0443.',
            '3. \u0412 \u041f\u043e\u0440\u044f\u0434\u043a\u0435:',
            '\u0430) \u0432 \u043f\u0443\u043d\u043a\u0442\u0435 2:',
            '\u043f\u043e\u0434\u043f\u0443\u043d\u043a\u0442 "\u0431" \u0438\u0437\u043b\u043e\u0436\u0438\u0442\u044c \u0432 \u0441\u043b\u0435\u0434\u0443\u044e\u0449\u0435\u0439 \u0440\u0435\u0434\u0430\u043a\u0446\u0438\u0438:',
            '"\u0431) replacement text";',
            '\u043f\u043e\u0434\u043f\u0443\u043d\u043a\u0442 "\u0432" \u043f\u0440\u0438\u0437\u043d\u0430\u0442\u044c \u0443\u0442\u0440\u0430\u0442\u0438\u0432\u0448\u0438\u043c \u0441\u0438\u043b\u0443;',
            '\u043f\u043e\u0434\u043f\u0443\u043d\u043a\u0442\u044b "\u0434" \u0438 "\u0435" \u0438\u0437\u043b\u043e\u0436\u0438\u0442\u044c \u0432 \u0441\u043b\u0435\u0434\u0443\u044e\u0449\u0435\u0439 \u0440\u0435\u0434\u0430\u043a\u0446\u0438\u0438:',
            '\u0434\u043e\u043f\u043e\u043b\u043d\u0438\u0442\u044c \u043f\u043e\u0434\u043f\u0443\u043d\u043a\u0442\u043e\u043c "\u0435(1)" \u0441\u043b\u0435\u0434\u0443\u044e\u0449\u0435\u0433\u043e \u0441\u043e\u0434\u0435\u0440\u0436\u0430\u043d\u0438\u044f:',
            '\u0431) \u0432 \u043f\u0443\u043d\u043a\u0442\u0435 3 \u0441\u043b\u043e\u0432\u0430 "old citation 3" \u0438\u0441\u043a\u043b\u044e\u0447\u0438\u0442\u044c;',
            '\u0432) \u0432 \u043f\u0443\u043d\u043a\u0442\u0435 4:',
            '\u0432 \u043f\u043e\u0434\u043f\u0443\u043d\u043a\u0442\u0435 "\u0430" \u0441\u043b\u043e\u0432\u0430 "\u0432\u044b\u043f\u0438\u0441\u043a\u043e\u0439 \u0438\u0437 \u043e\u0442\u0447\u0435\u0442\u043d\u043e\u0441\u0442\u0438" \u0437\u0430\u043c\u0435\u043d\u0438\u0442\u044c \u0441\u043b\u043e\u0432\u043e\u043c "\u0441\u043f\u0440\u0430\u0432\u043a\u043e\u0439";',
            '\u043f\u043e\u0434\u043f\u0443\u043d\u043a\u0442 "\u0432" \u0434\u043e\u043f\u043e\u043b\u043d\u0438\u0442\u044c \u0430\u0431\u0437\u0430\u0446\u0435\u043c \u0441\u043b\u0435\u0434\u0443\u044e\u0449\u0435\u0433\u043e \u0441\u043e\u0434\u0435\u0440\u0436\u0430\u043d\u0438\u044f:',
        ]

        intents = DeterministicIntentExtractor().extract(
            lines,
            source_document_label="Приказ Минфина России от 03.10.2025 N 141н",
        )

        operation_kinds = [intent.operation_kind for intent in intents]
        source_excerpts = [intent.source_excerpt for intent in intents]

        self.assertIn("repeal_point", operation_kinds)
        self.assertIn("replace_phrase_globally", operation_kinds)
        self.assertTrue(any(intent.section_hint == "preamble" for intent in intents))
        self.assertTrue(any(intent.old_text == "\u0432\u044b\u043f\u0438\u0441\u043a\u043e\u0439 \u0438\u0437 \u043e\u0442\u0447\u0435\u0442\u043d\u043e\u0441\u0442\u0438" for intent in intents))
        self.assertTrue(any(intent.point_ref == "3" for intent in intents if intent.operation_kind == "repeal_point"))
        self.assertTrue(any(intent.section_hint == "" for intent in intents if intent.operation_kind == "repeal_point"))

        joined_excerpts = "\n".join(source_excerpts)
        self.assertNotIn('\u0434\u043e\u043f\u043e\u043b\u043d\u0438\u0442\u044c \u043f\u043e\u0434\u043f\u0443\u043d\u043a\u0442\u043e\u043c "\u0435(1)"', joined_excerpts)
        self.assertNotIn('\u043f\u043e\u0434\u043f\u0443\u043d\u043a\u0442 "\u0431" \u0438\u0437\u043b\u043e\u0436\u0438\u0442\u044c', joined_excerpts)
        self.assertNotIn('\u043f\u043e\u0434\u043f\u0443\u043d\u043a\u0442\u044b "\u0434" \u0438 "\u0435" \u0438\u0437\u043b\u043e\u0436\u0438\u0442\u044c', joined_excerpts)
        self.assertNotIn('\u043f\u043e\u0434\u043f\u0443\u043d\u043a\u0442 "\u0432" \u0434\u043e\u043f\u043e\u043b\u043d\u0438\u0442\u044c \u0430\u0431\u0437\u0430\u0446\u0435\u043c', joined_excerpts)

    def test_phrase_deletion_uses_only_quotes_after_words_marker(self) -> None:
        line = (
            '\u0432 \u0430\u0431\u0437\u0430\u0446\u0435 \u0432\u0442\u043e\u0440\u043e\u043c '
            '\u043f\u043e\u0434\u043f\u0443\u043d\u043a\u0442\u0430 "\u0436" '
            '\u0441\u043b\u043e\u0432\u0430 "old citation" '
            '\u0438\u0441\u043a\u043b\u044e\u0447\u0438\u0442\u044c;'
        )

        intents = DeterministicIntentExtractor().extract([line], source_document_label="doc")

        self.assertEqual(1, len(intents))
        self.assertEqual("old citation", intents[0].old_text)
        self.assertEqual("\u0436", intents[0].subpoint_ref)
        self.assertEqual(2, intents[0].paragraph_ordinal)

    def test_merge_prefers_deterministic_duplicate_and_keeps_llm_complex_intent(self) -> None:
        analyzer = AmendmentAnalyzer.__new__(AmendmentAnalyzer)
        deterministic = ChangeIntent(
            change_id="d1",
            operation_kind="repeal_point",
            source_document_label="doc",
            point_ref="3",
            source_excerpt="same directive",
        )
        duplicate_from_llm = ChangeIntent(
            change_id="c1",
            operation_kind="repeal_point",
            source_document_label="doc",
            point_ref="3",
            source_excerpt="same directive",
        )
        complex_from_llm = ChangeIntent(
            change_id="c2",
            operation_kind="append_section_item",
            source_document_label="doc",
            new_text="complex insert",
            source_excerpt="complex directive",
        )

        merged = analyzer._merge_deterministic_intents(
            [deterministic],
            [duplicate_from_llm, complex_from_llm],
        )

        self.assertEqual(["d1", "c2"], [intent.change_id for intent in merged])


if __name__ == "__main__":
    unittest.main()
