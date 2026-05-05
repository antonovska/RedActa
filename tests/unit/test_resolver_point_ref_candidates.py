import sys
import types
from pathlib import Path
import unittest


sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))
sys.modules.setdefault("openai", types.SimpleNamespace(OpenAI=object))

from graph_pipeline.ooxml_reader import ParagraphRecord
from graph_pipeline.resolver_v2 import ResolverV2
from graph_pipeline.schema import ChangeIntent


def _records(*texts: str) -> list[ParagraphRecord]:
    return [
        ParagraphRecord(absolute_index=index, text=text)
        for index, text in enumerate(texts)
    ]


def _repeal_point_2_intent() -> ChangeIntent:
    return ChangeIntent(
        change_id="c1",
        operation_kind="repeal_point",
        source_document_label="Источник изменения",
        point_ref="2",
        point_number=2,
    )


def _act_with_explicit_appendix_heading() -> list[ParagraphRecord]:
    return _records(
        "УКАЗ",
        "1. Образовать комиссию.",
        "2. Утвердить прилагаемое Положение о комиссии.",
        "3. Назначить председателя.",
        "Приложение",
        "ПОЛОЖЕНИЕ",
        "1. Общие положения.",
        "2. Внутренний пункт положения.",
    )


def _act_with_approved_appendix_block(
    approval_lines: tuple[str, ...],
    title: str,
    content_lines: tuple[str, ...],
) -> list[ParagraphRecord]:
    return _records(
        "УКАЗ",
        "1. Образовать комиссию.",
        "2. Утвердить прилагаемое Положение о комиссии.",
        "3. Назначить председателя.",
        "Утверждено",
        *approval_lines,
        title,
        *content_lines,
    )


class ResolverPointRefCandidatesTest(unittest.TestCase):
    def test_letter_point_ref_builds_hierarchical_candidate(self) -> None:
        resolver = ResolverV2.__new__(ResolverV2)
        records = _records(
            "1. Общие положения",
            "2. Условия",
            "а) первый подпункт;",
            "б) целевой подпункт;",
            "3. Следующий пункт",
        )
        intent = ChangeIntent(
            change_id="c1",
            operation_kind="replace_point",
            source_document_label="Тестовый документ",
            point_ref="б",
            parent_point_number=2,
        )

        candidates = resolver._build_point_ref_candidates(records, intent, "б")

        self.assertEqual(1, len(candidates))
        self.assertEqual(3, candidates[0].absolute_paragraph_index)
        self.assertEqual("hierarchical", candidates[0].extra["candidate_source"])

    def test_phrase_match_normalizes_whitespace_variants(self) -> None:
        resolver = ResolverV2.__new__(ResolverV2)
        text = "слова с\u00a0неразрывным\nпробелом и переносом"
        phrase = "слова с неразрывным пробелом"

        self.assertTrue(resolver._text_contains_phrase_variant(text, phrase))

    def test_repeal_point_prefers_approving_main_point_before_disambiguation(self) -> None:
        resolver = ResolverV2.__new__(ResolverV2)

        def fail_select(_intent, _candidates):
            self.fail("obvious approving point should not require LLM disambiguation")

        resolver._select_candidate = fail_select
        records = _act_with_explicit_appendix_heading()

        operations = resolver._resolve_repeal_point(records, _repeal_point_2_intent())

        self.assertEqual([2], operations[0].paragraph_indices)
        self.assertEqual("replace_appendix_block", operations[1].operation_kind)
        self.assertEqual([4], operations[1].paragraph_indices)

    def test_repeal_point_cascade_handles_approved_appendix_without_appendix_heading(self) -> None:
        resolver = ResolverV2.__new__(ResolverV2)

        records = _act_with_approved_appendix_block(
            approval_lines=("Реквизит утверждения",),
            title="ПОЛОЖЕНИЕ",
            content_lines=(
                "1. Первый пункт приложения.",
                "2. Второй пункт приложения.",
            ),
        )
        operations = resolver._resolve_repeal_point(
            records,
            _repeal_point_2_intent(),
        )

        self.assertEqual([2], operations[0].paragraph_indices)
        self.assertEqual("replace_appendix_block", operations[1].operation_kind)
        self.assertEqual([7], operations[1].paragraph_indices)


if __name__ == "__main__":
    unittest.main()
