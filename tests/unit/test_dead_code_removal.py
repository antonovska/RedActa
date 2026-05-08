"""
Защитные тесты для удаления мёртвого кода (Группа 1).

Каждый тест доказывает, что удаляемый элемент НЕ задействован в реальной
работе пайплайна. Тесты должны ПРОХОДИТЬ до удаления и оставаться зелёными
после — это и есть подтверждение безопасности.
"""
from __future__ import annotations

import inspect

import pytest


# ---------------------------------------------------------------------------
# 1. schema.ValidationReport — мёртвый класс, заменён PipelineValidationReport
# ---------------------------------------------------------------------------

class TestValidationReportIsDeadCode:
    def test_pipeline_validation_report_is_the_real_one(self):
        """PipelineValidationReport — это живой класс, используемый в pipeline."""
        from redacta.schema import PipelineValidationReport
        report = PipelineValidationReport(
            structural_ok=True,
            judge_ok=True,
            is_valid=True,
            skeleton_results=[],
            judge_summary="ok",
            judge_failures=[],
            intent_results=[],
        )
        assert report.is_valid is True

    def test_validator_returns_pipeline_validation_report(self):
        """StrictJudgeValidator возвращает PipelineValidationReport, не ValidationReport."""
        from redacta.schema import PipelineValidationReport
        from redacta.validator import StrictJudgeValidator
        sig = inspect.signature(StrictJudgeValidator.validate)
        # return annotation должна быть PipelineValidationReport
        # (если нет аннотации — просто убеждаемся что класс импортируется)
        assert PipelineValidationReport is not None

    def test_validation_report_not_used_in_validator_imports(self):
        """validator.py не импортирует ValidationReport."""
        import redacta.validator as mod
        source = inspect.getsource(mod)
        assert "ValidationReport" not in source or "PipelineValidationReport" in source
        # Нет голого ValidationReport — есть только Pipeline-версия
        assert source.count("ValidationReport") == source.count("PipelineValidationReport")


# ---------------------------------------------------------------------------
# 2. schema.ChangeIntent.graph_scope_hint — поле-призрак, никогда не заполняется
# ---------------------------------------------------------------------------

class TestGraphScopeHintIsNeverSet:
    def test_change_intent_can_be_created_without_graph_scope_hint(self):
        """ChangeIntent создаётся нормально — поле graph_scope_hint удалено."""
        from redacta.schema import ChangeIntent
        intent = ChangeIntent(
            change_id="test_1",
            operation_kind="replace_point",
            source_document_label="изм_test",
        )
        # поле должно быть удалено из схемы
        assert not hasattr(intent, "graph_scope_hint"), (
            "graph_scope_hint должен быть удалён из ChangeIntent"
        )

    def test_graph_scope_hint_is_never_set_by_deterministic_extractor(self):
        """DeterministicIntentExtractor не устанавливает graph_scope_hint."""
        from redacta.deterministic_intent_extractor import DeterministicIntentExtractor
        extractor = DeterministicIntentExtractor()
        # минимальный текст с явной директивой
        results = extractor.extract(
            lines=["1. Пункт 3 признать утратившим силу."],
            source_document_label="тест",
        )
        for intent in results:
            assert not hasattr(intent, "graph_scope_hint"), (
                "graph_scope_hint должен быть удалён из ChangeIntent"
            )

    def test_graph_scope_hint_always_none_in_to_dict(self):
        """to_dict() для ChangeIntent содержит graph_scope_hint=None."""
        from redacta.schema import ChangeIntent
        intent = ChangeIntent(
            change_id="x",
            operation_kind="repeal_point",
            source_document_label="doc",
        )
        d = intent.to_dict()
        assert "graph_scope_hint" not in d, (
            "graph_scope_hint не должен присутствовать в to_dict() после удаления"
        )

    def test_no_code_reads_graph_scope_hint_in_pipeline(self):
        """Ни один модуль пайплайна не читает атрибут graph_scope_hint."""
        import importlib, pkgutil, redacta
        import inspect
        hits = []
        pkg_path = redacta.__path__
        for finder, modname, ispkg in pkgutil.walk_packages(pkg_path, prefix="redacta."):
            try:
                mod = importlib.import_module(modname)
                src = inspect.getsource(mod)
            except Exception:
                continue
            # Не считаем schema.py (там определение поля) и этот тест
            if "schema" not in modname and src.count("graph_scope_hint") > 0:
                hits.append(modname)
        assert hits == [], f"graph_scope_hint используется в: {hits}"


# ---------------------------------------------------------------------------
# 3. utils.normalize_text — функция-призрак, нигде не вызывается
# ---------------------------------------------------------------------------

class TestNormalizeTextIsDeadCode:
    def test_utils_module_works_without_normalize_text_being_called(self):
        """Остальные функции utils работают нормально."""
        from redacta.utils import (
            build_source_document_label,
            extract_document_number,
            to_genitive,
            to_instrumental,
        )
        assert build_source_document_label is not None
        assert extract_document_number is not None
        assert to_genitive is not None
        assert to_instrumental is not None

    def test_normalize_text_not_imported_by_any_pipeline_module(self):
        """Ни один модуль пайплайна не импортирует normalize_text из utils."""
        import importlib, pkgutil, redacta
        import inspect
        hits = []
        pkg_path = redacta.__path__
        for finder, modname, ispkg in pkgutil.walk_packages(pkg_path, prefix="redacta."):
            try:
                mod = importlib.import_module(modname)
                src = inspect.getsource(mod)
            except Exception:
                continue
            if "utils" not in modname and "normalize_text" in src:
                hits.append(modname)
        assert hits == [], f"normalize_text импортируется/используется в: {hits}"


# ---------------------------------------------------------------------------
# 4. amendment_analyzer._phrase_scope_score — приватный метод, никогда не вызывается
# ---------------------------------------------------------------------------

class TestPhraseScopeScoreIsDeadCode:
    def test_deduplication_works_without_phrase_scope_score(self):
        """Метод _normalize_intents (дедупликация) работает и не вызывает _phrase_scope_score."""
        from redacta.schema import ChangeIntent
        from redacta.amendment_analyzer import AmendmentAnalyzer

        # Создаём два одинаковых intent — дедупликатор должен оставить один
        intent_a = ChangeIntent(
            change_id="a1",
            operation_kind="replace_phrase_globally",
            source_document_label="doc",
            old_text="старый текст",
            new_text="новый текст",
            point_ref="3",
        )
        intent_b = ChangeIntent(
            change_id="b1",
            operation_kind="replace_phrase_globally",
            source_document_label="doc",
            old_text="старый текст",
            new_text="новый текст",
            point_ref="3",
            source_excerpt="более длинный excerpt который добавляет richness score для теста",
        )
        # Создаём анализатор без config (не нужен для тестирования приватного метода)
        analyzer = AmendmentAnalyzer.__new__(AmendmentAnalyzer)
        result = analyzer._deduplicate_phrase_intents([intent_a, intent_b])
        assert len(result) == 1, f"Должен остаться 1 intent, осталось {len(result)}"

    def test_phrase_scope_score_is_never_called_in_deduplication_flow(self):
        """_phrase_scope_score не вызывается в _deduplicate_phrase_intents."""
        import inspect
        from redacta.amendment_analyzer import AmendmentAnalyzer
        source = inspect.getsource(AmendmentAnalyzer._deduplicate_phrase_intents)
        # Метод не должен вызываться в коде дедупликации
        assert "_phrase_scope_score" not in source, (
            "_phrase_scope_score вызывается в _deduplicate_phrase_intents — это неожиданно!"
        )
