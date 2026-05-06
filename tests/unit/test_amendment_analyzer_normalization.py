from __future__ import annotations

from redacta.amendment_analyzer import AmendmentAnalyzer
from redacta.schema import ChangeIntent


def _analyzer() -> AmendmentAnalyzer:
    instance = object.__new__(AmendmentAnalyzer)
    return instance


def test_coverage_key_ignores_terminal_punctuation() -> None:
    analyzer = _analyzer()

    assert analyzer._coverage_key("4. По всему тексту договора термин \"клиент\" заменить на \"заказчик\";") == (
        analyzer._coverage_key("4. По всему тексту договора термин \"клиент\" заменить на \"заказчик\".")
    )


def test_global_phrase_scope_does_not_keep_directive_number_as_point_scope() -> None:
    analyzer = _analyzer()
    intent = ChangeIntent(
        change_id="c4",
        operation_kind="replace_phrase_globally",
        source_document_label="Распоряжение ОАО РЖД от 28.04.2020 N 944 р",
        source_excerpt="4. По всему тексту договора термин \"клиент\" заменить на \"заказчик\".",
        old_text="\"клиент\"",
        new_text="\"заказчик\"",
        point_ref="4",
        point_number=4,
        parent_point_ref="4",
        parent_point_number=4,
    )

    analyzer._normalize_phrase_text_fields(intent)
    analyzer._normalize_global_phrase_scope(intent)

    assert intent.old_text == "клиент"
    assert intent.new_text == "заказчик"
    assert intent.point_ref == ""
    assert intent.point_number is None
    assert intent.parent_point_ref == ""
    assert intent.parent_point_number is None


def test_extract_replaced_words_supports_term_and_phrase_replace_with_na() -> None:
    analyzer = _analyzer()

    assert analyzer._extract_replaced_words('термин "клиент" заменить на "заказчик"') == ("клиент", "заказчик")
    assert analyzer._extract_replaced_words(
        'фразу "60 (шестьдесят) календарных дней" заменить на "30 (тридцать) календарных дней"'
    ) == ("60 (шестьдесят) календарных дней", "30 (тридцать) календарных дней")
