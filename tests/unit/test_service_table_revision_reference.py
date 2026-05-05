from __future__ import annotations

from graph_pipeline.utils import format_revision_reference


def test_format_revision_reference_preserves_document_type_capitalization() -> None:
    label = "\u041f\u0440\u0438\u043a\u0430\u0437 \u041c\u0438\u043d\u0444\u0438\u043d\u0430 \u0420\u043e\u0441\u0441\u0438\u0438 \u043e\u0442 03.10.2025 N 141\u043d"

    assert format_revision_reference(label) == (
        "(\u0432 \u0440\u0435\u0434. \u041f\u0440\u0438\u043a\u0430\u0437\u0430 "
        "\u041c\u0438\u043d\u0444\u0438\u043d\u0430 \u0420\u043e\u0441\u0441\u0438\u0438 "
        "\u043e\u0442 03.10.2025 N 141\u043d)"
    )
