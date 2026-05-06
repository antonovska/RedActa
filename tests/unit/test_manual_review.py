from __future__ import annotations

from redacta.manual_review import split_operations_for_manual_review
from redacta.schema import ResolvedOperation


def test_split_operations_allows_only_resolved_safe_operation_kinds() -> None:
    safe = ResolvedOperation(
        operation_id="c1",
        operation_kind="replace_phrase_globally",
        status="resolved",
        source_document_label="doc",
        old_text="client",
        new_text="customer",
    )
    unsupported = ResolvedOperation(
        operation_id="c2",
        operation_kind="insert_point",
        status="resolved",
        source_document_label="doc",
        new_text="2. New point.",
    )
    ambiguous = ResolvedOperation(
        operation_id="c3",
        operation_kind="replace_point",
        status="ambiguous",
        source_document_label="doc",
        ambiguity_reason="point_ref not found",
        source_excerpt="replace point 3",
    )

    result = split_operations_for_manual_review([safe, unsupported, ambiguous])

    assert result.safe_to_apply == [safe]
    assert [item["operation_id"] for item in result.blocked_operations] == ["c2", "c3"]
    assert result.blocked_operations[0]["reason"] == "operation_kind requires manual review"
    assert result.blocked_operations[1]["reason"] == "point_ref not found"


def test_split_operations_blocks_global_replace_without_scope_signal() -> None:
    operation = ResolvedOperation(
        operation_id="c1",
        operation_kind="replace_phrase_globally",
        status="resolved",
        source_document_label="doc",
        source_excerpt="replace phrase in point 4",
    )

    result = split_operations_for_manual_review([operation])

    assert result.safe_to_apply == []
    assert result.blocked_operations == [
        {
            "operation_id": "c1",
            "operation_kind": "replace_phrase_globally",
            "status": "resolved",
            "reason": "global replacement scope is not explicit",
            "source_excerpt": "replace phrase in point 4",
        }
    ]
