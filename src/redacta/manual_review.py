from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .schema import ResolvedOperation


SAFE_OPERATION_KINDS = {
    "replace_point",
    "replace_phrase_globally",
    "repeal_point",
}


@dataclass
class ManualReviewSplit:
    safe_to_apply: list[ResolvedOperation]
    blocked_operations: list[dict[str, Any]]


def split_operations_for_manual_review(operations: list[ResolvedOperation]) -> ManualReviewSplit:
    safe_to_apply: list[ResolvedOperation] = []
    blocked_operations: list[dict[str, Any]] = []
    for operation in operations:
        reason = _manual_review_reason(operation)
        if reason:
            blocked_operations.append(_blocked_operation(operation, reason))
        else:
            safe_to_apply.append(operation)
    return ManualReviewSplit(safe_to_apply=safe_to_apply, blocked_operations=blocked_operations)


def _manual_review_reason(operation: ResolvedOperation) -> str:
    if operation.status != "resolved":
        return operation.ambiguity_reason or operation.status or "operation is not resolved"
    if operation.operation_kind not in SAFE_OPERATION_KINDS:
        return "operation_kind requires manual review"
    if operation.operation_kind == "replace_phrase_globally" and not _has_explicit_global_scope(operation):
        return "global replacement scope is not explicit"
    return ""


def _has_explicit_global_scope(operation: ResolvedOperation) -> bool:
    if operation.metadata.get("scope") == "global" or operation.metadata.get("global_scope") is True:
        return True
    if any(
        [
            operation.point_ref,
            operation.point_number is not None,
            operation.parent_point_ref,
            operation.parent_point_number is not None,
            operation.subpoint_ref,
            operation.paragraph_ordinal is not None,
            operation.anchor_text_hint,
        ]
    ):
        return False
    excerpt = operation.source_excerpt.lower()
    if "point" in excerpt or "paragraph" in excerpt:
        return False
    return True


def _blocked_operation(operation: ResolvedOperation, reason: str) -> dict[str, Any]:
    return {
        "operation_id": operation.operation_id,
        "operation_kind": operation.operation_kind,
        "status": operation.status,
        "reason": reason,
        "source_excerpt": operation.source_excerpt,
    }
