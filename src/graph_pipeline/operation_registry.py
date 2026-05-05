from __future__ import annotations

from dataclasses import dataclass
from enum import Enum

from .schema import ChangeIntent


class OperationSupport(str, Enum):
    SUPPORTED = "supported"
    DEFERRED = "deferred"


@dataclass(frozen=True)
class OperationSpec:
    kind: str
    support: OperationSupport
    required_any: tuple[tuple[str, ...], ...] = ()
    required_all: tuple[str, ...] = ()


OPERATION_REGISTRY: dict[str, OperationSpec] = {
    "insert_point": OperationSpec("insert_point", OperationSupport.SUPPORTED, required_all=("new_text",)),
    "replace_point": OperationSpec(
        "replace_point",
        OperationSupport.SUPPORTED,
        required_any=(("point_ref", "point_number"),),
        required_all=("new_text",),
    ),
    "replace_phrase_globally": OperationSpec(
        "replace_phrase_globally",
        OperationSupport.SUPPORTED,
        required_all=("old_text",),
    ),
    "append_words_to_point": OperationSpec(
        "append_words_to_point",
        OperationSupport.SUPPORTED,
        required_any=(("point_ref", "point_number"),),
        required_all=("new_text",),
    ),
    "repeal_point": OperationSpec(
        "repeal_point",
        OperationSupport.SUPPORTED,
        required_any=(("point_ref", "point_number"),),
    ),
    "append_section_item": OperationSpec(
        "append_section_item",
        OperationSupport.SUPPORTED,
        required_all=("new_text",),
    ),
    "replace_appendix_block": OperationSpec(
        "replace_appendix_block",
        OperationSupport.SUPPORTED,
        required_all=("new_block_lines",),
    ),
    "insert_list_entry": OperationSpec("insert_list_entry", OperationSupport.DEFERRED),
    "replace_person_role": OperationSpec("replace_person_role", OperationSupport.DEFERRED),
    "replace_structured_entry": OperationSpec(
        "replace_structured_entry",
        OperationSupport.SUPPORTED,
        required_any=(("structured_entry_ref", "table_row_ref", "point_ref", "point_number"),),
        required_all=("new_block_lines",),
    ),
    "replace_table_row": OperationSpec(
        "replace_table_row",
        OperationSupport.SUPPORTED,
        required_any=(("table_row_ref", "structured_entry_ref", "point_ref", "point_number"),),
        required_all=("new_block_lines",),
    ),
}


def _has_value(intent: ChangeIntent, field_name: str) -> bool:
    value = getattr(intent, field_name, None)
    if value is None:
        return False
    if isinstance(value, str):
        return bool(value.strip())
    if isinstance(value, list):
        return bool(value)
    return True


def validate_intent_fields(intent: ChangeIntent) -> list[str]:
    spec = OPERATION_REGISTRY.get(intent.operation_kind)
    if spec is None:
        return [f"unsupported operation_kind: {intent.operation_kind}"]

    errors: list[str] = []
    for field_name in spec.required_all:
        if not _has_value(intent, field_name):
            errors.append(f"missing required field: {field_name}")

    for group in spec.required_any:
        if not any(_has_value(intent, field_name) for field_name in group):
            errors.append("missing one of: " + ", ".join(group))

    return errors
