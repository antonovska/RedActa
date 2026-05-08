from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any


@dataclass
class ChangeIntent:
    change_id: str
    operation_kind: str
    source_document_label: str
    appendix_number: str = ""
    anchor_text_hint: str = ""
    point_ref: str = ""
    point_number: int | None = None
    structured_entry_ref: str | None = None
    table_row_ref: str | None = None
    table_column_ref: str | None = None
    parent_point_ref: str = ""
    parent_point_number: int | None = None
    subpoint_ref: str = ""
    paragraph_ordinal: int | None = None
    person_name_hint: str = ""
    new_text: str = ""
    old_text: str = ""
    appended_words: str = ""
    new_item_text: str = ""
    new_block_lines: list[str] = field(default_factory=list)
    section_hint: str = ""
    source_excerpt: str = ""
    confidence: float = 0.0

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class ResolutionCandidate:
    candidate_id: str
    absolute_paragraph_index: int
    paragraph_text: str
    section_path: str = ""
    extra: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class ResolvedOperation:
    operation_id: str
    operation_kind: str
    status: str
    source_document_label: str
    paragraph_indices: list[int] = field(default_factory=list)
    insert_after_index: int | None = None
    appendix_number: str = ""
    anchor_text_hint: str = ""
    point_ref: str = ""
    point_number: int | None = None
    parent_point_ref: str = ""
    parent_point_number: int | None = None
    subpoint_ref: str = ""
    paragraph_ordinal: int | None = None
    person_name_hint: str = ""
    section_hint: str = ""
    old_text: str = ""
    new_text: str = ""
    appended_words: str = ""
    new_item_text: str = ""
    new_block_lines: list[str] = field(default_factory=list)
    note_text: str = ""
    source_excerpt: str = ""
    ambiguity_reason: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class AmendmentDocumentMeta:
    source_path: str
    document_label: str
    document_number: str = ""
    document_date_iso: str = ""
    complexity: str = "plain"  # "plain" | "table_heavy" | "media_heavy"

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class AmendmentAnalysis:
    metadata: AmendmentDocumentMeta
    intents: list[ChangeIntent]
    raw_model_output: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "metadata": self.metadata.to_dict(),
            "intents": [item.to_dict() for item in self.intents],
            "raw_model_output": self.raw_model_output,
        }


@dataclass
class HeaderBlock:
    header_id: str
    scope: str
    start_paragraph_index: int
    end_paragraph_index: int
    appendix_number: str = ""
    title_lines: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class BaseAnalysis:
    base_doc: str
    header_blocks: list[HeaderBlock]
    complexity: str = "plain"  # "plain" | "table_heavy" | "media_heavy"

    def to_dict(self) -> dict[str, Any]:
        return {
            "base_doc": self.base_doc,
            "header_blocks": [item.to_dict() for item in self.header_blocks],
            "complexity": self.complexity,
        }


@dataclass
class ServiceTableSpec:
    table_id: str
    scope: str
    insert_after_paragraph_index: int
    document_labels: list[str]
    appendix_number: str = ""

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class ValidationChecklist:
    checks: list[dict[str, Any]]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class PipelineValidationReport:
    structural_ok: bool
    judge_ok: bool
    is_valid: bool
    skeleton_results: list[dict[str, Any]]
    judge_summary: str
    judge_failures: list[str]
    intent_results: list[dict[str, Any]]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


ResolvedOperationList = list[ResolvedOperation]
