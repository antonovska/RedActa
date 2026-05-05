from __future__ import annotations

from .schema import AmendmentAnalysis, BaseAnalysis, ResolvedOperationList, ServiceTableSpec, ValidationChecklist


class ValidationChecklistBuilder:
    def build(
        self,
        base_analysis: BaseAnalysis,
        amendment_analyses: list[AmendmentAnalysis],
        service_table_specs: list[ServiceTableSpec],
        resolved_operations: ResolvedOperationList,
        runtime_checks: list[dict] | None = None,
    ) -> ValidationChecklist:
        checks = list(runtime_checks or [])
        for spec in service_table_specs:
            checks.append(
                {
                    "stage": "05_validation",
                    "check_id": spec.table_id,
                    "kind": "service_table_present",
                    "ok": None,
                    "severity": "required",
                    "details": {
                        "scope": spec.scope,
                        "appendix_number": spec.appendix_number,
                        "expected_documents": list(spec.document_labels),
                    },
                }
            )
        for analysis in amendment_analyses:
            for intent in analysis.intents:
                checks.append(
                    {
                        "stage": "05_validation",
                        "check_id": f"change_{intent.change_id}_{analysis.metadata.document_label}",
                        "kind": intent.operation_kind,
                        "ok": None,
                        "severity": "required",
                        "details": {
                            "source_document_label": analysis.metadata.document_label,
                            "appendix_number": intent.appendix_number,
                            "point_ref": intent.point_ref,
                            "point_number": intent.point_number,
                            "parent_point_ref": intent.parent_point_ref,
                            "parent_point_number": intent.parent_point_number,
                            "subpoint_ref": intent.subpoint_ref,
                            "paragraph_ordinal": intent.paragraph_ordinal,
                            "section_hint": intent.section_hint,
                            "anchor_text_hint": intent.anchor_text_hint,
                            "expected_new_text": intent.new_text,
                            "expected_old_text": intent.old_text,
                            "expected_appended_words": intent.appended_words,
                            "expected_new_item_text": intent.new_item_text,
                            "expected_new_block_lines": list(intent.new_block_lines),
                            "source_excerpt": intent.source_excerpt,
                        },
                    }
                )
        expected_keys = [
            f"{analysis.metadata.document_label}::{intent.change_id}"
            for analysis in amendment_analyses
            for intent in analysis.intents
        ]
        operation_keys = [
            f"{operation.source_document_label}::{operation.operation_id}"
            for operation in resolved_operations
        ]
        missing_keys = [key for key in expected_keys if key not in operation_keys]
        extra_keys = [key for key in operation_keys if key not in expected_keys]
        non_resolved = [
            {
                "operation_key": f"{operation.source_document_label}::{operation.operation_id}",
                "status": operation.status,
                "ambiguity_reason": operation.ambiguity_reason,
            }
            for operation in resolved_operations
            if operation.status != "resolved"
        ]
        checks.append(
            {
                "stage": "05_validation",
                "check_id": "resolution_completeness",
                "kind": "resolution_gate",
                "ok": not missing_keys and not extra_keys and not non_resolved,
                "severity": "blocking",
                "details": {
                    "expected_intents": expected_keys,
                    "resolved_operations": operation_keys,
                    "missing_intents": missing_keys,
                    "extra_operations": extra_keys,
                    "non_resolved_operations": non_resolved,
                },
            }
        )
        for operation in resolved_operations:
            checks.append(
                {
                    "stage": "05_validation",
                    "check_id": f"resolved_{operation.operation_id}",
                    "kind": operation.operation_kind,
                    "ok": operation.status == "resolved",
                    "severity": "required",
                    "details": {
                        "status": operation.status,
                        "paragraph_indices": list(operation.paragraph_indices),
                        "insert_after_index": operation.insert_after_index,
                        "appendix_number": operation.appendix_number,
                        "point_ref": operation.point_ref,
                        "point_number": operation.point_number,
                        "parent_point_ref": operation.parent_point_ref,
                        "parent_point_number": operation.parent_point_number,
                        "subpoint_ref": operation.subpoint_ref,
                        "paragraph_ordinal": operation.paragraph_ordinal,
                        "section_hint": operation.section_hint,
                        "ambiguity_reason": operation.ambiguity_reason,
                    },
                }
            )
        return ValidationChecklist(checks=checks)
