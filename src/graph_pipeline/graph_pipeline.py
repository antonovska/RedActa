from __future__ import annotations

import argparse
import copy
import json
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from pathlib import Path
from typing import Any, Literal, TypedDict

from docx import Document

from .amendment_analyzer import AmendmentAnalyzer
from .base_agent import normalize_for_match, read_non_empty_paragraphs
from .base_analyzer import BaseAnalyzer
from .case_loader import load_case
from .config import load_models_config
from .editor import PipelineEditor
from .ooxml_reader import find_section_candidates, find_table_section_candidates, read_paragraph_records
from .pipeline_checklist import PipelineChecklist
from .resolver import PipelineResolver
from .run_case import (
    _analysis_gate_details,
    _enforce_resolution_gate,
    _record_analysis_checks,
    _record_initial_checks,
    _repair_analysis_once,
    _run_single_base_flow,
)
from .service_tables import build_service_table_specs
from .schema import ChangeIntent, PipelineValidationReport, ResolutionCandidate, ResolvedOperation
from .skeleton_builder import SkeletonBuilder
from .validation_checklist_builder import ValidationChecklistBuilder
from .validator import StrictJudgeValidator
from .validator import _read_lines_with_tables
from .utils import to_instrumental


Topology = Literal[
    "standard_single",
    "special_single_amendment_multi_base",
    "special_multi_amendment_single_base",
]


class PipelineState(TypedDict, total=False):
    case_id: str
    workspace_root: str
    models_config: str
    dry_run: bool

    case: dict[str, Any]
    topology: Topology
    amendment_docs: list[str]
    base_docs: list[str]

    config: dict[str, Any]
    artifacts_root: str
    debug_states_dir: str

    amendment_analyses: list[Any]
    base_analyses: dict[str, Any]
    amendment_analysis_dicts: list[dict[str, Any]]
    base_analysis_dicts: dict[str, dict[str, Any]]

    analysis_gate_ok: bool
    analysis_gate_details: dict[str, Any]
    analysis_repair_attempts: int
    topology_route: str

    result: dict[str, Any]
    validation: dict[str, Any]
    output_docs: list[str]

    current_base_doc: str
    current_artifacts_dir: str
    current_doc: str
    service_table_specs: list[Any]
    service_table_spec_dicts: list[dict[str, Any]]
    amendment_index: int
    current_resolution: dict[str, Any]
    resolution_items: list[dict[str, Any]]
    resolution_trace: list[dict[str, Any]]
    resolution_skip_relevance_filter: bool
    resolved_operations: list[Any]
    resolved_operation_dicts: list[dict[str, Any]]
    resolution_gate_ok: bool
    resolution_gate_details: dict[str, Any]
    resolution_repair_attempts: int
    pre_edit_ok: bool
    pre_edit_details: dict[str, Any]
    human_review_required: bool
    human_review_package: str
    steps: list[dict[str, Any]]
    all_operations: list[Any]
    all_operation_dicts: list[dict[str, Any]]
    all_statuses: list[str]
    working_doc_history: list[dict[str, Any]]
    final_base_analysis: Any
    validation_checklist: Any
    structural_results: list[dict[str, Any]]
    structural_ok: bool
    deterministic_ok: bool
    deterministic_failures: list[str]
    hard_resolution_failure: bool
    operation_summary: dict[str, int]
    judge_payload: dict[str, Any]
    judge_ok: bool
    judge_summary: str
    judge_failures: list[str]
    validation_report: Any

    events: list[dict[str, Any]]
    errors: list[dict[str, Any]]
    snapshots: list[str]
    _runtime: dict[str, Any]


def _now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _event(stage: str, message: str, **details: Any) -> dict[str, Any]:
    return {"ts": _now_iso(), "stage": stage, "message": message, "details": details}


def _path_list(items: list[Path]) -> list[str]:
    return [str(item) for item in items]


def _json_safe(value: Any) -> Any:
    if isinstance(value, Path):
        return str(value)
    if hasattr(value, "to_dict"):
        return value.to_dict()
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items() if key != "_runtime"}
    if isinstance(value, list):
        return [_json_safe(item) for item in value]
    if isinstance(value, tuple):
        return [_json_safe(item) for item in value]
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return repr(value)


def _public_snapshot(state: PipelineState) -> dict[str, Any]:
    hidden = {"_runtime", "config"}
    return {key: _json_safe(value) for key, value in state.items() if key not in hidden}


def _write_snapshot(state: PipelineState, stage: str) -> str:
    debug_dir = Path(state.get("debug_states_dir") or Path(state["workspace_root"]) / "debug_states")
    debug_dir.mkdir(parents=True, exist_ok=True)
    index = len(state.get("snapshots", [])) + 1
    path = debug_dir / f"{state['case_id']}_{index:02d}_{stage}.json"
    path.write_text(json.dumps(_public_snapshot(state), ensure_ascii=False, indent=2), encoding="utf-8")
    return str(path)


def _merge_state(state: PipelineState, updates: dict[str, Any], stage: str, message: str) -> PipelineState:
    next_state: PipelineState = copy.copy(state)
    next_state.update(updates)
    events = list(next_state.get("events", []))
    events.append(_event(stage, message))
    next_state["events"] = events
    snapshot = _write_snapshot(next_state, stage)
    next_state["snapshots"] = [*next_state.get("snapshots", []), snapshot]
    return next_state


def make_initial_state(
    *,
    case_id: str,
    workspace_root: Path,
    models_config: Path | None = None,
    dry_run: bool = False,
) -> PipelineState:
    workspace = workspace_root.resolve()
    return {
        "case_id": str(case_id),
        "workspace_root": str(workspace),
        "models_config": str(models_config.resolve()) if models_config else "",
        "dry_run": dry_run,
        "analysis_repair_attempts": 0,
        "events": [],
        "errors": [],
        "snapshots": [],
        "_runtime": {},
    }


def node_load_case(state: PipelineState) -> PipelineState:
    workspace_root = Path(state["workspace_root"])
    case = load_case(workspace_root, state["case_id"])
    topology = case.get("case_topology", "standard_single")
    amendment_docs = _path_list(case["amendment_docs"])
    base_paths = case.get("base_docs") or ([case["base_doc"]] if case.get("base_doc") else [])
    return _merge_state(
        state,
        {
            "case": case,
            "topology": topology,
            "amendment_docs": amendment_docs,
            "base_docs": _path_list(base_paths),
            "artifacts_root": str(workspace_root / "artifacts" / state["case_id"]),
            "debug_states_dir": str(workspace_root / "debug_states"),
        },
        "load_case",
        "case loaded",
    )


def node_initialize_runtime(state: PipelineState) -> PipelineState:
    config_path = Path(state["models_config"]) if state.get("models_config") else None
    config = load_models_config(config_path)
    runtime = {
        "checklist": PipelineChecklist(state["case_id"]),
        "amendment_analyzer": AmendmentAnalyzer(config),
        "base_analyzer": BaseAnalyzer(),
        "skeleton_builder": SkeletonBuilder(),
        "resolver": PipelineResolver(config),
        "editor": PipelineEditor(),
        "checklist_builder": ValidationChecklistBuilder(),
        "validator": StrictJudgeValidator(config),
    }
    case = state["case"]
    base_docs = case.get("base_docs") or ([case["base_doc"]] if case.get("base_doc") else [])
    _record_initial_checks(
        runtime["checklist"],
        case_topology=state["topology"],
        amendment_paths=case["amendment_docs"],
        base_docs=base_docs,
    )
    next_runtime = dict(state.get("_runtime", {}))
    next_runtime.update(runtime)
    return _merge_state(
        state,
        {"config": config, "_runtime": next_runtime},
        "initialize_runtime",
        "runtime objects initialized",
    )


def node_analyze_documents(state: PipelineState) -> PipelineState:
    if state.get("dry_run"):
        return _merge_state(
            state,
            {
                "amendment_analyses": [],
                "base_analyses": {},
                "amendment_analysis_dicts": [],
                "base_analysis_dicts": {},
            },
            "analyze_documents",
            "dry run skipped LLM and DOCX analysis",
        )

    runtime = state["_runtime"]
    case = state["case"]
    base_docs = case.get("base_docs") or ([case["base_doc"]] if case.get("base_doc") else [])
    started = time.perf_counter()
    with ThreadPoolExecutor(max_workers=2) as executor:
        amendments_future = executor.submit(runtime["amendment_analyzer"].analyze_many, case["amendment_docs"])
        bases_future = executor.submit(
            lambda: {str(base_doc): runtime["base_analyzer"].analyze(base_doc) for base_doc in base_docs}
        )
        amendment_analyses = amendments_future.result()
        base_analyses = bases_future.result()
    _record_analysis_checks(
        runtime["checklist"],
        amendment_analyses=amendment_analyses,
        base_analyses=base_analyses,
    )
    return _merge_state(
        state,
        {
            "amendment_analyses": amendment_analyses,
            "base_analyses": base_analyses,
            "amendment_analysis_dicts": [item.to_dict() for item in amendment_analyses],
            "base_analysis_dicts": {key: item.to_dict() for key, item in base_analyses.items()},
        },
        "analyze_documents",
        f"analysis completed in {time.perf_counter() - started:.1f}s",
    )


def node_analysis_gate(state: PipelineState) -> PipelineState:
    if state.get("dry_run"):
        return _merge_state(
            state,
            {"analysis_gate_ok": True, "analysis_gate_details": {"dry_run": True}},
            "analysis_gate",
            "dry run analysis gate passed",
        )
    ok, details = _analysis_gate_details(
        state["amendment_analyses"],
        state["base_analyses"],
        state["case"]["amendment_docs"],
    )
    return _merge_state(
        state,
        {"analysis_gate_ok": ok, "analysis_gate_details": details},
        "analysis_gate",
        "analysis gate passed" if ok else "analysis gate requires repair",
    )


def node_repair_analysis(state: PipelineState) -> PipelineState:
    if state.get("dry_run"):
        return _merge_state(state, {"analysis_gate_ok": True}, "repair_analysis", "dry run repair skipped")
    attempts = int(state.get("analysis_repair_attempts", 0))
    if attempts >= 1:
        errors = [
            *state.get("errors", []),
            _event("repair_analysis", "analysis repair attempt limit reached", attempts=attempts),
        ]
        return _merge_state(state, {"errors": errors}, "repair_analysis", "repair limit reached")
    runtime = state["_runtime"]
    repaired = _repair_analysis_once(
        runtime["checklist"],
        case_id=state["case_id"],
        amendment_analyzer=runtime["amendment_analyzer"],
        amendment_paths=state["case"]["amendment_docs"],
        amendment_analyses=state["amendment_analyses"],
        base_analyses=state["base_analyses"],
    )
    ok, details = _analysis_gate_details(repaired, state["base_analyses"], state["case"]["amendment_docs"])
    return _merge_state(
        state,
        {
            "amendment_analyses": repaired,
            "amendment_analysis_dicts": [item.to_dict() for item in repaired],
            "analysis_gate_ok": ok,
            "analysis_gate_details": details,
            "analysis_repair_attempts": attempts + 1,
        },
        "repair_analysis",
        "analysis repair completed",
    )


def node_route_by_topology(state: PipelineState) -> PipelineState:
    topology = state.get("topology", "standard_single")
    if topology == "special_single_amendment_multi_base":
        route = "multi_base"
    elif topology == "special_multi_amendment_single_base":
        route = "sequential_multi_amendment"
    else:
        route = "standard_single"
    return _merge_state(
        state,
        {"topology_route": route},
        "route_by_topology",
        f"topology routed to {route}",
    )


def node_execute_pipeline(state: PipelineState) -> PipelineState:
    if state.get("dry_run"):
        result = {
            "case_id": state["case_id"],
            "case_topology": state.get("topology"),
            "dry_run": True,
            "amendment_docs": state.get("amendment_docs", []),
            "base_docs": state.get("base_docs", []),
        }
        return _merge_state(
            state,
            {"result": result, "validation": {"is_valid": True, "dry_run": True}, "output_docs": []},
            "execute_pipeline",
            "dry run pipeline execution skipped",
        )

    runtime = state["_runtime"]
    case = state["case"]
    workspace_root = Path(state["workspace_root"])
    artifacts_root = Path(state["artifacts_root"])
    topology = state["topology"]
    amendment_analyses = state["amendment_analyses"]
    base_analyses = state["base_analyses"]

    if topology == "special_single_amendment_multi_base":
        base_runs = []
        for index, base_doc in enumerate(case.get("base_docs", []), 1):
            base_runs.append(
                _run_single_base_flow(
                    case_id=state["case_id"],
                    workspace_root=workspace_root,
                    artifacts_dir=artifacts_root / f"base_{index}",
                    amendment_analyses=amendment_analyses,
                    base_doc=base_doc,
                    base_analysis=base_analyses[str(base_doc)],
                    runtime_checklist=runtime["checklist"],
                    base_analyzer=runtime["base_analyzer"],
                    skeleton_builder=runtime["skeleton_builder"],
                    resolver=runtime["resolver"],
                    editor=runtime["editor"],
                    checklist_builder=runtime["checklist_builder"],
                    validator=runtime["validator"],
                )
            )
        validation = {
            "is_valid": all(item["validation"]["is_valid"] for item in base_runs),
            "structural_ok": all(item["validation"]["structural_ok"] for item in base_runs),
            "judge_ok": all(item["validation"]["judge_ok"] for item in base_runs),
        }
        result = {
            "case_id": state["case_id"],
            "case_topology": topology,
            "workspace_root": state["workspace_root"],
            "amendments": [item.to_dict() for item in amendment_analyses],
            "base_runs": base_runs,
            "validation": validation,
        }
        output_docs = [item["output_doc"] for item in base_runs if item.get("output_doc")]
        return _merge_state(
            state,
            {"result": result, "validation": validation, "output_docs": output_docs},
            "execute_pipeline",
            "multi-base branches executed",
        )

    base_doc = case.get("base_doc")
    if base_doc is None:
        raise ValueError("base_doc is required for single-base topology")
    single_result = _run_single_base_flow(
        case_id=state["case_id"],
        workspace_root=workspace_root,
        artifacts_dir=artifacts_root,
        amendment_analyses=amendment_analyses,
        base_doc=base_doc,
        base_analysis=base_analyses[str(base_doc)],
        runtime_checklist=runtime["checklist"],
        base_analyzer=runtime["base_analyzer"],
        skeleton_builder=runtime["skeleton_builder"],
        resolver=runtime["resolver"],
        editor=runtime["editor"],
        checklist_builder=runtime["checklist_builder"],
        validator=runtime["validator"],
    )
    result = {
        "case_id": state["case_id"],
        "case_topology": topology,
        "workspace_root": state["workspace_root"],
        "amendments": [item.to_dict() for item in amendment_analyses],
        **single_result,
    }
    if len(single_result.get("steps", [])) > 1:
        result["stage_outputs"] = [
            {
                "stage_index": index,
                "output_doc": step["output_doc"],
                "source_document_label": step["amendment"]["metadata"]["document_label"],
            }
            for index, step in enumerate(single_result["steps"], 1)
        ]
    return _merge_state(
        state,
        {
            "result": result,
            "validation": result["validation"],
            "output_docs": [result["output_doc"]] if result.get("output_doc") else [],
        },
        "execute_pipeline",
        "single-base pipeline executed",
    )


def node_prepare_single_base_flow(state: PipelineState) -> PipelineState:
    if state.get("dry_run"):
        return _merge_state(
            state,
            {
                "amendment_index": 0,
                "steps": [],
                "all_operations": [],
                "all_operation_dicts": [],
                "all_statuses": [],
                "working_doc_history": [],
                "resolution_repair_attempts": 0,
            },
            "prepare_single_base_flow",
            "dry run single-base state prepared",
        )
    base_doc = state["case"].get("base_doc")
    if base_doc is None:
        raise ValueError("base_doc is required for single-base topology")
    artifacts_dir = Path(state["artifacts_root"])
    artifacts_dir.mkdir(parents=True, exist_ok=True)
    return _merge_state(
        state,
        {
            "current_base_doc": str(base_doc),
            "current_artifacts_dir": str(artifacts_dir),
            "amendment_index": 0,
            "steps": [],
            "all_operations": [],
            "all_operation_dicts": [],
            "all_statuses": [],
            "working_doc_history": [],
            "resolution_repair_attempts": 0,
        },
        "prepare_single_base_flow",
        "single-base state prepared",
    )


def node_build_skeleton(state: PipelineState) -> PipelineState:
    if state.get("dry_run"):
        return _merge_state(state, {"current_doc": ""}, "build_skeleton", "dry run skeleton skipped")
    runtime = state["_runtime"]
    base_doc = Path(state["current_base_doc"])
    artifacts_dir = Path(state["current_artifacts_dir"])
    skeleton_doc = artifacts_dir / "working_skeleton.docx"
    base_analysis = state["base_analyses"][str(base_doc)]
    service_table_specs = runtime["skeleton_builder"].build(
        base_doc,
        base_analysis,
        state["amendment_analyses"],
        skeleton_doc,
    )
    checklist = runtime["checklist"]
    checklist.add(
        stage="02_skeleton",
        check_id=f"skeleton_created_{base_doc.stem}",
        kind="skeleton_created",
        ok=skeleton_doc.exists(),
        details={"base_doc": str(base_doc), "skeleton_doc": str(skeleton_doc)},
    )
    checklist.add(
        stage="02_skeleton",
        check_id=f"service_tables_planned_{base_doc.stem}",
        kind="service_tables_planned",
        ok=bool(service_table_specs),
        details={"service_table_specs": [item.to_dict() for item in service_table_specs]},
    )
    checklist.add(
        stage="02_skeleton",
        check_id=f"document_service_table_planned_{base_doc.stem}",
        kind="document_service_table_planned",
        ok=any(item.scope == "document" for item in service_table_specs),
        details={"document_scope_required": True},
    )
    for spec in service_table_specs:
        if spec.scope != "appendix":
            continue
        checklist.add(
            stage="02_skeleton",
            check_id=f"appendix_service_table_planned_{base_doc.stem}_{spec.appendix_number}",
            kind="appendix_service_table_planned",
            ok=True,
            details=spec.to_dict(),
        )
    return _merge_state(
        state,
        {
            "current_doc": str(skeleton_doc),
            "service_table_specs": service_table_specs,
            "service_table_spec_dicts": [item.to_dict() for item in service_table_specs],
            "working_doc_history": [
                *state.get("working_doc_history", []),
                {"stage": "skeleton", "output_doc": str(skeleton_doc)},
            ],
        },
        "build_skeleton",
        "working skeleton built",
    )


def node_resolve_current_amendment(state: PipelineState) -> PipelineState:
    if state.get("dry_run"):
        return _merge_state(state, {"resolved_operations": []}, "resolve_operations", "dry run resolution skipped")
    runtime = state["_runtime"]
    amendment_index = int(state.get("amendment_index", 0))
    amendment_analysis = state["amendment_analyses"][amendment_index]
    resolution = runtime["resolver"].resolve(Path(state["current_doc"]), amendment_analysis.intents)
    resolved_operations = resolution["resolved_operations"]
    runtime["checklist"].add(
        stage="03_resolution",
        check_id=f"resolution_{Path(state['current_base_doc']).stem}_{amendment_index + 1}",
        kind="resolution_completed",
        ok=bool(resolved_operations),
        details={
            "amendment": amendment_analysis.metadata.document_label,
            "operations": [item.to_dict() for item in resolved_operations],
            "debug_candidates": resolution["debug_candidates"],
            "summary": _operation_summary(resolved_operations),
        },
    )
    return _merge_state(
        state,
        {
            "current_resolution": resolution,
            "resolved_operations": resolved_operations,
            "resolved_operation_dicts": [item.to_dict() for item in resolved_operations],
            "resolution_repair_attempts": 0,
        },
        "resolve_operations",
        "current amendment resolved",
    )


def _ambiguous_operation(intent: ChangeIntent, reason: str) -> ResolvedOperation:
    return ResolvedOperation(
        operation_id=intent.change_id,
        operation_kind=intent.operation_kind,
        status="ambiguous",
        source_document_label=intent.source_document_label,
        appendix_number=intent.appendix_number,
        anchor_text_hint=intent.anchor_text_hint,
        point_ref=intent.point_ref,
        point_number=intent.point_number,
        parent_point_ref=intent.parent_point_ref,
        parent_point_number=intent.parent_point_number,
        subpoint_ref=intent.subpoint_ref,
        paragraph_ordinal=intent.paragraph_ordinal,
        person_name_hint=intent.person_name_hint,
        section_hint=intent.section_hint,
        old_text=intent.old_text,
        new_text=intent.new_text,
        appended_words=intent.appended_words,
        new_item_text=intent.new_item_text,
        new_block_lines=list(intent.new_block_lines),
        source_excerpt=intent.source_excerpt,
        ambiguity_reason=reason,
    )


def _unsupported_operation(intent: ChangeIntent) -> ResolvedOperation:
    return ResolvedOperation(
        operation_id=intent.change_id,
        operation_kind=intent.operation_kind,
        status="unsupported",
        source_document_label=intent.source_document_label,
        source_excerpt=intent.source_excerpt,
        ambiguity_reason="unsupported operation_kind",
    )


def _direct_new_block_operation(intent: ChangeIntent) -> ResolvedOperation | None:
    if not intent.new_block_lines:
        return None
    note = f"(введено {to_instrumental(intent.source_document_label)})"
    return ResolvedOperation(
        operation_id=intent.change_id,
        operation_kind=intent.operation_kind,
        status="resolved",
        source_document_label=intent.source_document_label,
        appendix_number=intent.appendix_number,
        section_hint=intent.section_hint,
        new_item_text=intent.new_item_text,
        new_block_lines=list(intent.new_block_lines),
        note_text=note,
        source_excerpt=intent.source_excerpt,
    )


def _build_append_section_candidates(
    resolver: Any,
    base_doc: Path,
    records: list[Any],
    intent: ChangeIntent,
) -> tuple[ResolvedOperation | None, list[ResolutionCandidate], str]:
    point_scoped = resolver._resolve_append_section_item_by_point_scope(records, intent)
    if point_scoped is not None:
        return point_scoped, [], "direct_point_scope"
    special_scoped = resolver._resolve_append_section_item_by_special_scope(records, intent)
    if special_scoped is not None:
        return special_scoped, [], "direct_special_scope"

    scoped_records = resolver._appendix_scope_records(records, intent)
    section_candidates = find_section_candidates(scoped_records, intent.section_hint)
    candidates: list[ResolutionCandidate] = []
    for idx, item in enumerate(section_candidates, 1):
        last_item = item["item_records"][-1]
        candidates.append(
            ResolutionCandidate(
                candidate_id=f"cand_{idx}",
                absolute_paragraph_index=last_item.absolute_index,
                paragraph_text=last_item.text,
                section_path=item["heading_text"],
                extra={"heading_text": item["heading_text"]},
            )
        )
    if not candidates:
        table_candidates = find_table_section_candidates(base_doc, intent.section_hint)
        for idx, item in enumerate(table_candidates, 1):
            candidates.append(
                ResolutionCandidate(
                    candidate_id=f"tbl_{idx}",
                    absolute_paragraph_index=-1,
                    paragraph_text=f"[table {item['table_index']} row {item['anchor_row_index']}] {item['heading_text']}",
                    section_path=item["heading_text"],
                    extra={
                        "heading_text": item["heading_text"],
                        "table_index": item["table_index"],
                        "anchor_row_index": item["anchor_row_index"],
                        "candidate_source": "table",
                    },
                )
            )
    if not candidates:
        direct = _direct_new_block_operation(intent)
        if direct is not None:
            return direct, [], "direct_new_block"
        return _ambiguous_operation(intent, "no section candidates"), [], "no_candidates"
    return None, candidates, "section_candidates"


def _operation_from_selected_candidate(
    intent: ChangeIntent,
    selected: ResolutionCandidate | None,
    candidates: list[ResolutionCandidate],
) -> ResolvedOperation:
    if selected is None:
        reason = "resolver LLM did not disambiguate"
        if intent.operation_kind in {"insert_list_entry", "replace_person_role"}:
            reason = "anchor_id disambiguation failed"
        return _ambiguous_operation(intent, reason if candidates else "no candidates")

    if intent.operation_kind == "append_section_item":
        note = f"(введено {to_instrumental(intent.source_document_label)})"
        return ResolvedOperation(
            operation_id=intent.change_id,
            operation_kind=intent.operation_kind,
            status="resolved",
            source_document_label=intent.source_document_label,
            insert_after_index=selected.absolute_paragraph_index if selected.absolute_paragraph_index >= 0 else None,
            section_hint=selected.section_path or intent.section_hint,
            new_item_text=intent.new_item_text,
            new_block_lines=list(intent.new_block_lines),
            note_text=note,
            source_excerpt=intent.source_excerpt,
        )
    if intent.operation_kind == "insert_list_entry":
        return ResolvedOperation(
            operation_id=intent.change_id,
            operation_kind=intent.operation_kind,
            status="resolved",
            source_document_label=intent.source_document_label,
            insert_after_index=selected.absolute_paragraph_index - 1,
            anchor_text_hint=intent.anchor_text_hint,
            new_text=intent.new_text,
            source_excerpt=intent.source_excerpt,
        )
    if intent.operation_kind == "replace_person_role":
        return ResolvedOperation(
            operation_id=intent.change_id,
            operation_kind=intent.operation_kind,
            status="resolved",
            source_document_label=intent.source_document_label,
            paragraph_indices=[selected.absolute_paragraph_index],
            person_name_hint=intent.person_name_hint,
            new_text=intent.new_text,
            source_excerpt=intent.source_excerpt,
        )
    return _unsupported_operation(intent)


def node_build_resolution_candidates(state: PipelineState) -> PipelineState:
    if state.get("dry_run"):
        return _merge_state(state, {"resolution_items": [], "resolution_trace": []}, "build_resolution_candidates", "dry run candidates skipped")
    resolver = state["_runtime"]["resolver"]._inner
    amendment_index = int(state.get("amendment_index", 0))
    amendment_analysis = state["amendment_analyses"][amendment_index]
    base_doc = Path(state["current_doc"])
    records = read_paragraph_records(base_doc)
    skip_relevance = bool(state.get("resolution_skip_relevance_filter", False))
    items: list[dict[str, Any]] = []
    trace: list[dict[str, Any]] = []
    for intent in amendment_analysis.intents:
        item: dict[str, Any] = {
            "intent": intent,
            "operation": None,
            "candidates": [],
            "selected_candidate_id": "",
            "selection_source": "",
            "phase": "candidate_generation",
        }
        if not skip_relevance and not resolver._intent_relevant_to_base(intent, Path(state["current_base_doc"]), records):
            item["operation"] = _ambiguous_operation(intent, "intent relevance filter rejected this intent for the current base")
            item["phase"] = "relevance_rejected"
            items.append(item)
            trace.append(
                {
                    "change_id": intent.change_id,
                    "operation_kind": intent.operation_kind,
                    "phase": item["phase"],
                    "candidate_count": 0,
                    "direct_status": item["operation"].status,
                    "skip_relevance_filter": skip_relevance,
                }
            )
            continue
        if intent.operation_kind == "insert_point":
            item["operation"] = resolver._resolve_insert_point(records, intent)
        elif intent.operation_kind == "replace_point":
            item["operation"] = resolver._resolve_replace_point(records, intent)
        elif intent.operation_kind == "replace_phrase_globally":
            item["operation"] = resolver._resolve_replace_phrase_globally(records, intent)
        elif intent.operation_kind == "append_words_to_point":
            item["operation"] = resolver._resolve_append_words_to_point(records, intent)
        elif intent.operation_kind == "repeal_point":
            item["operation"] = resolver._resolve_repeal_point(records, intent)
        elif intent.operation_kind == "replace_appendix_block":
            item["operation"] = resolver._resolve_replace_appendix_block(records, intent)
        elif intent.operation_kind == "append_section_item":
            operation, candidates, phase = _build_append_section_candidates(resolver, base_doc, records, intent)
            item["operation"] = operation
            item["candidates"] = candidates
            item["phase"] = phase
        elif intent.operation_kind in {"insert_list_entry", "replace_person_role"}:
            candidates = resolver._build_list_entry_candidates(records, intent)
            if candidates:
                item["candidates"] = candidates
                item["phase"] = "list_entry_candidates"
            else:
                direct = _direct_new_block_operation(intent)
                item["operation"] = direct or _ambiguous_operation(intent, "no list-entry candidates")
                item["phase"] = "direct_new_block" if direct else "no_candidates"
        else:
            item["operation"] = _unsupported_operation(intent)
            item["phase"] = "unsupported"
        items.append(item)
        trace.append(
            {
                "change_id": intent.change_id,
                "operation_kind": intent.operation_kind,
                "phase": item["phase"],
                "candidate_count": len(item["candidates"]),
                "direct_status": item["operation"].status if item["operation"] is not None else "",
                "skip_relevance_filter": skip_relevance,
            }
        )
    return _merge_state(
        state,
        {"resolution_items": items, "resolution_trace": trace},
        "build_resolution_candidates",
        "resolution candidates built",
    )


def node_semantic_rank_candidates(state: PipelineState) -> PipelineState:
    if state.get("dry_run"):
        return _merge_state(state, {}, "semantic_rank_candidates", "dry run semantic rank skipped")
    resolver = state["_runtime"]["resolver"]._inner
    items = list(state.get("resolution_items", []))
    trace_by_id = {item["change_id"]: dict(item) for item in state.get("resolution_trace", [])}
    for item in items:
        candidates = item.get("candidates", [])
        if not candidates:
            continue
        intent = item["intent"]
        before = [candidate.candidate_id for candidate in candidates]
        resolver._rank_candidates_semantically(intent, candidates)
        after = [candidate.candidate_id for candidate in candidates]
        trace = trace_by_id.setdefault(intent.change_id, {"change_id": intent.change_id})
        trace["semantic_ranked"] = before != after or any("semantic_score" in candidate.extra for candidate in candidates)
        trace["candidate_order"] = after
        trace["top_semantic_score"] = float((candidates[0].extra or {}).get("semantic_score", 0.0)) if candidates else 0.0
        trace["semantic_auto_threshold"] = float(resolver._semantic_auto_threshold)
        trace["semantic_auto_margin"] = float(resolver._semantic_auto_margin)
        trace["semantic_query"] = str((candidates[0].extra or {}).get("semantic_query", ""))
    return _merge_state(
        state,
        {"resolution_items": items, "resolution_trace": list(trace_by_id.values())},
        "semantic_rank_candidates",
        "semantic ranking completed",
    )


def node_select_resolution_candidates(state: PipelineState) -> PipelineState:
    if state.get("dry_run"):
        return _merge_state(state, {}, "select_resolution_candidates", "dry run candidate selection skipped")
    resolver = state["_runtime"]["resolver"]._inner
    items = list(state.get("resolution_items", []))
    trace_by_id = {item["change_id"]: dict(item) for item in state.get("resolution_trace", [])}
    for item in items:
        candidates = item.get("candidates", [])
        if item.get("operation") is not None or not candidates:
            continue
        intent = item["intent"]
        selected: ResolutionCandidate | None
        selection_source = ""
        if len(candidates) == 1:
            selected = candidates[0]
            selection_source = "single_candidate"
        else:
            selected = resolver._try_autopick_top_semantic_candidate(candidates)
            if selected is not None:
                selection_source = "semantic_autopick"
            else:
                selected = resolver._disambiguate_candidate(intent, candidates[: resolver._semantic_top_k])
                selection_source = "llm_disambiguation" if selected is not None else "llm_ambiguous"
        item["selected_candidate_id"] = selected.candidate_id if selected is not None else ""
        item["selection_source"] = selection_source
        trace = trace_by_id.setdefault(intent.change_id, {"change_id": intent.change_id})
        trace["selected_candidate_id"] = item["selected_candidate_id"]
        trace["selection_source"] = selection_source
    return _merge_state(
        state,
        {"resolution_items": items, "resolution_trace": list(trace_by_id.values())},
        "select_resolution_candidates",
        "candidate selection completed",
    )


def node_merge_resolved_operations(state: PipelineState) -> PipelineState:
    if state.get("dry_run"):
        return _merge_state(
            state,
            {"current_resolution": {"resolved_operations": [], "debug_candidates": {}}, "resolved_operations": []},
            "merge_resolved_operations",
            "dry run resolved operation merge skipped",
        )
    operations: list[ResolvedOperation] = []
    debug_candidates: dict[str, list[dict[str, Any]]] = {}
    for item in state.get("resolution_items", []):
        intent = item["intent"]
        candidates: list[ResolutionCandidate] = item.get("candidates", [])
        operation = item.get("operation")
        if operation is None:
            selected_id = str(item.get("selected_candidate_id", ""))
            selected = next((candidate for candidate in candidates if candidate.candidate_id == selected_id), None)
            operation = _operation_from_selected_candidate(intent, selected, candidates)
        operations.append(operation)
        if candidates:
            debug_candidates[intent.change_id] = [candidate.to_dict() for candidate in candidates]
    resolution = {"resolved_operations": operations, "debug_candidates": debug_candidates}
    amendment_index = int(state.get("amendment_index", 0))
    amendment_analysis = state["amendment_analyses"][amendment_index]
    is_repair = int(state.get("resolution_repair_attempts", 0)) > 0 and bool(state.get("resolution_skip_relevance_filter", False))
    state["_runtime"]["checklist"].add(
        stage="03_resolution",
        check_id=(
            f"resolution_repair_{Path(state['current_base_doc']).stem}_{amendment_index + 1}"
            if is_repair
            else f"resolution_{Path(state['current_base_doc']).stem}_{amendment_index + 1}"
        ),
        kind="resolution_repair_attempted" if is_repair else "resolution_completed",
        ok=bool(operations),
        details={
            "amendment": amendment_analysis.metadata.document_label,
            "operations": [item.to_dict() for item in operations],
            "debug_candidates": debug_candidates,
            "trace": state.get("resolution_trace", []),
            "summary": _operation_summary(operations),
        },
    )
    return _merge_state(
        state,
        {
            "current_resolution": resolution,
            "resolved_operations": operations,
            "resolved_operation_dicts": [item.to_dict() for item in operations],
            "resolution_skip_relevance_filter": False,
        },
        "merge_resolved_operations",
        "resolved operations merged",
    )


def _operation_summary(operations: list[Any]) -> dict[str, int]:
    return {
        "total": len(operations),
        "resolved": sum(1 for item in operations if item.status == "resolved"),
        "ambiguous": sum(1 for item in operations if item.status == "ambiguous"),
        "unsupported": sum(1 for item in operations if item.status == "unsupported"),
    }


def _resolution_gate_details(state: PipelineState) -> dict[str, Any]:
    amendment_analysis = state["amendment_analyses"][int(state.get("amendment_index", 0))]
    expected_ids = [intent.change_id for intent in amendment_analysis.intents]
    operations = state.get("resolved_operations", [])
    operation_ids = [operation.operation_id for operation in operations]
    return {
        "base_doc": state.get("current_base_doc", ""),
        "amendment": amendment_analysis.metadata.document_label,
        "expected_intent_ids": expected_ids,
        "operation_ids": operation_ids,
        "missing_intent_ids": [change_id for change_id in expected_ids if change_id not in operation_ids],
        "extra_operation_ids": [operation_id for operation_id in operation_ids if operation_id not in expected_ids],
        "non_resolved_operations": [
            {
                "operation_id": operation.operation_id,
                "status": operation.status,
                "reason": operation.ambiguity_reason,
            }
            for operation in operations
            if operation.status != "resolved"
        ],
    }


def node_resolution_gate(state: PipelineState) -> PipelineState:
    if state.get("dry_run"):
        return _merge_state(
            state,
            {"resolution_gate_ok": True, "resolution_gate_details": {"dry_run": True}},
            "resolution_gate",
            "dry run resolution gate passed",
        )
    amendment_index = int(state.get("amendment_index", 0))
    amendment_analysis = state["amendment_analyses"][amendment_index]
    ok = _enforce_resolution_gate(
        state["_runtime"]["checklist"],
        case_id=state["case_id"],
        base_doc=Path(state["current_base_doc"]),
        amendment_analysis=amendment_analysis,
        amendment_index=amendment_index + 1,
        resolved_operations=state["resolved_operations"],
        pass_name="repair" if int(state.get("resolution_repair_attempts", 0)) else "initial",
        raise_on_fail=False,
    )
    return _merge_state(
        state,
        {"resolution_gate_ok": ok, "resolution_gate_details": _resolution_gate_details(state)},
        "resolution_gate",
        "resolution gate passed" if ok else "resolution gate requires repair",
    )


def node_repair_resolution(state: PipelineState) -> PipelineState:
    if state.get("dry_run"):
        return _merge_state(state, {"resolution_gate_ok": True}, "repair_resolution", "dry run repair skipped")
    attempts = int(state.get("resolution_repair_attempts", 0))
    if attempts >= 1:
        errors = [
            *state.get("errors", []),
            _event("repair_resolution", "resolution repair attempt limit reached", attempts=attempts),
        ]
        return _merge_state(state, {"errors": errors}, "repair_resolution", "repair limit reached")
    return _merge_state(
        state,
        {
            "resolution_repair_attempts": attempts + 1,
            "resolution_skip_relevance_filter": True,
        },
        "repair_resolution",
        "resolution repair requested",
    )


def node_resolution_failed(state: PipelineState) -> PipelineState:
    raise RuntimeError(f"resolution gate failed after repair: {state.get('resolution_gate_details')}")


def node_apply_operations(state: PipelineState) -> PipelineState:
    if state.get("dry_run"):
        return _merge_state(state, {"amendment_index": int(state.get("amendment_index", 0)) + 1}, "apply_operations", "dry run edit skipped")
    runtime = state["_runtime"]
    amendment_index = int(state.get("amendment_index", 0))
    amendment_analysis = state["amendment_analyses"][amendment_index]
    output_doc = Path(state["current_artifacts_dir"]) / f"working_step_{amendment_index + 1}.docx"
    edit_result = runtime["editor"].edit(
        Path(state["current_doc"]),
        output_doc,
        state["current_resolution"]["resolved_operations"],
    )
    runtime["checklist"].add(
        stage="04_edit",
        check_id=f"edit_{Path(state['current_base_doc']).stem}_{amendment_index + 1}",
        kind="edit_applied",
        ok=output_doc.exists(),
        details={
            "amendment": amendment_analysis.metadata.document_label,
            "output_doc": str(output_doc),
            "statuses": list(edit_result["statuses"]),
        },
    )
    for drift_event in edit_result.get("drift_events", []):
        runtime["checklist"].add(
            stage="04_edit",
            check_id=f"drift_{drift_event['op_id']}_{amendment_index + 1}",
            kind="index_drift_event",
            ok=True,
            details=drift_event,
        )
    step = {
        "amendment": amendment_analysis.to_dict(),
        "resolution": {
            "resolved_operations": [item.to_dict() for item in state["resolved_operations"]],
            "debug_candidates": state["current_resolution"]["debug_candidates"],
        },
        "edit": edit_result,
        "output_doc": str(output_doc),
    }
    all_operations = [*state.get("all_operations", []), *state["resolved_operations"]]
    return _merge_state(
        state,
        {
            "steps": [*state.get("steps", []), step],
            "all_operations": all_operations,
            "all_operation_dicts": [item.to_dict() for item in all_operations],
            "all_statuses": [*state.get("all_statuses", []), *edit_result["statuses"]],
            "current_doc": str(output_doc),
            "amendment_index": amendment_index + 1,
            "working_doc_history": [
                *state.get("working_doc_history", []),
                {
                    "stage": "amendment",
                    "stage_index": amendment_index + 1,
                    "source_document_label": amendment_analysis.metadata.document_label,
                    "output_doc": str(output_doc),
                },
            ],
        },
        "apply_operations",
        "operations applied",
    )


def node_finalize_single_base_flow(state: PipelineState) -> PipelineState:
    if state.get("dry_run"):
        result = {
            "case_id": state["case_id"],
            "case_topology": state.get("topology"),
            "dry_run": True,
            "amendment_docs": state.get("amendment_docs", []),
            "base_docs": state.get("base_docs", []),
        }
        return _merge_state(
            state,
            {"result": result, "validation": {"is_valid": True, "dry_run": True}, "output_docs": []},
            "finalize_single_base_flow",
            "dry run finalized",
        )
    runtime = state["_runtime"]
    current_doc = Path(state["current_doc"])
    final_base_analysis = runtime["base_analyzer"].analyze(current_doc)
    final_specs = build_service_table_specs(final_base_analysis, state["amendment_analyses"])
    final_document = Document(current_doc)
    final_document.save(current_doc)
    runtime["checklist"].add(
        stage="04_edit",
        check_id=f"final_service_tables_{Path(state['current_base_doc']).stem}",
        kind="final_service_tables_inserted",
        ok=bool(final_specs),
        details={"service_table_specs": [item.to_dict() for item in final_specs], "output_doc": str(current_doc)},
    )
    checklist = runtime["checklist_builder"].build(
        base_analysis=final_base_analysis,
        amendment_analyses=state["amendment_analyses"],
        service_table_specs=final_specs,
        resolved_operations=state.get("all_operations", []),
        runtime_checks=runtime["checklist"].items(),
    )
    operation_summary = _operation_summary(state.get("all_operations", []))
    validation = runtime["validator"].validate(
        output_doc=current_doc,
        checklist=checklist,
        amendment_analyses=state["amendment_analyses"],
        base_analysis=final_base_analysis,
        operation_statuses=state.get("all_statuses", []),
        operation_summary=operation_summary,
    )
    result = {
        "case_id": state["case_id"],
        "case_topology": state["topology"],
        "workspace_root": state["workspace_root"],
        "base_doc": state["current_base_doc"],
        "base_analysis": final_base_analysis.to_dict(),
        "amendments": [item.to_dict() for item in state["amendment_analyses"]],
        "service_table_specs": [item.to_dict() for item in final_specs],
        "validation_checklist": checklist.to_dict(),
        "steps": state.get("steps", []),
        "stage_outputs": [
            {
                "stage_index": item["stage_index"],
                "output_doc": item["output_doc"],
                "source_document_label": item["source_document_label"],
            }
            for item in state.get("working_doc_history", [])
            if item.get("stage") == "amendment"
        ],
        "working_doc_history": state.get("working_doc_history", []),
        "output_doc": str(current_doc),
        "validation": validation.to_dict(),
    }
    return _merge_state(
        state,
        {
            "final_base_analysis": final_base_analysis,
            "validation_checklist": checklist,
            "result": result,
            "validation": validation.to_dict(),
            "output_docs": [str(current_doc)],
            "service_table_specs": final_specs,
            "service_table_spec_dicts": [item.to_dict() for item in final_specs],
        },
        "finalize_single_base_flow",
        "single-base flow finalized",
    )


def node_insert_final_service_tables(state: PipelineState) -> PipelineState:
    if state.get("dry_run"):
        return _merge_state(state, {"output_docs": []}, "insert_final_service_tables", "dry run service tables skipped")
    runtime = state["_runtime"]
    current_doc = Path(state["current_doc"])
    final_base_analysis = runtime["base_analyzer"].analyze(current_doc)
    final_specs = build_service_table_specs(final_base_analysis, state["amendment_analyses"])
    final_document = Document(current_doc)
    final_document.save(current_doc)
    runtime["checklist"].add(
        stage="04_edit",
        check_id=f"final_service_tables_{Path(state['current_base_doc']).stem}",
        kind="final_service_tables_inserted",
        ok=bool(final_specs),
        details={"service_table_specs": [item.to_dict() for item in final_specs], "output_doc": str(current_doc)},
    )
    return _merge_state(
        state,
        {
            "final_base_analysis": final_base_analysis,
            "service_table_specs": final_specs,
            "service_table_spec_dicts": [item.to_dict() for item in final_specs],
            "output_docs": [str(current_doc)],
        },
        "insert_final_service_tables",
        "final service tables inserted",
    )


def node_build_validation_checklist(state: PipelineState) -> PipelineState:
    if state.get("dry_run"):
        return _merge_state(state, {"validation_checklist": None}, "build_checklist", "dry run checklist skipped")
    runtime = state["_runtime"]
    checklist = runtime["checklist_builder"].build(
        base_analysis=state["final_base_analysis"],
        amendment_analyses=state["amendment_analyses"],
        service_table_specs=state["service_table_specs"],
        resolved_operations=state.get("all_operations", []),
        runtime_checks=runtime["checklist"].items(),
    )
    return _merge_state(
        state,
        {"validation_checklist": checklist},
        "build_checklist",
        "validation checklist built",
    )


def node_deterministic_validate(state: PipelineState) -> PipelineState:
    if state.get("dry_run"):
        return _merge_state(
            state,
            {
                "structural_results": [],
                "structural_ok": True,
                "deterministic_ok": True,
                "deterministic_failures": [],
                "hard_resolution_failure": False,
                "operation_summary": {"total": 0, "resolved": 0, "ambiguous": 0, "unsupported": 0},
            },
            "deterministic_validate",
            "dry run deterministic validation skipped",
        )
    validator = state["_runtime"]["validator"]
    output_doc = Path(state["current_doc"])
    structural_results = validator._validate_skeleton_tables(output_doc, state["validation_checklist"])
    structural_ok = all(item["ok"] for item in structural_results)
    paragraph_lines = read_non_empty_paragraphs(output_doc)
    output_lines = _read_lines_with_tables(output_doc)
    output_joined = "\n".join(normalize_for_match(line) for line in output_lines)
    deterministic_ok, deterministic_failures = validator._deterministic_intent_checks(
        state["amendment_analyses"],
        output_joined,
        paragraph_lines,
    )
    operation_summary = _operation_summary(state.get("all_operations", []))
    total_intents = sum(len(item.intents) for item in state["amendment_analyses"])
    total_ops = int(operation_summary.get("total", 0))
    resolved_ops = int(operation_summary.get("resolved", 0))
    ambiguous_ops = int(operation_summary.get("ambiguous", 0))
    unsupported_ops = int(operation_summary.get("unsupported", 0))
    all_ambiguous = total_ops > 0 and ambiguous_ops == total_ops
    hard_resolution_failure = bool(
        total_intents > 0
        and (
            total_ops == 0
            or total_ops != total_intents
            or resolved_ops != total_intents
            or ambiguous_ops > 0
            or unsupported_ops > 0
            or all_ambiguous
        )
    )
    judge_payload = {
        "base_analysis": state["final_base_analysis"].to_dict(),
        "amendments": [item.to_dict() for item in state["amendment_analyses"]],
        "checklist": state["validation_checklist"].to_dict(),
        "operation_statuses": state.get("all_statuses", []),
        "output_snapshot": output_lines[:250],
    }
    return _merge_state(
        state,
        {
            "structural_results": structural_results,
            "structural_ok": structural_ok,
            "deterministic_ok": deterministic_ok,
            "deterministic_failures": deterministic_failures,
            "hard_resolution_failure": hard_resolution_failure,
            "operation_summary": operation_summary,
            "judge_payload": judge_payload,
        },
        "deterministic_validate",
        "deterministic validation completed",
    )


def node_llm_judge_validate(state: PipelineState) -> PipelineState:
    if state.get("dry_run"):
        return _merge_state(
            state,
            {"judge_ok": True, "judge_summary": "dry run", "judge_failures": []},
            "llm_judge_validate",
            "dry run LLM judge skipped",
        )
    validator = state["_runtime"]["validator"]
    judge_ok = False
    judge_summary = ""
    judge_failures: list[str] = []
    try:
        data, _raw = validator.call_llm(
            validator._system_prompt,
            validator._user_template.format_map(
                {"judge_input_json": json.dumps(state["judge_payload"], ensure_ascii=False, indent=2)}
            ),
            max_tokens=1800,
        )
        judge_ok = bool(data.get("is_valid"))
        judge_summary = str(data.get("summary", ""))
        judge_failures = [str(item) for item in data.get("failures", [])]
    except Exception as exc:
        raw = f"[judge_error] {exc}"
        judge_summary = raw
        judge_failures = [raw]
    return _merge_state(
        state,
        {"judge_ok": judge_ok, "judge_summary": judge_summary, "judge_failures": judge_failures},
        "llm_judge_validate",
        "LLM judge validation completed",
    )


def node_compose_validation_report(state: PipelineState) -> PipelineState:
    if state.get("dry_run"):
        result = {
            "case_id": state["case_id"],
            "case_topology": state.get("topology"),
            "dry_run": True,
            "amendment_docs": state.get("amendment_docs", []),
            "base_docs": state.get("base_docs", []),
        }
        return _merge_state(
            state,
            {"result": result, "validation": {"is_valid": True, "dry_run": True}, "output_docs": []},
            "compose_validation_report",
            "dry run validation report composed",
        )
    judge_ok = bool(state.get("judge_ok"))
    judge_failures = list(state.get("judge_failures", []))
    if state.get("hard_resolution_failure"):
        summary = state.get("operation_summary", {})
        judge_ok = False
        judge_failures.append(
            "Hard fail: каждый intent должен иметь resolved operation "
            f"(intents={sum(len(item.intents) for item in state['amendment_analyses'])}, "
            f"total={int(summary.get('total', 0))}, resolved={int(summary.get('resolved', 0))}, "
            f"ambiguous={int(summary.get('ambiguous', 0))}, unsupported={int(summary.get('unsupported', 0))})."
        )
    if not judge_ok and state.get("deterministic_ok") and not state.get("hard_resolution_failure"):
        judge_ok = True
        judge_failures.append("LLM-judge fallback: deterministic intent checks passed.")
    elif state.get("deterministic_failures"):
        judge_failures.extend(state["deterministic_failures"][:10])
    is_valid = bool(state.get("structural_ok")) and judge_ok and not bool(state.get("hard_resolution_failure"))
    intent_results = [
        {
            "change_id": intent.change_id,
            "operation_kind": intent.operation_kind,
            "validated_by_judge": judge_ok,
        }
        for analysis in state["amendment_analyses"]
        for intent in analysis.intents
    ]
    validation = PipelineValidationReport(
        structural_ok=bool(state.get("structural_ok")),
        judge_ok=judge_ok,
        is_valid=is_valid,
        skeleton_results=state.get("structural_results", []),
        judge_summary=str(state.get("judge_summary", "")),
        judge_failures=judge_failures,
        intent_results=intent_results,
    )
    result = {
        "case_id": state["case_id"],
        "case_topology": state["topology"],
        "workspace_root": state["workspace_root"],
        "base_doc": state["current_base_doc"],
        "base_analysis": state["final_base_analysis"].to_dict(),
        "amendments": [item.to_dict() for item in state["amendment_analyses"]],
        "service_table_specs": [item.to_dict() for item in state["service_table_specs"]],
        "validation_checklist": state["validation_checklist"].to_dict(),
        "steps": state.get("steps", []),
        "stage_outputs": [
            {
                "stage_index": item["stage_index"],
                "output_doc": item["output_doc"],
                "source_document_label": item["source_document_label"],
            }
            for item in state.get("working_doc_history", [])
            if item.get("stage") == "amendment"
        ],
        "working_doc_history": state.get("working_doc_history", []),
        "output_doc": state["current_doc"],
        "validation": validation.to_dict(),
    }
    return _merge_state(
        state,
        {
            "validation_report": validation,
            "validation": validation.to_dict(),
            "result": result,
            "output_docs": [state["current_doc"]],
            "judge_ok": judge_ok,
            "judge_failures": judge_failures,
        },
        "compose_validation_report",
        "validation report composed",
    )


def node_export_artifacts(state: PipelineState) -> PipelineState:
    artifacts_root = Path(state.get("artifacts_root") or Path(state["workspace_root"]) / "artifacts" / state["case_id"])
    artifacts_root.mkdir(parents=True, exist_ok=True)
    event_log = artifacts_root / "graph_events.json"
    final_state = artifacts_root / "graph_final_state.json"
    event_log.write_text(json.dumps(state.get("events", []), ensure_ascii=False, indent=2), encoding="utf-8")
    final_state.write_text(json.dumps(_public_snapshot(state), ensure_ascii=False, indent=2), encoding="utf-8")
    return _merge_state(
        state,
        {"result": {**state.get("result", {}), "graph_events": str(event_log), "graph_final_state": str(final_state)}},
        "export_artifacts",
        "graph artifacts exported",
    )


def route_after_analysis_gate(state: PipelineState) -> str:
    if state.get("analysis_gate_ok"):
        return "route_by_topology"
    if int(state.get("analysis_repair_attempts", 0)) < 1:
        return "repair_analysis"
    return "route_by_topology"


def route_after_topology(state: PipelineState) -> str:
    if state.get("dry_run"):
        return "prepare_single_base_flow"
    if state.get("topology_route") == "multi_base":
        return "execute_multi_base"
    return "prepare_single_base_flow"


def route_after_resolution_gate(state: PipelineState) -> str:
    if state.get("resolution_gate_ok"):
        return "apply_operations"
    if int(state.get("resolution_repair_attempts", 0)) < 1:
        return "repair_resolution"
    return "resolution_failed"


def route_after_apply_operations(state: PipelineState) -> str:
    if state.get("dry_run"):
        return "insert_final_service_tables"
    if int(state.get("amendment_index", 0)) < len(state.get("amendment_analyses", [])):
        return "build_resolution_candidates"
    return "insert_final_service_tables"


class SequentialGraph:
    def invoke(self, state: PipelineState) -> PipelineState:
        current = node_load_case(state)
        current = node_initialize_runtime(current)
        current = node_analyze_documents(current)
        current = node_analysis_gate(current)
        if route_after_analysis_gate(current) == "repair_analysis":
            current = node_repair_analysis(current)
            current = node_analysis_gate(current)
        if not current.get("analysis_gate_ok") and not current.get("dry_run"):
            raise RuntimeError(f"analysis gate failed after repair: {current.get('analysis_gate_details')}")
        current = node_route_by_topology(current)
        if route_after_topology(current) == "execute_multi_base":
            current = node_execute_pipeline(current)
        else:
            current = node_prepare_single_base_flow(current)
            current = node_build_skeleton(current)
            while int(current.get("amendment_index", 0)) < len(current.get("amendment_analyses", [])):
                current = node_build_resolution_candidates(current)
                current = node_semantic_rank_candidates(current)
                current = node_select_resolution_candidates(current)
                current = node_merge_resolved_operations(current)
                current = node_resolution_gate(current)
                if route_after_resolution_gate(current) == "repair_resolution":
                    current = node_repair_resolution(current)
                    current = node_build_resolution_candidates(current)
                    current = node_semantic_rank_candidates(current)
                    current = node_select_resolution_candidates(current)
                    current = node_merge_resolved_operations(current)
                    current = node_resolution_gate(current)
                if not current.get("resolution_gate_ok") and not current.get("dry_run"):
                    current = node_resolution_failed(current)
                current = node_apply_operations(current)
            current = node_insert_final_service_tables(current)
            current = node_build_validation_checklist(current)
            current = node_deterministic_validate(current)
            current = node_llm_judge_validate(current)
            current = node_compose_validation_report(current)
        current = node_export_artifacts(current)
        return current


def build_graph() -> Any:
    try:
        from langgraph.graph import END, StateGraph
    except ImportError:
        return SequentialGraph()

    graph = StateGraph(PipelineState)
    graph.add_node("load_case", node_load_case)
    graph.add_node("initialize_runtime", node_initialize_runtime)
    graph.add_node("analyze_documents", node_analyze_documents)
    graph.add_node("analysis_gate", node_analysis_gate)
    graph.add_node("repair_analysis", node_repair_analysis)
    graph.add_node("route_by_topology", node_route_by_topology)
    graph.add_node("prepare_single_base_flow", node_prepare_single_base_flow)
    graph.add_node("build_skeleton", node_build_skeleton)
    graph.add_node("build_resolution_candidates", node_build_resolution_candidates)
    graph.add_node("semantic_rank_candidates", node_semantic_rank_candidates)
    graph.add_node("select_resolution_candidates", node_select_resolution_candidates)
    graph.add_node("merge_resolved_operations", node_merge_resolved_operations)
    graph.add_node("resolution_gate", node_resolution_gate)
    graph.add_node("repair_resolution", node_repair_resolution)
    graph.add_node("resolution_failed", node_resolution_failed)
    graph.add_node("apply_operations", node_apply_operations)
    graph.add_node("insert_final_service_tables", node_insert_final_service_tables)
    graph.add_node("build_checklist", node_build_validation_checklist)
    graph.add_node("deterministic_validate", node_deterministic_validate)
    graph.add_node("llm_judge_validate", node_llm_judge_validate)
    graph.add_node("compose_validation_report", node_compose_validation_report)
    graph.add_node("execute_pipeline", node_execute_pipeline)
    graph.add_node("export_artifacts", node_export_artifacts)
    graph.set_entry_point("load_case")
    graph.add_edge("load_case", "initialize_runtime")
    graph.add_edge("initialize_runtime", "analyze_documents")
    graph.add_edge("analyze_documents", "analysis_gate")
    graph.add_conditional_edges(
        "analysis_gate",
        route_after_analysis_gate,
        {
            "repair_analysis": "repair_analysis",
            "route_by_topology": "route_by_topology",
        },
    )
    graph.add_edge("repair_analysis", "analysis_gate")
    graph.add_conditional_edges(
        "route_by_topology",
        route_after_topology,
        {
            "execute_multi_base": "execute_pipeline",
            "prepare_single_base_flow": "prepare_single_base_flow",
        },
    )
    graph.add_edge("prepare_single_base_flow", "build_skeleton")
    graph.add_edge("build_skeleton", "build_resolution_candidates")
    graph.add_edge("build_resolution_candidates", "semantic_rank_candidates")
    graph.add_edge("semantic_rank_candidates", "select_resolution_candidates")
    graph.add_edge("select_resolution_candidates", "merge_resolved_operations")
    graph.add_edge("merge_resolved_operations", "resolution_gate")
    graph.add_conditional_edges(
        "resolution_gate",
        route_after_resolution_gate,
        {
            "apply_operations": "apply_operations",
            "repair_resolution": "repair_resolution",
            "resolution_failed": "resolution_failed",
        },
    )
    graph.add_edge("repair_resolution", "build_resolution_candidates")
    graph.add_conditional_edges(
        "apply_operations",
        route_after_apply_operations,
        {
            "build_resolution_candidates": "build_resolution_candidates",
            "insert_final_service_tables": "insert_final_service_tables",
        },
    )
    graph.add_edge("resolution_failed", END)
    graph.add_edge("insert_final_service_tables", "build_checklist")
    graph.add_edge("build_checklist", "deterministic_validate")
    graph.add_edge("deterministic_validate", "llm_judge_validate")
    graph.add_edge("llm_judge_validate", "compose_validation_report")
    graph.add_edge("compose_validation_report", "export_artifacts")
    graph.add_edge("execute_pipeline", "export_artifacts")
    graph.add_edge("export_artifacts", END)
    return graph.compile()


def run_graph(
    *,
    case_id: str,
    workspace_root: Path,
    models_config: Path | None = None,
    dry_run: bool = False,
) -> PipelineState:
    graph = build_graph()
    state = make_initial_state(
        case_id=case_id,
        workspace_root=workspace_root,
        models_config=models_config,
        dry_run=dry_run,
    )
    return graph.invoke(state)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run graph_pipeline through LangGraph orchestration")
    parser.add_argument("--case-id", required=True)
    parser.add_argument("--workspace-root", type=Path, default=Path(__file__).parent)
    parser.add_argument("--models-config", type=Path, default=None)
    parser.add_argument("--output-json", type=Path, default=None)
    parser.add_argument("--dry-run", action="store_true", help="Load case and graph state without LLM/DOCX execution")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    final_state = run_graph(
        case_id=args.case_id,
        workspace_root=args.workspace_root,
        models_config=args.models_config,
        dry_run=args.dry_run,
    )
    output = _public_snapshot(final_state)
    if args.output_json:
        args.output_json.parent.mkdir(parents=True, exist_ok=True)
        args.output_json.write_text(json.dumps(output, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(output.get("validation", {}), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
