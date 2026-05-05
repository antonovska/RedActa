from __future__ import annotations

from pathlib import Path
from typing import Any


def _real_docx_files(case_dir: Path) -> list[Path]:
    return sorted(
        path for path in case_dir.glob("*.docx")
        if not path.name.startswith("~$") and not path.name.startswith("updated")
    )


def _is_amendment_file(path: Path) -> bool:
    stem = path.stem.lower()
    return stem.startswith("изм") or stem.startswith("изменение")


def discover_cases(workspace_root: Path) -> dict[str, dict[str, Any]]:
    redactions_root = workspace_root / "redactions"
    cases: dict[str, dict[str, Any]] = {}
    for case_dir in sorted(path for path in redactions_root.iterdir() if path.is_dir()):
        case_id = case_dir.name
        files = _real_docx_files(case_dir)
        amendments = [path for path in files if _is_amendment_file(path)]
        bases = [path for path in files if path not in amendments]
        if not amendments or not bases:
            continue
        if len(amendments) == 1 and len(bases) == 1:
            cases[case_id] = {
                "case_id": case_id,
                "case_topology": "standard_single",
                "amendment_docs": [amendments[0]],
                "base_doc": bases[0],
            }
            continue
        if len(amendments) > 1 and len(bases) == 1:
            cases[case_id] = {
                "case_id": case_id,
                "case_topology": "special_multi_amendment_single_base",
                "amendment_docs": amendments,
                "base_doc": bases[0],
            }
            continue
        if len(amendments) == 1 and len(bases) > 1:
            cases[case_id] = {
                "case_id": case_id,
                "case_topology": "special_single_amendment_multi_base",
                "amendment_docs": [amendments[0]],
                "base_docs": bases,
            }
    return cases


def load_case(workspace_root: Path, case_id: str) -> dict[str, Any]:
    cases = discover_cases(workspace_root)
    if case_id not in cases:
        raise ValueError(f"Кейс не найден в локальной копии redactions: {case_id}")
    return cases[case_id]
