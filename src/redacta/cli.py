from __future__ import annotations

import argparse
import json
import time
from pathlib import Path
from typing import Any

from .case_loader import load_case
from .run_case import run_case


def _workspace_root(value: str | None) -> Path:
    if value:
        return Path(value).resolve()
    return Path.cwd().resolve()


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _run_case_command(args: argparse.Namespace) -> int:
    workspace_root = _workspace_root(args.workspace_root)
    models_config = Path(args.models_config).resolve() if args.models_config else None
    case = load_case(workspace_root, args.case_id)
    result = run_case(case, workspace_root, models_config)
    if args.output_json:
        _write_json(Path(args.output_json).resolve(), result)
    print(json.dumps(result["validation"], ensure_ascii=False, indent=2))
    return 0


def _run_batch_command(args: argparse.Namespace) -> int:
    workspace_root = _workspace_root(args.workspace_root)
    models_config = Path(args.models_config).resolve() if args.models_config else None
    results: list[dict[str, Any]] = []
    total = len(args.case_id)
    for index, case_id in enumerate(args.case_id, 1):
        print(f"[Batch] Running {index}/{total} case_id={case_id}", flush=True)
        started_at = time.perf_counter()
        try:
            case = load_case(workspace_root, case_id)
            results.append(run_case(case, workspace_root, models_config))
            elapsed = time.perf_counter() - started_at
            print(f"[Batch] Finished case_id={case_id} elapsed_seconds={elapsed:.2f}", flush=True)
        except Exception as exc:
            elapsed = time.perf_counter() - started_at
            print(f"[Batch] Error case_id={case_id} elapsed_seconds={elapsed:.2f} error={type(exc).__name__}", flush=True)
            results.append({"case_id": case_id, "error": f"{type(exc).__name__}: {exc}"})

    validations = [item["validation"] for item in results if "validation" in item]
    payload = {
        "summary": {
            "total_cases": len(results),
            "completed_cases": len(validations),
            "valid_cases": sum(1 for item in validations if item.get("is_valid")),
            "manual_review_required_cases": sum(
                1 for item in validations if item.get("manual_review_required")
            ),
            "failed_cases": sum(1 for item in results if "error" in item),
        },
        "results": results,
    }
    _write_json(Path(args.output_json).resolve(), payload)
    print(json.dumps(payload["summary"], ensure_ascii=False, indent=2))
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run redacta")
    subparsers = parser.add_subparsers(dest="command", required=True)

    run_case_parser = subparsers.add_parser("run-case", help="Run one local case")
    run_case_parser.add_argument("--case-id", required=True)
    run_case_parser.add_argument("--workspace-root", default=None)
    run_case_parser.add_argument("--models-config", default=None)
    run_case_parser.add_argument("--output-json", default=None)
    run_case_parser.set_defaults(func=_run_case_command)

    batch_parser = subparsers.add_parser("run-batch", help="Run multiple local cases")
    batch_parser.add_argument("--case-id", action="append", required=True)
    batch_parser.add_argument("--workspace-root", default=None)
    batch_parser.add_argument("--models-config", default=None)
    batch_parser.add_argument("--output-json", required=True)
    batch_parser.set_defaults(func=_run_batch_command)

    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    raise SystemExit(args.func(args))


if __name__ == "__main__":
    main()
