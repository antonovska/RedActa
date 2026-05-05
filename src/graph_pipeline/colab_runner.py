from __future__ import annotations

import shutil
from pathlib import Path
from typing import Any

from .run_case import run_case


def run_uploaded_pair(
    *,
    base_docx: str | Path,
    amendment_docx: str | Path,
    workspace_root: str | Path,
    models_config: str | Path,
    case_id: str = "colab",
) -> dict[str, Any]:
    workspace = Path(workspace_root).resolve()
    case_dir = workspace / "redactions" / case_id
    case_dir.mkdir(parents=True, exist_ok=True)

    base_path = case_dir / f"{Path(base_docx).name}"
    amendment_path = case_dir / f"{Path(amendment_docx).name}"
    shutil.copy2(base_docx, base_path)
    shutil.copy2(amendment_docx, amendment_path)

    case = {
        "case_id": case_id,
        "case_topology": "standard_single",
        "amendment_docs": [amendment_path],
        "base_doc": base_path,
    }
    return run_case(case, workspace, Path(models_config).resolve())
