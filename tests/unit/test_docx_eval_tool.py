from __future__ import annotations

import sys
from pathlib import Path

from docx import Document

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "tools"))

from eval_compare_docx import compare_docx_text


def _write_docx(path: Path, paragraphs: list[str]) -> None:
    document = Document()
    for paragraph in paragraphs:
        document.add_paragraph(paragraph)
    document.save(path)


def test_compare_docx_text_reports_counts_and_diff_blocks(tmp_path) -> None:
    result = tmp_path / "result.docx"
    reference = tmp_path / "reference.docx"
    _write_docx(result, ["one", "two changed", "three"])
    _write_docx(reference, ["one", "two", "three"])

    comparison = compare_docx_text(result, reference)

    assert comparison["result_paragraphs"] == 3
    assert comparison["reference_paragraphs"] == 3
    assert comparison["diff_blocks"] == 1
