from __future__ import annotations

from pathlib import Path
from typing import Any

from docx import Document
from docx.oxml import OxmlElement
from docx.oxml.ns import qn
from docx.text.paragraph import Paragraph

from .schema import AmendmentAnalysis, BaseAnalysis, ServiceTableSpec
from .utils import format_revision_reference

_SERVICE_TABLE_COLS = [60, 113, 9921, 113]


def build_service_table_specs(base_analysis: BaseAnalysis, amendment_analyses: list[AmendmentAnalysis]) -> list[ServiceTableSpec]:
    document_labels = [formatted for item in amendment_analyses if (formatted := format_revision_reference(item.metadata.document_label))]
    appendix_labels: dict[str, list[str]] = {}
    for analysis in amendment_analyses:
        formatted = format_revision_reference(analysis.metadata.document_label)
        if not formatted:
            continue
        appendix_numbers = sorted({intent.appendix_number for intent in analysis.intents if intent.appendix_number})
        for appendix_number in appendix_numbers:
            appendix_labels.setdefault(appendix_number, [])
            if formatted not in appendix_labels[appendix_number]:
                appendix_labels[appendix_number].append(formatted)

    specs: list[ServiceTableSpec] = []
    for header in base_analysis.header_blocks:
        if header.scope == "document":
            labels = list(document_labels)
        else:
            labels = list(appendix_labels.get(header.appendix_number, []))
            if not labels and not appendix_labels:
                labels = list(document_labels)
        if not labels:
            continue
        specs.append(
            ServiceTableSpec(
                table_id=f"service_table_{header.header_id}",
                scope=header.scope,
                appendix_number=header.appendix_number,
                insert_after_paragraph_index=header.end_paragraph_index,
                document_labels=labels,
            )
        )
    return specs


def insert_service_tables(document: Document, specs: list[ServiceTableSpec]) -> None:
    seen_counts: dict[tuple[str, ...], int] = {}
    for spec in sorted(specs, key=lambda item: item.insert_after_paragraph_index, reverse=True):
        key = tuple(spec.document_labels)
        required_count = seen_counts.get(key, 0) + 1
        if _service_table_count(document, spec.document_labels) >= required_count:
            seen_counts[key] = required_count
            continue
        anchor = document.paragraphs[spec.insert_after_paragraph_index]
        _insert_service_table_ooxml(anchor, spec.document_labels, document)
        seen_counts[key] = required_count


def _build_hardcoded_tbl_pr() -> OxmlElement:
    tbl_pr = OxmlElement("w:tblPr")

    tbl_ind = OxmlElement("w:tblInd")
    tbl_ind.set(qn("w:w"), "0")
    tbl_ind.set(qn("w:type"), "dxa")
    tbl_pr.append(tbl_ind)

    tbl_w = OxmlElement("w:tblW")
    tbl_w.set(qn("w:w"), "5000")
    tbl_w.set(qn("w:type"), "pct")
    tbl_pr.append(tbl_w)

    tbl_borders = OxmlElement("w:tblBorders")
    for side in ("top", "left", "bottom", "right", "insideV", "insideH"):
        border = OxmlElement(f"w:{side}")
        border.set(qn("w:val"), "nil")
        border.set(qn("w:sz"), "0")
        border.set(qn("w:space"), "0")
        tbl_borders.append(border)
    tbl_pr.append(tbl_borders)

    return tbl_pr


def _insert_service_table_ooxml(anchor: Paragraph, document_labels: list[str], document: Document) -> None:
    col_1, col_2, col_3, col_4 = _SERVICE_TABLE_COLS

    tbl = OxmlElement("w:tbl")
    tbl.append(_build_hardcoded_tbl_pr())

    grid = OxmlElement("w:tblGrid")
    for width in _SERVICE_TABLE_COLS:
        grid_col = OxmlElement("w:gridCol")
        grid_col.set(qn("w:w"), str(width))
        grid.append(grid_col)
    tbl.append(grid)

    tr = OxmlElement("w:tr")
    _add_service_table_cell(tr, col_1, "CED3F1", [])
    _add_service_table_cell(tr, col_2, "F4F3F8", [])
    _add_service_table_cell(tr, col_3, "F4F3F8", ["Список изменяющих документов", *document_labels])
    _add_service_table_cell(tr, col_4, "F4F3F8", [])
    tbl.append(tr)

    spacer = _build_empty_table_spacer_paragraph()
    anchor._p.addnext(spacer)
    spacer.addnext(tbl)


def _service_table_count(document: Document, document_labels: list[str]) -> int:
    expected = [label.lower() for label in document_labels]
    matches = 0
    for table in document.tables:
        table_text = " ".join(cell.text for row in table.rows for cell in row.cells).lower()
        if all(label in table_text for label in expected):
            matches += 1
    return matches


def _add_service_table_cell(row: Any, width: int, fill: str, texts: list[str]) -> None:
    tc = OxmlElement("w:tc")
    tc_pr = OxmlElement("w:tcPr")
    tc_w = OxmlElement("w:tcW")
    tc_w.set(qn("w:type"), "dxa")
    tc_w.set(qn("w:w"), str(width))
    tc_pr.append(tc_w)

    tc_borders = OxmlElement("w:tcBorders")
    for side in ("top", "left", "bottom", "right"):
        border = OxmlElement(f"w:{side}")
        border.set(qn("w:val"), "nil")
        border.set(qn("w:sz"), "0")
        border.set(qn("w:space"), "0")
        tc_borders.append(border)
    tc_pr.append(tc_borders)

    shd = OxmlElement("w:shd")
    shd.set(qn("w:val"), "clear")
    shd.set(qn("w:color"), "auto")
    shd.set(qn("w:fill"), fill)
    tc_pr.append(shd)

    tc_mar = OxmlElement("w:tcMar")
    for side in ("top", "left", "bottom", "right"):
        mar = OxmlElement(f"w:{side}")
        mar.set(qn("w:w"), "113")
        mar.set(qn("w:type"), "dxa")
        tc_mar.append(mar)
    tc_pr.append(tc_mar)
    tc.append(tc_pr)

    for text in texts:
        p = OxmlElement("w:p")
        p_pr = OxmlElement("w:pPr")
        jc = OxmlElement("w:jc")
        jc.set(qn("w:val"), "center")
        p_pr.append(jc)
        p.append(p_pr)
        r = OxmlElement("w:r")
        r_pr = OxmlElement("w:rPr")
        color = OxmlElement("w:color")
        color.set(qn("w:val"), "392C69")
        r_pr.append(color)
        r.append(r_pr)
        t = OxmlElement("w:t")
        t.text = text
        r.append(t)
        p.append(r)
        tc.append(p)

    if not texts:
        tc.append(OxmlElement("w:p"))
    row.append(tc)


def _build_empty_table_spacer_paragraph(style_id: str = "0", sz: str = "24") -> OxmlElement:
    paragraph = OxmlElement("w:p")
    paragraph_pr = OxmlElement("w:pPr")
    style = OxmlElement("w:pStyle")
    style.set(qn("w:val"), style_id)
    paragraph_pr.append(style)
    run_pr = OxmlElement("w:rPr")
    sz_el = OxmlElement("w:sz")
    sz_el.set(qn("w:val"), sz)
    run_pr.append(sz_el)
    sz_cs = OxmlElement("w:szCs")
    sz_cs.set(qn("w:val"), sz)
    run_pr.append(sz_cs)
    paragraph_pr.append(run_pr)
    paragraph.append(paragraph_pr)
    return paragraph
