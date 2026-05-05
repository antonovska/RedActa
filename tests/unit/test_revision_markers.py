from __future__ import annotations

from graph_pipeline.revision_markers import ConsultantMarkerFormatter, RevisionMarkerInserter
from graph_pipeline.editor_v2 import EditorV2
from graph_pipeline.schema import ResolvedOperation
from docx import Document


def _operation(kind: str, **kwargs: object) -> ResolvedOperation:
    return ResolvedOperation(
        operation_id="c1",
        operation_kind=kind,
        status="resolved",
        source_document_label="Приказ Минфина России от 03.10.2025 N 141н",
        **kwargs,
    )


def test_formats_subpoint_replacement_marker() -> None:
    marker = ConsultantMarkerFormatter().format_marker(
        _operation("replace_point", parent_point_ref="2", subpoint_ref="б")
    )

    assert marker == '(пп. "б" в ред. Приказа Минфина России от 03.10.2025 N 141н)'


def test_formats_inserted_subpoint_marker() -> None:
    marker = ConsultantMarkerFormatter().format_marker(
        _operation("append_section_item", parent_point_ref="2", subpoint_ref="е(1)")
    )

    assert marker == '(пп. "е(1)" введен Приказом Минфина России от 03.10.2025 N 141н)'


def test_formats_inserted_paragraph_marker() -> None:
    marker = ConsultantMarkerFormatter().format_marker(
        _operation("append_section_item", parent_point_ref="4", subpoint_ref="в", paragraph_ordinal=8)
    )

    assert marker == "(абзац введен Приказом Минфина России от 03.10.2025 N 141н)"


def test_structural_editor_does_not_insert_revision_markers(tmp_path) -> None:
    input_doc = tmp_path / "input.docx"
    output_doc = tmp_path / "output.docx"
    document = Document()
    document.add_paragraph("б) old;")
    document.save(input_doc)
    operation = _operation(
        "replace_point",
        paragraph_indices=[0],
        subpoint_ref="б",
        new_text="б) new;",
        note_text='(пп. "б" в ред. Приказа Минфина России от 03.10.2025 N 141н)',
    )

    result = EditorV2().edit(input_doc, output_doc, [operation])
    paragraphs = [" ".join(paragraph.text.split()) for paragraph in Document(output_doc).paragraphs if paragraph.text.strip()]

    assert paragraphs == ["б) new;"]
    assert result["applied_operations"][0]["operation_id"] == "c1"
    assert result["applied_operations"][0]["paragraph_indices"] == [0]


def test_revision_marker_inserter_adds_marker_after_structural_edit(tmp_path) -> None:
    docx_path = tmp_path / "output.docx"
    document = Document()
    document.add_paragraph("б) new;")
    document.save(docx_path)
    operation = _operation(
        "replace_point",
        paragraph_indices=[0],
        subpoint_ref="б",
    )

    inserted = RevisionMarkerInserter().insert_markers(docx_path, [operation])
    paragraphs = [" ".join(paragraph.text.split()) for paragraph in Document(docx_path).paragraphs if paragraph.text.strip()]

    assert paragraphs == [
        "б) new;",
        '(пп. "б" в ред. Приказа Минфина России от 03.10.2025 N 141н)',
    ]
    assert inserted == [
        {
            "operation_id": "c1",
            "paragraph_index": 0,
            "marker": '(пп. "б" в ред. Приказа Минфина России от 03.10.2025 N 141н)',
        }
    ]
