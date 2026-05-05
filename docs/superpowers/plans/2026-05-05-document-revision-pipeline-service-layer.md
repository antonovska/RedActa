# Document Revision Pipeline Service Layer Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Separate structural document edits from Consultant-style service tables and revision markers, while keeping reference-document comparison as a dev-only evaluation tool.

**Architecture:** `EditorV2` applies only content/structure changes and returns a resolved operation log. A new service layer post-pass inserts revision markers after all structural edits have stabilized. Golden/reference comparisons live in dev tooling and are never imported by production `run-case`.

**Tech Stack:** Python 3.12, `python-docx`, existing `graph_pipeline` dataclasses, pytest.

---

### Task 1: Consultant Marker Formatter

**Files:**
- Create: `src/graph_pipeline/revision_markers.py`
- Test: `tests/unit/test_revision_markers.py`

- [ ] **Step 1: Write failing formatter tests**

```python
from graph_pipeline.revision_markers import ConsultantMarkerFormatter
from graph_pipeline.schema import ResolvedOperation


def _operation(kind: str, **kwargs) -> ResolvedOperation:
    return ResolvedOperation(
        operation_id="c1",
        operation_kind=kind,
        status="resolved",
        source_document_label="Приказ Минфина России от 03.10.2025 N 141н",
        **kwargs,
    )


def test_formats_subpoint_replacement_marker():
    marker = ConsultantMarkerFormatter().format_marker(
        _operation("replace_point", parent_point_ref="2", subpoint_ref="б")
    )
    assert marker == '(пп. "б" в ред. Приказа Минфина России от 03.10.2025 N 141н)'


def test_formats_inserted_subpoint_marker():
    marker = ConsultantMarkerFormatter().format_marker(
        _operation("append_section_item", parent_point_ref="2", subpoint_ref="е(1)")
    )
    assert marker == '(пп. "е(1)" введен Приказом Минфина России от 03.10.2025 N 141н)'


def test_formats_inserted_paragraph_marker():
    marker = ConsultantMarkerFormatter().format_marker(
        _operation("append_section_item", parent_point_ref="4", subpoint_ref="в", paragraph_ordinal=8)
    )
    assert marker == "(абзац введен Приказом Минфина России от 03.10.2025 N 141н)"
```

- [ ] **Step 2: Verify RED**

Run: `python -m pytest tests/unit/test_revision_markers.py -q`  
Expected: import error for `graph_pipeline.revision_markers`.

- [ ] **Step 3: Implement minimal formatter**

Create `ConsultantMarkerFormatter.format_marker(operation)`:
- return explicit `operation.note_text` if present;
- use `to_instrumental(operation.source_document_label)`;
- for `replace_point` with `subpoint_ref`: `(пп. "X" в ред. <label>)`;
- for `append_section_item` with `subpoint_ref` and no `paragraph_ordinal`: `(пп. "X" введен <label>)`;
- for `append_section_item` with `paragraph_ordinal`: `(абзац введен <label>)`;
- fallback: `(в ред. <label>)`.

- [ ] **Step 4: Verify GREEN**

Run: `python -m pytest tests/unit/test_revision_markers.py -q`  
Expected: all tests pass.

- [ ] **Step 5: Commit**

```powershell
git add src/graph_pipeline/revision_markers.py tests/unit/test_revision_markers.py
git commit -m "Add Consultant-style revision marker formatter"
```

### Task 2: Structural Editor Operation Log

**Files:**
- Modify: `src/graph_pipeline/editor_v2.py`
- Test: `tests/unit/test_revision_markers.py`

- [ ] **Step 1: Write failing editor test**

Create a small DOCX in a temp path with one paragraph `б) old;`. Apply a `replace_point` operation with `note_text`. Assert:
- edited document contains replaced paragraph text;
- edited document does not contain `note_text`;
- `edit_result["applied_operations"]` contains `operation_id` and final paragraph index.

- [ ] **Step 2: Verify RED**

Run: `python -m pytest tests/unit/test_revision_markers.py::test_structural_editor_does_not_insert_revision_markers -q`  
Expected: fails because editor currently inserts notes and does not return `applied_operations`.

- [ ] **Step 3: Implement minimal operation log**

In `EditorV2.edit`, accumulate:

```python
applied_operations.append({
    "operation_id": operation.operation_id,
    "operation_kind": operation.operation_kind,
    "paragraph_indices": list(operation.paragraph_indices),
    "insert_after_index": operation.insert_after_index,
    "source_document_label": operation.source_document_label,
    "note_text": operation.note_text,
})
```

Remove direct `note_text` insertion from:
- `_apply_insert_point`
- `_apply_replace_point`
- `_apply_append_section_item`
- `_apply_append_words_to_point`
- `_apply_append_section_item_table`
- `_apply_replace_phrase_globally` marker insert loop

- [ ] **Step 4: Verify GREEN**

Run: `python -m pytest tests/unit/test_revision_markers.py -q`  
Expected: tests pass.

- [ ] **Step 5: Commit**

```powershell
git add src/graph_pipeline/editor_v2.py tests/unit/test_revision_markers.py
git commit -m "Keep revision markers out of structural editor"
```

### Task 3: Revision Marker Post-Pass

**Files:**
- Modify: `src/graph_pipeline/revision_markers.py`
- Modify: `src/graph_pipeline/run_case.py`
- Test: `tests/unit/test_revision_markers.py`

- [ ] **Step 1: Write failing post-pass test**

Create a DOCX with paragraph `б) new;`. Call `RevisionMarkerInserter.insert_markers(docx, operations)` with a resolved `replace_point` operation targeting paragraph 0. Assert marker is inserted as the next paragraph and original content remains unchanged.

- [ ] **Step 2: Verify RED**

Run: `python -m pytest tests/unit/test_revision_markers.py::test_revision_marker_inserter_adds_marker_after_structural_edit -q`  
Expected: fails because `RevisionMarkerInserter` does not exist.

- [ ] **Step 3: Implement post-pass**

Add `RevisionMarkerInserter.insert_markers(document_path, operations)`:
- open `Document(document_path)`;
- for each resolved operation with marker text, insert marker after the first `paragraph_indices` item;
- save document;
- return list of inserted marker summaries.

Call it in `run_case._run_single_base_flow` after all structural edit steps and before final `BaseAnalyzer`.

- [ ] **Step 4: Verify GREEN**

Run: `python -m pytest tests/unit/test_revision_markers.py -q`  
Expected: tests pass.

- [ ] **Step 5: Commit**

```powershell
git add src/graph_pipeline/revision_markers.py src/graph_pipeline/run_case.py tests/unit/test_revision_markers.py
git commit -m "Insert revision markers in post-processing pass"
```

### Task 4: Dev-Only DOCX Evaluation Tool

**Files:**
- Create: `tools/eval_compare_docx.py`
- Test: `tests/unit/test_docx_eval_tool.py`

- [ ] **Step 1: Write failing eval test**

Test a pure function `compare_docx_text(result_path, reference_path)` using two generated DOCX files. Assert it returns paragraph counts and diff block count.

- [ ] **Step 2: Verify RED**

Run: `python -m pytest tests/unit/test_docx_eval_tool.py -q`  
Expected: import error.

- [ ] **Step 3: Implement dev-only tool**

Create `tools/eval_compare_docx.py` with:
- `extract_docx_text(path)`;
- `compare_docx_text(result_path, reference_path)`;
- CLI guarded by `if __name__ == "__main__"`.

Do not import it from `src/graph_pipeline`.

- [ ] **Step 4: Verify GREEN**

Run: `python -m pytest tests/unit/test_docx_eval_tool.py -q`  
Expected: tests pass.

- [ ] **Step 5: Commit**

```powershell
git add tools/eval_compare_docx.py tests/unit/test_docx_eval_tool.py
git commit -m "Add dev-only DOCX evaluation tool"
```

### Task 5: Full Verification

**Files:**
- No new files.

- [ ] **Step 1: Run unit suite**

Run: `python -m pytest tests`  
Expected: all tests pass.

- [ ] **Step 2: Run case 04 pipeline**

Run:

```powershell
python -m graph_pipeline.cli run-case --case-id 04 --workspace-root C:\Users\appan\Downloads\RedActa_1\RedActa-main --models-config config\models.example.json --output-json C:\Users\appan\Downloads\RedActa_1\RedActa-main\artifacts\04\service_layer_result.json
```

Expected: pipeline exits 0. Validation may expose remaining structural bugs; do not patch with case-specific logic.

- [ ] **Step 3: Run dev-only comparison**

Run:

```powershell
python tools\eval_compare_docx.py C:\Users\appan\Downloads\RedActa_1\RedActa-main\artifacts\04\working_step_1.docx C:\Users\appan\Desktop\agents_4_\redactions\04\ред_Приказ Минфина России от 11.09.2020 N 191н.docx
```

Expected: report diff blocks. Use findings only to identify general structural bugs.

- [ ] **Step 4: Final status**

Run: `git status --short`  
Expected: clean worktree after commits, except ignored runtime artifacts outside git.
