from __future__ import annotations

import re
import time
from datetime import datetime
from pathlib import Path

from docx import Document
from docx.enum.text import WD_ALIGN_PARAGRAPH

from .schema import BaseAnalysis, HeaderBlock
from .document_classifier import classify_base_complexity


class BaseAnalyzer:
    def analyze(self, base_doc: Path) -> BaseAnalysis:
        started = time.perf_counter()
        ts_start = datetime.now().strftime("%H:%M:%S")
        print(f"[{ts_start}][BaseAnalyzer] start: {base_doc.name}", flush=True)
        document = Document(base_doc)
        paragraphs = document.paragraphs
        header_blocks: list[HeaderBlock] = []

        top_end = self._find_top_header_end(paragraphs)
        header_blocks.append(
            HeaderBlock(
                header_id="document_header",
                scope="document",
                start_paragraph_index=0,
                end_paragraph_index=top_end,
                title_lines=[paragraph.text.strip() for paragraph in paragraphs[: top_end + 1] if paragraph.text.strip()],
            )
        )

        _SUPPLEMENTARY_PREFIXES = ("приложение", "перечень", "положение")
        appendix_counter = 0
        for index, paragraph in enumerate(paragraphs):
            text = paragraph.text.strip()
            if not any(text.lower().startswith(prefix) for prefix in _SUPPLEMENTARY_PREFIXES):
                continue
            appendix_counter += 1
            appendix_number = self._extract_appendix_number(text) or str(appendix_counter)
            end_index = self._find_appendix_header_end(paragraphs, index)
            header_blocks.append(
                HeaderBlock(
                    header_id=f"appendix_{appendix_number}",
                    scope="appendix",
                    appendix_number=appendix_number,
                    start_paragraph_index=index,
                    end_paragraph_index=end_index,
                    title_lines=[paragraphs[idx].text.strip() for idx in range(index, end_index + 1) if paragraphs[idx].text.strip()],
                )
            )

        for start_index, end_index, title_lines in self._find_format_appendix_headers(paragraphs, top_end + 1):
            if self._intersects_existing_header(header_blocks, start_index, end_index):
                continue
            appendix_counter += 1
            appendix_number = str(appendix_counter)
            header_blocks.append(
                HeaderBlock(
                    header_id=f"appendix_{appendix_number}",
                    scope="appendix",
                    appendix_number=appendix_number,
                    start_paragraph_index=start_index,
                    end_paragraph_index=end_index,
                    title_lines=title_lines,
                )
            )

        analysis = BaseAnalysis(
            base_doc=str(base_doc),
            header_blocks=header_blocks,
            complexity=classify_base_complexity(base_doc),
        )
        elapsed = time.perf_counter() - started
        ts_end = datetime.now().strftime("%H:%M:%S")
        print(
            f"[{ts_end}][BaseAnalyzer] end: {base_doc.name} "
            f"(header_blocks={len(header_blocks)}, elapsed={elapsed:.1f}с)",
            flush=True,
        )
        return analysis

    def _find_top_header_end(self, paragraphs: list) -> int:
        boundary_equals = {
            "приказываю:",
            "постановляю:",
            "решил:",
        }
        boundary_starts = (
            "в соответствии",
            "руководствуясь",
            "в целях",
            "на основании",
            "в связи с",
            "приложение",
        )
        last_non_empty = 0
        for index, paragraph in enumerate(paragraphs[:60]):
            text = paragraph.text.strip()
            lower = text.lower()
            if not text:
                continue
            if lower in boundary_equals or any(lower.startswith(prefix) for prefix in boundary_starts) or re.match(r"^\d+\.\s", text):
                return max(0, last_non_empty)
            last_non_empty = index
        return last_non_empty

    def _find_appendix_header_end(self, paragraphs: list, start_idx: int) -> int:
        end_idx = start_idx
        for index in range(start_idx + 1, len(paragraphs)):
            text = paragraphs[index].text.strip()
            lower = text.lower()
            if not text:
                continue
            if re.match(r"^\d+\.\s", text):
                break
            if " - " in text:
                break
            if lower.startswith(("в соответствии", "руководствуясь", "на основании")):
                break
            if lower.startswith("приложение") and index > start_idx:
                break
            end_idx = index
        return end_idx

    def _find_format_appendix_headers(self, paragraphs: list, start_idx: int) -> list[tuple[int, int, list[str]]]:
        headers: list[tuple[int, int, list[str]]] = []
        index = max(0, start_idx)
        while index < len(paragraphs):
            if not self._paragraph_text(paragraphs[index]):
                index += 1
                continue
            if not self._is_right_aligned(paragraphs[index]):
                index += 1
                continue

            header = self._try_format_appendix_header(paragraphs, index)
            if header is None:
                index += 1
                continue
            headers.append(header)
            index = header[1] + 1
        return headers

    def _try_format_appendix_header(self, paragraphs: list, start_idx: int) -> tuple[int, int, list[str]] | None:
        right_indices: list[int] = []
        index = start_idx
        empty_gap = 0
        while index < len(paragraphs):
            text = self._paragraph_text(paragraphs[index])
            if not text:
                empty_gap += 1
                if empty_gap > 1:
                    break
                index += 1
                continue
            if not self._is_right_aligned(paragraphs[index]):
                break
            right_indices.append(index)
            empty_gap = 0
            index += 1
        if not right_indices:
            return None

        while index < len(paragraphs) and not self._paragraph_text(paragraphs[index]):
            index += 1
        if index >= len(paragraphs) or not self._is_center_aligned(paragraphs[index]):
            return None

        center_indices: list[int] = []
        while index < len(paragraphs):
            text = self._paragraph_text(paragraphs[index])
            if not text:
                index += 1
                continue
            if not self._is_center_aligned(paragraphs[index]):
                break
            center_indices.append(index)
            index += 1
        if not center_indices:
            return None

        title_indices = right_indices + center_indices
        title_lines = [self._paragraph_text(paragraphs[idx]) for idx in title_indices]
        return right_indices[0], center_indices[-1], title_lines

    def _paragraph_text(self, paragraph: object) -> str:
        return getattr(paragraph, "text", "").strip()

    def _is_right_aligned(self, paragraph: object) -> bool:
        return getattr(paragraph, "alignment", None) == WD_ALIGN_PARAGRAPH.RIGHT

    def _is_center_aligned(self, paragraph: object) -> bool:
        return getattr(paragraph, "alignment", None) == WD_ALIGN_PARAGRAPH.CENTER

    def _intersects_existing_header(self, header_blocks: list[HeaderBlock], start_idx: int, end_idx: int) -> bool:
        for header in header_blocks:
            if header.scope != "appendix":
                continue
            if start_idx <= header.end_paragraph_index and end_idx >= header.start_paragraph_index:
                return True
        return False

    def _extract_appendix_number(self, text: str) -> str:
        match = re.search(r"\b(\d+)\b", text)
        return match.group(1) if match else ""
