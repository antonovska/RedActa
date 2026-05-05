from __future__ import annotations

import re

from .base_agent import compact
from .schema import ChangeIntent


class DeterministicIntentExtractor:
    def extract(self, lines: list[str], source_document_label: str) -> list[ChangeIntent]:
        intents: list[ChangeIntent] = []
        current_point_ref = ""
        current_scope = ""

        for line in lines:
            text = compact(line)
            if not text:
                continue

            lower = text.lower()
            if re.match(r"^\d+\.\s+", text) and "в преамбуле" not in lower:
                current_scope = ""
            if "в преамбуле" in lower:
                current_scope = "preamble"

            point_scope = re.search(r"\bв\s+пункте\s+(\d+(?:\.\d+)?)\s*:?\s*$", lower)
            if point_scope:
                current_point_ref = point_scope.group(1)
                current_scope = ""
                continue

            repeal = self._extract_repeal_point(text, source_document_label, len(intents) + 1, current_scope)
            if repeal is not None:
                intents.append(repeal)
                continue

            phrase_deletions = self._extract_phrase_deletions(
                text,
                source_document_label,
                len(intents) + 1,
                current_point_ref,
                current_scope,
            )
            if phrase_deletions:
                intents.extend(phrase_deletions)
                continue

            replacement = self._extract_phrase_replacement(
                text,
                source_document_label,
                len(intents) + 1,
                current_point_ref,
                current_scope,
            )
            if replacement is not None:
                intents.append(replacement)

        return intents

    def _extract_repeal_point(
        self,
        text: str,
        source_document_label: str,
        next_id: int,
        current_scope: str,
    ) -> ChangeIntent | None:
        match = re.search(
            r"\bпункт\s+(\d+(?:\.\d+)?)\s+признать\s+утратившим\s+силу\b",
            text,
            flags=re.IGNORECASE,
        )
        if not match:
            return None
        point_ref = compact(match.group(1))
        return ChangeIntent(
            change_id=f"d{next_id}",
            operation_kind="repeal_point",
            source_document_label=source_document_label,
            point_ref=point_ref,
            point_number=int(point_ref) if point_ref.isdigit() else None,
            section_hint=current_scope,
            source_excerpt=text,
            confidence=1.0,
        )

    def _extract_phrase_deletions(
        self,
        text: str,
        source_document_label: str,
        next_id: int,
        current_point_ref: str,
        current_scope: str,
    ) -> list[ChangeIntent]:
        lower = text.lower()
        if "слова" not in lower or "исключить" not in lower:
            return []

        region_match = re.search(
            r"\bслов[ао]\b(.*?)\bисключить\b",
            text,
            flags=re.IGNORECASE | re.DOTALL,
        )
        if not region_match:
            return []

        phrases = [compact(item) for item in re.findall(r'[«"]([^»"]+)[»"]', region_match.group(1)) if compact(item)]
        if not phrases:
            return []

        point_ref = self._line_point_ref(text) or current_point_ref
        subpoint_ref = self._line_subpoint_ref(text)
        paragraph_ordinal = self._line_paragraph_ordinal(text)
        section_hint = "preamble" if "в преамбуле" in lower or current_scope == "preamble" else ""

        intents: list[ChangeIntent] = []
        for offset, phrase in enumerate(phrases):
            intents.append(
                ChangeIntent(
                    change_id=f"d{next_id + offset}",
                    operation_kind="replace_phrase_globally",
                    source_document_label=source_document_label,
                    point_ref=point_ref,
                    point_number=int(point_ref) if point_ref.isdigit() else None,
                    subpoint_ref=subpoint_ref,
                    paragraph_ordinal=paragraph_ordinal,
                    old_text=phrase,
                    new_text="",
                    section_hint=section_hint,
                    source_excerpt=text,
                    confidence=1.0,
                )
            )
        return intents

    def _extract_phrase_replacement(
        self,
        text: str,
        source_document_label: str,
        next_id: int,
        current_point_ref: str,
        current_scope: str,
    ) -> ChangeIntent | None:
        lower = text.lower()
        if "слова" not in lower or "заменить" not in lower:
            return None

        match = re.search(
            r'слова?\s+[«"]([^»"]+)[»"]\s+заменить\s+слов(?:ом|ами)\s+[«"]([^»"]+)[»"]',
            text,
            flags=re.IGNORECASE,
        )
        if not match:
            return None

        point_ref = self._line_point_ref(text) or current_point_ref
        subpoint_ref = self._line_subpoint_ref(text)
        paragraph_ordinal = self._line_paragraph_ordinal(text)
        return ChangeIntent(
            change_id=f"d{next_id}",
            operation_kind="replace_phrase_globally",
            source_document_label=source_document_label,
            point_ref=point_ref,
            point_number=int(point_ref) if point_ref.isdigit() else None,
            subpoint_ref=subpoint_ref,
            paragraph_ordinal=paragraph_ordinal,
            old_text=compact(match.group(1)),
            new_text=compact(match.group(2)),
            section_hint="preamble" if current_scope == "preamble" else "",
            source_excerpt=text,
            confidence=1.0,
        )

    def _line_point_ref(self, text: str) -> str:
        match = re.search(r"\bпункт\w*\s+(\d+(?:\.\d+)?)\b", text, flags=re.IGNORECASE)
        return compact(match.group(1)) if match else ""

    def _line_subpoint_ref(self, text: str) -> str:
        match = re.search(r'\bподпункт\w*\s+[«"]([^»"]+)[»"]', text, flags=re.IGNORECASE)
        return compact(match.group(1)).lower() if match else ""

    def _line_paragraph_ordinal(self, text: str) -> int | None:
        match = re.search(r"\bабзац\w*\s+([а-яё]+)\b", text, flags=re.IGNORECASE)
        if not match:
            return None
        ordinals = {
            "первом": 1,
            "первый": 1,
            "втором": 2,
            "второй": 2,
            "третьем": 3,
            "третий": 3,
            "четвертом": 4,
            "четвертый": 4,
            "пятом": 5,
            "пятый": 5,
            "шестом": 6,
            "шестой": 6,
            "седьмом": 7,
            "седьмой": 7,
            "восьмом": 8,
            "восьмой": 8,
            "девятом": 9,
            "девятый": 9,
            "десятом": 10,
            "десятый": 10,
            "одиннадцатом": 11,
            "одиннадцатый": 11,
        }
        return ordinals.get(match.group(1).lower())
