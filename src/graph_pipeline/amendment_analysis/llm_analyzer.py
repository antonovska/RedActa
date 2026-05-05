from __future__ import annotations

import json
from pathlib import Path
import re
from typing import Any

from ..base_agent import BaseAgent, compact, read_non_empty_paragraphs
from ..config import runtime_kwargs
from ..schema import ChangeIntent
from ..utils import build_source_document_label, normalize_member_entry_from_inclusion


PROMPTS_DIR = Path(__file__).parent / "prompts"


def _load_prompt_text(name: str) -> str:
    return (PROMPTS_DIR / name).read_text(encoding="utf-8").strip()


def json_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, indent=2)


class AmendmentLLMAnalyzer(BaseAgent):
    def __init__(self, config: dict[str, Any]) -> None:
        super().__init__(name="Amendment LLM Analyzer", **runtime_kwargs(config, "analyst"))
        self._system_prompt = _load_prompt_text("system.txt")
        self._user_template = _load_prompt_text("user_template.txt")
        self._repair_user_template = _load_prompt_text("repair_user_template.txt")

    def analyze(self, amendment_doc: Path) -> dict[str, Any]:
        lines = read_non_empty_paragraphs(amendment_doc)
        amendment_text = "\n".join(lines[:200])
        try:
            data, raw = self.call_llm(
                self._system_prompt,
                self._user_template.format_map({"amendment_text": amendment_text}),
                max_tokens=2000,
            )
        except ValueError as exc:
            data = {"intents": []}
            raw = f"[amendment_llm_analyzer llm_parse_error] {exc}"
        intents_raw = data.get("intents")
        if not isinstance(intents_raw, list):
            intents_raw = []

        source_document_label = build_source_document_label(amendment_doc)
        intents: list[ChangeIntent] = []
        for index, item in enumerate(intents_raw, 1):
            if not isinstance(item, dict):
                continue
            intent = ChangeIntent(
                change_id=compact(str(item.get("change_id", f"c{index}"))) or f"c{index}",
                operation_kind=compact(str(item.get("operation_kind", "unknown"))),
                source_document_label=source_document_label,
                appendix_number=compact(str(item.get("appendix_number", ""))),
                anchor_text_hint=compact(str(item.get("anchor_text_hint", ""))),
                point_ref=compact(str(item.get("point_ref", ""))),
                point_number=self._coerce_int(item.get("point_number")),
                parent_point_ref=compact(str(item.get("parent_point_ref", ""))),
                parent_point_number=self._coerce_int(item.get("parent_point_number")),
                subpoint_ref=compact(str(item.get("subpoint_ref", ""))).strip('"«»').lower(),
                paragraph_ordinal=self._coerce_int(item.get("paragraph_ordinal")),
                person_name_hint=compact(str(item.get("person_name_hint", ""))),
                new_text=compact(str(item.get("new_text", ""))),
                old_text=compact(str(item.get("old_text", ""))),
                appended_words=compact(str(item.get("appended_words", ""))),
                new_item_text=compact(str(item.get("new_item_text", ""))),
                new_block_lines=[compact(str(line)) for line in item.get("new_block_lines", []) if compact(str(line))],
                section_hint=compact(str(item.get("section_hint", ""))),
                source_excerpt=compact(str(item.get("source_excerpt", ""))),
                confidence=float(item.get("confidence", 0.0) or 0.0),
            )
            intents.append(intent)

        fallback_intents = self._fallback_extract(lines, source_document_label)
        intents = self._merge_with_fallback(intents, fallback_intents)
        return {
            "intents": intents,
            "raw_model_output": raw,
            "source_document_label": source_document_label,
        }

    def repair_analyze(
        self,
        amendment_doc: Path,
        previous_intents: list[dict[str, Any]],
        directives: list[str],
    ) -> dict[str, Any]:
        lines = read_non_empty_paragraphs(amendment_doc)
        amendment_text = "\n".join(lines[:240])
        try:
            data, raw = self.call_llm(
                self._system_prompt,
                self._repair_user_template.format_map(
                    {
                        "amendment_text": amendment_text,
                        "directives_json": json_dumps(directives),
                        "previous_intents_json": json_dumps(previous_intents),
                    }
                ),
                max_tokens=3000,
            )
        except ValueError as exc:
            data = {"intents": []}
            raw = f"[amendment_llm_analyzer repair_parse_error] {exc}"
        intents_raw = data.get("intents")
        if not isinstance(intents_raw, list):
            intents_raw = []

        source_document_label = build_source_document_label(amendment_doc)
        intents: list[ChangeIntent] = []
        for index, item in enumerate(intents_raw, 1):
            if not isinstance(item, dict):
                continue
            intent = ChangeIntent(
                change_id=compact(str(item.get("change_id", f"c{index}"))) or f"c{index}",
                operation_kind=compact(str(item.get("operation_kind", "unknown"))),
                source_document_label=source_document_label,
                appendix_number=compact(str(item.get("appendix_number", ""))),
                anchor_text_hint=compact(str(item.get("anchor_text_hint", ""))),
                point_ref=compact(str(item.get("point_ref", ""))),
                point_number=self._coerce_int(item.get("point_number")),
                parent_point_ref=compact(str(item.get("parent_point_ref", ""))),
                parent_point_number=self._coerce_int(item.get("parent_point_number")),
                subpoint_ref=compact(str(item.get("subpoint_ref", ""))).strip('"«»').lower(),
                paragraph_ordinal=self._coerce_int(item.get("paragraph_ordinal")),
                person_name_hint=compact(str(item.get("person_name_hint", ""))),
                new_text=compact(str(item.get("new_text", ""))),
                old_text=compact(str(item.get("old_text", ""))),
                appended_words=compact(str(item.get("appended_words", ""))),
                new_item_text=compact(str(item.get("new_item_text", ""))),
                new_block_lines=[compact(str(line)) for line in item.get("new_block_lines", []) if compact(str(line))],
                section_hint=compact(str(item.get("section_hint", ""))),
                source_excerpt=compact(str(item.get("source_excerpt", ""))),
                confidence=float(item.get("confidence", 0.0) or 0.0),
            )
            intents.append(intent)

        fallback_intents = self._fallback_extract(lines, source_document_label)
        intents = self._merge_with_fallback(intents, fallback_intents)
        return {
            "intents": intents,
            "raw_model_output": raw,
            "source_document_label": source_document_label,
        }

    def _coerce_int(self, value: Any) -> int | None:
        if value in (None, ""):
            return None
        try:
            return int(str(value).strip())
        except (TypeError, ValueError):
            return None

    def _merge_with_fallback(
        self,
        intents: list[ChangeIntent],
        fallback_intents: list[ChangeIntent],
    ) -> list[ChangeIntent]:
        if not fallback_intents:
            return intents
        if not intents:
            return fallback_intents

        merged: list[ChangeIntent] = []
        for index, intent in enumerate(intents):
            if index >= len(fallback_intents):
                merged.append(intent)
                continue
            fallback = fallback_intents[index]
            if self._should_prefer_fallback(intent, fallback):
                merged.append(fallback)
                continue
            if intent.operation_kind == "unknown":
                merged.append(fallback)
                continue
            if intent.operation_kind == "replace_phrase_globally" and not intent.new_text and fallback.new_text:
                intent.new_text = fallback.new_text
            if not intent.old_text and fallback.old_text:
                intent.old_text = fallback.old_text
            if not intent.new_text and fallback.new_text:
                intent.new_text = fallback.new_text
            if not intent.new_item_text and fallback.new_item_text:
                intent.new_item_text = fallback.new_item_text
            if not intent.section_hint and fallback.section_hint:
                intent.section_hint = fallback.section_hint
            if not intent.source_excerpt and fallback.source_excerpt:
                intent.source_excerpt = fallback.source_excerpt
            if not intent.point_number and fallback.point_number:
                intent.point_number = fallback.point_number
            if not intent.point_ref and fallback.point_ref:
                intent.point_ref = fallback.point_ref
            if not intent.parent_point_ref and fallback.parent_point_ref:
                intent.parent_point_ref = fallback.parent_point_ref
            if not intent.parent_point_number and fallback.parent_point_number:
                intent.parent_point_number = fallback.parent_point_number
            if not intent.subpoint_ref and fallback.subpoint_ref:
                intent.subpoint_ref = fallback.subpoint_ref
            if not intent.paragraph_ordinal and fallback.paragraph_ordinal:
                intent.paragraph_ordinal = fallback.paragraph_ordinal
            if not intent.appended_words and fallback.appended_words:
                intent.appended_words = fallback.appended_words
            if not intent.appendix_number and fallback.appendix_number:
                intent.appendix_number = fallback.appendix_number
            if not intent.anchor_text_hint and fallback.anchor_text_hint:
                intent.anchor_text_hint = fallback.anchor_text_hint
            if not intent.person_name_hint and fallback.person_name_hint:
                intent.person_name_hint = fallback.person_name_hint
            if not intent.new_block_lines and fallback.new_block_lines:
                intent.new_block_lines = list(fallback.new_block_lines)
            merged.append(intent)
        if len(fallback_intents) > len(intents):
            merged.extend(fallback_intents[len(intents):])
        return merged

    def _should_prefer_fallback(self, intent: ChangeIntent, fallback: ChangeIntent) -> bool:
        if intent.operation_kind == "unknown":
            return True
        if fallback.operation_kind == "append_words_to_point":
            return intent.operation_kind in {"append_section_item", "replace_point"} and not intent.appended_words
        if fallback.operation_kind == "repeal_point":
            return intent.operation_kind != "repeal_point"
        if fallback.operation_kind == "replace_appendix_block":
            return intent.operation_kind != "replace_appendix_block" and bool(fallback.appendix_number)
        if fallback.operation_kind == "replace_phrase_globally":
            if (
                intent.operation_kind == "replace_phrase_globally"
                and bool(fallback.old_text)
                and bool(fallback.source_excerpt)
                and fallback.old_text in fallback.source_excerpt
                and bool(intent.old_text)
                and bool(intent.source_excerpt)
                and intent.old_text not in intent.source_excerpt
            ):
                return True
            return (
                intent.operation_kind != "replace_phrase_globally"
                and bool(fallback.old_text)
                and "исключ" in (fallback.source_excerpt or "").lower()
            )
        return False

    def _fallback_extract(self, lines: list[str], source_document_label: str) -> list[ChangeIntent]:
        text = "\n".join(lines)
        intents: list[ChangeIntent] = []
        change_counter = 1

        def add_intent(**kwargs: Any) -> None:
            nonlocal change_counter
            intents.append(
                ChangeIntent(
                    change_id=f"c{change_counter}",
                    source_document_label=source_document_label,
                    confidence=1.0,
                    **kwargs,
                )
            )
            change_counter += 1

        insert_point = re.search(
            r"дополн(?:ить|ив)[^\n]*?пунктом\s+(\d+)\s+следующего\s+содержания\s*:",
            text,
            flags=re.IGNORECASE | re.DOTALL,
        )
        if insert_point:
            quoted = self._extract_payload_from_lines(lines, r"дополн(?:ить|ив)[^\n]*?пунктом\s+\d+\s+следующего\s+содержания\s*:")
            add_intent(
                operation_kind="insert_point",
                point_number=int(insert_point.group(1)),
                new_text=compact(quoted),
                source_excerpt=compact(text[insert_point.start(): insert_point.end()] + " " + quoted),
            )

        replace_point = re.search(
            r"пункт\s+(\d+)\s+[^\n]*?изложить\s+в\s+следующей\s+редакции\s*:",
            text,
            flags=re.IGNORECASE | re.DOTALL,
        )
        if replace_point:
            quoted = self._extract_payload_from_lines(lines, r"пункт\s+\d+\s+[^\n]*?изложить\s+в\s+следующей\s+редакции\s*:")
            add_intent(
                operation_kind="replace_point",
                point_number=int(replace_point.group(1)),
                new_text=compact(quoted),
                source_excerpt=compact(text[replace_point.start(): replace_point.end()] + " " + quoted),
            )

        replace_words = list(
            re.finditer(
            r"слова\s+[\"«]([^\"»]+)[\"»]\s+заменить\s+словами\s+[\"«]([^\"»]+)[\"»]",
            text,
            flags=re.IGNORECASE | re.DOTALL,
        )
        )
        for match in replace_words:
            add_intent(
                operation_kind="replace_phrase_globally",
                old_text=compact(match.group(1)),
                new_text=compact(match.group(2)),
                source_excerpt=compact(match.group(0)),
            )

        delete_words = list(
            re.finditer(
                r"слова\s+((?:[\"«][^\"»]+[\"»]\s*,?\s*)+)\s+исключить",
                text,
                flags=re.IGNORECASE | re.DOTALL,
            )
        )
        for match in delete_words:
            for old_text in re.findall(r"[\"«]([^\"»]+)[\"»]", match.group(1)):
                add_intent(
                    operation_kind="replace_phrase_globally",
                    old_text=compact(old_text),
                    new_text="",
                    source_excerpt=compact(match.group(0)),
                )

        append_matches = list(
            re.finditer(
                r"пункт\s+(\d+(?:\.\d+)?)\s+[^\n]*?дополнить\s+словами\s+[\"«]([^\"»]+)[\"»]",
                text,
                flags=re.IGNORECASE | re.DOTALL,
            )
        )
        if append_matches:
            for match in append_matches:
                point_ref = compact(match.group(1))
                point_number = int(point_ref) if point_ref.isdigit() else None
                add_intent(
                    operation_kind="append_words_to_point",
                    point_ref=point_ref,
                    point_number=point_number,
                    appended_words=compact(match.group(2)),
                    source_excerpt=compact(match.group(0)),
                )

        repeal_matches = list(
            re.finditer(
                r"пункт\s+(\d+(?:\.\d+)?)\s+[^\n]*?признать\s+утратившим\s+силу",
                text,
                flags=re.IGNORECASE | re.DOTALL,
            )
        )
        repeal_matches.extend(
            re.finditer(
                r"признать\s+утратившим\s+силу\s+пункт\s+(\d+(?:\.\d+)?)\b",
                text,
                flags=re.IGNORECASE | re.DOTALL,
            )
        )
        if repeal_matches:
            seen_repeals: set[str] = set()
            for match in repeal_matches:
                point_ref = compact(match.group(1))
                if point_ref in seen_repeals:
                    continue
                seen_repeals.add(point_ref)
                point_number = int(point_ref) if point_ref.isdigit() else None
                add_intent(
                    operation_kind="repeal_point",
                    point_ref=point_ref,
                    point_number=point_number,
                    source_excerpt=compact(match.group(0)),
                )

        append_item = re.search(
            r"дополнить\s+должностью[^\"]*\"([^\"]+)\"\s+(.+?)\.",
            text,
            flags=re.IGNORECASE | re.DOTALL,
        )
        if append_item:
            add_intent(
                operation_kind="append_section_item",
                new_item_text=compact(append_item.group(1)) + ".",
                section_hint=compact(append_item.group(2)),
                source_excerpt=compact(append_item.group(0)),
            )

        replace_appendix = re.search(
            r"изложить\s+приложение\s+n?\s*(\d+).*?в\s+редакции\s+приложения\s+к\s+настоящему\s+приказу",
            text,
            flags=re.IGNORECASE | re.DOTALL,
        )
        if replace_appendix:
            appendix_number = compact(replace_appendix.group(1))
            block_lines = self._extract_appendix_replacement_lines(lines, appendix_number)
            if block_lines:
                add_intent(
                    operation_kind="replace_appendix_block",
                    appendix_number=appendix_number,
                    new_block_lines=block_lines,
                    source_excerpt=compact(replace_appendix.group(0)),
                )

        include_member = re.search(
            r"включить\s+в\s+состав[^\n]*?([А-ЯЁ][а-яё]+(?:\s+[А-ЯЁ][а-яё]+){2}\s*-\s*.+?);",
            text,
            flags=re.IGNORECASE | re.DOTALL,
        )
        replace_role = re.search(
            r"должность\s+([А-ЯЁ][а-яё]+(?:\s+[А-ЯЁ][а-яё]+){2})\s+изложить\s+в\s+следующей\s+редакции:\s*\"([^\"]+)\"",
            text,
            flags=re.IGNORECASE | re.DOTALL,
        )
        if include_member or replace_role:
            if include_member:
                add_intent(
                    operation_kind="insert_list_entry",
                    anchor_text_hint="секретарь Комиссии",
                    new_text=normalize_member_entry_from_inclusion(compact(include_member.group(1))),
                    source_excerpt=compact(include_member.group(0)),
                )
            if replace_role:
                add_intent(
                    operation_kind="replace_person_role",
                    person_name_hint=compact(replace_role.group(1)),
                    new_text=compact(replace_role.group(2)),
                    source_excerpt=compact(replace_role.group(0)),
                )

        return intents

    def _extract_payload_from_lines(self, lines: list[str], marker_pattern: str) -> str:
        marker = re.compile(marker_pattern, flags=re.IGNORECASE)
        payload_parts: list[str] = []
        found = False
        for raw_line in lines:
            line = compact(raw_line)
            if not line:
                continue
            if not found:
                match = marker.search(line)
                if not match:
                    continue
                found = True
                remainder = compact(line[match.end():])
                if remainder:
                    payload_parts.append(remainder)
                continue
            if line in {"Министр", "Руководитель", "И.о. руководителя", "Заместитель руководителя"}:
                break
            if re.fullmatch(r"[А-ЯA-ZЁ][А-ЯA-ZЁ.\- ]{3,}", line):
                break
            payload_parts.append(line)

        payload = compact(" ".join(payload_parts))
        if payload.startswith('"'):
            payload = payload[1:].lstrip()
        payload = re.sub(r'"\s*\.\s*$', ".", payload)
        payload = re.sub(r'"\s*$', "", payload)
        return payload.strip()

    def _extract_appendix_replacement_lines(self, lines: list[str], appendix_number: str) -> list[str]:
        start_idx = None
        marker = re.compile(rf'^"?приложение\s+n?\s*{re.escape(appendix_number)}\b', re.IGNORECASE)
        for idx, line in enumerate(lines):
            if marker.match(compact(line)):
                start_idx = idx
                break
        if start_idx is None:
            return []
        block = [compact(line).strip('"') for line in lines[start_idx:] if compact(line)]
        if block and block[-1].endswith('"..'):
            block[-1] = block[-1][:-2] + '.'
        if block and block[-1].endswith('".'):
            block[-1] = block[-1][:-2] + '.'
        return block
