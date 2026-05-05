from __future__ import annotations

import re
import time
from dataclasses import replace
from datetime import datetime
from pathlib import Path
from typing import Any

from docx import Document

from .base_agent import compact, read_non_empty_paragraphs
from .amendment_analysis import AmendmentLLMAnalyzer
from .deterministic_intent_extractor import DeterministicIntentExtractor
from .document_classifier import classify_amendment_complexity
from .schema import ChangeIntent
from .schema import AmendmentAnalysis, AmendmentDocumentMeta
from .utils import build_source_document_label, extract_document_date, extract_document_number, sort_key_for_label


def normalize_structured_replacement_intent(intent: ChangeIntent) -> ChangeIntent:
    if intent.operation_kind != "replace_phrase_globally" or not intent.new_block_lines:
        return intent

    text = " ".join(
        compact(str(value))
        for value in (
            getattr(intent, "directive_text", ""),
            getattr(intent, "target_fragment", ""),
            intent.old_text,
            intent.source_excerpt,
        )
        if value
    )
    if not text:
        return intent

    row_match = re.search(r"\bстрок[ауи]\s+([0-9]+)\b.*?\bизложить\b", text, flags=re.IGNORECASE | re.DOTALL)
    if row_match:
        return replace(intent, operation_kind="replace_table_row", table_row_ref=compact(row_match.group(1)))

    entry_match = re.search(
        r"\bпозици[юия]\s+[\"«]([^\"»]+)[\"»].*?\bизложить\b",
        text,
        flags=re.IGNORECASE | re.DOTALL,
    )
    if entry_match:
        return replace(
            intent,
            operation_kind="replace_structured_entry",
            structured_entry_ref=compact(entry_match.group(1)),
        )

    return intent


class AmendmentAnalyzer:
    def __init__(self, config: dict[str, Any]) -> None:
        self._inner = AmendmentLLMAnalyzer(config)
        self._deterministic = DeterministicIntentExtractor()

    def analyze(self, amendment_doc: Path) -> AmendmentAnalysis:
        lines = read_non_empty_paragraphs(amendment_doc)
        deterministic_intents = self._deterministic.extract(lines, build_source_document_label(amendment_doc))
        result = self._inner.analyze(amendment_doc)
        result["intents"] = self._merge_deterministic_intents(deterministic_intents, result.get("intents", []))
        return self._build_analysis(amendment_doc, lines, result)

    def repair_analysis(
        self,
        amendment_doc: Path,
        previous_analysis: AmendmentAnalysis,
        directives: list[str],
    ) -> AmendmentAnalysis:
        lines = read_non_empty_paragraphs(amendment_doc)
        result = self._inner.repair_analyze(
            amendment_doc,
            [intent.to_dict() for intent in previous_analysis.intents],
            directives,
        )
        result["coverage_directives"] = list(directives)
        return self._build_analysis(amendment_doc, lines, result)

    def _merge_deterministic_intents(
        self,
        deterministic_intents: list[ChangeIntent],
        llm_intents: list[ChangeIntent],
    ) -> list[ChangeIntent]:
        if not deterministic_intents:
            return llm_intents
        merged = list(deterministic_intents)
        deterministic_excerpts = {compact(intent.source_excerpt).lower() for intent in deterministic_intents}
        for intent in llm_intents:
            excerpt = compact(intent.source_excerpt).lower()
            if excerpt and excerpt in deterministic_excerpts:
                continue
            merged.append(intent)
        return merged

    def _build_analysis(self, amendment_doc: Path, lines: list[str], result: dict[str, Any]) -> AmendmentAnalysis:
        label = build_source_document_label(amendment_doc)
        doc_date = extract_document_date(label)
        intents = self._normalize_intents(result["intents"], lines)
        directives = result.get("coverage_directives") or []
        if directives:
            intents = self._ensure_directive_coverage(intents, [str(item) for item in directives], label, lines)
        intents = self._rebuild_phrase_deletions(intents)
        intents = self._deduplicate_phrase_intents(intents)
        # Если документ содержит таблицу с данными — это table insert операция.
        # Принудительно перезаписываем operation_kind и new_block_lines для всех intents.
        table_payload = self._extract_table_payload(amendment_doc)
        if table_payload:
            for intent in intents:
                intent.operation_kind = "append_section_item"
                intent.new_block_lines = list(table_payload)
        meta = AmendmentDocumentMeta(
            source_path=str(amendment_doc),
            document_label=label,
            document_number=extract_document_number(label),
            document_date_iso=doc_date.isoformat() if doc_date else "",
            complexity=classify_amendment_complexity(amendment_doc),
        )
        return AmendmentAnalysis(
            metadata=meta,
            intents=intents,
            raw_model_output=result["raw_model_output"],
        )

    def _ensure_directive_coverage(
        self,
        intents: list[ChangeIntent],
        directives: list[str],
        source_document_label: str,
        lines: list[str],
    ) -> list[ChangeIntent]:
        covered_directives: set[int] = set()
        normalized_excerpts = [compact(intent.source_excerpt).lower() for intent in intents if intent.source_excerpt]
        used_excerpts: set[int] = set()
        for index, directive in enumerate(directives):
            directive_core = self._directive_core(directive).lower()
            for excerpt_index, excerpt in enumerate(normalized_excerpts):
                if excerpt_index in used_excerpts:
                    continue
                if directive_core and (directive_core in excerpt or excerpt in directive_core):
                    covered_directives.add(index)
                    used_excerpts.add(excerpt_index)
                    break
        if len(covered_directives) == len(directives):
            return intents

        next_number = len(intents) + 1
        completed = list(intents)
        full_text = "\n".join(lines)
        declared_appendix_numbers = self._extract_declared_appendix_numbers(lines)
        for index, directive in enumerate(directives):
            if index in covered_directives:
                continue
            intent = ChangeIntent(
                change_id=f"c{next_number}",
                operation_kind="unknown",
                source_document_label=source_document_label,
                source_excerpt=compact(directive),
                confidence=0.0,
            )
            self._normalize_coverage_fallback_intent(intent, directive, lines, full_text, declared_appendix_numbers)
            completed.append(intent)
            next_number += 1
        return completed

    def _normalize_coverage_fallback_intent(
        self,
        intent: ChangeIntent,
        directive: str,
        lines: list[str],
        full_text: str,
        declared_appendix_numbers: list[str],
    ) -> None:
        directive_core = self._directive_core(directive)
        intent.source_excerpt = directive_core
        self._apply_atomic_unit_hint(intent, directive_core, directive)
        self._fill_structural_hints(intent, lines, full_text)
        self._normalize_operation_aliases(intent)
        self._normalize_intent_by_source_patterns(intent, lines, full_text)
        self._normalize_appendix_rewrite_intent(intent, lines)
        self._normalize_point_level_append_intent(intent)
        if not intent.appendix_number:
            inferred_appendix = self._infer_appendix_number(intent, lines, declared_appendix_numbers)
            if inferred_appendix:
                intent.appendix_number = inferred_appendix

    def _directive_core(self, directive: str) -> str:
        return compact(str(directive).split("[atomic_unit=", 1)[0]).strip()

    def _apply_atomic_unit_hint(self, intent: ChangeIntent, directive_core: str, directive: str) -> None:
        match = re.search(r"\[atomic_unit=(\d+)/(\d+)\]", directive, flags=re.IGNORECASE)
        if not match:
            return
        ordinal = int(match.group(1))
        total = int(match.group(2))
        if ordinal < 1 or total < 1:
            return
        subpoints = [
            item.lower()
            for item in re.findall(r"подпункт(?:ы|а|е|ом)?\s+(.+?)(?:\s+изложить|\s+признать|\s+дополнить|$)", directive_core, flags=re.IGNORECASE)
        ]
        markers: list[str] = []
        for segment in subpoints:
            markers.extend(re.findall(r"[\"«]([а-яёa-z](?:\(\d+\))?)[\"»]", segment, flags=re.IGNORECASE))
        if len(markers) >= ordinal:
            intent.subpoint_ref = compact(markers[ordinal - 1]).lower()
            intent.point_ref = intent.subpoint_ref
            intent.point_number = None

    def analyze_many(self, amendment_docs: list[Path]) -> list[AmendmentAnalysis]:
        analyses: list[AmendmentAnalysis] = []
        total = len(amendment_docs)
        for index, path in enumerate(amendment_docs, 1):
            started = time.perf_counter()
            ts_start = datetime.now().strftime("%H:%M:%S")
            print(
                f"[{ts_start}][AmendmentAnalyzer] start {index}/{total}: {path.name}",
                flush=True,
            )
            analysis = self.analyze(path)
            elapsed = time.perf_counter() - started
            ts_end = datetime.now().strftime("%H:%M:%S")
            print(
                f"[{ts_end}][AmendmentAnalyzer] end   {index}/{total}: {path.name} "
                f"(intents={len(analysis.intents)}, elapsed={elapsed:.1f}с)",
                flush=True,
            )
            analyses.append(analysis)
        analyses.sort(key=lambda item: (sort_key_for_label(item.metadata.document_label), item.metadata.source_path))
        return analyses

    def _normalize_intents(self, intents: list[ChangeIntent], lines: list[str]) -> list[ChangeIntent]:
        declared_appendix_numbers = self._extract_declared_appendix_numbers(lines)
        full_text = "\n".join(lines)
        normalized: list[ChangeIntent] = []
        for index, intent in enumerate(intents):
            self._fill_structural_hints(intent, lines, full_text)
            self._normalize_operation_aliases(intent)
            self._normalize_intent_by_source_patterns(intent, lines, full_text)
            self._normalize_appendix_rewrite_intent(intent, lines)
            self._normalize_point_level_append_intent(intent)
            intent = normalize_structured_replacement_intent(intent)
            self._apply_first_intent_preamble_rule(intent, index, len(intents))
            if intent.appendix_number:
                normalized.append(intent)
                continue
            inferred_appendix = self._infer_appendix_number(intent, lines, declared_appendix_numbers)
            if inferred_appendix:
                intent.appendix_number = inferred_appendix
            normalized.append(intent)
        normalized = self._rebuild_phrase_deletions(normalized)
        normalized = self._inject_cascade_appendix_repeal_intents(normalized)
        return normalized

    def _inject_cascade_appendix_repeal_intents(self, intents: list[ChangeIntent]) -> list[ChangeIntent]:
        """For repeal_point intents whose source text explicitly mentions an appendix being
        repealed alongside the point, generate a companion repeal_appendix_block intent.

        This covers amendments that say e.g. «признать утратившими силу пункт 2 и
        прилагаемое к нему Положение». For the common case where the amendment only
        says «признать утратившим силу пункт N» (without mentioning the appendix), the
        cascade is detected later in the resolver against the base-document paragraph text.
        """
        existing_ids = {intent.change_id for intent in intents}
        cascade: list[ChangeIntent] = []
        for intent in intents:
            if intent.operation_kind != "repeal_point":
                continue
            source = (intent.source_excerpt or "").lower()
            if not ("утрат" in source and "сил" in source):
                continue
            if "прилагаем" not in source and "приложени" not in source:
                continue
            cascade_id = f"{intent.change_id}_cascade_appendix"
            if cascade_id in existing_ids:
                continue
            appendix_number = self._extract_appendix_number(intent.source_excerpt or "")
            cascade.append(
                ChangeIntent(
                    change_id=cascade_id,
                    operation_kind="repeal_appendix_block",
                    source_document_label=intent.source_document_label,
                    appendix_number=appendix_number,
                    source_excerpt=intent.source_excerpt,
                    section_hint=intent.section_hint,
                    confidence=intent.confidence,
                )
            )
        return intents + cascade

    def _deduplicate_phrase_intents(self, intents: list[ChangeIntent]) -> list[ChangeIntent]:
        grouped: dict[tuple[str, str, str], list[ChangeIntent]] = {}
        for intent in intents:
            if intent.operation_kind != "replace_phrase_globally" or not compact(intent.old_text):
                continue
            key = (
                intent.operation_kind,
                compact(intent.old_text).lower(),
                compact(intent.new_text).lower(),
            )
            grouped.setdefault(key, []).append(intent)

        keep_ids: set[int] = set()
        duplicate_ids: set[int] = set()
        for group in grouped.values():
            if len(group) == 1:
                keep_ids.add(id(group[0]))
                continue

            scoped = [intent for intent in group if self._phrase_target_scope_score(intent) > 0]
            candidates = scoped or group[:1]
            best_by_scope: dict[tuple[str, str, str, int | None, str, str], ChangeIntent] = {}
            for intent in candidates:
                scope_key = (
                    compact(intent.point_ref).lower(),
                    compact(intent.parent_point_ref).lower(),
                    compact(intent.subpoint_ref).lower(),
                    intent.paragraph_ordinal,
                    compact(intent.section_hint).lower(),
                    compact(intent.appendix_number).lower(),
                )
                current = best_by_scope.get(scope_key)
                if current is None or self._phrase_richness_score(intent) > self._phrase_richness_score(current):
                    best_by_scope[scope_key] = intent
            keep_ids.update(id(intent) for intent in best_by_scope.values())
            duplicate_ids.update(id(intent) for intent in group)

        deduped: list[ChangeIntent] = []
        emitted: set[int] = set()
        for intent in intents:
            intent_id = id(intent)
            if intent_id in duplicate_ids:
                if intent_id not in keep_ids or intent_id in emitted:
                    continue
                emitted.add(intent_id)
            deduped.append(intent)
        return deduped

    def _phrase_scope_score(self, intent: ChangeIntent) -> int:
        return self._phrase_target_scope_score(intent) + self._phrase_excerpt_score(intent)

    def _phrase_target_scope_score(self, intent: ChangeIntent) -> int:
        score = 0
        for value in (
            intent.point_ref,
            intent.parent_point_ref,
            intent.subpoint_ref,
            intent.section_hint,
            intent.appendix_number,
        ):
            if compact(value):
                score += 1
        if intent.paragraph_ordinal is not None:
            score += 1
        return score

    def _phrase_excerpt_score(self, intent: ChangeIntent) -> int:
        if compact(intent.source_excerpt):
            return min(3, len(compact(intent.source_excerpt)) // 80)
        return 0

    def _phrase_richness_score(self, intent: ChangeIntent) -> int:
        return self._phrase_target_scope_score(intent) * 10 + self._phrase_excerpt_score(intent)

    def _fill_structural_hints(self, intent: ChangeIntent, lines: list[str], full_text: str) -> None:
        source = intent.source_excerpt or self._find_best_source_line(intent, lines) or full_text
        self._fill_nested_target_hints(intent, source)
        self._fill_nested_context_from_nearby_lines(intent, source, lines)
        if intent.point_ref:
            intent.point_ref = self._normalize_ref(intent.point_ref)
        if intent.parent_point_ref:
            intent.parent_point_ref = self._normalize_ref(intent.parent_point_ref)
        if not intent.point_ref:
            point_ref = self._extract_point_ref(source)
            if point_ref:
                intent.point_ref = point_ref
        if not intent.point_number and intent.point_ref.isdigit():
            intent.point_number = int(intent.point_ref)
        if not intent.parent_point_number and intent.parent_point_ref.isdigit():
            intent.parent_point_number = int(intent.parent_point_ref)
        if not intent.appendix_number:
            appendix_number = self._extract_appendix_number(source)
            if appendix_number:
                intent.appendix_number = appendix_number

    def _fill_nested_target_hints(self, intent: ChangeIntent, source: str) -> None:
        source_subpoint = self._extract_subpoint_ref(source)
        if source_subpoint and not intent.subpoint_ref:
            intent.subpoint_ref = source_subpoint
            intent.point_ref = source_subpoint
            intent.point_number = None
        if intent.paragraph_ordinal is None:
            intent.paragraph_ordinal = self._extract_paragraph_ordinal(source)
        nested = self._extract_nested_target(source)
        if not nested:
            return
        parent_point_ref, subpoint_ref = nested
        if not intent.parent_point_ref:
            intent.parent_point_ref = parent_point_ref
        if not intent.parent_point_number and parent_point_ref.isdigit():
            intent.parent_point_number = int(parent_point_ref)
        if not intent.subpoint_ref:
            intent.subpoint_ref = subpoint_ref
        intent.point_ref = subpoint_ref
        intent.point_number = None

    def _fill_nested_context_from_nearby_lines(self, intent: ChangeIntent, source: str, lines: list[str]) -> None:
        if intent.parent_point_number or intent.parent_point_ref:
            return
        if not intent.subpoint_ref and "подпункт" not in source.lower():
            return
        source_norm = compact(source).lower()
        if not source_norm:
            return
        for index, line in enumerate(lines):
            if source_norm not in compact(line).lower():
                continue
            context = " ".join(lines[max(0, index - 8):index + 1])
            point_ref = self._extract_nearest_structural_parent_point(lines, index) or self._extract_last_point_ref(context)
            if point_ref:
                intent.parent_point_ref = point_ref
                if point_ref.isdigit():
                    intent.parent_point_number = int(point_ref)
            if not intent.subpoint_ref:
                nested = self._extract_nested_target(context)
                if nested:
                    _parent, subpoint_ref = nested
                    intent.subpoint_ref = subpoint_ref
                    intent.point_ref = subpoint_ref
                    intent.point_number = None
            return

    def _normalize_operation_aliases(self, intent: ChangeIntent) -> None:
        operation = compact(intent.operation_kind).lower()
        operation = operation.replace("-", "_").replace(" ", "_")
        alias_map = {
            "replace_nested_subpoint": "replace_point",
            "nested_subpoint_replacement": "replace_point",
            "repeal_nested_subpoint": "repeal_point",
            "nested_subpoint_repeal": "repeal_point",
            "append_nested_subpoint": "append_section_item",
            "replace_nested_paragraph": "replace_point",
            "replace_paragraph_in_subpoint": "replace_point",
            "append_paragraph_to_subpoint": "append_section_item",
            "delete_phrase_in_subpoint": "replace_phrase_globally",
            "delete_phrase_in_paragraph": "replace_phrase_globally",
            "preamble_phrase_deletion": "replace_phrase_globally",
            # Structured table operations — маппим на append_section_item,
            # editor умеет вставлять строки через _apply_append_section_item_table
            "replace_table_row": "append_section_item",
            "replace_structured_entry": "append_section_item",
            "insert_table_row": "append_section_item",
            "insert_appendix_item": "append_section_item",
            "append_appendix_item": "append_section_item",
            "insert_section_item": "append_section_item",
        }
        if operation in alias_map:
            intent.operation_kind = alias_map[operation]

    def _normalize_intent_by_source_patterns(self, intent: ChangeIntent, lines: list[str], full_text: str) -> None:
        source = intent.source_excerpt or ""
        if source and not any(compact(source) in compact(line) for line in lines) and compact(source) not in compact(full_text):
            better = self._find_best_source_line(intent, lines)
            if better:
                intent.source_excerpt = better
            source = better or ""
        if not source:
            source = self._find_best_source_line(intent, lines)
        if not source:
            source = full_text
        source_lower = source.lower()

        appended_words = ""
        if intent.operation_kind in {"append_section_item", "replace_point", "unknown"} and (
            intent.point_ref or intent.point_number or intent.subpoint_ref
        ):
            appended_words = self._extract_appended_words(source)
            if appended_words:
                intent.operation_kind = "append_words_to_point"
                intent.appended_words = appended_words
                return

        if "утрат" in source_lower and "сил" in source_lower and (
            intent.point_ref or intent.point_number or intent.subpoint_ref
        ):
            intent.operation_kind = "repeal_point"
            return

        if "исключ" in source_lower:
            excluded_text = self._extract_excluded_words(source) or self._extract_excluded_words(full_text)
            if excluded_text:
                intent.operation_kind = "replace_phrase_globally"
                intent.old_text = excluded_text
                intent.new_text = ""
                return

        if "замен" in source_lower:
            replacement = self._extract_replaced_words(source)
            if replacement:
                intent.operation_kind = "replace_phrase_globally"
                intent.old_text = replacement[0]
                intent.new_text = replacement[1]
                return

        if "дополнить" in source_lower and (
            "подпункт" in source_lower or "абзац" in source_lower
        ):
            extracted = (
                self._extract_replacement_text(source)
                or self._extract_text_after_directive_colon(source)
                or self._extract_following_block(source, lines, intent.subpoint_ref)
            )
            if extracted:
                intent.operation_kind = "append_section_item"
                intent.new_item_text = extracted
                return

        if (
            "изложить" in source_lower
            and "редакци" in source_lower
            and (intent.point_ref or intent.point_number or intent.subpoint_ref)
            and not intent.new_text
        ):
            extracted = (
                self._extract_replacement_text(source)
                or self._extract_text_after_directive_colon(source)
                or self._extract_following_block(source, lines, intent.subpoint_ref)
                or self._extract_replacement_text(full_text)
            )
            if extracted:
                intent.operation_kind = "replace_point"
                intent.new_text = extracted
                return

    def _find_best_source_line(self, intent: ChangeIntent, lines: list[str]) -> str:
        point_ref = compact(intent.point_ref)
        point_number = str(intent.point_number) if intent.point_number else ""
        for line in lines:
            lower = line.lower()
            if point_ref and (f"пункт {point_ref}" in lower or f"п. {point_ref}" in lower):
                return line
            if point_number and (f"пункт {point_number}" in lower or f"п. {point_number}" in lower):
                return line
        return ""

    def _extract_appended_words(self, text: str) -> str:
        match = re.search(
            r"дополнить\s+словами?\s+«([^»]+)»|дополнить\s+словами?\s+\"([^\"]+)\"",
            text,
            flags=re.IGNORECASE | re.DOTALL,
        )
        if not match:
            return ""
        for value in match.groups():
            if value:
                return compact(value)
        return ""

    def _extract_replacement_text(self, text: str) -> str:
        match = re.search(
            r"(?:в\s+следующей\s+редакции|в\s+редакции)\s*[:\-]?\s*[«\"]([^»\"]+)[»\"]",
            text,
            flags=re.IGNORECASE | re.DOTALL,
        )
        if not match:
            return ""
        return compact(match.group(1))

    def _extract_text_after_directive_colon(self, text: str) -> str:
        match = re.search(
            r"(?:в\s+следующей\s+редакции|в\s+редакции|следующего\s+содержания|следующими\s+абзацами)\s*[:\-]\s*(.+)$",
            text,
            flags=re.IGNORECASE | re.DOTALL,
        )
        if not match:
            return ""
        return self._strip_outer_quotes(match.group(1))

    def _extract_following_block(self, source: str, lines: list[str], marker: str = "") -> str:
        if not source:
            return ""
        normalized_source = compact(source).lower()
        for index, line in enumerate(lines):
            if normalized_source not in compact(line).lower():
                continue
            collected: list[str] = []
            for candidate in lines[index + 1:index + 5]:
                value = compact(candidate).strip()
                if not value:
                    continue
                lower = value.lower()
                if lower.startswith(("подпункт", "абзац", "в пункте", "в подпункте", "б) ", "в) ")):
                    break
                if re.match(r"^[\"«]?[а-яёa-z](?:\(\d+\))?\)\s+", value, flags=re.IGNORECASE):
                    collected.append(self._strip_outer_quotes(value))
                    continue
                if value.startswith(("\"", "«")):
                    collected.append(self._strip_outer_quotes(value))
                    continue
                if collected:
                    collected.append(self._strip_outer_quotes(value))
                    continue
                break
            if marker:
                marker_pattern = re.compile(rf"^[\"«]?{re.escape(marker)}\)\s+", flags=re.IGNORECASE)
                for item in collected:
                    if marker_pattern.match(item):
                        return compact(item)
            return compact(" ".join(collected))
        return ""

    def _strip_outer_quotes(self, text: str) -> str:
        value = compact(text).strip()
        while value and value[0] in "\"«":
            value = value[1:].strip()
        while value and value[-1] in "\"»":
            value = value[:-1].strip()
        if value.endswith('";') or value.endswith("»;"):
            value = value[:-2].strip() + ";"
        return value

    def _extract_excluded_words(self, text: str) -> str:
        match = re.search(
            r"слова?\s+[«\"]([^»\"]+)[»\"]\s+исключить",
            text,
            flags=re.IGNORECASE | re.DOTALL,
        )
        if not match:
            return ""
        return compact(match.group(1))

    def _extract_all_excluded_phrases(self, text: str) -> list[str]:
        """Извлекает все quoted-фразы из конструкций вида:
        слова "...", "..." исключить
        """
        if not text:
            return []
        region_match = re.search(
            r"слова?\b(.*?)\bисключить",
            text,
            flags=re.IGNORECASE | re.DOTALL,
        )
        if not region_match:
            return []
        region = region_match.group(1)
        phrases = re.findall(r'[«\"]([^»\"]+)[»\"]', region)
        return [compact(phrase) for phrase in phrases if compact(phrase)]

    def _rebuild_phrase_deletions(self, intents: list[ChangeIntent]) -> list[ChangeIntent]:
        """Если old_text replace_phrase_globally не содержится в source_excerpt,
        пересобираем old_text из quoted-фраз source_excerpt.
        """
        from collections import defaultdict

        groups: dict[str, list[ChangeIntent]] = defaultdict(list)
        for intent in intents:
            groups[intent.source_excerpt].append(intent)

        result: list[ChangeIntent] = []
        processed_excerpts: set[str] = set()

        for intent in intents:
            excerpt = intent.source_excerpt
            if excerpt in processed_excerpts:
                continue

            group = groups[excerpt]
            phrases = self._extract_all_excluded_phrases(excerpt)

            phrase_intents = [
                item for item in group
                if item.operation_kind == "replace_phrase_globally"
            ]
            if not phrases and phrase_intents:
                replaced = self._extract_replaced_words(excerpt)
                if replaced and any(item.old_text not in excerpt for item in phrase_intents):
                    for item in phrase_intents:
                        if item.old_text not in excerpt:
                            item.old_text = replaced[0]
                            item.new_text = replaced[1]
                    result.extend(group)
                    processed_excerpts.add(excerpt)
                    continue
            if not phrases or not phrase_intents:
                result.extend(group)
                processed_excerpts.add(excerpt)
                continue

            needs_rebuild = any(
                item.old_text not in excerpt
                for item in phrase_intents
            )
            if not needs_rebuild:
                result.extend(group)
                processed_excerpts.add(excerpt)
                continue

            phrase_idx = 0
            for item in group:
                if item.operation_kind != "replace_phrase_globally" or phrase_idx >= len(phrases):
                    result.append(item)
                    continue
                item.old_text = phrases[phrase_idx]
                result.append(item)
                phrase_idx += 1

            base = group[0]
            while phrase_idx < len(phrases):
                new_intent = ChangeIntent(
                    change_id=f"{base.change_id}_extra_{phrase_idx}",
                    operation_kind="replace_phrase_globally",
                    source_document_label=base.source_document_label,
                    appendix_number=base.appendix_number,
                    old_text=phrases[phrase_idx],
                    new_text="",
                    source_excerpt=base.source_excerpt,
                    confidence=base.confidence,
                )
                result.append(new_intent)
                phrase_idx += 1

            processed_excerpts.add(excerpt)

        return result

    def _apply_first_intent_preamble_rule(
        self,
        intent: ChangeIntent,
        intent_index: int,
        total_intents: int,
    ) -> None:
        """Первое изменение без явного scope hint — с высокой вероятностью преамбула."""
        if intent_index != 0:
            return
        if intent.section_hint:
            return
        if intent.point_ref or intent.point_number is not None or intent.subpoint_ref:
            return
        source_lower = (intent.source_excerpt or "").lower()
        structural_markers = [
            "пункт", "подпункт", "абзац", "приложение",
            "статья", "раздел", "часть", "преамбул",
        ]
        if any(marker in source_lower for marker in structural_markers):
            return
        intent.section_hint = "преамбула"

    def _extract_replaced_words(self, text: str) -> tuple[str, str] | None:
        match = re.search(
            r"слова?\s+[«\"]([^»\"]+)[»\"]\s+заменить\s+слов(?:ом|ами)?\s+[«\"]([^»\"]+)[»\"]",
            text,
            flags=re.IGNORECASE | re.DOTALL,
        )
        if not match:
            return None
        return compact(match.group(1)), compact(match.group(2))

    def _extract_point_ref(self, text: str) -> str:
        match = re.search(r"(пункт|пункта|пункте|пунктом|п\.?)\s+([0-9]+(\.[0-9]+)?)", text, flags=re.IGNORECASE)
        if not match:
            return ""
        return compact(match.group(2))

    def _extract_last_point_ref(self, text: str) -> str:
        matches = list(re.finditer(r"(пункт|пункта|пункте|пунктом|п\.?)\s+([0-9]+(\.[0-9]+)?)", text, flags=re.IGNORECASE))
        if not matches:
            return ""
        return compact(matches[-1].group(2))

    def _extract_nearest_structural_parent_point(self, lines: list[str], source_index: int) -> str:
        patterns = [
            re.compile(r"^[а-яёa-z]\)\s+в\s+пункте\s+([0-9]+(\.[0-9]+)?)\s*:?$", flags=re.IGNORECASE),
            re.compile(r"^в\s+пункте\s+([0-9]+(\.[0-9]+)?)\s*:?$", flags=re.IGNORECASE),
        ]
        for index in range(source_index, max(-1, source_index - 12), -1):
            value = compact(lines[index])
            for pattern in patterns:
                match = pattern.search(value)
                if match:
                    return compact(match.group(1))
        return ""

    def _extract_nested_target(self, text: str) -> tuple[str, str] | None:
        parent = self._extract_point_ref(text)
        subpoint = self._extract_subpoint_ref(text)
        if parent and subpoint:
            return compact(parent), subpoint

        reverse = re.search(
            r"подпункт\w*\s+[\"«]?([а-яёa-z](\([0-9]+\))?).*?пункта\s+([0-9]+(\.[0-9]+)?)",
            text,
            flags=re.IGNORECASE | re.DOTALL,
        )
        if reverse:
            return compact(reverse.group(3)), compact(reverse.group(1)).lower()
        return None

    def _extract_subpoint_ref(self, text: str) -> str:
        matches = list(
            re.finditer(
                r"подпункт\w*\s+[\"«]?([а-яёa-z](\([0-9]+\))?)",
                text,
                flags=re.IGNORECASE | re.DOTALL,
            )
        )
        if not matches:
            return ""
        return compact(matches[-1].group(1)).lower()

    def _extract_paragraph_ordinal(self, text: str) -> int | None:
        match = re.search(
            r"абзац[а-яё]*\s+(перв(ый|ом|ого)|втор(ой|ом|ого)|трет(ий|ьем|ьего)|четверт(ый|ом|ого)|пят(ый|ом|ого)|шест(ой|ом|ого)|седьм(ой|ом|ого)|восьм(ой|ом|ого)|девят(ый|ом|ого)|десят(ый|ом|ого))",
            text,
            flags=re.IGNORECASE,
        )
        if not match:
            return None
        stem = match.group(1).lower()
        ordinals = {
            "перв": 1,
            "втор": 2,
            "трет": 3,
            "четверт": 4,
            "пят": 5,
            "шест": 6,
            "седьм": 7,
            "восьм": 8,
            "девят": 9,
            "десят": 10,
        }
        for prefix, value in ordinals.items():
            if stem.startswith(prefix):
                return value
        return None

    def _normalize_ref(self, value: str) -> str:
        cleaned = compact(value).strip('"«»').lower()
        match = re.search(r"(\d+(?:\.\d+)?|[а-яёa-z](?:\(\d+\))?)", cleaned, flags=re.IGNORECASE)
        return match.group(1).lower() if match else cleaned

    def _normalize_appendix_rewrite_intent(self, intent: ChangeIntent, lines: list[str]) -> None:
        text_parts = [
            intent.source_excerpt or "",
            intent.section_hint or "",
            "\n".join(lines[:80]),
        ]
        text = "\n".join(text_parts).lower()
        is_appendix_rewrite = (
            "изложить приложение" in text
            and "в редакции приложения" in text
        )
        if not is_appendix_rewrite:
            return
        if intent.operation_kind == "replace_point" and (intent.point_number in (None, 0)) and not intent.new_text:
            intent.operation_kind = "replace_appendix_block"

    def _normalize_point_level_append_intent(self, intent: ChangeIntent) -> None:
        is_point_scoped = bool(intent.point_ref) or (intent.point_number is not None and intent.point_number > 0)
        if intent.operation_kind != "append_section_item" or not is_point_scoped:
            return
        if intent.appended_words:
            intent.operation_kind = "append_words_to_point"
            return
        if intent.new_text:
            intent.operation_kind = "replace_point"

    def _infer_appendix_number(
        self,
        intent: ChangeIntent,
        lines: list[str],
        declared_appendix_numbers: list[str],
    ) -> str:
        excerpt_appendix = self._extract_appendix_number(intent.source_excerpt)
        if excerpt_appendix:
            return excerpt_appendix

        if len(declared_appendix_numbers) == 1:
            return declared_appendix_numbers[0]

        text = "\n".join(lines)
        if intent.source_excerpt:
            window_appendix = self._extract_appendix_number_from_context(text, intent.source_excerpt)
            if window_appendix:
                return window_appendix
        return ""

    def _extract_declared_appendix_numbers(self, lines: list[str]) -> list[str]:
        heading_lines: list[str] = []
        for line in lines[:40]:
            value = compact(line)
            lower = value.lower()
            if re.match(r"^\d+\.", value) or lower.startswith(("в соответствии", "в целях", "на основании", "приказываю")):
                break
            heading_lines.append(value)
        heading_text = "\n".join(heading_lines)
        numbers: list[str] = []
        for match in re.finditer(r"приложени(?:е|я)\s+n?\s*(\d+)", heading_text, flags=re.IGNORECASE):
            appendix_number = compact(match.group(1))
            if appendix_number and appendix_number not in numbers:
                numbers.append(appendix_number)
        return numbers

    def _extract_appendix_number_from_context(self, full_text: str, source_excerpt: str) -> str:
        try:
            start_index = full_text.lower().find(source_excerpt.lower())
        except Exception:
            start_index = -1
        if start_index < 0:
            return ""
        window_start = max(0, start_index - 600)
        window_end = min(len(full_text), start_index + len(source_excerpt) + 200)
        return self._extract_appendix_number(full_text[window_start:window_end])

    def _extract_appendix_number(self, text: str) -> str:
        match = re.search(r"приложени(?:е|я)\s+n?\s*(\d+)", text, flags=re.IGNORECASE)
        return compact(match.group(1)) if match else ""

    def _extract_table_payload(self, amendment_doc: Path) -> list[str]:
        document = Document(amendment_doc)
        for table in document.tables:
            payload: list[str] = []
            for row in table.rows:
                cells = [compact(cell.text) for cell in row.cells]
                if not any(cells):
                    continue
                non_empty = [item for item in cells if item]
                unique = []
                for item in non_empty:
                    if item not in unique:
                        unique.append(item)
                if len(unique) == 1:
                    payload.append(f"section\t{unique[0]}")
                else:
                    payload.append("row\t" + "\t".join(cells))
            if payload:
                return payload
        return []
