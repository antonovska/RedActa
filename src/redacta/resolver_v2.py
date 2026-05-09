from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

from .base_agent import BaseAgent, EmbeddingClient
from .config import embedding_runtime_config, runtime_kwargs
from .ooxml_reader import (
    find_all_point_blocks,
    find_all_point_paragraphs,
    find_appendix_start,
    find_last_subpoint_in_point,
    find_point_paragraph,
    find_point_ref_paragraph,
    find_section_candidates,
    find_subpoint_in_point,
    find_table_section_candidates,
    read_paragraph_records,
)
from .prompt_loader import load_prompt_text
from .schema import ChangeIntent, ResolutionCandidate, ResolvedOperation
from .semantic_embeddings import HuggingFaceEmbeddingClient, LocalEmbeddingHttpClient
from .utils import looks_short_list_item, surname_stem, to_genitive, to_instrumental

_RUSSIAN_SUBPOINT_LETTERS = "абвгдежзиклмнопрстуфхцчшщэюя"


def _previous_subpoint_ref(ref: str) -> str | None:
    """Вычисляет предыдущий подпункт по алфавиту.

    Примеры:
        "б"    → "а"
        "ж"    → "е"
        "е(1)" → "е"   (составная ссылка — якорь это базовая буква)
        "а"    → None   (первый подпункт, предыдущего нет)
    """
    compound = re.match(r"^([а-яёa-z])\(\d+\)$", ref, re.IGNORECASE)
    if compound:
        return compound.group(1).lower()
    letter = ref.strip().lower()
    if len(letter) == 1 and letter in _RUSSIAN_SUBPOINT_LETTERS:
        idx = _RUSSIAN_SUBPOINT_LETTERS.index(letter)
        return None if idx == 0 else _RUSSIAN_SUBPOINT_LETTERS[idx - 1]
    return None



# NOTE: Only masculine forms are included because "абзац" is masculine in Russian.
# If future use requires feminine ("часть седьмая") or neuter forms, extend accordingly.
_ORDINAL_MAP = {
    "первый": 1, "первого": 1, "первому": 1, "первым": 1, "первом": 1,
    "второй": 2, "второго": 2, "второму": 2, "вторым": 2, "втором": 2,
    "третий": 3, "третьего": 3, "третьему": 3, "третьим": 3, "третьем": 3,
    "четвертый": 4, "четвертого": 4, "четвёртый": 4, "четвёртого": 4,
    "пятый": 5, "пятого": 5, "пятому": 5, "пятым": 5, "пятом": 5,
    "шестой": 6, "шестого": 6, "шестому": 6, "шестым": 6, "шестом": 6,
    "седьмой": 7, "седьмого": 7, "седьмому": 7, "седьмым": 7, "седьмом": 7,
    "восьмой": 8, "восьмого": 8, "восьмому": 8, "восьмым": 8, "восьмом": 8,
    "девятый": 9, "девятого": 9, "девятому": 9, "девятым": 9, "девятом": 9,
    "десятый": 10, "десятого": 10, "десятому": 10, "десятым": 10, "десятом": 10,
    "одиннадцатый": 11, "одиннадцатого": 11, "одиннадцатому": 11, "одиннадцатым": 11, "одиннадцатом": 11,
    "двенадцатый": 12, "двенадцатого": 12, "двенадцатому": 12, "двенадцатым": 12, "двенадцатом": 12,
    "тринадцатый": 13, "тринадцатого": 13, "тринадцатому": 13, "тринадцатым": 13, "тринадцатом": 13,
    "четырнадцатый": 14, "четырнадцатого": 14, "четырнадцатому": 14, "четырнадцатым": 14, "четырнадцатом": 14,
    "пятнадцатый": 15, "пятнадцатого": 15, "пятнадцатому": 15, "пятнадцатым": 15, "пятнадцатом": 15,
    "шестнадцатый": 16, "шестнадцатого": 16, "шестнадцатому": 16, "шестнадцатым": 16, "шестнадцатом": 16,
    "семнадцатый": 17, "семнадцатого": 17, "семнадцатому": 17, "семнадцатым": 17, "семнадцатом": 17,
    "восемнадцатый": 18, "восемнадцатого": 18, "восемнадцатому": 18, "восемнадцатым": 18, "восемнадцатом": 18,
    "девятнадцатый": 19, "девятнадцатого": 19, "девятнадцатому": 19, "девятнадцатым": 19, "девятнадцатом": 19,
    "двадцатый": 20, "двадцатого": 20, "двадцатому": 20, "двадцатым": 20, "двадцатом": 20,
}


def _parse_paragraph_ordinal(source_excerpt: str) -> int | None:
    """Parse 'абзац N-й' from source_excerpt, return 1-based ordinal or None.

    Supports both word-based ordinals ("абзац седьмой") and numeric ("абзац 7").
    """
    if not source_excerpt:
        return None
    excerpt_lower = source_excerpt.lower()
    m = re.search(r"абзац(?:ем|а|у|е)?\s+(\S+)", excerpt_lower)
    if not m:
        return None
    word = m.group(1)
    if word.isdigit():
        return int(word)
    return _ORDINAL_MAP.get(word)



class ResolverV2(BaseAgent):
    def __init__(self, config: dict[str, Any]) -> None:
        super().__init__(name="Resolver v2", **runtime_kwargs(config, "resolver_disambiguation"))
        self._system_prompt = load_prompt_text("resolver_disambiguation_system.txt")
        self._user_template = load_prompt_text("resolver_disambiguation_user_template.txt")
        embedding_cfg = embedding_runtime_config(config)
        self._semantic_ranking_enabled = bool(embedding_cfg["enabled"])
        self._semantic_top_k = max(1, int(embedding_cfg["top_k"]))
        self._semantic_auto_threshold = float(embedding_cfg["auto_threshold"])
        self._semantic_auto_margin = float(embedding_cfg["auto_margin"])
        self._embedding_client: Any | None = None
        self._semantic_ranking_available = False
        if self._semantic_ranking_enabled:
            if embedding_cfg["provider"] == "local_http":
                self._embedding_client = LocalEmbeddingHttpClient(
                    service_url=str(embedding_cfg["service_url"]),
                    timeout=min(float(embedding_cfg["timeout"]), 60.0),
                )
            elif embedding_cfg["provider"] == "huggingface":
                self._embedding_client = HuggingFaceEmbeddingClient(
                    model_name=str(embedding_cfg["model"]),
                    query_prompt=str(embedding_cfg["query_prompt"]),
                    document_prompt=str(embedding_cfg["document_prompt"]),
                    device=embedding_cfg["device"] or None,
                )
            else:
                self._embedding_client = EmbeddingClient(
                    base_url=str(embedding_cfg["base_url"]),
                    model=str(embedding_cfg["model"]),
                    api_key=str(embedding_cfg["api_key"]),
                    timeout=float(embedding_cfg["timeout"]),
                )
            self._semantic_ranking_available = True

    def resolve(
        self,
        base_doc: Path,
        intents: list[ChangeIntent],
        mode: str = "anchor_id",
        skip_relevance_filter: bool = False,
    ) -> dict[str, Any]:
        records = read_paragraph_records(base_doc)
        resolved: list[ResolvedOperation] = []
        debug_candidates: dict[str, list[dict[str, Any]]] = {}

        for intent in intents:
            if not skip_relevance_filter and not self._intent_relevant_to_base(intent, base_doc, records):
                resolved.append(
                    ResolvedOperation(
                        operation_id=intent.change_id,
                        operation_kind=intent.operation_kind,
                        status="ambiguous",
                        source_document_label=intent.source_document_label,
                        appendix_number=intent.appendix_number,
                        anchor_text_hint=intent.anchor_text_hint,
                        point_ref=intent.point_ref,
                        point_number=intent.point_number,
                        parent_point_ref=intent.parent_point_ref,
                        parent_point_number=intent.parent_point_number,
                        subpoint_ref=intent.subpoint_ref,
                        paragraph_ordinal=intent.paragraph_ordinal,
                        person_name_hint=intent.person_name_hint,
                        section_hint=intent.section_hint,
                        old_text=intent.old_text,
                        new_text=intent.new_text,
                        appended_words=intent.appended_words,
                        new_item_text=intent.new_item_text,
                        new_block_lines=list(intent.new_block_lines),
                        source_excerpt=intent.source_excerpt,
                        ambiguity_reason="intent relevance filter rejected this intent for the current base",
                    )
                )
                continue
            if intent.operation_kind == "insert_point":
                resolved.append(self._resolve_insert_point(records, intent))
                continue
            if intent.operation_kind == "replace_point":
                resolved.append(self._resolve_replace_point(records, intent))
                continue
            if intent.operation_kind == "replace_phrase_globally":
                resolved.append(self._resolve_replace_phrase_globally(records, intent))
                continue
            if intent.operation_kind == "append_words_to_point":
                resolved.append(self._resolve_append_words_to_point(records, intent))
                continue
            if intent.operation_kind == "repeal_point":
                resolved.extend(self._resolve_repeal_point(records, intent))
                continue
            if intent.operation_kind == "repeal_appendix_block":
                resolved.append(self._resolve_repeal_appendix_block(records, intent))
                continue
            if intent.operation_kind == "append_section_item":
                operation, candidates = self._resolve_append_section_item(base_doc, records, intent)
                resolved.append(operation)
                debug_candidates[intent.change_id] = [candidate.to_dict() for candidate in candidates]
                continue
            if intent.operation_kind == "replace_appendix_block":
                resolved.append(self._resolve_replace_appendix_block(records, intent))
                continue
            if intent.operation_kind == "insert_list_entry":
                if mode == "heuristic":
                    resolved.append(self._resolve_insert_list_entry(records, intent))
                else:
                    operation, candidates = self._resolve_insert_list_entry_by_candidates(records, intent)
                    resolved.append(operation)
                    debug_candidates[intent.change_id] = [candidate.to_dict() for candidate in candidates]
                continue
            if intent.operation_kind == "replace_person_role":
                if mode == "heuristic":
                    resolved.append(self._resolve_replace_person_role(records, intent))
                else:
                    operation, candidates = self._resolve_replace_person_role_by_candidates(records, intent)
                    resolved.append(operation)
                    debug_candidates[intent.change_id] = [candidate.to_dict() for candidate in candidates]
                continue
            if intent.operation_kind == "out_of_scope":
                resolved.append(
                    ResolvedOperation(
                        operation_id=intent.change_id,
                        operation_kind="out_of_scope",
                        status="resolved",
                        source_document_label=intent.source_document_label,
                        source_excerpt=intent.source_excerpt,
                    )
                )
                continue
            resolved.append(
                ResolvedOperation(
                    operation_id=intent.change_id,
                    operation_kind=intent.operation_kind,
                    status="unsupported",
                    source_document_label=intent.source_document_label,
                    source_excerpt=intent.source_excerpt,
                    ambiguity_reason="unsupported operation_kind",
                )
            )
        return {
            "resolved_operations": resolved,
            "debug_candidates": debug_candidates,
        }

    def _intent_relevant_to_base(self, intent: ChangeIntent, base_doc: Path, records: list[Any]) -> bool:
        excerpt = intent.source_excerpt.lower()
        if not excerpt:
            return True
        base_name = base_doc.name.lower().replace("_", " ")
        header_text = " ".join(record.text for record in records[:8]).lower()
        combined = base_name + " " + header_text

        # Ищем ВСЕ N-номера в excerpt: первый может быть номером самой поправки,
        # второй — номером базового документа. Достаточно, чтобы хотя бы один совпал.
        number_matches = re.findall(r"\bn\s*([0-9a-zа-я-]+)\b", excerpt, flags=re.IGNORECASE)
        if number_matches:
            if not any(num.lower() in combined for num in number_matches):
                return False

        year_matches = re.findall(r"\b(?:19|20)\d{2}\b", excerpt)
        if year_matches and not any(year in combined for year in year_matches):
            return False

        return True

    def _resolve_insert_point(self, records: list[Any], intent: ChangeIntent) -> ResolvedOperation:
        """Resolve an insert_point operation.

        Приоритеты навигации:
        1. parent_point_number + point_ref (буква) → вставка подпункта внутри пункта
        2. point_number (число) → вставка верхнеуровневого пункта
        """
        insert_text = intent.new_text or intent.new_item_text
        if not insert_text and not intent.new_block_lines:
            return ResolvedOperation(
                operation_id=intent.change_id,
                operation_kind=intent.operation_kind,
                status="ambiguous",
                source_document_label=intent.source_document_label,
                source_excerpt=intent.source_excerpt,
                ambiguity_reason="missing new_text, new_item_text and new_block_lines",
            )

        scoped_records = self._appendix_scope_records(records, intent)
        point_ref = intent.point_ref or ""
        parent_pn = intent.parent_point_number
        is_letter_ref = bool(re.match(r"^[а-яёa-z](?:\(\d+\))?$", point_ref, re.IGNORECASE))

        # Path 1: вставка подпункта внутри пункта
        if parent_pn is not None and is_letter_ref:
            anchor_record = self._find_insert_anchor_in_point(scoped_records, intent, parent_pn, point_ref)
            if anchor_record is None:
                # Fallback: вставить после последнего подпункта пункта
                for block in find_all_point_blocks(scoped_records, parent_pn):
                    anchor_record = find_last_subpoint_in_point(block)
                    if anchor_record is not None:
                        break
            if anchor_record is not None:
                note = (
                    f"(подп. {point_ref} п. {parent_pn} введен "
                    f"{to_instrumental(intent.source_document_label)})"
                )
                return ResolvedOperation(
                    operation_id=intent.change_id,
                    operation_kind=intent.operation_kind,
                    status="resolved",
                    source_document_label=intent.source_document_label,
                    insert_after_index=anchor_record.absolute_index,
                    point_ref=point_ref,
                    point_number=parent_pn,
                    parent_point_number=parent_pn,
                    subpoint_ref=point_ref,
                    new_text=intent.new_text,
                    new_item_text=intent.new_item_text,
                    new_block_lines=list(intent.new_block_lines),
                    note_text=note,
                    source_excerpt=intent.source_excerpt,
                )
            return ResolvedOperation(
                operation_id=intent.change_id,
                operation_kind=intent.operation_kind,
                status="ambiguous",
                source_document_label=intent.source_document_label,
                point_ref=point_ref,
                parent_point_number=parent_pn,
                source_excerpt=intent.source_excerpt,
                ambiguity_reason=f"parent point {parent_pn} not found or has no subpoints",
            )

        # Path 2: вставка верхнеуровневого пункта
        if intent.point_number is None:
            return ResolvedOperation(
                operation_id=intent.change_id,
                operation_kind=intent.operation_kind,
                status="ambiguous",
                source_document_label=intent.source_document_label,
                source_excerpt=intent.source_excerpt,
                ambiguity_reason="missing point_number or new_text",
            )
        anchor = find_point_paragraph(scoped_records, intent.point_number - 1)
        if anchor is None:
            return ResolvedOperation(
                operation_id=intent.change_id,
                operation_kind=intent.operation_kind,
                status="ambiguous",
                source_document_label=intent.source_document_label,
                point_number=intent.point_number,
                new_text=intent.new_text,
                source_excerpt=intent.source_excerpt,
                ambiguity_reason="previous point not found",
            )
        note = f"(п. {intent.point_number} введен {to_instrumental(intent.source_document_label)})"
        return ResolvedOperation(
            operation_id=intent.change_id,
            operation_kind=intent.operation_kind,
            status="resolved",
            source_document_label=intent.source_document_label,
            insert_after_index=anchor.absolute_index,
            point_number=intent.point_number,
            new_text=intent.new_text,
            note_text=note,
            source_excerpt=intent.source_excerpt,
        )

    def _find_insert_anchor_in_point(
        self,
        scoped_records: list[Any],
        intent: ChangeIntent,
        parent_pn: int,
        point_ref: str,
    ) -> Any | None:
        """Находит абзац, ПОСЛЕ которого нужно вставить новый подпункт point_ref.

        Стратегии (по приоритету):
        1. anchor_text_hint — если LLM указал конкретный текст якоря
        2. Предыдущий подпункт по алфавиту: для «е(1)» → «е», для «ж» → «е»
        """
        # Стратегия 1: anchor_text_hint
        if getattr(intent, "anchor_text_hint", None):
            hint = intent.anchor_text_hint.strip().lower()
            for block in find_all_point_blocks(scoped_records, parent_pn):
                for rec in reversed(block):
                    if hint in rec.text.lower():
                        return rec

        # Стратегия 2: предыдущий подпункт по алфавиту
        prev_ref = _previous_subpoint_ref(point_ref)
        if prev_ref is not None:
            for block in find_all_point_blocks(scoped_records, parent_pn):
                prev_subpoint = find_subpoint_in_point(block, prev_ref)
                if prev_subpoint:
                    return prev_subpoint[-1]

        return None

    def _resolve_replace_point(self, records: list[Any], intent: ChangeIntent) -> ResolvedOperation:
        point_number = intent.point_number if intent.point_number and intent.point_number > 0 else None
        scoped_records = self._appendix_scope_records(records, intent)

        # Подпункт: point_number не задан, но задан parent_point_number + point_ref (буква подпункта)
        if point_number is None and intent.parent_point_number and intent.point_ref and intent.new_text:
            subpoint_records = self._find_subpoint_records(scoped_records, intent.parent_point_number, intent.point_ref)
            if subpoint_records:
                # Проверяем, указан ли конкретный абзац ("абзац седьмой подпункта в")
                ordinal = _parse_paragraph_ordinal(intent.source_excerpt)
                if ordinal is not None and 1 <= ordinal <= len(subpoint_records):
                    target_record = subpoint_records[ordinal - 1]
                    note = f"(подп. {intent.point_ref} п. {intent.parent_point_number} в ред. {to_genitive(intent.source_document_label)})"
                    return ResolvedOperation(
                        operation_id=intent.change_id,
                        operation_kind=intent.operation_kind,
                        status="resolved",
                        source_document_label=intent.source_document_label,
                        paragraph_indices=[target_record.absolute_index],
                        point_ref=intent.point_ref,
                        subpoint_ref=intent.point_ref,
                        parent_point_ref=intent.parent_point_ref,
                        parent_point_number=intent.parent_point_number,
                        paragraph_ordinal=ordinal,
                        new_text=intent.new_text,
                        note_text=note,
                        source_excerpt=intent.source_excerpt,
                    )
                note = f"(подп. {intent.point_ref} п. {intent.parent_point_number} в ред. {to_genitive(intent.source_document_label)})"
                return ResolvedOperation(
                    operation_id=intent.change_id,
                    operation_kind=intent.operation_kind,
                    status="resolved",
                    source_document_label=intent.source_document_label,
                    paragraph_indices=[record.absolute_index for record in subpoint_records],
                    point_ref=intent.point_ref,
                    # subpoint_ref нужен handler'у чтобы не вызывать _delete_point_continuation_paragraphs
                    subpoint_ref=intent.point_ref,
                    parent_point_ref=intent.parent_point_ref,
                    parent_point_number=intent.parent_point_number,
                    new_text=intent.new_text,
                    note_text=note,
                    source_excerpt=intent.source_excerpt,
                )
            return ResolvedOperation(
                operation_id=intent.change_id,
                operation_kind=intent.operation_kind,
                status="ambiguous",
                source_document_label=intent.source_document_label,
                point_ref=intent.point_ref,
                parent_point_number=intent.parent_point_number,
                source_excerpt=intent.source_excerpt,
                ambiguity_reason="nested subpoint anchor not found",
            )

        if point_number is None and intent.anchor_text_hint and intent.new_text:
            hint = intent.anchor_text_hint.lower()
            anchor = next((record for record in scoped_records if hint in record.text.lower()), None)
            if anchor is not None:
                note = f"(в ред. {to_genitive(intent.source_document_label)})"
                return ResolvedOperation(
                    operation_id=intent.change_id,
                    operation_kind=intent.operation_kind,
                    status="resolved",
                    source_document_label=intent.source_document_label,
                    paragraph_indices=[anchor.absolute_index],
                    point_ref=intent.point_ref or "",
                    point_number=None,
                    new_text=intent.new_text,
                    note_text=note,
                    source_excerpt=intent.source_excerpt,
                )
        # Баг 5: point_ref вида "1.1", "2.3" — десятичная ссылка без point_number.
        # Ищем абзац, начинающийся с этого паттерна (напр. "1.1. текст").
        if point_number is None and intent.point_ref and re.match(r"^\d+\.\d+", intent.point_ref) and intent.new_text:
            decimal_pattern = re.compile(rf"^{re.escape(intent.point_ref)}[\.\s]")
            anchor = next((record for record in scoped_records if decimal_pattern.match(record.text)), None)
            if anchor is not None:
                note = f"(в ред. {to_genitive(intent.source_document_label)})"
                return ResolvedOperation(
                    operation_id=intent.change_id,
                    operation_kind=intent.operation_kind,
                    status="resolved",
                    source_document_label=intent.source_document_label,
                    paragraph_indices=[anchor.absolute_index],
                    point_ref=intent.point_ref,
                    point_number=None,
                    new_text=intent.new_text,
                    note_text=note,
                    source_excerpt=intent.source_excerpt,
                )
        if point_number is None or not intent.new_text:
            return ResolvedOperation(
                operation_id=intent.change_id,
                operation_kind=intent.operation_kind,
                status="ambiguous",
                source_document_label=intent.source_document_label,
                source_excerpt=intent.source_excerpt,
                ambiguity_reason="missing point_number or new_text",
            )
        all_targets = find_all_point_paragraphs(scoped_records, point_number)
        if not all_targets:
            return ResolvedOperation(
                operation_id=intent.change_id,
                operation_kind=intent.operation_kind,
                status="ambiguous",
                source_document_label=intent.source_document_label,
                point_number=point_number,
                new_text=intent.new_text,
                source_excerpt=intent.source_excerpt,
                ambiguity_reason="point not found",
            )
        note = f"(п. {point_number} в ред. {to_genitive(intent.source_document_label)})"
        if len(all_targets) == 1:
            target = all_targets[0]
            return ResolvedOperation(
                operation_id=intent.change_id,
                operation_kind=intent.operation_kind,
                status="resolved",
                source_document_label=intent.source_document_label,
                paragraph_indices=[target.absolute_index],
                point_ref=intent.point_ref or str(point_number),
                point_number=point_number,
                new_text=intent.new_text,
                note_text=note,
                source_excerpt=intent.source_excerpt,
            )
        # Несколько совпадений — строим candidates и дизамбигуируем через LLM
        candidates = [
            ResolutionCandidate(
                candidate_id=f"point_{i + 1}",
                absolute_paragraph_index=rec.absolute_index,
                paragraph_text=rec.text,
                section_path=f"пункт {point_number} (вхождение {i + 1})",
                extra={"candidate_source": "point_ref", "occurrence": i + 1},
            )
            for i, rec in enumerate(all_targets)
        ]
        selected = self._select_candidate(intent, candidates)
        if selected is None:
            # fallback: берём первое совпадение
            target = all_targets[0]
            return ResolvedOperation(
                operation_id=intent.change_id,
                operation_kind=intent.operation_kind,
                status="resolved",
                source_document_label=intent.source_document_label,
                paragraph_indices=[target.absolute_index],
                point_ref=intent.point_ref or str(point_number),
                point_number=point_number,
                new_text=intent.new_text,
                note_text=note,
                source_excerpt=intent.source_excerpt,
                ambiguity_reason="point_ref disambiguation failed, used first occurrence",
            )
        return ResolvedOperation(
            operation_id=intent.change_id,
            operation_kind=intent.operation_kind,
            status="resolved",
            source_document_label=intent.source_document_label,
            paragraph_indices=[selected.absolute_paragraph_index],
            point_ref=intent.point_ref or str(point_number),
            point_number=point_number,
            new_text=intent.new_text,
            note_text=note,
            source_excerpt=intent.source_excerpt,
        )

    def _resolve_replace_phrase_globally(self, records: list[Any], intent: ChangeIntent) -> ResolvedOperation:
        if not intent.old_text:
            return ResolvedOperation(
                operation_id=intent.change_id,
                operation_kind=intent.operation_kind,
                status="ambiguous",
                source_document_label=intent.source_document_label,
                source_excerpt=intent.source_excerpt,
                ambiguity_reason="missing old_text",
            )
        search_records = self._target_scope_records(records, intent)
        indices = [
            record.absolute_index
            for record in search_records
            if self._text_contains_phrase_variant(record.text, intent.old_text)
        ]
        if not indices:
            return ResolvedOperation(
                operation_id=intent.change_id,
                operation_kind=intent.operation_kind,
                status="ambiguous",
                source_document_label=intent.source_document_label,
                old_text=intent.old_text,
                new_text=intent.new_text,
                source_excerpt=intent.source_excerpt,
                ambiguity_reason="old_text not found",
            )
        note = f"(в ред. {to_genitive(intent.source_document_label)})"
        return ResolvedOperation(
            operation_id=intent.change_id,
            operation_kind=intent.operation_kind,
            status="resolved",
            source_document_label=intent.source_document_label,
            paragraph_indices=indices,
            old_text=intent.old_text,
            new_text=intent.new_text,
            note_text=note,
            source_excerpt=intent.source_excerpt,
        )

    def _resolve_append_words_to_point(self, records: list[Any], intent: ChangeIntent) -> ResolvedOperation:
        point_ref = intent.point_ref or (str(intent.point_number) if intent.point_number is not None else "")
        candidates = self._build_point_ref_candidates(records, intent, point_ref)
        if not candidates:
            return ResolvedOperation(
                operation_id=intent.change_id,
                operation_kind=intent.operation_kind,
                status="ambiguous",
                source_document_label=intent.source_document_label,
                point_ref=point_ref,
                appended_words=intent.appended_words,
                source_excerpt=intent.source_excerpt,
                ambiguity_reason="point_ref not found",
            )
        selected = self._select_candidate(intent, candidates)
        if selected is None:
            return ResolvedOperation(
                operation_id=intent.change_id,
                operation_kind=intent.operation_kind,
                status="ambiguous",
                source_document_label=intent.source_document_label,
                appendix_number=intent.appendix_number,
                point_ref=point_ref,
                appended_words=intent.appended_words,
                source_excerpt=intent.source_excerpt,
                ambiguity_reason="point_ref disambiguation failed",
            )
        note = f"(в ред. {to_genitive(intent.source_document_label)})"
        return ResolvedOperation(
            operation_id=intent.change_id,
            operation_kind=intent.operation_kind,
            status="resolved",
            source_document_label=intent.source_document_label,
            paragraph_indices=[selected.absolute_paragraph_index],
            appendix_number=intent.appendix_number,
            point_ref=point_ref,
            point_number=intent.point_number,
            parent_point_ref=intent.parent_point_ref,
            parent_point_number=intent.parent_point_number,
            subpoint_ref=intent.subpoint_ref,
            paragraph_ordinal=intent.paragraph_ordinal,
            appended_words=intent.appended_words,
            note_text=note,
            source_excerpt=intent.source_excerpt,
        )

    def _resolve_repeal_point(self, records: list[Any], intent: ChangeIntent) -> list[ResolvedOperation]:
        point_ref = intent.point_ref or (str(intent.point_number) if intent.point_number is not None else "")
        # Подпункт: point_number не задан, но задан parent_point_number + point_ref (буква подпункта)
        if intent.parent_point_number and intent.point_ref and not intent.point_number:
            scoped_records = self._appendix_scope_records(records, intent)
            subpoint_records = self._find_subpoint_records(scoped_records, intent.parent_point_number, intent.point_ref)
            if subpoint_records:
                label = f"{intent.point_ref}) утратил силу. - {intent.source_document_label};"
                return [ResolvedOperation(
                    operation_id=intent.change_id,
                    operation_kind=intent.operation_kind,
                    status="resolved",
                    source_document_label=intent.source_document_label,
                    paragraph_indices=[record.absolute_index for record in subpoint_records],
                    point_ref=intent.point_ref,
                    parent_point_ref=intent.parent_point_ref,
                    parent_point_number=intent.parent_point_number,
                    new_text=label,
                    source_excerpt=intent.source_excerpt,
                )]
            return [ResolvedOperation(
                operation_id=intent.change_id,
                operation_kind=intent.operation_kind,
                status="ambiguous",
                source_document_label=intent.source_document_label,
                point_ref=intent.point_ref,
                parent_point_number=intent.parent_point_number,
                source_excerpt=intent.source_excerpt,
                ambiguity_reason="nested subpoint anchor not found",
            )]
        candidates = self._build_point_ref_candidates(records, intent, point_ref)
        if not candidates:
            return [ResolvedOperation(
                operation_id=intent.change_id,
                operation_kind=intent.operation_kind,
                status="ambiguous",
                source_document_label=intent.source_document_label,
                point_ref=point_ref,
                source_excerpt=intent.source_excerpt,
                ambiguity_reason="point_ref not found",
            )]
        approving_candidates = [
            candidate for candidate in candidates
            if self._APPROVES_APPENDIX_RE.search(candidate.paragraph_text)
        ]
        selected = approving_candidates[0] if len(approving_candidates) == 1 else self._select_candidate(intent, candidates)
        if selected is None:
            return [ResolvedOperation(
                operation_id=intent.change_id,
                operation_kind=intent.operation_kind,
                status="ambiguous",
                source_document_label=intent.source_document_label,
                appendix_number=intent.appendix_number,
                point_ref=point_ref,
                source_excerpt=intent.source_excerpt,
                ambiguity_reason="point_ref disambiguation failed",
            )]
        label = self._build_repeal_label(intent, point_ref)
        primary = ResolvedOperation(
            operation_id=intent.change_id,
            operation_kind=intent.operation_kind,
            status="resolved",
            source_document_label=intent.source_document_label,
            paragraph_indices=[selected.absolute_paragraph_index],
            appendix_number=intent.appendix_number,
            point_ref=point_ref,
            point_number=intent.point_number,
            parent_point_ref=intent.parent_point_ref,
            parent_point_number=intent.parent_point_number,
            subpoint_ref=intent.subpoint_ref,
            paragraph_ordinal=intent.paragraph_ordinal,
            new_text=label,
            source_excerpt=intent.source_excerpt,
        )
        result: list[ResolvedOperation] = [primary]
        if self._APPROVES_APPENDIX_RE.search(selected.paragraph_text):
            appendix_number = self._extract_appendix_number_from_text(selected.paragraph_text)
            cascade_intent = ChangeIntent(
                change_id=f"{intent.change_id}_cascade_appendix",
                operation_kind="repeal_appendix_block",
                source_document_label=intent.source_document_label,
                appendix_number=appendix_number,
                source_excerpt=intent.source_excerpt,
            )
            result.append(self._resolve_repeal_appendix_block(records, cascade_intent))
        return result

    _APPROVES_APPENDIX_RE = re.compile(r"\bутвердить\s+прилагаем", re.IGNORECASE)

    def _inject_cascade_repeal_intents(self, records: list[Any], intents: list[ChangeIntent]) -> None:
        existing_ids = {intent.change_id for intent in intents}
        cascade: list[ChangeIntent] = []
        for intent in intents:
            if intent.operation_kind != "repeal_point":
                continue
            cascade_id = f"{intent.change_id}_cascade_appendix"
            if cascade_id in existing_ids:
                continue
            record = self._find_repeal_point_record(records, intent)
            if record is None or not self._APPROVES_APPENDIX_RE.search(record.text):
                continue
            appendix_number = self._extract_appendix_number_from_text(record.text)
            cascade.append(ChangeIntent(
                change_id=cascade_id,
                operation_kind="repeal_appendix_block",
                source_document_label=intent.source_document_label,
                appendix_number=appendix_number,
                source_excerpt=intent.source_excerpt,
                confidence=intent.confidence,
            ))
            existing_ids.add(cascade_id)
        intents.extend(cascade)

    def _find_repeal_point_record(self, records: list[Any], intent: ChangeIntent) -> Any | None:
        point_ref = intent.point_ref or (str(intent.point_number) if intent.point_number is not None else "")
        if not point_ref or not point_ref.isdigit():
            return None
        scoped = self._appendix_scope_records(records, intent)
        pattern = re.compile(rf"^{re.escape(point_ref)}\.\s+")
        for record in scoped:
            if pattern.match(record.text):
                return record
        return None

    def _resolve_repeal_appendix_block(
        self, records: list[Any], intent: ChangeIntent
    ) -> ResolvedOperation:
        appendix_record = self._find_appendix_record(records, intent.appendix_number)
        if appendix_record is None:
            return ResolvedOperation(
                operation_id=intent.change_id,
                operation_kind="replace_appendix_block",
                status="ambiguous",
                source_document_label=intent.source_document_label,
                appendix_number=intent.appendix_number,
                source_excerpt=intent.source_excerpt,
                ambiguity_reason="appendix start paragraph not found in base document",
            )
        repeal_label = f"Утратило силу. - {intent.source_document_label}."
        return ResolvedOperation(
            operation_id=intent.change_id,
            operation_kind="replace_appendix_block",
            status="resolved",
            source_document_label=intent.source_document_label,
            paragraph_indices=[appendix_record.absolute_index],
            appendix_number=intent.appendix_number,
            new_block_lines=[repeal_label],
            source_excerpt=intent.source_excerpt,
        )

    def _extract_appendix_number_from_text(self, text: str) -> str:
        match = re.search(r"приложени(?:е|я)\s+n?\s*(\d+)", text, re.IGNORECASE)
        return match.group(1) if match else ""

    def _find_appendix_record(self, records: list[Any], appendix_number: str) -> Any | None:
        if appendix_number:
            return find_appendix_start(records, appendix_number)
        pattern = re.compile(r"^приложение\b", re.IGNORECASE)
        for record in records:
            if pattern.match(record.text):
                return record
        return self._find_approved_appendix_content_record(records)

    def _find_approved_appendix_content_record(self, records: list[Any]) -> Any | None:
        approval_index = None
        for idx, record in enumerate(records):
            if record.text.strip().lower() == "утверждено":
                approval_index = idx
                break
        if approval_index is None:
            return None

        title_index = None
        for idx in range(approval_index + 1, min(len(records), approval_index + 12)):
            title = records[idx].text.strip().lower()
            if title in {"положение", "порядок", "правила", "перечень", "состав"}:
                title_index = idx
                break
        if title_index is None:
            return None

        point_pattern = re.compile(r"^\d+\.\s+")
        for record in records[title_index + 1:]:
            if point_pattern.match(record.text):
                return record
        return None

    def _resolve_replace_appendix_block(self, records: list[Any], intent: ChangeIntent) -> ResolvedOperation:
        appendix = find_appendix_start(records, intent.appendix_number)
        if appendix is None or not intent.new_block_lines:
            return ResolvedOperation(
                operation_id=intent.change_id,
                operation_kind=intent.operation_kind,
                status="ambiguous",
                source_document_label=intent.source_document_label,
                appendix_number=intent.appendix_number,
                source_excerpt=intent.source_excerpt,
                ambiguity_reason="appendix not found or empty replacement",
            )
        return ResolvedOperation(
            operation_id=intent.change_id,
            operation_kind=intent.operation_kind,
            status="resolved",
            source_document_label=intent.source_document_label,
            paragraph_indices=[appendix.absolute_index],
            appendix_number=intent.appendix_number,
            new_block_lines=list(intent.new_block_lines),
            source_excerpt=intent.source_excerpt,
        )

    def _resolve_insert_list_entry(self, records: list[Any], intent: ChangeIntent) -> ResolvedOperation:
        anchor_hint = intent.anchor_text_hint.lower()
        anchor_candidates = [
            record for record in records
            if anchor_hint and anchor_hint in record.text.lower()
        ]
        target = anchor_candidates[-1] if anchor_candidates else None
        if target is None:
            target = records[-1] if records else None
        if target is None:
            return ResolvedOperation(
                operation_id=intent.change_id,
                operation_kind=intent.operation_kind,
                status="ambiguous",
                source_document_label=intent.source_document_label,
                anchor_text_hint=intent.anchor_text_hint,
                new_text=intent.new_text,
                source_excerpt=intent.source_excerpt,
                ambiguity_reason="anchor line not found",
            )
        return ResolvedOperation(
            operation_id=intent.change_id,
            operation_kind=intent.operation_kind,
            status="resolved",
            source_document_label=intent.source_document_label,
            insert_after_index=target.absolute_index - 1,
            anchor_text_hint=intent.anchor_text_hint,
            new_text=intent.new_text,
            source_excerpt=intent.source_excerpt,
        )

    def _resolve_insert_list_entry_by_candidates(
        self,
        records: list[Any],
        intent: ChangeIntent,
    ) -> tuple[ResolvedOperation, list[ResolutionCandidate]]:
        candidates = self._build_list_entry_candidates(records, intent)
        if not candidates:
            if intent.new_block_lines:
                note = f"(введено {to_instrumental(intent.source_document_label)})"
                return (
                    ResolvedOperation(
                        operation_id=intent.change_id,
                        operation_kind=intent.operation_kind,
                        status="resolved",
                        source_document_label=intent.source_document_label,
                        section_hint=intent.section_hint,
                        new_item_text=intent.new_item_text,
                        new_block_lines=list(intent.new_block_lines),
                        note_text=note,
                        source_excerpt=intent.source_excerpt,
                    ),
                    [],
                )
            return (
                ResolvedOperation(
                    operation_id=intent.change_id,
                    operation_kind=intent.operation_kind,
                    status="ambiguous",
                    source_document_label=intent.source_document_label,
                    anchor_text_hint=intent.anchor_text_hint,
                    new_text=intent.new_text,
                    source_excerpt=intent.source_excerpt,
                    ambiguity_reason="no list-entry candidates",
                ),
                [],
            )
        selected = self._select_candidate(intent, candidates)
        if selected is None:
            return (
                ResolvedOperation(
                    operation_id=intent.change_id,
                    operation_kind=intent.operation_kind,
                    status="ambiguous",
                    source_document_label=intent.source_document_label,
                    anchor_text_hint=intent.anchor_text_hint,
                    new_text=intent.new_text,
                    source_excerpt=intent.source_excerpt,
                    ambiguity_reason="anchor_id disambiguation failed",
                ),
                candidates,
            )
        return (
            ResolvedOperation(
                operation_id=intent.change_id,
                operation_kind=intent.operation_kind,
                status="resolved",
                source_document_label=intent.source_document_label,
                insert_after_index=selected.absolute_paragraph_index - 1,
                anchor_text_hint=intent.anchor_text_hint,
                new_text=intent.new_text,
                source_excerpt=intent.source_excerpt,
            ),
            candidates,
        )

    def _resolve_replace_person_role(self, records: list[Any], intent: ChangeIntent) -> ResolvedOperation:
        surname = surname_stem(intent.person_name_hint)
        person_candidates = [record for record in records if surname and surname in record.text.lower()]
        target = person_candidates[-1] if person_candidates else None
        if target is None:
            return ResolvedOperation(
                operation_id=intent.change_id,
                operation_kind=intent.operation_kind,
                status="ambiguous",
                source_document_label=intent.source_document_label,
                person_name_hint=intent.person_name_hint,
                new_text=intent.new_text,
                source_excerpt=intent.source_excerpt,
                ambiguity_reason="person line not found",
            )
        return ResolvedOperation(
            operation_id=intent.change_id,
            operation_kind=intent.operation_kind,
            status="resolved",
            source_document_label=intent.source_document_label,
            paragraph_indices=[target.absolute_index],
            person_name_hint=intent.person_name_hint,
            new_text=intent.new_text,
            source_excerpt=intent.source_excerpt,
        )

    def _resolve_replace_person_role_by_candidates(
        self,
        records: list[Any],
        intent: ChangeIntent,
    ) -> tuple[ResolvedOperation, list[ResolutionCandidate]]:
        candidates = self._build_list_entry_candidates(records, intent)
        if not candidates:
            if intent.new_block_lines:
                note = f"(введено {to_instrumental(intent.source_document_label)})"
                return (
                    ResolvedOperation(
                        operation_id=intent.change_id,
                        operation_kind=intent.operation_kind,
                        status="resolved",
                        source_document_label=intent.source_document_label,
                        appendix_number=intent.appendix_number,
                        section_hint=intent.section_hint,
                        new_item_text=intent.new_item_text,
                        new_block_lines=list(intent.new_block_lines),
                        note_text=note,
                        source_excerpt=intent.source_excerpt,
                    ),
                    [],
                )
            return (
                ResolvedOperation(
                    operation_id=intent.change_id,
                    operation_kind=intent.operation_kind,
                    status="ambiguous",
                    source_document_label=intent.source_document_label,
                    person_name_hint=intent.person_name_hint,
                    new_text=intent.new_text,
                    source_excerpt=intent.source_excerpt,
                    ambiguity_reason="no list-entry candidates",
                ),
                [],
            )
        selected = self._select_candidate(intent, candidates)
        if selected is None:
            person_matches = [
                candidate
                for candidate in candidates
                if bool((candidate.extra or {}).get("person_hint_match"))
            ]
            if len(person_matches) == 1:
                selected = person_matches[0]
        if selected is None:
            return (
                ResolvedOperation(
                    operation_id=intent.change_id,
                    operation_kind=intent.operation_kind,
                    status="ambiguous",
                    source_document_label=intent.source_document_label,
                    person_name_hint=intent.person_name_hint,
                    new_text=intent.new_text,
                    source_excerpt=intent.source_excerpt,
                    ambiguity_reason="anchor_id disambiguation failed",
                ),
                candidates,
            )
        return (
            ResolvedOperation(
                operation_id=intent.change_id,
                operation_kind=intent.operation_kind,
                status="resolved",
                source_document_label=intent.source_document_label,
                paragraph_indices=[selected.absolute_paragraph_index],
                person_name_hint=intent.person_name_hint,
                new_text=intent.new_text,
                source_excerpt=intent.source_excerpt,
            ),
            candidates,
        )

    def _build_list_entry_candidates(self, records: list[Any], intent: ChangeIntent) -> list[ResolutionCandidate]:
        appendix_records = self._appendix_scope_records(records, intent)
        entry_records = [record for record in appendix_records if " - " in record.text]
        candidates: list[ResolutionCandidate] = []
        for index, record in enumerate(entry_records, 1):
            candidates.append(
                ResolutionCandidate(
                    candidate_id=f"anchor_{index}",
                    absolute_paragraph_index=record.absolute_index,
                    paragraph_text=record.text,
                    section_path="appendix_list_entry",
                    extra={
                        "anchor_hint_match": intent.anchor_text_hint and intent.anchor_text_hint.lower() in record.text.lower(),
                        "person_hint_match": bool(intent.person_name_hint) and surname_stem(intent.person_name_hint) in record.text.lower(),
                    },
                )
            )
        return candidates

    def _appendix_scope_records(self, records: list[Any], intent: ChangeIntent) -> list[Any]:
        appendix_starts = [idx for idx, record in enumerate(records) if record.text.lower().startswith("приложение")]
        if not appendix_starts:
            return records
        if intent.appendix_number:
            appendix_pattern = re.compile(
                rf"^приложение\s+n?\s*{re.escape(intent.appendix_number)}(?:\b|$)",
                flags=re.IGNORECASE,
            )
            scoped_start = None
            for idx in appendix_starts:
                if appendix_pattern.match(records[idx].text):
                    scoped_start = idx
                    break
            if scoped_start is not None:
                next_starts = [idx for idx in appendix_starts if idx > scoped_start]
                scoped_end = next_starts[0] if next_starts else len(records)
                return records[scoped_start:scoped_end]
        # Нет appendix_number → намерение про основную часть документа.
        # Возвращаем только записи до первого «Приложение».
        return records[: appendix_starts[0]]

    def _target_scope_records(self, records: list[Any], intent: ChangeIntent) -> list[Any]:
        scoped_records = self._appendix_scope_records(records, intent)
        nested_records = self._nested_subpoint_block_records(scoped_records, intent)
        if nested_records:
            if intent.paragraph_ordinal is not None and intent.paragraph_ordinal > 0:
                ordinal_index = intent.paragraph_ordinal - 1
                if ordinal_index < len(nested_records):
                    return [nested_records[ordinal_index]]
            return nested_records
        if self._is_preamble_scope(intent):
            return self._preamble_records(scoped_records)
        return scoped_records

    def _is_preamble_scope(self, intent: ChangeIntent) -> bool:
        hint = (intent.section_hint or "").strip().lower()
        return hint in {"преамбула", "preamble"} or "преамбул" in hint

    def _is_global_scope(self, intent: ChangeIntent) -> bool:
        return (intent.section_hint or "").strip().lower() in {"global", "весь документ"}

    def _preamble_records(self, records: list[Any]) -> list[Any]:
        preamble: list[Any] = []
        top_point_pattern = re.compile(r"^\d+\.\s+")
        appendix_pattern = re.compile(r"^приложение(?:\s|$)", flags=re.IGNORECASE)
        for record in records:
            text = record.text.strip()
            if top_point_pattern.match(text) or appendix_pattern.match(text):
                break
            if text:
                preamble.append(record)
        return preamble

    def _build_point_ref_candidates(
        self,
        records: list[Any],
        intent: ChangeIntent,
        point_ref: str,
    ) -> list[ResolutionCandidate]:
        candidates: list[ResolutionCandidate] = []

        # Приоритет 1: существующая логика вложенных подпунктов (subpoint_ref)
        nested_candidates = self._build_nested_subpoint_candidates(records, intent)
        candidates.extend(nested_candidates)

        if point_ref:
            scoped_records = self._appendix_scope_records(records, intent)

            # Приоритет 2: иерархическая навигация parent_point_number + буква point_ref
            is_letter_ref = bool(re.match(r"^[а-яёa-z](?:\(\d+\))?$", point_ref, re.IGNORECASE))
            parent_pn = intent.parent_point_number
            if parent_pn and is_letter_ref:
                subpoint_occurrences = self._find_all_subpoint_occurrences(
                    scoped_records, parent_pn, point_ref
                )
                for occ_idx, occurrence in enumerate(subpoint_occurrences, 1):
                    for para_idx, rec in enumerate(occurrence, 1):
                        candidates.append(
                            ResolutionCandidate(
                                candidate_id=f"hier_{parent_pn}_{point_ref}_occ{occ_idx}_p{para_idx}",
                                absolute_paragraph_index=rec.absolute_index,
                                paragraph_text=rec.text,
                                section_path=f"пункт {parent_pn} / подпункт {point_ref} (вхождение {occ_idx})",
                                extra={
                                    "candidate_source": "hierarchical",
                                    "parent_point_number": parent_pn,
                                    "subpoint_ref": point_ref,
                                    "occurrence": occ_idx,
                                },
                            )
                        )

            # Приоритет 3: числовой point_ref — поиск по паттерну «N. »
            pattern = re.compile(rf"^{re.escape(point_ref)}\.\s+")
            matches = [record for record in scoped_records if pattern.match(record.text)]
            for index, record in enumerate(matches, 1):
                candidates.append(
                    ResolutionCandidate(
                        candidate_id=f"point_{index}",
                        absolute_paragraph_index=record.absolute_index,
                        paragraph_text=record.text,
                        section_path=intent.section_hint,
                        extra={"candidate_source": "point_ref", "appendix_number": intent.appendix_number},
                    )
                )

        return candidates

    def _build_nested_subpoint_candidates(
        self,
        records: list[Any],
        intent: ChangeIntent,
    ) -> list[ResolutionCandidate]:
        scoped_records = self._appendix_scope_records(records, intent)
        if not intent.subpoint_ref or not intent.parent_point_number:
            return []
        occurrences = self._find_all_subpoint_occurrences(
            scoped_records, intent.parent_point_number, intent.subpoint_ref
        )
        if not occurrences:
            return []
        candidates: list[ResolutionCandidate] = []
        for occ_idx, occurrence in enumerate(occurrences, 1):
            target_records = occurrence
            if intent.paragraph_ordinal is not None and intent.paragraph_ordinal > 0:
                ordinal_index = intent.paragraph_ordinal - 1
                if ordinal_index >= len(occurrence):
                    continue
                target_records = [occurrence[ordinal_index]]
            for offset, record in enumerate(target_records, 1):
                candidates.append(
                    ResolutionCandidate(
                        candidate_id=f"subpoint_{len(candidates) + 1}",
                        absolute_paragraph_index=record.absolute_index,
                        paragraph_text=record.text,
                        section_path=f"пункт {intent.parent_point_ref or intent.parent_point_number} / подпункт {intent.subpoint_ref} (вхождение {occ_idx})",
                        extra={
                            "candidate_source": "nested_subpoint",
                            "parent_point_ref": intent.parent_point_ref,
                            "subpoint_ref": intent.subpoint_ref,
                            "paragraph_ordinal": intent.paragraph_ordinal,
                            "relative_offset": offset,
                            "occurrence": occ_idx,
                        },
                    )
                )
        return candidates

    def _nested_subpoint_block_records(
        self,
        scoped_records: list[Any],
        intent: ChangeIntent,
    ) -> list[Any]:
        """Возвращает абзацы подпункта intent.subpoint_ref внутри пункта intent.parent_point_number.

        Делегирует в _find_subpoint_records, который корректно перебирает все блоки пункта
        через find_all_point_blocks — нужно для документов с дублирующимися номерами пунктов.
        """
        if not intent.subpoint_ref or not intent.parent_point_number:
            return []
        return self._find_subpoint_records(scoped_records, intent.parent_point_number, intent.subpoint_ref)

    def _find_subpoint_records(
        self,
        scoped_records: list[Any],
        parent_point_number: int,
        point_ref: str,
    ) -> list[Any]:
        """Ищет подпункт point_ref внутри пункта parent_point_number.

        Перебирает ВСЕ блоки с нужным номером пункта и возвращает подпункт из
        первого блока, где он нашёлся. Это нужно для документов, где один и тот же
        номер встречается в нескольких секциях (напр., «2.» в теле приказа и «2.»
        в тексте Порядка).
        """
        for block in find_all_point_blocks(scoped_records, parent_point_number):
            subpoint = find_subpoint_in_point(block, point_ref)
            if subpoint:
                return subpoint
        return []

    def _find_all_subpoint_occurrences(
        self,
        scoped_records: list[Any],
        parent_point_number: int,
        point_ref: str,
    ) -> list[list[Any]]:
        """Ищет ВСЕ вхождения подпункта point_ref внутри пункта parent_point_number.

        Перебирает ВСЕ блоки с нужным номером пункта и возвращает список всех
        найденных подпунктов (каждый подпункт — список абзацев). Это нужно для
        дизамбигуации, когда один и тот же номер встречается в нескольких секциях.
        """
        result: list[list[Any]] = []
        for block in find_all_point_blocks(scoped_records, parent_point_number):
            subpoint = find_subpoint_in_point(block, point_ref)
            if subpoint:
                result.append(subpoint)
        return result

    def _last_nested_subpoint_record(self, records: list[Any], intent: ChangeIntent) -> Any | None:
        """Возвращает последний заголовок подпункта (а), б), ...) в пункте parent_point_number.

        Перебирает все блоки с нужным номером пункта через find_all_point_blocks, чтобы
        корректно работать с документами, где один и тот же номер встречается несколько раз
        (тело приказа + текст Порядка).
        """
        if not intent.subpoint_ref or not intent.parent_point_number:
            return None
        scoped_records = self._appendix_scope_records(records, intent)
        for block in find_all_point_blocks(scoped_records, intent.parent_point_number):
            last = find_last_subpoint_in_point(block)
            if last is not None:
                return last
        return None

    def _last_record_in_nested_subpoint(self, records: list[Any], intent: ChangeIntent) -> Any | None:
        nested_records = self._nested_subpoint_block_records(self._appendix_scope_records(records, intent), intent)
        if not nested_records:
            return None
        return nested_records[-1]

    def _build_legacy_nested_subpoint_candidates(
        self,
        records: list[Any],
        intent: ChangeIntent,
    ) -> list[ResolutionCandidate]:
        scoped_records = self._appendix_scope_records(records, intent)
        if not intent.subpoint_ref or not intent.parent_point_number:
            return []
        candidates: list[ResolutionCandidate] = []
        for offset, record in enumerate(scoped_records, 1):
            subpoint_pattern = re.compile(rf"^{re.escape(intent.subpoint_ref)}\)\s+", re.IGNORECASE)
            if not subpoint_pattern.match(record.text):
                continue
            candidates.append(
                ResolutionCandidate(
                    candidate_id=f"subpoint_{len(candidates) + 1}",
                    absolute_paragraph_index=record.absolute_index,
                    paragraph_text=record.text,
                    section_path=f"пункт {intent.parent_point_ref or intent.parent_point_number} / подпункт {intent.subpoint_ref}",
                    extra={
                        "candidate_source": "nested_subpoint",
                        "parent_point_ref": intent.parent_point_ref,
                        "subpoint_ref": intent.subpoint_ref,
                        "relative_offset": offset,
                    },
                )
            )
        return candidates

    def _build_repeal_label(self, intent: ChangeIntent, point_ref: str) -> str:
        if intent.subpoint_ref:
            return f'{intent.subpoint_ref}) утратил силу. - {intent.source_document_label};'
        return f"{point_ref}. Утратил силу. - {intent.source_document_label}."

    def _text_contains_phrase_variant(self, text: str, phrase: str) -> bool:
        text_lower = text.lower()
        normalized_text_lower = self._normalize_phrase_match_text(text).lower()
        for variant in self._phrase_variants(phrase):
            if variant.lower() in text_lower:
                return True
            if self._normalize_phrase_match_text(variant).lower() in normalized_text_lower:
                return True
        return False

    def _normalize_phrase_match_text(self, text: str) -> str:
        text = text.replace(" ", " ").replace("­", "")
        text = re.sub(r"[–—]", "-", text)
        text = text.replace("ё", "е").replace("Ё", "Е")
        return re.sub(r"\s+", " ", text).strip()

    def _phrase_variants(self, phrase: str) -> list[str]:
        variants = [phrase]
        quote_swapped = re.sub(r'"([^"]*)"', r"«\1»", phrase)
        if quote_swapped not in variants:
            variants.append(quote_swapped)
        ascii_swapped = phrase.replace("«", '"').replace("»", '"')
        if ascii_swapped not in variants:
            variants.append(ascii_swapped)
        ye_variant = phrase.replace("ё", "е").replace("Ё", "Е")
        if ye_variant not in variants:
            variants.append(ye_variant)
        return [v for v in variants if v]

    def _resolve_append_section_item(
        self,
        base_doc: Path,
        records: list[Any],
        intent: ChangeIntent,
    ) -> tuple[ResolvedOperation, list[ResolutionCandidate]]:
        # Ранний выход для table-insert: new_block_lines содержит row\t — это вставка
        # строки в таблицу. Resolver не ищет параграфы — editor сам найдёт таблицу.
        if any(line.startswith("row\t") for line in (intent.new_block_lines or [])):
            note = f"(введено {to_instrumental(intent.source_document_label)})"
            return (
                ResolvedOperation(
                    operation_id=intent.change_id,
                    operation_kind=intent.operation_kind,
                    status="resolved",
                    source_document_label=intent.source_document_label,
                    section_hint=intent.section_hint,
                    new_item_text=intent.new_item_text,
                    new_block_lines=list(intent.new_block_lines),
                    note_text=note,
                    source_excerpt=intent.source_excerpt,
                ),
                [],
            )

        point_scoped = self._resolve_append_section_item_by_point_scope(records, intent)
        if point_scoped is not None:
            return point_scoped, []

        special_scoped = self._resolve_append_section_item_by_special_scope(records, intent)
        if special_scoped is not None:
            return special_scoped, []

        scoped_records = self._appendix_scope_records(records, intent)
        section_candidates = find_section_candidates(scoped_records, intent.section_hint)
        candidates: list[ResolutionCandidate] = []
        for idx, item in enumerate(section_candidates, 1):
            last_item = item["item_records"][-1]
            candidates.append(
                ResolutionCandidate(
                    candidate_id=f"cand_{idx}",
                    absolute_paragraph_index=last_item.absolute_index,
                    paragraph_text=last_item.text,
                    section_path=item["heading_text"],
                    extra={"heading_text": item["heading_text"], "overlap": item["overlap"]},
                )
            )

        # Autopick: если первый кандидат имеет overlap строго выше второго — берём без LLM.
        if len(section_candidates) >= 1:
            top_overlap = section_candidates[0]["overlap"]
            second_overlap = section_candidates[1]["overlap"] if len(section_candidates) >= 2 else 0
            if top_overlap > second_overlap:
                best = candidates[0]
                note = f"(введено {to_instrumental(intent.source_document_label)})"
                return (
                    ResolvedOperation(
                        operation_id=intent.change_id,
                        operation_kind=intent.operation_kind,
                        status="resolved",
                        source_document_label=intent.source_document_label,
                        insert_after_index=best.absolute_paragraph_index,
                        section_hint=best.section_path or intent.section_hint,
                        new_item_text=intent.new_item_text,
                        new_block_lines=list(intent.new_block_lines),
                        note_text=note,
                        source_excerpt=intent.source_excerpt,
                    ),
                    candidates,
                )

        if not candidates:
            table_candidates = find_table_section_candidates(base_doc, intent.section_hint)
            for idx, item in enumerate(table_candidates, 1):
                candidates.append(
                    ResolutionCandidate(
                        candidate_id=f"tbl_{idx}",
                        absolute_paragraph_index=-1,
                        paragraph_text=f"[table {item['table_index']} row {item['anchor_row_index']}] {item['heading_text']}",
                        section_path=item["heading_text"],
                        extra={
                            "heading_text": item["heading_text"],
                            "table_index": item["table_index"],
                            "anchor_row_index": item["anchor_row_index"],
                            "candidate_source": "table",
                        },
                    )
                )

        if not candidates:
            if intent.new_block_lines:
                note = f"(введено {to_instrumental(intent.source_document_label)})"
                return (
                    ResolvedOperation(
                        operation_id=intent.change_id,
                        operation_kind=intent.operation_kind,
                        status="resolved",
                        source_document_label=intent.source_document_label,
                        appendix_number=intent.appendix_number,
                        section_hint=intent.section_hint,
                        new_item_text=intent.new_item_text,
                        new_block_lines=list(intent.new_block_lines),
                        note_text=note,
                        source_excerpt=intent.source_excerpt,
                    ),
                    [],
                )
            return (
                ResolvedOperation(
                    operation_id=intent.change_id,
                    operation_kind=intent.operation_kind,
                    status="ambiguous",
                    source_document_label=intent.source_document_label,
                    new_item_text=intent.new_item_text,
                    section_hint=intent.section_hint,
                    source_excerpt=intent.source_excerpt,
                    ambiguity_reason="no section candidates",
                ),
                [],
            )
        selected = self._select_candidate(intent, candidates)
        if selected is None:
            return (
                ResolvedOperation(
                    operation_id=intent.change_id,
                    operation_kind=intent.operation_kind,
                    status="ambiguous",
                    source_document_label=intent.source_document_label,
                    new_item_text=intent.new_item_text,
                    section_hint=intent.section_hint,
                    source_excerpt=intent.source_excerpt,
                    ambiguity_reason="resolver LLM did not disambiguate",
                ),
                candidates,
            )
        note = f"(введено {to_instrumental(intent.source_document_label)})"
        return (
            ResolvedOperation(
                operation_id=intent.change_id,
                operation_kind=intent.operation_kind,
                status="resolved",
                source_document_label=intent.source_document_label,
                insert_after_index=selected.absolute_paragraph_index if selected.absolute_paragraph_index >= 0 else None,
                section_hint=selected.section_path or intent.section_hint,
                new_item_text=intent.new_item_text,
                new_block_lines=list(intent.new_block_lines),
                note_text=note,
                source_excerpt=intent.source_excerpt,
            ),
            candidates,
        )

    def _resolve_append_section_item_by_point_scope(
        self,
        records: list[Any],
        intent: ChangeIntent,
    ) -> ResolvedOperation | None:
        nested_scoped = self._resolve_append_section_item_by_nested_scope(records, intent)
        if nested_scoped is not None:
            return nested_scoped

        point_number = intent.point_number if intent.point_number and intent.point_number > 0 else None
        if point_number is None:
            point_ref_digits = re.search(r"\d+", intent.point_ref or "")
            if point_ref_digits:
                point_number = int(point_ref_digits.group(0))
        # Баг 3 fallback: парсим point_number из section_hint ("пункт 5", "пункт 3")
        if point_number is None:
            section_hint_match = re.search(r"пункт\s+(\d+)", (intent.section_hint or "").lower())
            if section_hint_match:
                point_number = int(section_hint_match.group(1))
        if point_number is None:
            return None
        if not intent.new_item_text and not intent.new_block_lines:
            return None

        scoped_records = self._appendix_scope_records(records, intent)
        target = find_point_paragraph(scoped_records, point_number)
        if target is None:
            return None

        try:
            start_idx = next(idx for idx, record in enumerate(scoped_records) if record.absolute_index == target.absolute_index)
        except StopIteration:
            return None

        insert_after = target.absolute_index
        top_point_pattern = re.compile(r"^\d+\.\s+")
        for idx in range(start_idx + 1, len(scoped_records)):
            text = scoped_records[idx].text
            if top_point_pattern.match(text):
                break
            insert_after = scoped_records[idx].absolute_index

        note = f"(введено {to_instrumental(intent.source_document_label)})"
        return ResolvedOperation(
            operation_id=intent.change_id,
            operation_kind=intent.operation_kind,
            status="resolved",
            source_document_label=intent.source_document_label,
            insert_after_index=insert_after,
            appendix_number=intent.appendix_number,
            point_ref=intent.point_ref,
            point_number=point_number,
            section_hint=intent.section_hint,
            new_item_text=intent.new_item_text,
            new_block_lines=list(intent.new_block_lines),
            note_text=note,
            source_excerpt=intent.source_excerpt,
        )

    def _find_nested_scope_anchor(
        self,
        records: list,
        intent,
    ):
        """Route anchor search based on source_excerpt content.

        Case 1: "дополнить подпунктом" -- inserting a NEW subpoint.
        Case 2: "дополнить абзацем"   -- adding paragraph to EXISTING subpoint.
        Case 3: generic "подпункт"    -- default subpoint lookup.
        Case 4: fallback              -- last record in nested subpoint.
        """
        excerpt_lower = (intent.source_excerpt or "").lower()
        # Case 1: inserting a NEW subpoint.
        if "дополнить подпунктом" in excerpt_lower:
            scoped = self._appendix_scope_records(records, intent)
            anchor = self._find_insert_anchor_in_point(
                scoped, intent, intent.parent_point_number, intent.subpoint_ref,
            )
            if anchor is None:
                anchor = self._last_nested_subpoint_record(records, intent)
            return anchor
        # Case 2: adding a paragraph to an EXISTING subpoint.
        if "дополнить абзацем" in excerpt_lower or "дополнить абзац" in excerpt_lower:
            anchor = self._last_record_in_nested_subpoint(records, intent)
            if anchor is None:
                anchor = self._last_nested_subpoint_record(records, intent)
            return anchor
        # Case 3: generic subpoint reference.
        if "подпункт" in excerpt_lower:
            anchor = self._last_record_in_nested_subpoint(records, intent)
            if anchor is None:
                anchor = self._last_nested_subpoint_record(records, intent)
            return anchor
        # Case 4: fallback.
        return self._last_record_in_nested_subpoint(records, intent)

    def _resolve_append_section_item_by_nested_scope(
        self,
        records: list[Any],
        intent: ChangeIntent,
    ) -> ResolvedOperation | None:
        if not intent.subpoint_ref or not intent.parent_point_number:
            return None
        if not intent.new_item_text and not intent.new_block_lines:
            return None

        anchor = self._find_nested_scope_anchor(records, intent)
        if anchor is None:
            return ResolvedOperation(
                operation_id=intent.change_id,
                operation_kind=intent.operation_kind,
                status="ambiguous",
                source_document_label=intent.source_document_label,
                appendix_number=intent.appendix_number,
                point_ref=intent.point_ref,
                parent_point_ref=intent.parent_point_ref,
                parent_point_number=intent.parent_point_number,
                subpoint_ref=intent.subpoint_ref,
                section_hint=intent.section_hint,
                new_item_text=intent.new_item_text,
                new_block_lines=list(intent.new_block_lines),
                source_excerpt=intent.source_excerpt,
                ambiguity_reason="nested subpoint anchor not found",
            )

        note = f"(введено {to_instrumental(intent.source_document_label)})"
        return ResolvedOperation(
            operation_id=intent.change_id,
            operation_kind=intent.operation_kind,
            status="resolved",
            source_document_label=intent.source_document_label,
            insert_after_index=anchor.absolute_index,
            appendix_number=intent.appendix_number,
            point_ref=intent.point_ref,
            parent_point_ref=intent.parent_point_ref,
            parent_point_number=intent.parent_point_number,
            subpoint_ref=intent.subpoint_ref,
            paragraph_ordinal=intent.paragraph_ordinal,
            section_hint=intent.section_hint,
            new_item_text=intent.new_item_text or " ".join(intent.new_block_lines),
            new_block_lines=list(intent.new_block_lines),
            note_text=note,
            source_excerpt=intent.source_excerpt,
        )

    def _resolve_append_section_item_by_special_scope(
        self,
        records: list[Any],
        intent: ChangeIntent,
    ) -> ResolvedOperation | None:
        if not (self._is_preamble_scope(intent) or self._is_global_scope(intent)):
            return None
        if not intent.new_item_text and not intent.new_block_lines:
            return None

        scoped_records = self._target_scope_records(records, intent)
        if not scoped_records:
            return ResolvedOperation(
                operation_id=intent.change_id,
                operation_kind=intent.operation_kind,
                status="ambiguous",
                source_document_label=intent.source_document_label,
                appendix_number=intent.appendix_number,
                section_hint=intent.section_hint,
                new_item_text=intent.new_item_text,
                new_block_lines=list(intent.new_block_lines),
                source_excerpt=intent.source_excerpt,
                ambiguity_reason="special scope has no anchor records",
            )

        anchor = scoped_records[-1]
        note = f"(введено {to_instrumental(intent.source_document_label)})"
        return ResolvedOperation(
            operation_id=intent.change_id,
            operation_kind=intent.operation_kind,
            status="resolved",
            source_document_label=intent.source_document_label,
            insert_after_index=anchor.absolute_index,
            appendix_number=intent.appendix_number,
            section_hint=intent.section_hint,
            new_item_text=intent.new_item_text or " ".join(intent.new_block_lines),
            new_block_lines=list(intent.new_block_lines),
            note_text=note,
            source_excerpt=intent.source_excerpt,
        )

    def _select_candidate(
        self,
        intent: ChangeIntent,
        candidates: list[ResolutionCandidate],
    ) -> ResolutionCandidate | None:
        if not candidates:
            return None
        if len(candidates) == 1:
            return candidates[0]
        self._rank_candidates_semantically(intent, candidates)
        semantic_pick = self._try_autopick_top_semantic_candidate(candidates)
        if semantic_pick is not None:
            return semantic_pick
        return self._disambiguate_candidate(intent, candidates[: self._semantic_top_k])

    def _rank_candidates_semantically(
        self,
        intent: ChangeIntent,
        candidates: list[ResolutionCandidate],
    ) -> None:
        if not self._semantic_ranking_available or self._embedding_client is None or len(candidates) < 2:
            return
        query = self._build_semantic_query(intent)
        candidate_texts = [self._candidate_semantic_text(candidate) for candidate in candidates]
        try:
            scores = self._embedding_client.find_most_similar(query, candidate_texts, top_k=len(candidate_texts))
        except Exception:
            self._semantic_ranking_available = False
            return

        score_map = {index: score for index, score in scores}
        ranked = sorted(
            enumerate(candidates),
            key=lambda item: score_map.get(item[0], -1.0),
            reverse=True,
        )
        reordered: list[ResolutionCandidate] = []
        for rank, (original_index, candidate) in enumerate(ranked, 1):
            candidate.extra["semantic_score"] = round(float(score_map.get(original_index, 0.0)), 4)
            candidate.extra["semantic_rank"] = rank
            candidate.extra["semantic_query"] = query
            reordered.append(candidate)
        candidates[:] = reordered

    def _try_autopick_top_semantic_candidate(
        self,
        candidates: list[ResolutionCandidate],
    ) -> ResolutionCandidate | None:
        if not candidates:
            return None
        top_score = float((candidates[0].extra or {}).get("semantic_score", 0.0))
        second_score = float((candidates[1].extra or {}).get("semantic_score", 0.0)) if len(candidates) > 1 else 0.0
        if top_score < self._semantic_auto_threshold:
            return None
        if len(candidates) > 1 and (top_score - second_score) < self._semantic_auto_margin:
            return None
        candidates[0].extra["semantic_autopick"] = True
        return candidates[0]

    def _build_semantic_query(self, intent: ChangeIntent) -> str:
        parts = [
            intent.operation_kind,
            intent.appendix_number,
            intent.point_ref,
            str(intent.point_number) if intent.point_number is not None else "",
            intent.section_hint,
            intent.anchor_text_hint,
            intent.person_name_hint,
            intent.old_text,
            intent.new_text,
            intent.new_item_text,
            " ".join(intent.new_block_lines or []),
            intent.source_excerpt,
        ]
        return "\n".join(part.strip() for part in parts if part and part.strip())

    def _candidate_semantic_text(self, candidate: ResolutionCandidate) -> str:
        parts = [
            candidate.section_path,
            candidate.paragraph_text,
            str((candidate.extra or {}).get("heading_text", "")),
            str((candidate.extra or {}).get("candidate_source", "")),
        ]
        return "\n".join(part.strip() for part in parts if part and part.strip())

    def _disambiguate_candidate(
        self,
        intent: ChangeIntent,
        candidates: list[ResolutionCandidate],
    ) -> ResolutionCandidate | None:
        data, _raw = self.call_llm(
            self._system_prompt,
            self._user_template.format_map(
                {
                    "intent_json": json.dumps(intent.to_dict(), ensure_ascii=False, indent=2),
                    "candidates_json": json.dumps([item.to_dict() for item in candidates], ensure_ascii=False, indent=2),
                }
            ),
            max_tokens=800,
        )
        if data.get("still_ambiguous"):
            return None
        selected_id = data.get("selected_candidate_id")
        for candidate in candidates:
            if candidate.candidate_id == selected_id:
                return candidate
        return None
