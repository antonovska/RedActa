from __future__ import annotations

import ast
import json
import re
from pathlib import Path
from typing import Any, Optional

try:
    from docx import Document
    from docx.oxml import OxmlElement
    from docx.text.paragraph import Paragraph
except ImportError as exc:
    raise SystemExit(
        "Не найден пакет python-docx. Установите: python -m pip install --user python-docx"
    ) from exc

try:
    from openai import OpenAI
except ImportError as exc:
    raise SystemExit(
        "Не найден пакет openai. Установите: python -m pip install --user openai"
    ) from exc


# ---------------------------------------------------------------------------
# Загрузчик промптов из файлов agents/prompts/*.md
# ---------------------------------------------------------------------------

_PROMPTS_DIR = Path(__file__).parent / "prompts"


def _extract_prompt_section(text: str, section: str) -> str:
    """Извлечь секцию (System или User) из .md-файла промпта."""
    m = re.search(rf"(?m)^##\s+{section}\s*\n(.*?)(?=^##\s|\Z)", text, re.DOTALL)
    if not m:
        raise FileNotFoundError(f"Секция '## {section}' не найдена в файле промпта")
    return m.group(1).strip()


def load_prompt(name: str) -> tuple[str, str]:
    """Загрузить (system_text, user_template) из prompts/{name}.md.

    User template использует {variable} placeholder-ы (str.format_map).
    Фигурные скобки JSON внутри промпта должны быть экранированы как {{ и }}.
    """
    text = (_PROMPTS_DIR / f"{name}.md").read_text(encoding="utf-8")
    return _extract_prompt_section(text, "System"), _extract_prompt_section(text, "User")


# ---------------------------------------------------------------------------
# Утилиты для работы с текстом
# ---------------------------------------------------------------------------

def compact(text: str) -> str:
    """Схлопнуть пробельные символы в один пробел."""
    return " ".join(text.split()).strip()


def normalize_for_match(text: str) -> str:
    """Нормализовать строку для нечёткого поиска."""
    text = text.replace(" ", " ").replace("­", "")   # nbsp, soft hyphen
    lowered = compact(text).lower()
    lowered = lowered.replace("«", "").replace("»", "").replace('"', "")
    lowered = lowered.replace("–", "-").replace("—", "-")
    return lowered.rstrip(".;:")


def tokenize(text: str) -> list[str]:
    """Разбить текст на токены (слова длиной > 2)."""
    return [t for t in re.findall(r"[a-zа-я0-9\-]+", text.lower()) if len(t) > 2]


def is_section_heading(text: str) -> bool:
    """Определить, является ли строка заголовком раздела."""
    value = compact(text)
    if not value:
        return False
    lower = value.lower()
    if value.endswith(":"):
        return True
    if re.match(r"^[ivxlcdm]+\.", lower):
        return True
    if re.match(r"^\d+\.", value):
        return True
    if lower.startswith(("раздел ", "глава ", "часть ", "подраздел ", "отдел ", "управление ")):
        return True
    letters_only = re.sub(r"[^a-zа-я]", "", lower, flags=re.IGNORECASE)
    if letters_only and value.upper() == value and len(value.split()) >= 3:
        return True
    return False


def is_heading_continuation(previous_text: str, current_text: str) -> bool:
    """Определить, является ли current_text продолжением заголовка."""
    prev = compact(previous_text)
    current = compact(current_text)
    if not prev or not current:
        return False
    if not is_section_heading(prev) or not is_section_heading(current):
        return False
    current_lower = current.lower()
    if current_lower.startswith(("и ", "по ", "в ", "на ")):
        return True
    first_char = current[0]
    if first_char.isalpha() and first_char.lower() == first_char:
        return True
    return False


# ---------------------------------------------------------------------------
# Утилиты для работы с .docx
# ---------------------------------------------------------------------------

def read_non_empty_paragraphs(doc_path: Path) -> list[str]:
    """Прочитать непустые абзацы документа."""
    document = Document(doc_path)
    return [compact(p.text) for p in document.paragraphs if compact(p.text)]


def delete_paragraph(paragraph: Paragraph) -> None:
    """Удалить абзац из документа."""
    element = paragraph._element
    parent = element.getparent()
    if parent is not None:
        parent.remove(element)


# ---------------------------------------------------------------------------
# Утилиты для парсинга JSON из ответа модели
# ---------------------------------------------------------------------------

def extract_message_text(content: Any) -> str:
    """Извлечь текст из ответа модели."""
    if content is None:
        return ""
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts: list[str] = []
        for item in content:
            if isinstance(item, dict):
                text = item.get("text")
                if text:
                    parts.append(str(text))
            else:
                parts.append(str(item))
        return "\n".join(parts).strip()
    return str(content)


def strip_think_blocks(text: str) -> str:
    """Удалить <tool_call>...</tool_call> блоки из ответа модели (для reasoning-моделей)."""
    return re.sub(r"<tool_call>.*?</tool_call>", "", text, flags=re.DOTALL).strip()


def parse_first_json_object(raw_text: str) -> dict[str, Any]:
    """Извлечь первый JSON-объект из текстового ответа модели."""
    cleaned = strip_think_blocks(raw_text).strip()
    cleaned = cleaned.replace("﻿", "")
    cleaned = cleaned.replace("“", '"').replace("”", '"').replace("„", '"')
    cleaned = cleaned.replace("‘", "'").replace("’", "'")
    if cleaned.startswith("```"):
        cleaned = re.sub(r"^```[a-zA-Z0-9_-]*\s*", "", cleaned)
        cleaned = re.sub(r"\s*```$", "", cleaned)

    candidates = [cleaned]
    starts = [i for i, c in enumerate(cleaned) if c == "{"] or [i for i, c in enumerate(raw_text) if c == "{"]
    ends = [i for i, c in enumerate(cleaned) if c == "}"] or [i for i, c in enumerate(raw_text) if c == "}"]
    for s in starts:
        for e in reversed(ends):
            if e <= s:
                continue
            source = cleaned if e < len(cleaned) else raw_text
            candidates.append(source[s : e + 1])

    for candidate in candidates:
        candidate = candidate.strip()
        if not candidate:
            continue
        try:
            parsed = json.loads(candidate)
            if isinstance(parsed, dict):
                return parsed
            if isinstance(parsed, list) and len(parsed) == 1 and isinstance(parsed[0], dict):
                return parsed[0]
        except json.JSONDecodeError:
            pass
        try:
            parsed = ast.literal_eval(candidate)
            if isinstance(parsed, dict):
                return parsed
            if isinstance(parsed, list) and len(parsed) == 1 and isinstance(parsed[0], dict):
                return parsed[0]
        except (ValueError, SyntaxError, TypeError):
            continue

    raise ValueError("Модель не вернула корректный JSON-объект")


# ---------------------------------------------------------------------------
# Базовый класс агента
# ---------------------------------------------------------------------------

class BaseAgent:
    """
    Базовый класс для всех агентов мультиагентной системы.

    Инициализируется параметрами подключения к OpenAI-совместимому backend и предоставляет
    общий метод call_llm() для вызова модели.
    """

    def __init__(
        self,
        name: str,
        base_url: str = "http://127.0.0.1:1234/v1",
        model: str = "gigachat3.1-10b-a1.8b",
        api_key: str = "lm-studio",
        temperature: float = 0.0,
        timeout: float = 9000.0,
        max_retries: int = 4,
        enable_thinking: bool = False,
    ) -> None:
        self.name = name
        self.temperature = temperature
        self.model_name = model
        self.backend_label = self._detect_backend_label(base_url)
        self._client = OpenAI(
            base_url=base_url,
            api_key=api_key,
            max_retries=max_retries,
            timeout=timeout,
        )
        # Автоопределение модели, если указано «local-model» или «auto»
        if model in {"local-model", "auto"}:
            try:
                models = self._client.models.list()
                if getattr(models, "data", None):
                    self.model_name = models.data[0].id
            except Exception:
                self.model_name = model

        _thinking_patterns = ("thinking", "qwq", "deepseek-r1", "r1-")
        self._suppress_thinking: bool = (
            not enable_thinking
            and any(p in self.model_name.lower() for p in _thinking_patterns)
        )

    # -----------------------------------------------------------------------
    # Вызов LLM
    # -----------------------------------------------------------------------

    def call_llm(
        self,
        system_prompt: str,
        user_prompt: str,
        temperature: Optional[float] = None,
        max_tokens: Optional[int] = None,
    ) -> tuple[dict[str, Any], str]:
        temp = temperature if temperature is not None else self.temperature
        effective_user_prompt = (
            "/no_think\n" + user_prompt if self._suppress_thinking else user_prompt
        )
        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": effective_user_prompt},
        ]
        try:
            response = self._request_completion(messages, temp, max_tokens=max_tokens)
        except Exception as exc:
            exc_name = exc.__class__.__name__
            if exc_name in {"APIConnectionError", "APITimeoutError", "ConnectError", "ReadTimeout"}:
                raise RuntimeError(
                    f"[{self.name}] Не удалось подключиться к {self.backend_label} API. "
                    f"Проверьте, что {self.backend_label} запущен и модель загружена."
                ) from exc
            raise RuntimeError(
                f"[{self.name}] {self.backend_label} вернуло ошибку: {exc}"
            ) from exc

        raw = extract_message_text(response.choices[0].message.content)
        try:
            data = parse_first_json_object(raw)
        except ValueError:
            repaired = self.repair_json_response(raw)
            data = parse_first_json_object(repaired)
            raw = repaired
        return data, raw

    def _request_completion(
        self,
        messages: list[dict[str, str]],
        temperature: float,
        max_tokens: Optional[int] = None,
    ) -> Any:
        kwargs: dict[str, Any] = dict(
            model=self.model_name,
            messages=messages,
            temperature=temperature,
            frequency_penalty=0.0,
            presence_penalty=0.0,
        )
        if max_tokens is not None:
            kwargs["max_tokens"] = max_tokens
        return self._client.chat.completions.create(**kwargs)

    def repair_json_response(self, raw_text: str) -> str:
        """Ask the model to normalize its own answer into a valid JSON object."""
        _sys, _ = load_prompt("base_repair_json")
        _REPAIR_MAX = 2500
        _REPAIR_TAIL = 500
        if len(raw_text) > _REPAIR_MAX + _REPAIR_TAIL:
            clipped = raw_text[:_REPAIR_MAX] + " ... " + raw_text[-_REPAIR_TAIL:]
        else:
            clipped = raw_text
        repair_messages = [
            {"role": "system", "content": _sys},
            {"role": "user", "content": clipped},
        ]
        response = self._request_completion(repair_messages, 0.0, max_tokens=800)
        return extract_message_text(response.choices[0].message.content)

    def log(self, message: str) -> None:
        """Вывести сообщение с именем агента."""
        print(f"[{self.name}] {message}")

    @staticmethod
    def _detect_backend_label(base_url: str) -> str:
        lowered = (base_url or "").lower()
        if "11434" in lowered or "ollama" in lowered:
            return "Ollama"
        if "1234" in lowered or "lmstudio" in lowered:
            return "LM Studio"
        return "LLM backend"


# ---------------------------------------------------------------------------
# Клиент для работы с эмбеддингами через LM Studio API
# ---------------------------------------------------------------------------

import math


def cosine_similarity(a: list[float], b: list[float]) -> float:
    """Вычислить косинусное сходство между двумя векторами."""
    dot = sum(x * y for x, y in zip(a, b))
    norm_a = math.sqrt(sum(x * x for x in a))
    norm_b = math.sqrt(sum(x * x for x in b))
    if norm_a == 0 or norm_b == 0:
        return 0.0
    return dot / (norm_a * norm_b)


class EmbeddingClient:
    """
    Клиент для получения эмбеддингов через OpenAI-совместимый API.
    Используется только для embedding_provider, отличных от local_http.
    Основной локальный путь проекта сейчас идет через LocalEmbeddingHttpClient.
    """

    def __init__(
        self,
        base_url: str = "http://127.0.0.1:1234/v1",
        model: str = "USER2-base",
        api_key: str = "lm-studio",
        timeout: float = 9000.0,
    ) -> None:
        self.model = model
        self._client = OpenAI(
            base_url=base_url,
            api_key=api_key,
            max_retries=4,
            timeout=timeout,
        )

    def embed(self, texts: list[str]) -> list[list[float]]:
        """Получить эмбеддинги для списка текстов."""
        if not texts:
            return []
        response = self._client.embeddings.create(
            model=self.model,
            input=texts,
        )
        return [item.embedding for item in response.data]

    def embed_one(self, text: str) -> list[float]:
        """Получить эмбеддинг для одного текста."""
        return self.embed([text])[0]

    def find_most_similar(
        self, query: str, candidates: list[str], top_k: int = 1
    ) -> list[tuple[int, float]]:
        if not candidates:
            return []
        all_texts = [query] + candidates
        embeddings = self.embed(all_texts)
        query_emb = embeddings[0]
        scores = [
            (i, cosine_similarity(query_emb, embeddings[i + 1]))
            for i in range(len(candidates))
        ]
        scores.sort(key=lambda x: x[1], reverse=True)
        return scores[:top_k]
