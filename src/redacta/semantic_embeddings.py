from __future__ import annotations

import json
import math
from typing import Any
from urllib.error import URLError
from urllib.request import Request, urlopen

try:
    from sentence_transformers import SentenceTransformer
except ImportError:  # pragma: no cover
    SentenceTransformer = None  # type: ignore[assignment,misc]


def cosine_similarity(a: list[float], b: list[float]) -> float:
    dot = sum(x * y for x, y in zip(a, b))
    norm_a = math.sqrt(sum(x * x for x in a))
    norm_b = math.sqrt(sum(x * x for x in b))
    if norm_a == 0 or norm_b == 0:
        return 0.0
    return dot / (norm_a * norm_b)


class LocalEmbeddingHttpClient:
    def __init__(self, service_url: str, timeout: float = 30.0) -> None:
        self.service_url = service_url.rstrip("/")
        self.timeout = timeout

    def embed(self, texts: list[str]) -> list[list[float]]:
        if not texts:
            return []
        payload = json.dumps({"texts": texts}, ensure_ascii=False).encode("utf-8")
        request = Request(
            f"{self.service_url}/embed",
            data=payload,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urlopen(request, timeout=self.timeout) as response:
                data: dict[str, Any] = json.loads(response.read().decode("utf-8"))
        except (OSError, URLError, json.JSONDecodeError) as exc:
            raise RuntimeError(f"local embedding service unavailable: {exc}") from exc

        embeddings = data.get("embeddings")
        if not isinstance(embeddings, list):
            raise RuntimeError("local embedding service returned no embeddings")
        return embeddings

    def find_most_similar(
        self,
        query: str,
        candidates: list[str],
        top_k: int = 1,
    ) -> list[tuple[int, float]]:
        if not candidates:
            return []
        embeddings = self.embed([query] + candidates)
        query_embedding = embeddings[0]
        scores = [
            (index, cosine_similarity(query_embedding, embeddings[index + 1]))
            for index in range(len(candidates))
        ]
        scores.sort(key=lambda item: item[1], reverse=True)
        return scores[:top_k]


class HuggingFaceEmbeddingClient:
    """
    Клиент эмбеддингов на базе HuggingFace sentence-transformers.

    Модель загружается лениво при первом вызове embed() — тяжёлые веса
    не занимают память до фактического использования.

    Поддерживает инструкционные префиксы для асимметричного поиска.
    Для deepvk/USER2-base рекомендуется:
        query_prompt    = "search_query: "
        document_prompt = "search_document: "

    При пустых промптах работает как обычная симметричная модель.
    """

    def __init__(
        self,
        model_name: str = "deepvk/USER2-base",
        query_prompt: str = "",
        document_prompt: str = "",
        device: str | None = None,
        normalize_embeddings: bool = True,
    ) -> None:
        self.model_name = model_name
        self.query_prompt = query_prompt
        self.document_prompt = document_prompt
        self.device = device
        self.normalize_embeddings = normalize_embeddings
        self._model: Any | None = None

    def _get_model(self) -> Any:
        if self._model is None:
            if SentenceTransformer is None:  # pragma: no cover
                raise ImportError(
                    "sentence-transformers не установлен. "
                    "Установите: pip install sentence-transformers"
                )
            self._model = SentenceTransformer(self.model_name, device=self.device)
        return self._model

    def embed(self, texts: list[str]) -> list[list[float]]:
        """Кодирует тексты с document_prompt. Совместимо с интерфейсом других клиентов."""
        if not texts:
            return []
        model = self._get_model()
        prefixed = [f"{self.document_prompt}{t}" if self.document_prompt else t for t in texts]
        vectors = model.encode(
            prefixed,
            normalize_embeddings=self.normalize_embeddings,
            convert_to_numpy=True,
        )
        return [v.tolist() for v in vectors]

    def _embed_query(self, query: str) -> list[float]:
        """Кодирует запрос с query_prompt (внутреннее использование)."""
        model = self._get_model()
        text = f"{self.query_prompt}{query}" if self.query_prompt else query
        vectors = model.encode(
            [text],
            normalize_embeddings=self.normalize_embeddings,
            convert_to_numpy=True,
        )
        return vectors[0].tolist()

    def find_most_similar(
        self,
        query: str,
        candidates: list[str],
        top_k: int = 1,
    ) -> list[tuple[int, float]]:
        """
        Возвращает индексы и cosine-scores топ-k кандидатов, отсортированных
        по убыванию сходства с запросом.
        """
        if not candidates:
            return []
        query_emb = self._embed_query(query)
        doc_embs = self.embed(candidates)
        scores = [
            (i, cosine_similarity(query_emb, doc_embs[i]))
            for i in range(len(candidates))
        ]
        scores.sort(key=lambda x: x[1], reverse=True)
        return scores[:top_k]
