from __future__ import annotations

import json
import math
from typing import Any
from urllib.error import URLError
from urllib.request import Request, urlopen


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
