from __future__ import annotations

import json
from pathlib import Path
from typing import Any


def load_models_config(config_path: Path | None = None) -> dict[str, Any]:
    path = config_path or (Path(__file__).parent / "config" / "models.json")
    data = json.loads(path.read_text(encoding="utf-8-sig"))
    if not isinstance(data, dict):
        raise ValueError("models.json должен содержать JSON-объект")
    return data


def runtime_kwargs(config: dict[str, Any], role: str) -> dict[str, Any]:
    runtime = config.get("runtime", {})
    models = config.get("models", {})
    return {
        "base_url": runtime.get("base_url", "http://127.0.0.1:1234/v1"),
        "api_key": runtime.get("api_key", "lm-studio"),
        "temperature": float(runtime.get("temperature", 0.0)),
        "timeout": float(runtime.get("timeout", 9000.0)),
        "max_retries": int(runtime.get("max_retries", 4)),
        "enable_thinking": bool(runtime.get("enable_thinking", False)),
        "model": str(models.get(role) or models.get("default") or "gigachat3.1-10b-a1.8b"),
    }


def embedding_runtime_config(config: dict[str, Any]) -> dict[str, Any]:
    runtime = config.get("runtime", {})
    models = config.get("models", {})
    provider = str(runtime.get("embedding_provider") or "local_http")
    return {
        "enabled": bool(runtime.get("semantic_ranking_enabled", True)),
        "provider": provider,
        "service_url": str(runtime.get("embedding_service_url") or "http://127.0.0.1:8010"),
        "base_url": runtime.get("base_url", "http://127.0.0.1:1234/v1"),
        "api_key": runtime.get("api_key", "lm-studio"),
        "timeout": float(runtime.get("timeout", 9000.0)),
        "model": str(models.get("embedding") or runtime.get("embedding_model") or "USER2-base"),
        "top_k": int(runtime.get("semantic_top_k", 5)),
        "auto_threshold": float(runtime.get("semantic_auto_threshold", 0.72)),
        "auto_margin": float(runtime.get("semantic_auto_margin", 0.08)),
    }
