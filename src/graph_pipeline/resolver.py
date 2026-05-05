from __future__ import annotations

from pathlib import Path
from typing import Any

from .resolver_v2 import ResolverV2
from .schema import ChangeIntent


class PipelineResolver:
    def __init__(self, config: dict[str, Any]) -> None:
        self._inner = ResolverV2(config)

    def resolve(self, working_doc: Path, intents: list[ChangeIntent], repair: bool = False) -> dict[str, Any]:
        return self._inner.resolve(working_doc, intents, mode="anchor_id", skip_relevance_filter=repair)
