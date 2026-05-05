from __future__ import annotations

from typing import Any


class PipelineChecklist:
    def __init__(self, case_id: str) -> None:
        self.case_id = case_id
        self._items: list[dict[str, Any]] = []

    def add(
        self,
        *,
        stage: str,
        check_id: str,
        kind: str,
        ok: bool,
        details: dict[str, Any] | None = None,
    ) -> None:
        self._items.append(
            {
                "case_id": self.case_id,
                "stage": stage,
                "check_id": check_id,
                "kind": kind,
                "ok": ok,
                "details": details or {},
            }
        )

    def items(self) -> list[dict[str, Any]]:
        return list(self._items)
