from __future__ import annotations

from dataclasses import asdict, dataclass, field


@dataclass
class CandidateLedgerEntry:
    candidate_id: str
    label: str
    score: float | None = None
    evidence: str | None = None
    reject_reason: str | None = None


@dataclass
class CandidateLedger:
    change_id: str
    operation_kind: str
    candidates: list[CandidateLedgerEntry] = field(default_factory=list)
    selected_candidate_id: str | None = None
    selected_reason: str | None = None
    ambiguous: bool = False
    ambiguity_reason: str | None = None

    def add_candidate(
        self,
        candidate_id: str,
        label: str,
        score: float | None = None,
        evidence: str | None = None,
    ) -> None:
        self.candidates.append(
            CandidateLedgerEntry(
                candidate_id=candidate_id,
                label=label,
                score=score,
                evidence=evidence,
            )
        )

    def reject(self, candidate_id: str, reason: str) -> None:
        for candidate in self.candidates:
            if candidate.candidate_id == candidate_id:
                candidate.reject_reason = reason
                return
        raise ValueError(f"unknown candidate_id: {candidate_id}")

    def select(self, candidate_id: str, reason: str) -> None:
        if not any(candidate.candidate_id == candidate_id for candidate in self.candidates):
            raise ValueError(f"unknown candidate_id: {candidate_id}")
        self.selected_candidate_id = candidate_id
        self.selected_reason = reason
        self.ambiguous = False
        self.ambiguity_reason = None

    def mark_ambiguous(self, reason: str) -> None:
        self.ambiguous = True
        self.ambiguity_reason = reason
        self.selected_candidate_id = None
        self.selected_reason = None

    def to_dict(self) -> dict:
        return {
            "change_id": self.change_id,
            "operation_kind": self.operation_kind,
            "candidates": [asdict(candidate) for candidate in self.candidates],
            "selected_candidate_id": self.selected_candidate_id,
            "selected_reason": self.selected_reason,
            "ambiguous": self.ambiguous,
            "ambiguity_reason": self.ambiguity_reason,
        }
