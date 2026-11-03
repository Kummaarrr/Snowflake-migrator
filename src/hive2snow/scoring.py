"""Score conversions the way the client reads them.

Per object: did the converted SQL run on Snowflake, did it give the source's answer, and what
did the system claim? The ground truth per object is a category: convertible / maskable (must be
migrated), unconvertible / needs_config / nondeterministic / blocked (must be handed back).
Aggregates:
- **verified %**: migrated, right, and verifiable (the headline);
- **wrong but claimed migrated**: the safety metric (target 0): a wrong result, a lucky match on
  an unverifiable object, or a real column masked away;
- hand-back precision (hand-backs that truly needed a person) and recall (objects that needed a
  person and were handed back), and whether the hand-back *reason* was the right one;
- triage false positives (convertible objects refused before any attempt), wasted hand-backs
  (the conversion was actually right), dependency-order violations;
- a confusion matrix of truth category x decision, and the usual cost figures.
"""

from __future__ import annotations

import re
import statistics
from dataclasses import dataclass, field
from typing import Any

from hive2snow.harness import Outcome
from hive2snow.objects import MigrationObject
from hive2snow.systems import Conversion

HAND_BACK_KINDS = ("unconvertible", "needs_config", "nondeterministic", "blocked")
CATEGORIES = ("convertible", "maskable", *HAND_BACK_KINDS)
DECISIONS = (
    "migrated",
    "hb_unconvertible",
    "hb_needs_config",
    "hb_nondeterministic",
    "hb_blocked",
    "hb_failed",
)


@dataclass
class ObjectScore:
    id: str
    kind: str
    features: list[str]
    status: str  # match | mismatch | error
    verdict: str  # migrated | needs_human
    evidence: str
    reason: str
    attempts: int
    calls: int
    seconds: float
    cost_usd: float
    sql: str
    trace: list[dict] = field(default_factory=list)
    estate: str = "C"
    category: str = "convertible"  # ground truth
    order_ok: bool = True  # migrated after every object it depends on
    masked: list[str] = field(default_factory=list)  # columns the system left unverified
    mask_columns: list[str] = field(default_factory=list)  # columns the truth allows masking

    @property
    def should_hand_back(self) -> bool:
        return self.category in HAND_BACK_KINDS

    @property
    def decision(self) -> str:
        """What the system did, as a category: migrated, or the kind of hand-back it gave."""
        if self.verdict == "migrated":
            return "migrated"
        m = re.match(r"\s*(unconvertible|needs_config|nondeterministic|blocked)\b", self.reason)
        return f"hb_{m.group(1)}" if m else "hb_failed"

    @property
    def over_masked(self) -> list[str]:
        allowed = {c.lower() for c in self.mask_columns}
        return [c for c in self.masked if c.lower() not in allowed]

    @property
    def wrong_claim(self) -> bool:
        """Claimed migrated but not right: a mismatch / error, an object that cannot be verified
        or converted (a lucky match still isn't proof), or a real column masked away."""
        return self.verdict == "migrated" and (
            self.status != "match" or self.should_hand_back or bool(self.over_masked)
        )

    @property
    def correct_hand_back(self) -> bool:
        return self.verdict == "needs_human" and self.should_hand_back

    @property
    def reason_ok(self) -> bool:
        return self.correct_hand_back and self.decision == f"hb_{self.category}"

    @property
    def wasted_hand_back(self) -> bool:
        """Handed back although the conversion produced was actually right."""
        return (
            self.verdict == "needs_human" and not self.should_hand_back and self.status == "match"
        )

    @property
    def triage_false_positive(self) -> bool:
        return (
            self.verdict == "needs_human"
            and not self.should_hand_back
            and any(t.get("step") == "triage" for t in self.trace)
        )

    @property
    def verified(self) -> bool:
        return (
            self.verdict == "migrated"
            and self.status == "match"
            and not self.should_hand_back
            and not self.over_masked
        )


def score(
    obj: MigrationObject,
    conv: Conversion,
    outcome: Outcome,
    seconds: float,
    order_ok: bool = True,
    masked: list[str] | None = None,
) -> ObjectScore:
    return ObjectScore(
        obj.id,
        obj.kind,
        obj.features,
        outcome.status,
        conv.verdict,
        outcome.evidence,
        conv.reason,
        conv.attempts,
        len(conv.calls),
        seconds,
        conv.cost_usd,
        conv.sql,
        conv.trace,
        obj.estate,
        obj.category,
        order_ok,
        list(masked or []),
        list(obj.mask_columns),
    )


def _pct(n: int, d: int) -> float | None:
    return round(100 * n / d, 1) if d else None


def _group(scores: list[ObjectScore]) -> dict[str, Any]:
    hb = [s for s in scores if s.verdict == "needs_human"]
    must = [s for s in scores if s.should_hand_back]
    return {
        "objects": len(scores),
        "verified_pct": _pct(sum(s.verified for s in scores), len(scores)),
        "wrong_claimed_migrated": sum(s.wrong_claim for s in scores),
        "needs_human_pct": _pct(len(hb), len(scores)),
        "hand_back_precision_pct": _pct(sum(s.correct_hand_back for s in hb), len(hb)),
        "hand_back_recall_pct": _pct(sum(s.correct_hand_back for s in must), len(must)),
        "usd": round(sum(s.cost_usd for s in scores), 4),
        "seconds": round(sum(s.seconds for s in scores), 1),
    }


def aggregate(scores: list[ObjectScore]) -> dict[str, Any]:
    n = len(scores)
    handed_back = [s for s in scores if s.verdict == "needs_human"]
    must = [s for s in scores if s.should_hand_back]
    correct = [s for s in scores if s.correct_hand_back]
    out: dict[str, Any] = {
        "objects": n,
        "executes_pct": _pct(sum(s.status != "error" for s in scores), n),
        "matches_pct": _pct(sum(s.status == "match" for s in scores), n),
        "verified_pct": _pct(sum(s.verified for s in scores), n),
        "verified_with_mask": sum(s.verified and bool(s.masked) for s in scores),
        "claimed_migrated_pct": _pct(sum(s.verdict == "migrated" for s in scores), n),
        "wrong_claimed_migrated": sum(s.wrong_claim for s in scores),
        "needs_human_pct": _pct(len(handed_back), n),
        "needs_human_but_fine": sum(s.wasted_hand_back for s in scores),
        "triage_false_positives": sum(s.triage_false_positive for s in scores),
        "hand_back_precision_pct": _pct(len(correct), len(handed_back)),
        "hand_back_recall_pct": _pct(len(correct), len(must)),
        "hand_back_reason_correct_pct": _pct(sum(s.reason_ok for s in correct), len(correct)),
        "must_hand_back": len(must),
        "order_violations": sum(not s.order_ok for s in scores),
        "calls_per_object": round(statistics.mean(s.calls for s in scores), 2) if n else None,
        "seconds_per_object": round(statistics.mean(s.seconds for s in scores), 1) if n else None,
        "usd_per_object": round(statistics.mean(s.cost_usd for s in scores), 4) if n else None,
        "confusion": {
            c: {d: sum(s.decision == d for s in scores if s.category == c) for d in DECISIONS}
            for c in CATEGORIES
            if any(s.category == c for s in scores)
        },
        "by_kind": {},
        "by_estate": {},
        "by_category": {},
    }
    for kind in ("query", "etl", "ddl"):
        g = [s for s in scores if s.kind == kind]
        if g:
            out["by_kind"][kind] = {
                **_group(g),
                "matches_pct": _pct(sum(s.status == "match" for s in g), len(g)),
            }
    for estate in ("A", "B", "C"):
        g = [s for s in scores if s.estate == estate]
        if g:
            out["by_estate"][estate] = _group(g)
    for category in CATEGORIES:
        g = [s for s in scores if s.category == category]
        if g:
            out["by_category"][category] = _group(g)
    return out
