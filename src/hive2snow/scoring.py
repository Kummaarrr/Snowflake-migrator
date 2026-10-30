"""Score conversions the way the client reads them.

Per object: did the converted SQL run on Snowflake, did it give the source's answer, and what
did the system claim? Aggregates:
- executes %: ran on Snowflake without error;
- **matches %**: target result equals the source result (the headline);
- claimed migrated %: the system's own verdict;
- **wrong but claimed migrated**: the safety metric (target 0) - a client who trusts the
  verdict would ship a wrong query;
- needs-human %: objects handed back, and how many of those were actually fine (wasted review);
- model calls, seconds and $ per object.
"""

from __future__ import annotations

import statistics
from dataclasses import dataclass, field
from typing import Any

from hive2snow.harness import Outcome
from hive2snow.objects import MigrationObject
from hive2snow.systems import Conversion


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

    @property
    def wrong_claim(self) -> bool:
        return self.verdict == "migrated" and self.status != "match"


def score(obj: MigrationObject, conv: Conversion, outcome: Outcome, seconds: float) -> ObjectScore:
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
    )


def _pct(n: int, d: int) -> float | None:
    return round(100 * n / d, 1) if d else None


def aggregate(scores: list[ObjectScore]) -> dict[str, Any]:
    n = len(scores)
    handed_back = [s for s in scores if s.verdict == "needs_human"]
    out: dict[str, Any] = {
        "objects": n,
        "executes_pct": _pct(sum(s.status != "error" for s in scores), n),
        "matches_pct": _pct(sum(s.status == "match" for s in scores), n),
        "claimed_migrated_pct": _pct(sum(s.verdict == "migrated" for s in scores), n),
        "wrong_claimed_migrated": sum(s.wrong_claim for s in scores),
        "needs_human_pct": _pct(len(handed_back), n),
        "needs_human_but_fine": sum(s.status == "match" for s in handed_back),
        "calls_per_object": round(statistics.mean(s.calls for s in scores), 2) if n else None,
        "seconds_per_object": round(statistics.mean(s.seconds for s in scores), 1) if n else None,
        "usd_per_object": round(statistics.mean(s.cost_usd for s in scores), 4) if n else None,
        "by_kind": {},
    }
    for kind in ("query", "etl", "ddl"):
        g = [s for s in scores if s.kind == kind]
        if g:
            out["by_kind"][kind] = {
                "objects": len(g),
                "matches_pct": _pct(sum(s.status == "match" for s in g), len(g)),
                "wrong_claimed_migrated": sum(s.wrong_claim for s in g),
            }
    return out
