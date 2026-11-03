"""Re-measure objects of a saved run whose evaluation failed for infrastructure reasons.

    uv run python scripts/reevaluate.py evals/results/v2_haiku_test_r1.json A_T01 [B12 ...]

The conversion (SQL, verdict, calls, cost) is left exactly as the system produced it; only the
harness's evaluation of that SQL is run again and the object's status / evidence and the run's
aggregate are rewritten. Use it when the evidence shows a transport error (a Snowflake result
chunk that failed to download, a dead Spark gateway), never for a SQL error or a data mismatch -
those are the measurement. Every re-evaluation is recorded in the object's trace.
"""

from __future__ import annotations

import datetime as dt
import json
import sys

from hive2snow.config import PROJECT_ROOT
from hive2snow.engines import Snowflake
from hive2snow.harness import evaluate, register
from hive2snow.objects import MigrationObject
from hive2snow.scoring import ObjectScore, aggregate


def main(result_file: str, ids: list[str]) -> int:
    path = PROJECT_ROOT / result_file
    data = json.loads(path.read_text(encoding="utf-8"))
    raw = json.loads((PROJECT_ROOT / "data" / "objects.json").read_text(encoding="utf-8"))
    objects = {
        o["id"]: MigrationObject(**{k: v for k, v in o.items() if k != "source_stable"})
        for o in raw
    }
    register(list(objects.values()))
    sf = Snowflake()
    for rec in data["objects"]:
        if rec["id"] not in ids or not rec["sql"].strip():
            continue
        before = (rec["status"], rec["evidence"][:120])
        out = evaluate(sf, objects[rec["id"]], rec["sql"], rec.get("masked") or None)
        rec["status"], rec["evidence"] = out.status, out.evidence
        rec.setdefault("trace", []).append(
            {
                "step": "reevaluate",
                "when": dt.datetime.now(dt.UTC).isoformat(timespec="seconds"),
                "previous": before,
                "status": out.status,
            }
        )
        print(f"{rec['id']}: {before[0]} -> {out.status}  {out.evidence[:120]}")
    scores = [ObjectScore(**{k: v for k, v in rec.items()}) for rec in data["objects"]]
    data["aggregate"] = aggregate(scores)
    path.write_text(json.dumps(data, indent=1), encoding="utf-8")
    a = data["aggregate"]
    print(
        f"aggregate: verified {a['verified_pct']}%, wrong claims {a['wrong_claimed_migrated']}, "
        f"precision {a['hand_back_precision_pct']}% / recall {a['hand_back_recall_pct']}%"
    )
    return 0


if __name__ == "__main__":
    if len(sys.argv) < 3:
        sys.exit(__doc__)
    sys.exit(main(sys.argv[1], sys.argv[2:]))
