"""Run a system over a split and score it.

    uv run python scripts/run_eval.py --system v0 --split dev
    uv run python scripts/run_eval.py --system v0-llm --model haiku --split dev
    uv run python scripts/run_eval.py --system v1 --model haiku --split test --tag r1

Results: evals/results/<system>[_<model>]_<split>[_<tag>].json. Model responses are cached, so an
unchanged run is free; --tag gives an independent repeat (fresh cache namespace). Snowflake runs
are never cached (they are the measurement).
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from dataclasses import asdict, replace
from typing import Any

from tqdm import tqdm

from hive2snow import agent, llm, systems
from hive2snow.config import PROJECT_ROOT, Env
from hive2snow.engines import Snowflake
from hive2snow.harness import evaluate
from hive2snow.llm import LEDGER, BudgetExceeded
from hive2snow.objects import MigrationObject
from hive2snow.scoring import ObjectScore, aggregate, score

MODELS = {"haiku": "claude-haiku-5-5", "sonnet": "claude-sonnet-5-5", "opus": "claude-opus-5-5"}
V1_VARIANTS = {
    "v1": agent.Config(),
    "v1-nodraft": agent.Config(use_sqlglot_draft=False),
    "v1-nofeedback": agent.Config(feedback=False),
    "v1-noescalate": agent.Config(escalate_model=None),
    "v1-nodiff": agent.Config(diff_rounds=0),
}


def load_objects(split: str, ids: list[str] | None) -> list[MigrationObject]:
    raw = json.loads((PROJECT_ROOT / "data" / "objects.json").read_text(encoding="utf-8"))
    objs = [MigrationObject(**o) for o in raw]
    return [o for o in objs if (ids and o.id in ids) or (not ids and o.split == split)]


def run_one(system: str, model: str, obj: MigrationObject, sf: Snowflake) -> ObjectScore:
    start = time.perf_counter()
    try:
        if system in ("v0", "v0-unquoted"):
            conv = systems.v0_sqlglot(obj, unquoted=system == "v0-unquoted")
        elif system == "v0-llm":
            conv = systems.v0_llm(obj, model)
        else:
            conv = agent.migrate(obj, sf, replace(V1_VARIANTS[system], model=model))
    except BudgetExceeded:
        raise
    except Exception as e:
        conv = systems.Conversion("", "needs_human", f"system error: {type(e).__name__}: {e}")
    outcome = evaluate(sf, obj, conv.sql) if conv.sql.strip() else None
    if outcome is None:
        from hive2snow.harness import Outcome

        outcome = Outcome("error", "no SQL produced")
    return score(obj, conv, outcome, time.perf_counter() - start)


def report(agg: dict[str, Any], label: str) -> str:
    rows = [
        ("Executes on Snowflake", f"{agg['executes_pct']}%"),
        ("**Same result as the source**", f"{agg['matches_pct']}%"),
        ("Claimed migrated", f"{agg['claimed_migrated_pct']}%"),
        ("**Wrong but claimed migrated**", str(agg["wrong_claimed_migrated"])),
        (
            "Handed to a human",
            f"{agg['needs_human_pct']}% ({agg['needs_human_but_fine']} were fine)",
        ),
        ("Model calls / object", str(agg["calls_per_object"])),
        ("Seconds / object", str(agg["seconds_per_object"])),
        ("$ / object", str(agg["usd_per_object"])),
    ]
    lines = [f"## {label}", "", "| Metric | Value |", "|---|---|"]
    lines += [f"| {k} | {v} |" for k, v in rows]
    lines += [
        "",
        "By kind: "
        + ", ".join(
            f"{k} {v['matches_pct']}% match of {v['objects']} ({v['wrong_claimed_migrated']} wrong claims)"
            for k, v in agg["by_kind"].items()
        ),
    ]
    return "\n".join(lines)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--system", default="v0", choices=["v0", "v0-unquoted", "v0-llm", *V1_VARIANTS])
    ap.add_argument("--model", default="haiku", choices=MODELS)
    ap.add_argument("--split", default="dev", choices=["dev", "test"])
    ap.add_argument("--ids", nargs="*")
    ap.add_argument("--tag", default="")
    ap.add_argument("--max-cost", type=float, default=Env().max_run_cost_usd)
    args = ap.parse_args()

    objects = load_objects(args.split, args.ids)
    llm.set_cache_namespace(args.tag)
    LEDGER.cap_usd = args.max_cost
    sf = Snowflake()
    model = MODELS[args.model]
    scores = [run_one(args.system, model, o, sf) for o in tqdm(objects, desc=args.system)]
    agg = aggregate(scores)
    rule_based = args.system in ("v0", "v0-unquoted")
    name = args.system + ("" if rule_based else f"_{args.model}") + f"_{args.split}"
    name += f"_{args.tag}" if args.tag else ""
    out = PROJECT_ROOT / "evals" / "results" / f"{name}.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(
        json.dumps({"aggregate": agg, "objects": [asdict(s) for s in scores]}, indent=1),
        encoding="utf-8",
    )
    label = (
        f"{args.system}"
        + ("" if rule_based else f" / {model}")
        + f" / {args.split} ({len(objects)} objects)"
    )
    print(report(agg, label))
    print(f"\n{LEDGER.summary()}  ->  {out.relative_to(PROJECT_ROOT).as_posix()}")
    for s in scores:
        if s.status != "match":
            print(f"  {s.id:4} {s.kind:5} {s.status:8} claimed={s.verdict:11} {s.evidence[:110]}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
