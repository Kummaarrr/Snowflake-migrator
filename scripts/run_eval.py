"""Run a system over a split (optionally one estate) and score it.

    uv run python scripts/run_eval.py --system v0 --split dev
    uv run python scripts/run_eval.py --system v0-llm --model haiku --split dev
    uv run python scripts/run_eval.py --system v1 --model haiku --split test --tag r1
    uv run python scripts/run_eval.py --system v2 --model haiku --split dev --estates A

Results: evals/results/<system>[_<model>]_<split>[_<estates>][_<tag>].json. Model responses are
cached, so an unchanged run is free; --tag gives an independent repeat (fresh cache namespace).
Snowflake runs are never cached (they are the measurement). Baselines get the objects in
dependency order too, so an upstream table exists when a downstream object runs.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from dataclasses import asdict, replace
from typing import Any

from tqdm import tqdm

from hive2snow import agent, deps, estate_agent, harness, llm, systems
from hive2snow.config import PROJECT_ROOT, Env
from hive2snow.engines import Snowflake
from hive2snow.harness import Outcome, evaluate
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
V2_VARIANTS = {
    "v2": estate_agent.EstateConfig(),
    "v2-notriage": estate_agent.EstateConfig(triage=False),
    "v2-nodoublerun": estate_agent.EstateConfig(double_run=False),
    "v2-noorder": estate_agent.EstateConfig(dependency_order=False, reverify_downstream=False),
    "v2-noreverify": estate_agent.EstateConfig(dependency_order=False, reverify_downstream=True),
}
RULE_BASED = ("v0", "v0-unquoted")
REFERENCE = PROJECT_ROOT / "data" / "reference_conversions.json"


def load_objects(
    split: str, ids: list[str] | None, estates: list[str] | None = None
) -> tuple[list[MigrationObject], dict[str, MigrationObject]]:
    raw = json.loads((PROJECT_ROOT / "data" / "objects.json").read_text(encoding="utf-8"))
    objs = [MigrationObject(**{k: v for k, v in o.items() if k != "source_stable"}) for o in raw]
    harness.register(objs)  # upstream replay on the source needs every object known
    chosen = [o for o in objs if (ids and o.id in ids) or (not ids and o.split == split)]
    if estates:
        chosen = [o for o in chosen if o.estate in estates]
    return chosen, {o.id: o for o in objs}


def clean_start(chosen: list[MigrationObject], sf: Snowflake) -> None:
    """Drop every table / view an object in this run creates, so a run never reads a table
    left behind by an earlier run (which would hide an ordering mistake or a wrong upstream)."""
    for obj in chosen:
        if obj.creates:
            sf.use(obj.dataset)
            harness.clean_target(sf, obj)


def provision_upstream(
    chosen: list[MigrationObject], every: dict[str, MigrationObject], sf: Snowflake
) -> list[str]:
    """Create, on Snowflake, the upstream objects a chosen object depends on but which are not
    in this run (a test report over a dev aggregate), from verified reference conversions - the
    same correct upstream for every system. Returns the ids provisioned."""
    in_run = {o.id for o in chosen}
    needed: list[str] = []

    def visit(o: MigrationObject) -> None:
        for d in o.depends_on:
            if d in in_run or d in needed or d not in every:
                continue
            visit(every[d])
            needed.append(d)

    for o in chosen:
        visit(o)
    if not needed:
        return []
    refs = json.loads(REFERENCE.read_text(encoding="utf-8")) if REFERENCE.exists() else {}
    for oid in needed:
        obj = every[oid]
        if oid not in refs:
            print(f"  no reference conversion for upstream {oid}: downstream objects may fail")
            continue
        sf.use(obj.dataset)
        harness.clean_target(sf, obj)
        r = sf.run(refs[oid])
        if not r.ok:
            print(f"  provisioning {oid} failed: {r.error[:120]}")
    return needed


def save_reference(scores: list[ObjectScore]) -> int:
    """Record verified conversions (right result, truly convertible) as reference upstream."""
    refs = json.loads(REFERENCE.read_text(encoding="utf-8")) if REFERENCE.exists() else {}
    added = 0
    for s in scores:
        if s.verified and s.sql.strip() and not s.masked:
            refs[s.id] = s.sql
            added += 1
    REFERENCE.write_text(json.dumps(dict(sorted(refs.items())), indent=1), encoding="utf-8")
    return added


def run_one(system: str, model: str, obj: MigrationObject, sf: Snowflake) -> ObjectScore:
    start = time.perf_counter()
    try:
        if system in RULE_BASED:
            conv = systems.v0_sqlglot(obj, unquoted=system == "v0-unquoted")
        elif system == "v0-llm":
            conv = systems.v0_llm(obj, model)
        else:
            conv = agent.migrate(obj, sf, replace(V1_VARIANTS[system], model=model))
    except BudgetExceeded:
        raise
    except Exception as e:  # a failed object is scored, not fatal
        conv = systems.Conversion("", "needs_human", f"system error: {type(e).__name__}: {e}")
    outcome = (
        evaluate(sf, obj, conv.sql) if conv.sql.strip() else Outcome("error", "no SQL produced")
    )
    return score(obj, conv, outcome, time.perf_counter() - start)


def run_estate(
    system: str, model: str, objects: list[MigrationObject], sf: Snowflake
) -> list[ObjectScore]:
    base = V2_VARIANTS[system]
    cfg = estate_agent.EstateConfig(
        inner=replace(base.inner, model=model),
        triage=base.triage,
        double_run=base.double_run,
        dependency_order=base.dependency_order,
        reverify_downstream=base.reverify_downstream,
    )
    start = time.perf_counter()
    migrated = estate_agent.migrate_estate(objects, sf, cfg, progress=True)
    per_object = (time.perf_counter() - start) / max(1, len(objects))
    scores = []
    for m in migrated:
        outcome = m.outcome or Outcome("error", "not run (handed back before conversion)")
        scores.append(score(m.obj, m.conversion, outcome, per_object, m.order_ok, m.masked))
    return scores


def report(agg: dict[str, Any], label: str) -> str:
    rows = [
        ("Executes on Snowflake", f"{agg['executes_pct']}%"),
        ("Same result as the source", f"{agg['matches_pct']}%"),
        (
            "**Verified** (migrated, right, verifiable)",
            f"{agg['verified_pct']}% ({agg['verified_with_mask']} with clock columns masked)",
        ),
        ("Claimed migrated", f"{agg['claimed_migrated_pct']}%"),
        ("**Wrong but claimed migrated**", str(agg["wrong_claimed_migrated"])),
        (
            "Handed to a human",
            f"{agg['needs_human_pct']}% ({agg['needs_human_but_fine']} were fine; "
            f"{agg['triage_false_positives']} refused at triage wrongly)",
        ),
        (
            "Hand-back precision / recall",
            f"{agg['hand_back_precision_pct']}% / {agg['hand_back_recall_pct']}% (must hand back: {agg['must_hand_back']})",
        ),
        ("Hand-back reason correct", f"{agg['hand_back_reason_correct_pct']}%"),
        ("Dependency-order violations", str(agg["order_violations"])),
        ("Model calls / object", str(agg["calls_per_object"])),
        ("Seconds / object", str(agg["seconds_per_object"])),
        ("$ / object", str(agg["usd_per_object"])),
    ]
    lines = [f"## {label}", "", "| Metric | Value |", "|---|---|"]
    lines += [f"| {k} | {v} |" for k, v in rows]
    lines += [
        "",
        "By estate: "
        + ", ".join(
            f"{k}: {v['verified_pct']}% verified of {v['objects']}, {v['wrong_claimed_migrated']} wrong claims, "
            f"{v['needs_human_pct']}% handed back (precision {v['hand_back_precision_pct']}%), ${v['usd']}"
            for k, v in agg.get("by_estate", {}).items()
        ),
    ]
    lines += [
        "",
        "By kind: "
        + ", ".join(
            f"{k} {v['verified_pct']}% verified of {v['objects']} ({v['wrong_claimed_migrated']} wrong claims)"
            for k, v in agg["by_kind"].items()
        ),
    ]
    conf = agg.get("confusion", {})
    if conf:
        decisions = [d for d in next(iter(conf.values())) if any(c[d] for c in conf.values())]
        lines += ["", "Truth x decision: | " + " | ".join(decisions) + " |"]
        for cat, row in conf.items():
            lines.append(
                f"  {cat:16} | " + " | ".join(f"{row[d]:^{len(d)}}" for d in decisions) + " |"
            )
    return "\n".join(lines)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--system", default="v0", choices=[*RULE_BASED, "v0-llm", *V1_VARIANTS, *V2_VARIANTS]
    )
    ap.add_argument("--model", default="haiku", choices=MODELS)
    ap.add_argument("--split", default="dev", choices=["dev", "test"])
    ap.add_argument("--estates", nargs="*", help="restrict to estates A / B / C")
    ap.add_argument("--ids", nargs="*")
    ap.add_argument("--tag", default="")
    ap.add_argument("--max-cost", type=float, default=Env().max_run_cost_usd)
    ap.add_argument(
        "--save-reference",
        action="store_true",
        help="record this run's verified conversions as reference upstream for other runs",
    )
    ap.add_argument("--no-provision", action="store_true", help="skip upstream provisioning")
    args = ap.parse_args()

    objects, every = load_objects(args.split, args.ids, args.estates)
    llm.set_cache_namespace(args.tag)
    LEDGER.cap_usd = args.max_cost
    sf = Snowflake()
    model = MODELS[args.model]
    clean_start(objects, sf)
    if not args.no_provision:
        provisioned = provision_upstream(objects, every, sf)
        if provisioned:
            print(f"provisioned upstream from reference conversions: {', '.join(provisioned)}")
    if args.system in V2_VARIANTS:
        scores = run_estate(args.system, model, objects, sf)
    else:
        ordered = deps.migration_order(objects)
        scores = [run_one(args.system, model, o, sf) for o in tqdm(ordered, desc=args.system)]
    agg = aggregate(scores)
    rule_based = args.system in RULE_BASED
    name = args.system + ("" if rule_based else f"_{args.model}") + f"_{args.split}"
    name += "_" + "".join(args.estates) if args.estates else ""
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
        + f" / {args.split}{' ' + ''.join(args.estates) if args.estates else ''} ({len(objects)} objects)"
    )
    print(report(agg, label))
    print(f"\n{LEDGER.summary()}  ->  {out.relative_to(PROJECT_ROOT).as_posix()}")
    if args.save_reference:
        print(f"reference conversions saved: {save_reference(scores)}")
    for s in scores:
        miss = s.verdict == "needs_human" and not s.should_hand_back
        if s.wrong_claim or miss or (s.correct_hand_back and not s.reason_ok):
            tag = (
                "WRONG-CLAIM"
                if s.wrong_claim
                else "WASTED-HANDBACK"
                if s.wasted_hand_back
                else "TRIAGE-FP"
                if s.triage_false_positive
                else "wrong-reason"
                if s.correct_hand_back
                else "handed-back"
            )
            text = (s.reason if s.verdict == "needs_human" else s.evidence)[:110]
            print(f"  {s.id:5} {s.estate} {s.category[:6]:6} {s.status:8} {tag:15} {text}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
