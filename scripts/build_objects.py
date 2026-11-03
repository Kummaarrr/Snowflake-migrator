"""Build and freeze the migration object set: data/objects.json (+ source truths).

    uv run python scripts/build_objects.py            # all three estates
    uv run python scripts/build_objects.py --estates A B

Estate C: every TPC-DS query (DuckDB's text) rewritten into legacy Hive style (hiveify), run on
Spark and kept only if its result equals DuckDB's result for the original (an independent engine
proves the rewrite faithful) and it returns rows within a time budget. Verified once and cached.
Estate A (layered pipeline, scale 1) and B (legacy traps, with categories) are hand-written;
their dependencies are extracted from the SQL and their source truths computed with upstream
objects replayed. Unconvertible objects may fail on the source (that is their point); every
other object must run. The source is run twice for every object and the `source_stable` flag
records whether the two results agreed.

Split (frozen, seeded): estate A at the layer level so dependencies stay intact (staging views
all dev; aggregates and reports split as chains); B and C stratified by category / kind. A test
object may depend on a dev object (a report on a dev aggregate): run_eval provisions such
upstream objects on Snowflake from `data/reference_conversions.json` (verified dev conversions)
so every system is measured against the same, correct upstream.
"""

from __future__ import annotations

import argparse
import json
import random
import sys
from collections.abc import Callable
from dataclasses import asdict

import duckdb
from tqdm import tqdm

from hive2snow import deps, diff
from hive2snow.config import DATA_DIR
from hive2snow.engines import parquet_dir, spark
from hive2snow.estate_a import AGGREGATES, REPORTS, STAGING
from hive2snow.estate_b import ESTATE_B
from hive2snow.harness import register, source_is_stable, source_truth
from hive2snow.hiveify import hiveify
from hive2snow.objects import MigrationObject

SEED = 20261030
MAX_TPCDS = 99
TIME_BUDGET_S = 40
TEST_SHARE = 0.45
OUT = DATA_DIR / "objects.json"
KEPT = DATA_DIR / "generated" / "tpcds_kept.json"


def tpcds_objects() -> list[MigrationObject]:
    cached = (
        [MigrationObject(**o) for o in json.loads(KEPT.read_text(encoding="utf-8"))]
        if KEPT.exists()
        else []
    )
    done = {int(o.id[1:]) for o in cached}
    kept = cached + _verify_tpcds(skip=done)
    kept.sort(key=lambda o: o.id)
    KEPT.write_text(json.dumps([asdict(o) for o in kept], indent=1), encoding="utf-8")
    for o in kept:
        o.estate, o.dataset = "C", "sf01"
    return kept


def _verify_tpcds(skip: set[int] = frozenset()) -> list[MigrationObject]:
    con = duckdb.connect()
    con.execute("INSTALL tpcds; LOAD tpcds;")
    for path in parquet_dir("sf01").glob("*.parquet"):
        con.execute(f"CREATE VIEW {path.stem} AS SELECT * FROM '{path.as_posix()}'")
    queries = con.execute(
        "SELECT query_nr, query FROM tpcds_queries() ORDER BY query_nr"
    ).fetchall()
    s = spark()
    s.use("sf01")
    kept: list[MigrationObject] = []
    skipped: dict[int, str] = {}
    for nr, original in tqdm(queries, desc="tpcds"):
        if nr in skip or len(kept) + len(skip) >= MAX_TPCDS:
            continue
        try:
            hive_sql = hiveify(original, seed=SEED + nr)
        except Exception as e:  # a query sqlglot can't rewrite is skipped
            skipped[nr] = f"hiveify: {type(e).__name__}"
            continue
        try:
            ref = con.execute(original).df()
        except Exception as e:
            skipped[nr] = f"duckdb: {str(e)[:60]}"
            continue
        r = s.run(hive_sql)
        if not r.ok or r.frame is None:
            skipped[nr] = f"spark: {r.error[:80]}"
            continue
        if len(r.frame) == 0:
            skipped[nr] = "empty result"
            continue
        if r.seconds > TIME_BUDGET_S:
            skipped[nr] = f"slow ({r.seconds:.0f}s)"
            continue
        ref.columns = [c.lower() for c in ref.columns]
        d = diff.compare(ref, r.frame, diff.has_order_by(original))
        if not d.match:
            skipped[nr] = f"rewrite differs from DuckDB: {d.summary[:60]}"
            continue
        kept.append(
            MigrationObject(
                f"Q{nr:02d}",
                "query",
                f"TPC-DS query {nr} in legacy Hive style",
                hive_sql,
                features=["tpcds", *sorted(_features(hive_sql))],
                estate="C",
            )
        )
    if skipped:
        print(f"TPC-DS: kept {len(kept)} new, skipped {len(skipped)}")
        for nr, why in sorted(skipped.items()):
            print(f"  q{nr}: {why}")
    return kept


def _features(sql: str) -> set[str]:
    low = sql.lower()
    needles = [
        ("nvl", "nvl("), ("substr", "substr("), ("date_add", "date_add("),
        ("date_sub", "date_sub("), ("backticks", "`"), ("set_hive", "set hive"),
        ("cast_string", "as string"), ("rollup", "rollup"), ("window", " over ("),
        ("cte", "with "), ("union", "union"),
    ]  # fmt: skip
    return {key for key, needle in needles if needle in low}


def split_estate_a(rng: random.Random) -> None:
    """Staging views are dev (everything depends on them). Test aggregates are drawn at random;
    everything downstream of a test object is test too, so the dev set is upstream-closed (a
    dev run never needs a test object on Snowflake) and a test report's aggregate is test.
    Reports that read only staging views are drawn at random."""
    for o in STAGING:
        o.split = "dev"
    aggs = list(AGGREGATES)
    rng.shuffle(aggs)
    test_ids = {o.id for o in aggs[: round(len(aggs) * TEST_SHARE)]}
    agg_ids = {o.id for o in AGGREGATES}
    for o in deps.migration_order(STAGING + AGGREGATES + REPORTS):
        if o.id in test_ids or any(d in test_ids for d in o.depends_on):
            test_ids.add(o.id)
    for o in AGGREGATES + REPORTS:
        if o.id in test_ids:
            o.split = "test"
        elif not any(d in agg_ids for d in o.depends_on):  # reads only staging views
            o.split = "test" if rng.random() < TEST_SHARE else "dev"
        else:
            o.split = "dev"


def split_stratified(
    objects: list[MigrationObject], rng: random.Random, key: Callable[[MigrationObject], str]
) -> None:
    groups: dict[str, list[MigrationObject]] = {}
    for o in objects:
        groups.setdefault(key(o), []).append(o)
    for group in groups.values():
        rng.shuffle(group)
        n_test = round(len(group) * TEST_SHARE)
        for i, o in enumerate(group):
            o.split = "test" if i < n_test else "dev"


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--estates", nargs="*", default=["A", "B", "C"])
    ap.add_argument("--no-stability", action="store_true", help="skip the double run")
    ap.add_argument(
        "--split-only",
        action="store_true",
        help="rebuild records (categories, dependencies, split) from the code; keep the truths "
        "and stability flags already in data/objects.json",
    )
    args = ap.parse_args()
    objects: list[MigrationObject] = []
    if "A" in args.estates:
        objects += STAGING + AGGREGATES + REPORTS
    if "B" in args.estates:
        objects += ESTATE_B
    if "C" in args.estates:
        objects += tpcds_objects()
    deps.annotate(objects)
    register(objects)
    bad = []
    stability: dict[str, bool | None] = {}
    if args.split_only:
        previous = json.loads(OUT.read_text(encoding="utf-8"))
        stability = {o["id"]: o["source_stable"] for o in previous}
    else:
        for obj in tqdm(deps.migration_order(objects), desc="truth"):
            r = source_truth(obj, refresh=True)
            ok = r.ok and r.frame is not None and (obj.kind == "ddl" or len(r.frame) > 0)
            if not ok and obj.category not in ("unconvertible", "needs_config", "blocked"):
                bad.append((obj.id, r.error or "empty"))
            stability[obj.id] = source_is_stable(obj) if ok and not args.no_stability else None
    if bad:
        print("objects failing on the source:", bad)
        return 1
    rng = random.Random(SEED)
    split_estate_a(rng)
    split_stratified([o for o in objects if o.estate == "B"], rng, lambda o: o.category)
    split_stratified([o for o in objects if o.estate == "C"], rng, lambda o: o.kind)
    records = []
    for o in objects:
        d = asdict(o)
        d["source_stable"] = stability.get(o.id)
        records.append(d)
    OUT.write_text(json.dumps(records, indent=1), encoding="utf-8")
    by = {e: sum(o.estate == e for o in objects) for e in ("A", "B", "C")}
    cats = {
        c: sum(o.category == c for o in objects)
        for c in (
            "convertible",
            "maskable",
            "unconvertible",
            "needs_config",
            "nondeterministic",
            "blocked",
        )
    }
    unstable = sorted(i for i, v in stability.items() if v is False)
    n_test = sum(o.split == "test" for o in objects)
    print(f"{len(objects)} objects by estate {by}, categories {cats}; test {n_test}")
    print(f"source unstable on a double run: {unstable}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
