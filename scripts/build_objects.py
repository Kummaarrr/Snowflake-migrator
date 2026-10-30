"""Build and freeze the migration object set: data/objects.json (+ source truths).

    uv run python scripts/build_objects.py

TPC-DS queries: each of the 99 standard queries (DuckDB's text) is rewritten into legacy Hive
style (hiveify), run on Spark, and kept only if its result equals DuckDB's result for the
original (an independent engine proves the rewrite is faithful), returns rows and runs within
a time budget. The first MAX_TPCDS that qualify are taken. Hand-written legacy scripts and DDLs
are verified on Spark. The split is frozen (stratified by kind, seeded).
"""

from __future__ import annotations

import json
import random
import sys
from dataclasses import asdict

import duckdb
from tqdm import tqdm

from hive2snow import diff
from hive2snow.config import DATA_DIR
from hive2snow.engines import PARQUET_DIR, spark
from hive2snow.harness import source_truth
from hive2snow.hiveify import hiveify
from hive2snow.objects import DDL, LEGACY, MigrationObject

SEED = 20261030
MAX_TPCDS = 40
TIME_BUDGET_S = 40
TEST_SHARE = 0.45
OUT = DATA_DIR / "objects.json"


KEPT = DATA_DIR / "generated" / "tpcds_kept.json"


def tpcds_objects() -> list[MigrationObject]:
    if KEPT.exists():  # verified once; the rewrite is deterministic
        return [MigrationObject(**o) for o in json.loads(KEPT.read_text(encoding="utf-8"))]
    kept = _verify_tpcds()
    KEPT.write_text(json.dumps([asdict(o) for o in kept], indent=1), encoding="utf-8")
    return kept


def _verify_tpcds() -> list[MigrationObject]:
    con = duckdb.connect()
    con.execute("INSTALL tpcds; LOAD tpcds;")
    for path in PARQUET_DIR.glob("*.parquet"):
        con.execute(f"CREATE VIEW {path.stem} AS SELECT * FROM '{path.as_posix()}'")
    queries = con.execute(
        "SELECT query_nr, query FROM tpcds_queries() ORDER BY query_nr"
    ).fetchall()
    s = spark()
    kept: list[MigrationObject] = []
    skipped: dict[int, str] = {}
    for nr, original in tqdm(queries, desc="tpcds"):
        if len(kept) >= MAX_TPCDS:
            break
        try:
            hive_sql = hiveify(original, seed=SEED + nr)
        except Exception as e:
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
            )
        )
    print(f"TPC-DS: kept {len(kept)}, skipped {len(skipped)}")
    for nr, why in sorted(skipped.items()):
        print(f"  q{nr}: {why}")
    return kept


def _features(sql: str) -> set[str]:
    low = sql.lower()
    out = set()
    for key, needle in [
        ("nvl", "nvl("),
        ("substr", "substr("),
        ("date_add", "date_add("),
        ("date_sub", "date_sub("),
        ("backticks", "`"),
        ("set_hive", "set hive"),
        ("cast_string", "as string"),
        ("rollup", "rollup"),
        ("window", " over ("),
        ("cte", "with "),
        ("union", "union"),
    ]:
        if needle in low:
            out.add(key)
    return out


def main() -> int:
    objects = tpcds_objects() + LEGACY + DDL
    bad = []
    for obj in tqdm(objects, desc="truth"):
        r = source_truth(obj, refresh=True)
        if not r.ok or r.frame is None or (obj.kind != "ddl" and len(r.frame) == 0):
            bad.append((obj.id, r.error or "empty"))
    if bad:
        print("objects failing on the source:", bad)
        return 1
    rng = random.Random(SEED)
    for kind in ("query", "etl", "ddl"):
        group = [o for o in objects if o.kind == kind]
        rng.shuffle(group)
        n_test = round(len(group) * TEST_SHARE)
        for i, o in enumerate(group):
            o.split = "test" if i < n_test else "dev"
    OUT.write_text(json.dumps([asdict(o) for o in objects], indent=1), encoding="utf-8")
    kinds = {k: sum(o.kind == k for o in objects) for k in ("query", "etl", "ddl")}
    print(
        f"{len(objects)} objects {kinds} -> {OUT}; test {sum(o.split == 'test' for o in objects)}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
