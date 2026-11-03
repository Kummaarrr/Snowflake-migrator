"""Generate TPC-DS data and load it into both engines.

    uv run python scripts/build_data.py            # DuckDB dsdgen -> Parquet -> Snowflake
    uv run python scripts/build_data.py --sf 0.1   # scale factor (default 0.1, ~1M rows)

Spark reads the Parquet files directly (views registered at session start). Snowflake gets the
same files through an internal stage and COPY INTO, one table per file. Idempotent: existing
Snowflake tables with the right row count are kept.
"""

from __future__ import annotations

import argparse
import sys

import duckdb
from tqdm import tqdm

from hive2snow.engines import Snowflake, parquet_dir, snowflake_schema

# DuckDB -> Snowflake type names
TYPES = {
    "INTEGER": "INTEGER",
    "BIGINT": "BIGINT",
    "VARCHAR": "VARCHAR",
    "DATE": "DATE",
    "DOUBLE": "DOUBLE",
    "TIME": "TIME",
    "BOOLEAN": "BOOLEAN",
}


def generate(sf: float, dataset: str) -> dict[str, int]:
    PARQUET_DIR = parquet_dir(dataset)
    PARQUET_DIR.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    con.execute("INSTALL tpcds; LOAD tpcds;")
    con.execute(f"CALL dsdgen(sf={sf})")
    counts = {}
    for (table,) in con.execute("SHOW TABLES").fetchall():
        path = PARQUET_DIR / f"{table}.parquet"
        con.execute(f"COPY (SELECT * FROM {table}) TO '{path.as_posix()}' (FORMAT parquet)")
        counts[table] = con.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
    return counts


def snowflake_ddl(table: str, dataset: str) -> str:
    con = duckdb.connect()
    path = (parquet_dir(dataset) / f"{table}.parquet").as_posix()
    schema = con.execute(f"DESCRIBE SELECT * FROM '{path}'").fetchall()
    defs = []
    for name, typ, *_ in schema:
        t = typ.upper()
        sf_type = t if t.startswith("DECIMAL") else TYPES.get(t, "VARCHAR")
        defs.append(f'"{name.upper()}" {sf_type}')
    columns = ",\n  ".join(defs)
    return f"CREATE OR REPLACE TABLE {table.upper()} (\n  {columns}\n)"


def load_snowflake(counts: dict[str, int], dataset: str) -> None:
    sf = Snowflake(snowflake_schema(dataset))
    sf.execute("CREATE STAGE IF NOT EXISTS tpcds_stage FILE_FORMAT = (TYPE = PARQUET)")
    shown = sf.run("SHOW TABLES")
    existing = (
        set(shown.frame["name"].str.lower())
        if shown.ok and shown.frame is not None and len(shown.frame)
        else set()
    )
    for table, n in tqdm(counts.items(), desc="snowflake"):
        if table in existing:
            got = sf.run(f"SELECT count(*) FROM {table}").frame
            if got is not None and int(got.iloc[0, 0]) == n:
                continue
        path = (parquet_dir(dataset) / f"{table}.parquet").as_posix()
        sf.execute(snowflake_ddl(table, dataset))
        if n > 500_000:  # big tables: upload in parallel chunks (one PUT of a huge file times out)
            chunks = parquet_dir(dataset) / "_upload" / table
            chunks.mkdir(parents=True, exist_ok=True)
            duckdb.connect().execute(
                f"COPY (SELECT * FROM '{path}') TO '{chunks.as_posix()}' "
                "(FORMAT parquet, FILE_SIZE_BYTES 50000000, OVERWRITE_OR_IGNORE true)"
            )
            path = (chunks / "*.parquet").as_posix()
        sf.execute(
            f"PUT 'file://{path}' @tpcds_stage/{dataset}/{table}/ "
            "AUTO_COMPRESS=FALSE OVERWRITE=TRUE PARALLEL=8"
        )
        sf.execute(
            f"COPY INTO {table.upper()} FROM @tpcds_stage/{dataset}/{table}/ "
            "FILE_FORMAT = (TYPE = PARQUET) MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE"
        )
        got = sf.run(f"SELECT count(*) FROM {table}").frame
        assert got is not None and int(got.iloc[0, 0]) == n, (table, n, got)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--sf", type=float, default=0.1)
    ap.add_argument("--dataset", default="sf01", choices=["sf01", "sf1"])
    ap.add_argument("--skip-snowflake", action="store_true")
    ap.add_argument("--load-only", action="store_true", help="reuse the Parquet files on disk")
    args = ap.parse_args()
    if args.load_only:
        con = duckdb.connect()
        counts = {
            f.stem: con.execute(f"SELECT count(*) FROM '{f.as_posix()}'").fetchone()[0]
            for f in sorted(parquet_dir(args.dataset).glob("*.parquet"))
        }
    else:
        counts = generate(args.sf, args.dataset)
    print(f"{len(counts)} tables, {sum(counts.values()):,} rows -> {parquet_dir(args.dataset)}")
    if not args.skip_snowflake:
        load_snowflake(counts, args.dataset)
        print("Snowflake loaded")
    return 0


if __name__ == "__main__":
    sys.exit(main())
