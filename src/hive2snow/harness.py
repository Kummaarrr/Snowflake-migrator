"""Run a migration object on both sides and judge the conversion.

- Source truth: the object's Hive SQL run on Spark (cached as Parquet under data/truth/).
- Target: the converted SQL run on Snowflake, in a per-object clean state (its own tables and
  views are dropped first), then compared with the truth (diff.compare); a DDL table is judged
  by DESCRIBE TABLE against the columns the Hive DDL declares.
- Outcome per object: match / mismatch / error, with the evidence text the agent (and the
  report) can use.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path

import pandas as pd
import sqlglot

from hive2snow import diff
from hive2snow.config import DATA_DIR
from hive2snow.engines import RunResult, Snowflake, spark, split_statements
from hive2snow.hive_writes import top_level_columns
from hive2snow.objects import MigrationObject

TRUTH_DIR = DATA_DIR / "truth"
TYPE_CLASS = {  # Hive type -> class a Snowflake column must map to
    "STRING": "text",
    "VARCHAR": "text",
    "CHAR": "text",
    "TINYINT": "number",
    "SMALLINT": "number",
    "INT": "number",
    "INTEGER": "number",
    "BIGINT": "number",
    "DECIMAL": "number",
    "NUMERIC": "number",
    "FLOAT": "float",
    "DOUBLE": "float",
    "REAL": "float",
    "BOOLEAN": "boolean",
    "BINARY": "binary",
    "DATE": "date",
    "TIMESTAMP": "timestamp",
    "ARRAY": "array",
    "MAP": "object",
    "STRUCT": "object",
}
SNOW_CLASS = {
    "TEXT": "text",
    "VARCHAR": "text",
    "CHAR": "text",
    "STRING": "text",
    "NUMBER": "number",
    "DECIMAL": "number",
    "INT": "number",
    "INTEGER": "number",
    "BIGINT": "number",
    "SMALLINT": "number",
    "TINYINT": "number",
    "BYTEINT": "number",
    "FLOAT": "float",
    "FLOAT4": "float",
    "FLOAT8": "float",
    "DOUBLE": "float",
    "REAL": "float",
    "BOOLEAN": "boolean",
    "BINARY": "binary",
    "VARBINARY": "binary",
    "DATE": "date",
    "TIMESTAMP_NTZ": "timestamp",
    "TIMESTAMP_LTZ": "timestamp",
    "TIMESTAMP_TZ": "timestamp",
    "TIMESTAMP": "timestamp",
    "ARRAY": "array",
    "OBJECT": "object",
    "VARIANT": "variant",
}


@dataclass
class Outcome:
    status: str  # match | mismatch | error
    evidence: str  # error text or diff feedback
    target_seconds: float = 0.0


def expected_columns(hive_ddl: str) -> list[tuple[str, str]]:
    """(name, type class) for every column a Hive CREATE TABLE declares, partition columns
    included (they are ordinary columns in Snowflake)."""
    tree = sqlglot.parse_one(split_statements(hive_ddl)[-1], read="hive")
    cols: list[tuple[str, str]] = []
    for name, typ in top_level_columns(tree):
        base = re.sub(r"[(<].*", "", typ).upper()
        cols.append((name.lower(), TYPE_CLASS.get(base, "text")))
    return cols


def _column_matches(expected: str, actual: str) -> bool:
    if expected == actual:
        return True
    # acceptable mappings: complex types as VARIANT; float as NUMBER(.., >0) is not checked here
    return actual == "variant" and expected in ("array", "object")


def describe_target(sf: Snowflake, table: str) -> tuple[list[tuple[str, str]], str]:
    r = sf.run(f"DESCRIBE TABLE {table}")
    if not r.ok or r.frame is None:
        return [], r.error
    cols = []
    for _, row in r.frame.iterrows():
        base = re.sub(r"\(.*", "", str(row["type"])).upper()
        cols.append((str(row["name"]).lower(), SNOW_CLASS.get(base, base.lower())))
    return cols, ""


def truth_path(obj: MigrationObject) -> Path:
    return TRUTH_DIR / f"{obj.id}.parquet"


def source_truth(obj: MigrationObject, refresh: bool = False) -> RunResult:
    """The source result of the object (Spark), cached."""
    path = truth_path(obj)
    meta = path.with_suffix(".json")
    if path.exists() and meta.exists() and not refresh:
        info = json.loads(meta.read_text(encoding="utf-8"))
        return RunResult(True, pd.read_parquet(path), "", info["seconds"], info["statements"])
    s = spark()
    s.reset()
    sql = obj.hive_sql if not obj.check_sql else obj.hive_sql + ";\n" + obj.check_sql
    r = s.run(sql)
    s.reset()
    if r.ok and r.frame is not None:
        TRUTH_DIR.mkdir(parents=True, exist_ok=True)
        frame = r.frame.copy()
        for c in frame.columns:  # complex cells -> text so Parquet can hold them
            if frame[c].dtype == object and any(
                isinstance(v, list | dict | tuple) for v in frame[c]
            ):
                frame[c] = frame[c].map(
                    lambda v: json.dumps(v, default=str) if v is not None else None
                )
        frame.to_parquet(path, index=False)
        meta.write_text(
            json.dumps({"seconds": r.seconds, "statements": r.statements}), encoding="utf-8"
        )
    return r


def compare_sql(obj: MigrationObject) -> str:
    """The statement whose result is compared: the object's last SELECT (or the view check)."""
    if obj.check_sql:
        return obj.check_sql
    return split_statements(obj.hive_sql)[-1]


def ordered(obj: MigrationObject) -> bool:
    return diff.has_order_by(compare_sql(obj))


def clean_target(sf: Snowflake, obj: MigrationObject) -> None:
    for name in obj.creates:
        sf.run(f"DROP TABLE IF EXISTS {name}")
        sf.run(f"DROP VIEW IF EXISTS {name}")


def evaluate(sf: Snowflake, obj: MigrationObject, converted_sql: str) -> Outcome:
    """Run the converted SQL on Snowflake and judge it against the source truth."""
    clean_target(sf, obj)
    truth = source_truth(obj)
    if not truth.ok or truth.frame is None:
        return Outcome("error", f"source truth unavailable: {truth.error}")
    r = sf.run(converted_sql)
    if not r.ok:
        return Outcome("error", r.error, r.seconds)
    if obj.kind == "ddl" and obj.ddl_table:
        actual, err = describe_target(sf, obj.ddl_table)
        if err:
            return Outcome("error", f"table not created: {err}", r.seconds)
        expected = expected_columns(obj.hive_sql)
        problems = []
        if [c for c, _ in actual] != [c for c, _ in expected]:
            problems.append(
                f"columns differ: expected {[c for c, _ in expected]}, got {[c for c, _ in actual]}"
            )
        else:
            for (name, want), (_, got) in zip(expected, actual, strict=True):
                if not _column_matches(want, got):
                    problems.append(f"column {name}: expected a {want} type, got {got}")
        if problems:
            return Outcome("mismatch", "; ".join(problems), r.seconds)
        return Outcome("match", f"{len(actual)} columns as declared", r.seconds)
    target = r.frame
    if obj.kind in ("etl", "ddl") or r.statements > 1:
        # the comparison statement is the last SELECT; a converted script may have ended with
        # a DDL statement instead, so run the comparison statement explicitly
        check = sf.run(_target_compare_sql(obj, converted_sql))
        if not check.ok:
            return Outcome("error", check.error, r.seconds + check.seconds)
        target = check.frame
    assert target is not None
    d = diff.compare(truth.frame, target, ordered(obj))
    return Outcome("match" if d.match else "mismatch", d.feedback(), r.seconds)


def _target_compare_sql(obj: MigrationObject, converted_sql: str) -> str:
    """The last SELECT of the converted script (the agent may have reshaped it), else the
    object's check SQL transpiled."""
    parts = split_statements(converted_sql)
    for stmt in reversed(parts):
        if re.match(r"^\s*(SELECT|WITH)\b", stmt, re.IGNORECASE):
            return stmt
    return sqlglot.transpile(compare_sql(obj), read="hive", write="snowflake")[0]
