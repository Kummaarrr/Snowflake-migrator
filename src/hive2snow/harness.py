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
_REGISTRY: dict[str, MigrationObject] = {}


def register(objects: list[MigrationObject]) -> None:
    """Make objects known so an object's upstream dependencies can be replayed on the source
    before it runs (estate A's aggregates read the staging views, reports read the aggregates)."""
    _REGISTRY.update({o.id: o for o in objects})


def upstream_sql(obj: MigrationObject) -> list[str]:
    """Hive SQL of every transitive dependency, in dependency order (final SELECTs dropped)."""
    seen: list[str] = []

    def visit(o: MigrationObject) -> None:
        for dep in o.depends_on:
            d = _REGISTRY.get(dep)
            if d is not None and d.id not in seen:
                visit(d)
                seen.append(d.id)

    visit(obj)
    out = []
    for i in seen:
        statements = split_statements(_REGISTRY[i].hive_sql)
        if _REGISTRY[i].kind == "etl" and re.match(r"^\s*SELECT\b", statements[-1], re.IGNORECASE):
            statements = statements[:-1]
        out += statements
    return out


SAMPLE_ROWS = 500
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
    s.use(obj.dataset)
    s.reset()
    for stmt in upstream_sql(obj):
        pre = s.run(stmt)
        if not pre.ok:
            s.reset()
            return RunResult(False, None, f"upstream {stmt[:40]!r} failed: {pre.error}", 0.0)
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


def source_is_stable(obj: MigrationObject, mask: list[str] | None = None) -> bool | None:
    """Run the object on the source a second time (with a different shuffle layout, so ties
    and partition-dependent order can show) and compare with the cached truth: a difference
    proves the result is not deterministic (None when the object doesn't run). `mask` columns
    are ignored (clock columns of a maskable object)."""
    truth = source_truth(obj)
    if not truth.ok or truth.frame is None:
        return None
    s = spark()
    s.use(obj.dataset)
    s.reset()
    s.session.conf.set("spark.sql.shuffle.partitions", "3")
    try:
        for stmt in upstream_sql(obj):
            if not s.run(stmt).ok:  # an upstream hiccup is not evidence about the object
                return None
        sql = obj.hive_sql if not obj.check_sql else obj.hive_sql + ";\n" + obj.check_sql
        again = s.run(sql)
    finally:
        s.session.conf.set("spark.sql.shuffle.partitions", "8")
        s.reset()
    if not again.ok or again.frame is None:
        return None
    a, b = _apply_mask(truth.frame, again.frame, mask if mask is not None else obj.mask_columns)
    return diff.compare(a, b, ordered(obj), order_positions(obj)).match


def run_on_source(obj: MigrationObject, sql: str) -> RunResult:
    """Run a probe statement on the source with the object's upstream objects and its own
    preceding statements replayed (used by the agent's tie and stability checks)."""
    s = spark()
    s.use(obj.dataset)
    s.reset()
    try:
        for stmt in upstream_sql(obj) + split_statements(obj.hive_sql)[:-1]:
            pre = s.run(stmt)
            if not pre.ok:
                return RunResult(False, None, f"upstream failed: {pre.error}", 0.0)
        return s.run(sql)
    finally:
        s.reset()


def _apply_mask(
    source: pd.DataFrame, target: pd.DataFrame, mask: list[str]
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Drop masked columns from both frames: by name on the source, by the same positions on
    the target (engines label expressions differently)."""
    if not mask:
        return source, target
    wanted = {m.lower() for m in mask}
    keep = [i for i, c in enumerate(source.columns) if str(c).lower() not in wanted]
    src = source.iloc[:, keep]
    tgt = target.iloc[:, keep] if target.shape[1] == source.shape[1] else target
    return src, tgt


def order_positions(obj: MigrationObject) -> list[int] | None:
    """Output positions of the comparison statement's top-level ORDER BY keys, when every key
    is an output column (by name, alias or ordinal); None otherwise (strict ordered compare)."""
    if not ordered(obj):
        return None
    try:
        tree = sqlglot.parse_one(compare_sql(obj), read="hive")
    except Exception:
        return None
    select = tree if isinstance(tree, sqlglot.exp.Select) else tree.find(sqlglot.exp.Select)
    order = (
        tree.args.get("order") if isinstance(tree, sqlglot.exp.Select | sqlglot.exp.Union) else None
    )
    if select is None or order is None:
        return None
    names = []
    for e in select.expressions:
        if isinstance(e, sqlglot.exp.Star):
            return None
        names.append((e.alias_or_name or "").lower())
    texts = [e.unalias().sql(dialect="hive").lower() for e in select.expressions]
    positions: list[int] = []
    for item in order.expressions:
        key = item.this
        if isinstance(key, sqlglot.exp.Literal) and key.is_int:
            positions.append(int(key.this) - 1)
            continue
        text = key.sql(dialect="hive").lower()
        name = key.name.lower() if isinstance(key, sqlglot.exp.Column) else ""
        pos = next((i for i, n in enumerate(names) if n and n == name), None)
        if pos is None:
            pos = next((i for i, t in enumerate(texts) if t == text), None)
        if pos is None:
            return None
        positions.append(pos)
    return positions


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


def evaluate(
    sf: Snowflake, obj: MigrationObject, converted_sql: str, mask: list[str] | None = None
) -> Outcome:
    """Run the converted SQL on Snowflake and judge it against the source truth. `mask`: output
    columns left out of the comparison (the agent's choice; defaults to the ground truth's
    clock columns for maskable objects)."""
    mask = obj.mask_columns if mask is None else mask
    sf.use(obj.dataset)
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
    if obj.large:
        return _validate_in_warehouse(sf, obj, converted_sql, r.seconds)
    if truth.frame.shape[1] != target.shape[1]:
        d = diff.compare(truth.frame, target, ordered(obj))  # the column-count message
    else:
        src, tgt = _apply_mask(truth.frame, target, mask)
        pos = order_positions(obj)
        if mask and pos is not None:
            kept = [
                i
                for i, c in enumerate(truth.frame.columns)
                if str(c).lower() not in {m.lower() for m in mask}
            ]
            pos = [kept.index(p) for p in pos if p in kept] or None
        d = diff.compare(src, tgt, ordered(obj), pos)
    note = f" (columns masked: {', '.join(mask)})" if mask else ""
    return Outcome("match" if d.match else "mismatch", d.feedback() + note, r.seconds)


# ---------------------------------------------------------------- in-warehouse validation
AGG_TEMPLATE = "SELECT count(*) AS n, {cols} FROM ({sql}) v"


def _agg_exprs(columns: list[str], types: dict[str, str], dialect: str) -> str:
    """Engine-independent per-column aggregates: nulls, distinct count, and sum / min / max
    (numbers) or total length / min / max (text) - the same SQL shape on both sides. On
    Snowflake the columns are addressed by position ($1, $2, ...): the converted SQL may name
    or case them differently."""
    parts = []
    for i, c in enumerate(columns, start=1):
        q = f"${i}" if dialect == "snowflake" else f"`{c}`"
        t = types.get(c, "text")
        parts.append(f"sum(CASE WHEN {q} IS NULL THEN 1 ELSE 0 END) AS {c}__nulls")
        parts.append(f"count(DISTINCT {q}) AS {c}__distinct")
        if t == "number":
            parts.append(f"round(sum(CAST({q} AS DOUBLE)), 2) AS {c}__sum")
            parts.append(f"min({q}) AS {c}__min")
            parts.append(f"max({q}) AS {c}__max")
        elif t in ("date", "timestamp"):
            parts.append(f"min({q}) AS {c}__min")
            parts.append(f"max({q}) AS {c}__max")
        else:
            parts.append(
                f"sum(length(CAST({q} AS {'VARCHAR' if dialect == 'snowflake' else 'STRING'}))) AS {c}__len"
            )
            parts.append(f"min({q}) AS {c}__min")
            parts.append(f"max({q}) AS {c}__max")
    return ", ".join(parts)


def _column_types(frame: pd.DataFrame) -> dict[str, str]:
    out = {}
    for c in frame.columns:
        kind = frame[c].dtype.kind
        if kind in "iufb":
            out[c] = "number"
        elif kind == "M":
            out[c] = "timestamp"
        else:
            sample = next((v for v in frame[c] if v is not None), None)
            out[c] = "date" if hasattr(sample, "year") and not hasattr(sample, "hour") else "text"
    return out


def _validate_in_warehouse(
    sf: Snowflake, obj: MigrationObject, converted_sql: str, seconds: float
) -> Outcome:
    """Large results: compare aggregates computed inside each engine plus a deterministic
    sample (first SAMPLE_ROWS rows by every column) cell by cell."""
    truth = source_truth(obj)
    assert truth.frame is not None
    columns = list(truth.frame.columns)
    types = _column_types(truth.frame)
    src_agg, src_sample = _source_side(obj, columns, types)
    if not src_agg.ok or src_agg.frame is None:
        return Outcome("error", f"source aggregates failed: {src_agg.error}", seconds)
    tgt_sql = _target_compare_sql(obj, converted_sql)
    tgt_agg = sf.run(AGG_TEMPLATE.format(cols=_agg_exprs(columns, types, "snowflake"), sql=tgt_sql))
    if not tgt_agg.ok or tgt_agg.frame is None:
        return Outcome(
            "error", f"target aggregates failed: {tgt_agg.error}", seconds + tgt_agg.seconds
        )
    d = diff.compare(src_agg.frame, tgt_agg.frame, ordered=True)
    if not d.match:
        return Outcome(
            "mismatch", "in-warehouse aggregates differ: " + d.feedback(), seconds + tgt_agg.seconds
        )
    order_t = ", ".join(f"{i} NULLS FIRST" for i in range(1, len(columns) + 1))
    tgt_sample = sf.run(f"SELECT * FROM ({tgt_sql}) v ORDER BY {order_t} LIMIT {SAMPLE_ROWS}")
    if (
        not src_sample.ok
        or not tgt_sample.ok
        or src_sample.frame is None
        or tgt_sample.frame is None
    ):
        return Outcome("error", f"sample failed: {src_sample.error or tgt_sample.error}", seconds)
    d2 = diff.compare(src_sample.frame, tgt_sample.frame, ordered=True)
    if not d2.match:
        return Outcome(
            "mismatch", "aggregates match but the ordered sample differs: " + d2.feedback(), seconds
        )
    n = int(str(src_agg.frame.iloc[0, 0]))
    return Outcome(
        "match",
        f"in-warehouse: {n:,} rows, {len(columns)} columns' aggregates match; "
        f"{len(src_sample.frame)}-row ordered sample matches",
        seconds + tgt_agg.seconds + tgt_sample.seconds,
    )


def _source_side(
    obj: MigrationObject, columns: list[str], types: dict[str, str]
) -> tuple[RunResult, RunResult]:
    """The source's in-warehouse aggregates and ordered sample, computed once per object and
    cached next to the truth (they are fixed by the source; the target side is what changes
    between attempts and systems)."""
    agg_path = TRUTH_DIR / f"{obj.id}.agg.parquet"
    sample_path = TRUTH_DIR / f"{obj.id}.sample.parquet"
    if agg_path.exists() and sample_path.exists():
        return (
            RunResult(True, pd.read_parquet(agg_path), "", 0.0),
            RunResult(True, pd.read_parquet(sample_path), "", 0.0),
        )
    s = spark()
    s.use(obj.dataset)
    s.reset()
    try:
        for stmt in upstream_sql(obj) + split_statements(obj.hive_sql)[:-1]:  # rebuild
            s.run(stmt)
        src_sql = compare_sql(obj)
        agg = s.run(AGG_TEMPLATE.format(cols=_agg_exprs(columns, types, "hive"), sql=src_sql))
        order = ", ".join(f"`{c}`" for c in columns)
        sample = s.run(f"SELECT * FROM ({src_sql}) v ORDER BY {order} LIMIT {SAMPLE_ROWS}")
    finally:
        s.reset()
    if agg.ok and agg.frame is not None and sample.ok and sample.frame is not None:
        agg.frame.to_parquet(agg_path, index=False)
        sample.frame.to_parquet(sample_path, index=False)
    return agg, sample


def _target_compare_sql(obj: MigrationObject, converted_sql: str) -> str:
    """The last SELECT of the converted script (the agent may have reshaped it), else the
    object's check SQL transpiled."""
    parts = split_statements(converted_sql)
    for stmt in reversed(parts):
        if re.match(r"^\s*(SELECT|WITH)\b", stmt, re.IGNORECASE):
            return stmt
    return sqlglot.transpile(compare_sql(obj), read="hive", write="snowflake")[0]
