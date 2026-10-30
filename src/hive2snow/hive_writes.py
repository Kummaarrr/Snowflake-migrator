"""Hive write statements on the local Spark source, emulated with in-memory views.

Spark on this Windows laptop has no Hadoop native binaries, so it cannot write table files.
The eval objects that write (`CREATE TABLE`, `INSERT OVERWRITE ... PARTITION`, `INSERT INTO`,
`DROP TABLE`) are executed by this small emulator instead: the table lives as a temp view; an
INSERT OVERWRITE replaces the whole table or just the partitions it names (static) or produces
(dynamic), as Hive does. Reads and every SELECT run on Spark SQL itself. The harness notes
which objects went through the emulator.
"""

from __future__ import annotations

import re
from typing import Any

import sqlglot
from sqlglot import exp

_SET = re.compile(r"^\s*(SET|ADD\s+JAR|USE)\b", re.IGNORECASE)
_INSERT = re.compile(
    r"^\s*INSERT\s+(OVERWRITE|INTO)\s+(?:TABLE\s+)?(?P<table>[`\w.]+)\s*"
    r"(?:PARTITION\s*\((?P<part>[^)]*)\))?\s*(?P<body>(?:SELECT|WITH|VALUES|\().*)$",
    re.IGNORECASE | re.DOTALL,
)
_DROP = re.compile(r"^\s*DROP\s+(TABLE|VIEW)\s+(IF\s+EXISTS\s+)?(?P<table>[`\w.]+)", re.IGNORECASE)
_VIEW = re.compile(
    r"^\s*CREATE\s+(?:OR\s+REPLACE\s+)?VIEW\s+(?:IF\s+NOT\s+EXISTS\s+)?(?P<table>[`\w.]+)\s+AS\s+(?P<body>.*)$",
    re.IGNORECASE | re.DOTALL,
)
_CTAS = re.compile(
    r"^\s*CREATE\s+(?:TEMPORARY\s+)?(?:EXTERNAL\s+)?TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?"
    r"(?P<table>[`\w.]+)(?=\s|\()(?P<rest>.*)$",
    re.IGNORECASE | re.DOTALL,
)


def top_level_columns(parsed: Any) -> list[tuple[str, str]]:
    """(name, hive type) of a CREATE TABLE's own columns and partition columns, in order;
    fields nested inside STRUCT types are not columns."""
    cols: list[tuple[str, str]] = []
    schema = parsed.this if isinstance(parsed.this, exp.Schema) else None
    defs = list(schema.expressions) if schema is not None else []
    for prop in parsed.find_all(exp.PartitionedByProperty):
        inner = prop.this
        defs += list(inner.expressions) if isinstance(inner, exp.Schema) else [inner]
    for col in defs:
        if isinstance(col, exp.ColumnDef):
            kind = col.args.get("kind")
            typ = kind.sql(dialect="hive") if kind is not None else "string"
            if col.name not in {c for c, _ in cols}:
                cols.append((col.name, typ))
    return cols


HIVE_TO_SPARK_TYPES = {
    "STRING": "string",
    "INT": "int",
    "BIGINT": "bigint",
    "DOUBLE": "double",
    "FLOAT": "float",
    "BOOLEAN": "boolean",
    "DATE": "date",
    "TIMESTAMP": "timestamp",
    "SMALLINT": "smallint",
    "TINYINT": "tinyint",
    "BINARY": "binary",
}


def _materialize(session: Any, df: Any) -> Any:
    """A DataFrame whose plan holds the rows themselves, not a reference to a view: the view
    is about to be replaced by this very frame (and INSERT ... SELECT FROM the same table must
    read the old rows). ETL tables here are small."""
    return session.createDataFrame(df.collect(), schema=df.schema)


def _name(raw: str) -> str:
    return raw.replace("`", "").split(".")[-1].lower()


def is_write(statement: str) -> bool:
    return bool(
        _SET.match(statement)
        or _INSERT.match(statement)
        or _DROP.match(statement)
        or _VIEW.match(statement)
        or _CTAS.match(statement)
    )


def execute(session: Any, statement: str) -> Any:
    """Run one write-side statement; returns a DataFrame for CTAS/INSERT (the table content)."""
    if _SET.match(statement):
        return None  # session settings, jars and USE db: no effect on a single-schema eval
    if m := _DROP.match(statement):
        session.catalog.dropTempView(_name(m.group("table")))
        return None
    if m := _VIEW.match(statement):  # a view over temp views must itself be temporary
        df = _materialize(session, session.sql(m.group("body")))
        df.createOrReplaceTempView(_name(m.group("table")))
        return df
    if m := _INSERT.match(statement):
        return _insert(
            session,
            m.group(1).upper() == "OVERWRITE",
            _name(m.group("table")),
            m.group("part"),
            m.group("body"),
        )
    if m := _CTAS.match(statement):
        return _create(session, _name(m.group("table")), m.group("rest"))
    raise ValueError(f"not a write statement: {statement[:60]}")


def _create(session: Any, table: str, rest: str) -> Any:
    ctas = re.search(r"\bAS\s*\(?\s*(SELECT|WITH)\b", rest, re.IGNORECASE)
    if ctas:
        body = (
            rest[ctas.start(1) :].strip().rstrip(")")
            if rest[ctas.end(0) - len(ctas.group(1)) - 1 : ctas.end(0) - len(ctas.group(1))] == "("
            else rest[ctas.start(1) :]
        )
        df = _materialize(session, session.sql(body.strip()))
        df.createOrReplaceTempView(table)
        return df
    # column definitions + optional PARTITIONED BY (partition columns become regular columns)
    parsed = sqlglot.parse_one(f"CREATE TABLE {table} {rest}", read="hive")
    cols = top_level_columns(parsed)
    selects = ", ".join(f"CAST(NULL AS {_spark_type(t)}) AS `{c}`" for c, t in cols)
    df = session.sql(f"SELECT {selects} WHERE FALSE")
    df.createOrReplaceTempView(table)
    return df


def _spark_type(hive_type: str) -> str:
    t = hive_type.strip()
    up = t.upper()
    if up in HIVE_TO_SPARK_TYPES:
        return HIVE_TO_SPARK_TYPES[up]
    if up.startswith(("VARCHAR", "CHAR")):
        return "string"
    return t  # decimal(p,s), array<...>, map<...>, struct<...>


def _insert(session: Any, overwrite: bool, table: str, part: str | None, body: str) -> Any:
    from pyspark.sql import functions as F

    new = session.sql(body.strip("() \n"))
    static: dict[str, str] = {}
    dynamic: list[str] = []
    if part:
        for item in part.split(","):
            item = item.strip()
            if "=" in item:
                k, v = item.split("=", 1)
                static[k.strip().strip("`")] = v.strip()
            else:
                dynamic.append(item.strip("`"))
    for k, v in static.items():
        new = new.withColumn(k, F.expr(v))
    exists = session.catalog.tableExists(table)
    if exists:
        old = session.table(table)
        cols = list(old.columns)  # the table's order: data columns, then partition columns
        # Hive INSERT is positional: the SELECT's columns (plus the static partition values
        # appended above) line up with the table's columns in order
        new = new.toDF(*cols) if len(new.columns) == len(cols) else new
        new = new.select(*cols)
        if overwrite:
            if static or dynamic:  # replace only the affected partitions
                keys = [*static, *dynamic]
                touched = new.select(*keys).distinct()
                # a join on names moves the keys to the front: restore the table's order
                old = old.join(touched, on=keys, how="left_anti").select(*cols)
            else:
                old = old.limit(0)
        merged = old.unionByName(new)
    else:
        merged = new
    merged = _materialize(session, merged)
    merged.createOrReplaceTempView(table)
    return merged
