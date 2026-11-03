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
_MULTI = re.compile(
    r"^\s*FROM\s+(?P<src>.+?)\s+(?=INSERT\s+(OVERWRITE|INTO)\b)(?P<inserts>.*)$",
    re.IGNORECASE | re.DOTALL,
)
_MULTI_PART = re.compile(
    r"INSERT\s+(OVERWRITE|INTO)\s+(?:TABLE\s+)?(?P<table>[`\w.]+)\s*"
    r"(?:PARTITION\s*\((?P<part>[^)]*)\))?\s*(?P<body>SELECT\b.*?)"
    r"(?=INSERT\s+(?:OVERWRITE|INTO)\b|$)",
    re.IGNORECASE | re.DOTALL,
)
_CLAUSE = re.compile(
    r"\b(WHERE|GROUP\s+BY|HAVING|ORDER\s+BY|DISTRIBUTE\s+BY|SORT\s+BY|LIMIT)\b", re.IGNORECASE
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


_HIVEVAR = re.compile(
    r"^\s*SET\s+(?:hivevar|hiveconf):(?P<name>\w+)\s*=\s*(?P<value>.*?)\s*$", re.IGNORECASE | re.S
)
_MACRO = re.compile(
    r"^\s*CREATE\s+TEMPORARY\s+MACRO\s+(?P<name>\w+)\s*\((?P<params>[^)]*)\)\s*(?P<body>.*)$",
    re.IGNORECASE | re.DOTALL,
)


def preprocess(statements: list[str]) -> list[str]:
    """Two Hive CLI features Spark SQL lacks: `SET hivevar:x=v` + `${hivevar:x}` substitution,
    and `CREATE TEMPORARY MACRO name(params) body` expanded at its call sites (one level of
    parentheses in the arguments). Both statements are consumed."""
    variables: dict[str, str] = {}
    macros: dict[str, tuple[list[str], str]] = {}
    out: list[str] = []
    for statement in statements:
        if m := _HIVEVAR.match(statement):
            variables[m.group("name")] = m.group("value").strip("'\"")
            continue
        if m := _MACRO.match(statement):
            params = [
                p.strip().split()[0].strip("`") for p in m.group("params").split(",") if p.strip()
            ]
            macros[m.group("name").lower()] = (params, m.group("body").strip())
            continue
        for name, value in variables.items():
            statement = re.sub(r"\$\{(?:hivevar:|hiveconf:)?" + name + r"\}", value, statement)
        for name, (params, body) in macros.items():

            def expand(call: re.Match[str], params: list[str] = params, body: str = body) -> str:
                args = [a.strip() for a in call.group(1).split(",")] if params else []
                expanded = body
                for p, a in zip(params, args, strict=False):
                    expanded = re.sub(r"\b" + re.escape(p) + r"\b", f"({a})", expanded)
                return f"({expanded})"

            statement = re.sub(r"\b" + name + r"\s*\(([^()]*)\)", expand, statement, flags=re.I)
        out.append(statement)
    return out


def is_write(statement: str) -> bool:
    return bool(
        _SET.match(statement)
        or _INSERT.match(statement)
        or _MULTI.match(statement)
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
    if m := _MULTI.match(statement):  # Hive multi-insert: one source scan, several targets
        src = m.group("src").strip()
        df = None
        for part in _MULTI_PART.finditer(m.group("inserts")):
            body = part.group("body").strip()
            # "SELECT ... [WHERE / GROUP BY ...]" -> "SELECT ... FROM src [WHERE / GROUP BY ...]"
            clause = _CLAUSE.search(body)
            select = body[: clause.start()] if clause else body
            tail = body[clause.start() :] if clause else ""
            df = _insert(
                session,
                part.group(1).upper() == "OVERWRITE",
                _name(part.group("table")),
                part.group("part"),
                f"{select} FROM {src} {tail}".strip(),
            )
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
    exists = session.catalog.tableExists(table)
    if exists:
        old = session.table(table)
        cols = list(old.columns)  # the table's order: data columns, then partition columns
        # Hive INSERT is positional: the SELECT's columns line up with the table's data columns
        # and then its dynamic partition columns, in table order; static partition values are
        # not in the SELECT and are filled in from the PARTITION clause
        selected = [c for c in cols if c not in static]
        new = new.toDF(*selected) if len(new.columns) == len(selected) else new
        for k, v in static.items():
            new = new.withColumn(k, F.expr(v))
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
        for k, v in static.items():
            new = new.withColumn(k, F.expr(v))
        merged = new
    merged = _materialize(session, merged)
    merged.createOrReplaceTempView(table)
    return merged
