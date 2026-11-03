"""Dependencies between migration objects: which tables an object reads and writes.

Extracted from the Hive SQL with sqlglot (CTE names and the object's own created tables are
not external reads). The estate graph follows: object X depends on object Y when X reads a
table Y creates. Migration order is a topological sort; the agent re-verifies downstream
objects when an upstream object's conversion changes.
"""

from __future__ import annotations

import re
from collections import defaultdict
from graphlib import TopologicalSorter

import sqlglot
from sqlglot import exp

from hive2snow.engines import split_statements
from hive2snow.objects import MigrationObject

_SKIP = re.compile(r"^\s*(SET|USE|ADD\s+JAR|CREATE\s+TEMPORARY\s+FUNCTION|DROP\s+FUNCTION)\b", re.I)


def tables_read_written(hive_sql: str) -> tuple[set[str], set[str]]:
    reads: set[str] = set()
    writes: set[str] = set()
    for statement in split_statements(hive_sql):
        if _SKIP.match(statement):
            continue
        try:
            tree = sqlglot.parse_one(statement, read="hive")
        except Exception:
            continue
        ctes = {c.alias_or_name.lower() for c in tree.find_all(exp.CTE)}
        for node in tree.find_all(exp.Create, exp.Insert, exp.Drop):
            target = node.this
            table = target.this if isinstance(target, exp.Schema) else target
            if isinstance(table, exp.Table):
                writes.add(table.name.lower())
        for table in tree.find_all(exp.Table):
            name = table.name.lower()
            if name and name not in ctes:
                reads.add(name)
    return reads - writes, writes


def annotate(objects: list[MigrationObject]) -> None:
    """Fill reads / depends_on from the SQL (creates is declared by the object)."""
    producer: dict[str, str] = {}
    for o in objects:
        for t in o.creates:
            producer[t.lower()] = o.id
    for o in objects:
        reads, _writes = tables_read_written(o.hive_sql)
        o.reads = sorted(reads)
        o.depends_on = sorted({producer[t] for t in reads if t in producer and producer[t] != o.id})


def migration_order(objects: list[MigrationObject]) -> list[MigrationObject]:
    by_id = {o.id: o for o in objects}
    graph = {o.id: {d for d in o.depends_on if d in by_id} for o in objects}
    return [by_id[i] for i in TopologicalSorter(graph).static_order()]


def downstream(objects: list[MigrationObject]) -> dict[str, list[str]]:
    out: dict[str, list[str]] = defaultdict(list)
    for o in objects:
        for d in o.depends_on:
            out[d].append(o.id)
    return dict(out)
