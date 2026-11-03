"""v2: the estate agent. Migrates a set of objects the way a migration team would:

1. **triage** each object before spending anything:
   - constructs whose code is not in the SQL (jar UDFs, metastore UDFs, TRANSFORM scripts,
     reflect on non-JDK classes) -> handed back as *unconvertible* with a fix hint; engine
     hashes and JDK reflect calls are attempted (a UDF can reproduce them, the data decides);
   - constructs that need environment mapping (HDFS / S3 paths, external locations, directory
     exports) -> handed back as *needs_config*;
   - results no query can fix (random, sampling, undefined order, ties in a ranking or at a
     LIMIT cut, approximate functions, clock values in filters) -> handed back as
     *nondeterministic*; detected from the SQL and by probing the source (ties) and running it
     twice with a different shuffle layout;
   - clock columns in the output only (load_ts) -> *maskable*: converted, verified with those
     columns masked, and the verdict says so;
2. **order** the rest by dependencies (an aggregate after its staging views, a report after
   its aggregate); an object whose upstream failed is handed back as *blocked*, not repaired;
3. **migrate** each object with the v1 loop (draft -> run -> repair from the error -> compare
   -> repair from the diff), verifying large results in-warehouse;
4. **re-verify** downstream objects whose upstream conversion changed after they were checked
   (a no-op in dependency order; it matters when ordering is off, which the ablation shows);
5. report per object and per estate.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any

import sqlglot
from sqlglot import exp

from hive2snow import agent, deps, harness
from hive2snow.engines import Snowflake, split_statements
from hive2snow.hive_builtins import BUILTINS
from hive2snow.hive_writes import top_level_columns
from hive2snow.objects import MigrationObject
from hive2snow.systems import Conversion

UNCONVERTIBLE = [
    (
        r"\bADD\s+JAR\b|\bCREATE\s+(?:TEMPORARY\s+)?FUNCTION\b",
        "custom UDF loaded from a jar (Hive UDF API). Fix: reimplement the class as a Snowflake SQL / Python / Java UDF from its source, then re-run verification",
    ),
    (
        r"\bTRANSFORM\s*\([^)]*\)\s*USING\s*'",
        "Hive TRANSFORM streams rows through an external script. Fix: port the script to a Snowflake Python UDTF, or run it outside the warehouse",
    ),
    (
        r"\b(?:reflect|java_method)\s*\(\s*'(?!java\.)",
        "reflect() / java_method() on a class outside the JDK: its code is not available here. Fix: port the method as a Snowflake UDF from its source and re-verify",
    ),
]
NEEDS_CONFIG = [
    (
        r"\bLOAD\s+DATA\s+(?:LOCAL\s+)?INPATH\b",
        "LOAD DATA INPATH reads a filesystem path. Needs: the path mapped to a stage (PUT / external stage), then COPY INTO the table",
    ),
    (
        r"\bINSERT\s+OVERWRITE\s+(?:LOCAL\s+)?DIRECTORY\b",
        "INSERT OVERWRITE DIRECTORY writes to a filesystem. Needs: a stage and file format for COPY INTO @stage FROM (query)",
    ),
    (
        r"\bLOCATION\s*'|\bALTER\s+TABLE\b.*\bSET\s+LOCATION\b|\bMSCK\s+REPAIR\b",
        "the table's data lives at an external LOCATION. Needs: a Snowflake external stage / external table (or a load) for that path, and the partition discovery MSCK did",
    ),
]
NONDETERMINISTIC = [
    (r"\brand\s*\(|\brandn\s*\(", "rand() makes the result differ on every run"),
    (r"\bTABLESAMPLE\b", "TABLESAMPLE picks a random sample"),
    (
        r"\buuid\s*\(|\bmonotonically_increasing_id\s*\(",
        "generated ids differ on every run and across engines",
    ),
    (
        r"(?<!sort_array\()(?<!array_sort\()\bcollect_(?:list|set)\s*\(",
        "collect_list / collect_set element order is undefined (wrap in sort_array to make it verifiable)",
    ),
    (r"\bfirst\s*\(|\blast\s*\(", "first() / last() pick an arbitrary row of the group"),
    (
        r"\bapprox_count_distinct\s*\(|\bpercentile_approx\s*\(|\bapprox_percentile\s*\(",
        "approximate functions give different estimates in different engines; agree a tolerance with the owner",
    ),
]
CLOCK = r"\bcurrent_timestamp\b|\bnow\s*\(|\bunix_timestamp\s*\(\s*\)|\bcurrent_date\b|\bcurrent_timezone\s*\("
TIE_SENSITIVE = {"row_number", "first_value", "last_value", "lag", "lead", "ntile", "nth_value"}
_SKIP_STMT = re.compile(
    r"^\s*(SET|USE|ADD\s+JAR|CREATE\s+(TEMPORARY\s+)?(FUNCTION|MACRO)|DROP\s+FUNCTION|MSCK|LOAD\s+DATA)\b",
    re.I,
)


@dataclass
class Triage:
    category: str  # convertible | maskable | unconvertible | needs_config | nondeterministic
    reason: str = ""
    mask: list[str] = field(default_factory=list)  # maskable: output columns left unverified


def _strip(sql: str, literals: bool = True) -> str:
    """SQL without comments and (by default) with string literal contents blanked, so rules
    don't fire on words inside comments or data."""
    sql = re.sub(r"--[^\n]*", " ", sql)
    sql = re.sub(r"/\*.*?\*/", " ", sql, flags=re.S)
    if not literals:
        return sql
    sql = re.sub(r"'(?:[^'\\]|\\.)*'", "''", sql)
    return re.sub(r'"(?:[^"\\]|\\.)*"', '""', sql)


def _parsed(sql: str) -> list[Any]:
    out: list[Any] = []
    for stmt in split_statements(sql):
        if _SKIP_STMT.match(stmt):
            continue
        try:
            out.append(sqlglot.parse_one(stmt, read="hive"))
        except Exception:
            continue
    return out


def _macros(sql: str) -> set[str]:
    return {m.lower() for m in re.findall(r"\bCREATE\s+TEMPORARY\s+MACRO\s+(\w+)", sql, re.I)}


def unknown_functions(sql: str) -> list[str]:
    """Function calls that are neither source built-ins nor macros defined in the script."""
    known = BUILTINS | _macros(sql)
    names: list[str] = []
    for tree in _parsed(sql):
        for fn in tree.find_all(exp.Anonymous):
            name = str(fn.this).lower()
            if name and name not in known and name not in names:
                names.append(name)
    return names


def _clock_mask(obj: MigrationObject) -> tuple[list[str] | None, str]:
    """Output columns that carry clock values, when the clock appears only in SELECT lists
    (a plain report column, or an INSERT's column mapped through the table's DDL). Returns
    (None, why) when the clock drives a filter / join or its column can't be identified."""
    clock = re.compile(CLOCK, re.I)
    table_cols: dict[str, list[str]] = {}
    mask: list[str] = []
    trees = _parsed(obj.hive_sql + (";\n" + obj.check_sql if obj.check_sql else ""))
    for tree in trees:
        if isinstance(tree, exp.Create) and isinstance(tree.this, exp.Schema):
            table_cols[tree.this.this.name.lower()] = [c for c, _ in top_level_columns(tree)]
        hits = [
            n
            for n in tree.find_all(exp.Func, exp.Column)
            if clock.search(n.sql(dialect="hive"))
            and not any(clock.search(c.sql(dialect="hive")) for c in n.iter_expressions())
        ]  # innermost nodes only
        for n in hits:
            # the clock must sit directly in a SELECT list (possibly inside an alias / cast)
            holder: Any = n
            while holder.parent is not None and not isinstance(holder.parent, exp.Select):
                holder = holder.parent
            sel = holder.parent
            if sel is None or holder not in sel.expressions:
                return (
                    None,
                    "a clock value drives a filter, join or grouping: the result depends on the day it runs",
                )
            pos = sel.expressions.index(holder)
            if isinstance(tree, exp.Insert):
                target = tree.this.this if isinstance(tree.this, exp.Schema) else tree.this
                cols = (
                    table_cols.get(target.name.lower()) if isinstance(target, exp.Table) else None
                )
                if not cols or pos >= len(cols):
                    return (
                        None,
                        "a clock value is written by an INSERT whose target columns are unknown",
                    )
                name = cols[pos]
            else:
                name = (holder.alias_or_name or "").lower()
                if not name or isinstance(holder, exp.Star):
                    return None, "a clock column without an alias cannot be masked reliably"
            if name not in mask:
                mask.append(name)
    return mask, ""


def _limit_without_order(obj: MigrationObject, trees: list[Any]) -> bool:
    """A LIMIT with no ORDER BY picks arbitrary rows - unless it is the outer statement's LIMIT
    and the result has fewer rows than the limit (TPC-DS's habitual `LIMIT 100` on a one-row
    aggregate), which cuts nothing."""
    outer_rows = None
    truth = harness.source_truth(obj) if harness.truth_path(obj).exists() else None
    if truth is not None and truth.ok and truth.frame is not None:
        outer_rows = len(truth.frame)
    for tree in trees:
        for sel in [*tree.find_all(exp.Select), *([tree] if isinstance(tree, exp.Union) else [])]:
            limit = sel.args.get("limit")
            if limit is None or sel.args.get("order") is not None:
                continue
            is_outer = sel is tree or (isinstance(tree, exp.Union) and sel is tree)
            n = limit.expression
            if (
                is_outer
                and outer_rows is not None
                and isinstance(n, exp.Literal)
                and n.is_int
                and outer_rows < int(n.this)
            ):
                continue
            return True
    return False


def _window_checks(obj: MigrationObject, trees: list[Any], probe: bool) -> str:
    """Tie-sensitive window functions: no ORDER BY is arbitrary; with ORDER BY, probe the
    source for duplicate (partition, order) keys - tied rows get ranks in an undefined order."""
    for tree in trees:
        for w in tree.find_all(exp.Window):
            fn = w.this
            while isinstance(fn, exp.IgnoreNulls | exp.RespectNulls):
                fn = fn.this
            name = (
                str(fn.this).lower()
                if isinstance(fn, exp.Anonymous)
                else fn.sql_name().lower()
                if isinstance(fn, exp.Func)
                else fn.key
            )
            if name not in TIE_SENSITIVE:
                continue
            order = w.args.get("order")
            if order is None:
                return f"{name}() without ORDER BY in its window picks arbitrary rows"
            if not probe:
                continue
            sel = w.find_ancestor(exp.Select)
            if sel is None:
                continue
            keys = [e.this if isinstance(e, exp.Ordered) else e for e in order.expressions]
            part = w.args.get("partition_by") or []
            base = sel.copy()
            base.set("order", None)
            base.set("limit", None)
            base.set("distinct", None)
            base.set("expressions", [k.copy().as_(f"k{i}") for i, k in enumerate([*part, *keys])])
            n = len(part) + len(keys)
            with_clause = tree.args.get("with")
            prefix = (
                with_clause.sql(dialect="hive") + " "
                if with_clause is not None and base.args.get("with") is None
                else ""
            )
            probe_sql = (
                f"{prefix}SELECT 1 FROM ({base.sql(dialect='hive')}) tie_probe "
                f"GROUP BY {', '.join(f'k{i}' for i in range(n))} HAVING count(*) > 1 LIMIT 1"
            )
            r = harness.run_on_source(obj, probe_sql)
            if r.ok and r.frame is not None and len(r.frame) > 0:
                return f"{name}() ranks rows that tie on its ORDER BY keys: their order is undefined (add a unique tiebreaker)"
    return ""


def _limit_cut_ties(obj: MigrationObject) -> str:
    """Top-level ORDER BY ... LIMIT n: if rows beyond the cut tie with the last row kept, which
    of them appear is undefined."""
    sql = harness.compare_sql(obj)
    m = re.search(r"\bLIMIT\s+(\d+)\s*$", sql, re.I)
    if not m or not harness.ordered(obj):
        return ""
    positions = harness.order_positions(obj)
    truth = harness.source_truth(obj)
    if (
        positions is None
        or not truth.ok
        or truth.frame is None
        or len(truth.frame) < int(m.group(1))
    ):
        return ""
    full = harness.run_on_source(obj, sql[: m.start()])
    if not full.ok or full.frame is None or len(full.frame) <= len(truth.frame):
        return ""

    def key(row: tuple) -> tuple[str, ...]:
        return tuple(str(row[p]) for p in positions)

    last = key(tuple(truth.frame.iloc[-1]))
    in_cut = sum(key(row) == last for row in truth.frame.itertuples(index=False, name=None))
    tied = [row for row in full.frame.itertuples(index=False, name=None) if key(row) == last]
    in_full = len(tied)
    if len({tuple(map(str, row)) for row in tied}) <= 1:
        return ""  # the tied rows are identical: whichever make the cut, the result is the same
    if in_full > in_cut:
        return f"ORDER BY ... LIMIT with ties at the cut: {in_full} rows share the last kept sort key, only {in_cut} fit"
    return ""


def triage(obj: MigrationObject, source_checks: bool = True) -> Triage:
    """Decide before converting. `source_checks`: run the source for tie probes and the double
    run (False = rules only, for tests and the ablation)."""
    clean = _strip(obj.hive_sql)
    for pattern, reason in UNCONVERTIBLE:
        # the reflect rule reads the class name, so it looks at the literals
        text = _strip(obj.hive_sql, literals=False) if "reflect" in pattern else clean
        if re.search(pattern, text, re.IGNORECASE | re.DOTALL):
            return Triage("unconvertible", reason)
    if unknown := unknown_functions(obj.hive_sql):
        names = ", ".join(unknown)
        return Triage(
            "unconvertible",
            f"{names}: not a built-in, so a metastore UDF. Fix: locate its source (SHOW FUNCTION / the metastore) and reimplement it as a Snowflake UDF, then re-verify",
        )
    for pattern, reason in NEEDS_CONFIG:
        if re.search(pattern, clean, re.IGNORECASE | re.DOTALL):
            return Triage("needs_config", reason)
    for pattern, reason in NONDETERMINISTIC:
        if re.search(pattern, clean, re.IGNORECASE):
            return Triage("nondeterministic", reason)
    mask: list[str] = []
    if re.search(CLOCK, clean, re.I):
        found, why = _clock_mask(obj)
        if found is None:
            return Triage("nondeterministic", why)
        mask = found
    trees = _parsed(obj.hive_sql + (";\n" + obj.check_sql if obj.check_sql else ""))
    if _limit_without_order(obj, trees):
        return Triage("nondeterministic", "LIMIT without ORDER BY returns an arbitrary subset")
    if why := _window_checks(obj, trees, source_checks):
        return Triage("nondeterministic", why)
    if source_checks:
        if why := _limit_cut_ties(obj):
            return Triage("nondeterministic", why)
        if harness.source_is_stable(obj, mask) is False:
            return Triage("nondeterministic", "the source gives different results on two runs")
    if mask:
        return Triage("maskable", f"clock columns verified by masking: {', '.join(mask)}", mask)
    return Triage("convertible")


@dataclass(frozen=True)
class EstateConfig:
    inner: agent.Config = field(default_factory=agent.Config)
    triage: bool = True  # hand back unconvertible / non-deterministic objects before converting
    double_run: bool = True  # probe the source (ties) and run it twice to detect non-determinism
    dependency_order: bool = True  # False: objects in file-name order (reports before staging)
    reverify_downstream: bool = True


@dataclass
class Migrated:
    obj: MigrationObject
    conversion: Conversion
    outcome: harness.Outcome | None
    order_ok: bool
    reverified: bool = False
    masked: list[str] = field(default_factory=list)


def _hand_back(category: str, reason: str, extra: dict) -> Conversion:
    return Conversion(
        "",
        "needs_human",
        f"{category}: {reason}",
        [],
        0,
        [{"step": "triage", "category": category, "reason": reason, **extra}],
    )


def migrate_estate(
    objects: list[MigrationObject],
    sf: Snowflake,
    cfg: EstateConfig | None = None,
    progress: bool = False,
) -> list[Migrated]:
    cfg = cfg or EstateConfig()
    harness.register(objects)
    in_run = {o.id for o in objects}
    # without dependency ordering the estate is taken the way a folder lists it: by file name
    order = (
        deps.migration_order(objects)
        if cfg.dependency_order
        else sorted(objects, key=lambda o: o.id)
    )
    done: dict[str, Migrated] = {}
    results: list[Migrated] = []
    iterator = order
    if progress:
        from tqdm import tqdm

        iterator = tqdm(order, desc="estate")
    for obj in iterator:
        local_deps = [d for d in obj.depends_on if d in in_run]
        order_ok = all(d in done for d in local_deps)
        mask: list[str] = []
        if cfg.triage:
            t = triage(obj, cfg.double_run)
            if t.category not in ("convertible", "maskable"):
                m = Migrated(obj, _hand_back(t.category, t.reason, {}), None, order_ok)
                done[obj.id] = m
                results.append(m)
                continue
            mask = t.mask
        blocked = [d for d in local_deps if d in done and done[d].conversion.verdict != "migrated"]
        # upstream objects outside this run are still inspected: if their code cannot be
        # converted (rules only, no source runs), everything below them is blocked too
        for d in obj.depends_on:
            if d not in in_run and d in harness._REGISTRY:
                up = triage(harness._REGISTRY[d], source_checks=False)
                if up.category in ("unconvertible", "needs_config"):
                    blocked.append(d)
        if blocked:
            conv = Conversion(
                "",
                "needs_human",
                f"blocked: upstream object(s) {', '.join(blocked)} not migrated; fix those first",
                [],
                0,
                [{"step": "blocked", "upstream": blocked}],
            )
            m = Migrated(obj, conv, None, order_ok)
            done[obj.id] = m
            results.append(m)
            continue
        try:
            conv = agent.migrate(obj, sf, cfg.inner, mask)
        except Exception as e:  # a failed object is handed back, never fatal for the estate
            if type(e).__name__ == "BudgetExceeded":
                raise
            conv = Conversion(
                "", "needs_human", f"system error: {type(e).__name__}: {str(e)[:300]}", [], 0, []
            )
        outcome = harness.evaluate(sf, obj, conv.sql) if conv.sql.strip() else None
        m = Migrated(obj, conv, outcome, order_ok, masked=mask)
        done[obj.id] = m
        results.append(m)
    if cfg.reverify_downstream:
        _reverify(results, sf)
    return results


def _reverify(results: list[Migrated], sf: Snowflake) -> None:
    """Objects verified before an upstream object was (re)converted are checked again."""
    position = {m.obj.id: i for i, m in enumerate(results)}
    for m in results:
        if m.conversion.verdict != "migrated" or not m.conversion.sql:
            continue
        later_upstream = [
            d for d in m.obj.depends_on if d in position and position[d] > position[m.obj.id]
        ]
        if not later_upstream:
            continue
        outcome = harness.evaluate(sf, m.obj, m.conversion.sql)
        m.outcome, m.reverified = outcome, True
        if outcome.status != "match":
            m.conversion = Conversion(
                m.conversion.sql,
                "needs_human",
                f"re-verification after upstream change failed: {outcome.evidence[:300]}",
                m.conversion.calls,
                m.conversion.attempts,
                [*m.conversion.trace, {"step": "reverify", "status": outcome.status}],
            )
