"""Mutation test of the verifier: how many injected semantic bugs does the comparison catch?

    uv run python scripts/mutation_test.py evals/results/v2_haiku_test_r1.json

Takes every object the run verified (converted SQL that matched the source), applies known
semantic mutations one at a time to the Snowflake SQL, runs the mutated SQL through the same
harness, and counts how often the verifier flags it. A verifier that lets mutations through is
not a verifier. Mutations mirror the traps a migration actually produces:
- drop NULLS FIRST (NULL ordering); flip a DATEDIFF argument order; TRY_CAST -> CAST;
- off-by-one on a numeric or date boundary (BETWEEN x AND y -> BETWEEN x AND y-1);
- COALESCE(a, 0) -> a (NULL handling); drop a DISTINCT; change a rounding scale;
- ::STRING -> ::INT on a flattened value (type after FLATTEN).
Only mutations that change the SQL text are counted. No model calls; Snowflake only.
"""

from __future__ import annotations

import json
import re
import sys
from collections import Counter
from collections.abc import Callable

import pandas as pd

from hive2snow.config import PROJECT_ROOT
from hive2snow.diff import _norm, _sort_key
from hive2snow.engines import Snowflake
from hive2snow.harness import _target_compare_sql, clean_target, evaluate, ordered, register
from hive2snow.objects import MigrationObject

Repl = str | Callable[[re.Match[str]], str]
MUTATIONS: list[tuple[str, str, Repl, int]] = [  # (name, pattern, replacement, flags)
    ("drop_nulls_first", r"\s+NULLS\s+FIRST", "", re.IGNORECASE),
    (
        "datediff_swap",
        r"DATEDIFF\(\s*'?day'?\s*,\s*([^,()]+)\s*,\s*([^,()]+)\)",
        r"DATEDIFF('day', \2, \1)",
        re.IGNORECASE,
    ),
    ("try_cast_to_cast", r"\bTRY_CAST\(", "CAST(", re.IGNORECASE),
    (
        "between_off_by_one",
        r"BETWEEN\s+(\d+)\s+AND\s+(\d+)",
        lambda m: f"BETWEEN {m.group(1)} AND {int(m.group(2)) - 1}",
        re.IGNORECASE,
    ),
    ("coalesce_zero_dropped", r"COALESCE\(([^,()]+),\s*0\)", r"\1", re.IGNORECASE),
    ("drop_distinct", r"COUNT\(\s*DISTINCT\s+", "COUNT(", re.IGNORECASE),
    ("round_scale", r"ROUND\(([^()]*(?:\([^()]*\))?[^()]*),\s*2\)", r"ROUND(\1, 1)", re.IGNORECASE),
    ("flatten_type", r"::\s*(?:STRING|VARCHAR)\b", "::INT", re.IGNORECASE),
    ("sum_to_avg", r"\bSUM\(", "AVG(", re.IGNORECASE),
    (
        "inner_to_left",
        r"\bINNER\s+JOIN\b|(?<![A-Z])\bJOIN\b(?!\s*LATERAL)",
        "LEFT JOIN",
        re.IGNORECASE,
    ),
]


def target_frame(sf: Snowflake, obj: MigrationObject, sql: str) -> pd.DataFrame | None:
    """The output the verifier compares, produced by `sql` on Snowflake (clean state)."""
    sf.use(obj.dataset)
    clean_target(sf, obj)
    r = sf.run(sql)
    if not r.ok:
        return None
    if obj.kind == "ddl" and obj.ddl_table:
        d = sf.run(f"DESCRIBE TABLE {obj.ddl_table}")
        return d.frame[["name", "type"]] if d.ok and d.frame is not None else None
    if obj.kind in ("etl", "ddl") or r.statements > 1:
        c = sf.run(_target_compare_sql(obj, sql))
        return c.frame if c.ok else None
    return r.frame


def strict_equal(a: pd.DataFrame, b: pd.DataFrame, is_ordered: bool) -> bool:
    if a.shape != b.shape:
        return False

    def rows(f: pd.DataFrame) -> list[tuple]:
        out = []
        for row in f.itertuples(index=False, name=None):
            out.append(tuple(round(v, 9) if isinstance(v, float) else v for v in map(_norm, row)))
        return out

    ra, rb = rows(a), rows(b)
    if not is_ordered:
        ra, rb = sorted(ra, key=_sort_key), sorted(rb, key=_sort_key)
    return ra == rb


def main(result_file: str) -> int:
    data = json.loads((PROJECT_ROOT / result_file).read_text(encoding="utf-8"))
    raw = json.loads((PROJECT_ROOT / "data" / "objects.json").read_text(encoding="utf-8"))
    objects = {
        o["id"]: MigrationObject(**{k: v for k, v in o.items() if k != "source_stable"})
        for o in raw
    }
    register(list(objects.values()))
    sf = Snowflake()
    verified = [r for r in data["objects"] if r["verdict"] == "migrated" and r["status"] == "match"
                and r.get("category", "convertible") in ("convertible", "maskable") and r["sql"].strip()]  # fmt: skip
    caught: Counter[str] = Counter()
    applied: Counter[str] = Counter()
    equivalent: Counter[str] = Counter()
    missed: list[str] = []
    equivalents: list[str] = []
    for r in verified:
        obj = objects[r["id"]]
        original: pd.DataFrame | None = None
        for name, pattern, repl, flags in MUTATIONS:
            mutated = re.sub(pattern, repl, r["sql"], count=1, flags=flags)
            if mutated == r["sql"]:
                continue
            applied[name] += 1
            out = evaluate(sf, obj, mutated)
            if out.status != "match":
                caught[name] += 1
                continue
            # survived: is the mutated output actually different from the unmutated output on
            # the same Snowflake data (strict: exact values, exact row order when ordered)?
            if original is None:
                original = target_frame(sf, obj, r["sql"])
            changed = target_frame(sf, obj, mutated)
            if (
                original is not None
                and changed is not None
                and strict_equal(original, changed, ordered(obj))
            ):
                equivalent[name] += 1
                equivalents.append(f"{r['id']}:{name}")
            else:
                missed.append(f"{r['id']}:{name}")
    total_applied, total_caught = sum(applied.values()), sum(caught.values())
    total_equiv = sum(equivalent.values())
    effective = total_applied - total_equiv
    lines = [
        "# Mutation test of the verifier",
        "",
        f"Source run: `{result_file}`; {len(verified)} verified objects; {total_applied} mutations "
        "applied (each a known semantic bug, injected one at a time into a verified conversion).",
        "",
        "A mutation the verifier lets through is checked once more: the mutated SQL and the "
        "unmutated SQL are run on the same Snowflake data and their outputs compared strictly "
        "(exact values, exact row order when the query orders). Identical outputs mean the bug "
        "does not show on this data (e.g. `NULLS FIRST` dropped where the sort key has no NULLs, "
        "`DISTINCT` dropped where the values are already distinct): no verifier could catch it "
        "with this data, so it is reported as *equivalent here*, not as a miss. Different outputs "
        "that still passed are real misses of the verifier's comparison rules.",
        "",
        "| Mutation | Applied | Caught | Equivalent here | Missed | Catch rate (non-equivalent) |",
        "|---|---|---|---|---|---|",
    ]
    for name, *_ in MUTATIONS:
        if applied[name]:
            n_missed = applied[name] - caught[name] - equivalent[name]
            eff = applied[name] - equivalent[name]
            rate = f"{100 * caught[name] / eff:.0f}%" if eff else "-"
            lines.append(
                f"| {name} | {applied[name]} | {caught[name]} | {equivalent[name]} | {n_missed} | {rate} |"
            )
    raw_rate = 100 * total_caught / total_applied if total_applied else 0
    eff_rate = 100 * total_caught / effective if effective else 0
    lines += [
        "",
        f"**Overall: {total_caught} of {effective} mutations that change the result were caught "
        f"({eff_rate:.0f}%)**; {total_equiv} more did not change the result on this data; raw "
        f"catch rate {total_caught} of {total_applied} ({raw_rate:.0f}%).",
    ]
    if missed:
        lines += ["", "Missed (output changed, verifier passed): " + ", ".join(missed)]
    if equivalents:
        lines += ["", "Equivalent on this data: " + ", ".join(equivalents)]
    text = "\n".join(lines) + "\n"
    out_path = PROJECT_ROOT / "evals" / "results" / "MUTATION_test.md"
    out_path.write_text(text, encoding="utf-8")
    print(text)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1] if len(sys.argv) > 1 else "evals/results/v2_haiku_test_r1.json"))
