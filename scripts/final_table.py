"""Final tables from saved results (no model or Snowflake calls).

uv run python scripts/final_table.py   # -> evals/results/FINAL_test.md, ABLATION_dev.md
"""

from __future__ import annotations

import json
import statistics
from typing import Any

from hive2snow.config import PROJECT_ROOT

RESULTS = PROJECT_ROOT / "evals" / "results"
SYSTEMS = [
    ("v0", "v0 - sqlglot (rule-based)"),
    ("v0-unquoted", "v0+ - sqlglot, identifiers unquoted"),
    ("v0-llm_haiku", "v0-LLM - Haiku 5.5, one call"),
    ("v0-llm_opus", "v0-LLM - Opus 5.5, one call"),
    ("v1_haiku", "v1 - agent, Haiku 5.5"),
]
ROWS = [
    ("executes_pct", "Executes on Snowflake", "%"),
    ("matches_pct", "**Same result as the source**", "%"),
    ("claimed_migrated_pct", "Claimed migrated", "%"),
    ("wrong_claimed_migrated", "**Wrong but claimed migrated**", "n"),
    ("needs_human_pct", "Handed to a human", "%"),
    ("needs_human_but_fine", "...of which were actually fine", "n"),
    ("calls_per_object", "Model calls / object", "x"),
    ("seconds_per_object", "Seconds / object", "s"),
    ("usd_per_object", "$ / object", "$"),
]


def _load(name: str) -> dict[str, Any] | None:
    p = RESULTS / f"{name}.json"
    return json.loads(p.read_text(encoding="utf-8")) if p.exists() else None


def _fmt(v: float, unit: str) -> str:
    return {
        "%": f"{v:.0f}%",
        "n": f"{v:.1f}" if v != int(v) else f"{int(v)}",
        "x": f"{v:.1f}",
        "s": f"{v:.0f}",
        "$": f"${v:.4f}",
    }[unit]


def _cell(values: list[float], unit: str) -> str:
    if not values:
        return "-"
    m = statistics.mean(values)
    if max(values) == min(values):
        return _fmt(m, unit)
    return f"{_fmt(m, unit)} ({_fmt(min(values), unit)}-{_fmt(max(values), unit)})"


def final() -> str:
    runs = {k: [r for t in ("r1", "r2", "r3") if (r := _load(f"{k}_test_{t}"))] for k, _ in SYSTEMS}
    for k, _ in SYSTEMS:  # deterministic systems have one run
        if not runs[k] and (r := _load(f"{k}_test")):
            runs[k] = [r]
    present = [(k, label) for k, label in SYSTEMS if runs[k]]
    n = runs[present[0][0]][0]["aggregate"]["objects"] if present else 0
    lines = [
        "# Final results - test split",
        "",
        f"{n} held-out migration objects (queries, ETL scripts, DDL) never used while building; "
        "model systems run 3 times independently; mean (range).",
        "",
        "| Metric | " + " | ".join(label for _, label in present) + " |",
        "|---|" + "---|" * len(present),
    ]
    for key, label, unit in ROWS:
        cells = [
            _cell([r["aggregate"][key] for r in runs[k] if r["aggregate"][key] is not None], unit)
            for k, _ in present
        ]
        lines.append(f"| {label} | " + " | ".join(cells) + " |")
    lines += [
        "",
        "By kind, % with the same result as the source:",
        "",
        "| Kind | " + " | ".join(label for _, label in present) + " |",
        "|---|" + "---|" * len(present),
    ]
    for kind in ("query", "etl", "ddl"):
        cells = [
            _cell(
                [r["aggregate"]["by_kind"].get(kind, {}).get("matches_pct") or 0 for r in runs[k]],
                "%",
            )
            for k, _ in present
        ]
        lines.append(f"| {kind} | " + " | ".join(cells) + " |")
    lines += ["", "Runs available: " + ", ".join(f"{k} x{len(runs[k])}" for k, _ in present)]
    return "\n".join(lines) + "\n"


def ablation() -> str:
    variants = [
        ("v1_haiku_dev", "v1 (all steps)"),
        ("v1-nodraft_haiku_dev", "without the sqlglot draft (model drafts)"),
        ("v1-nofeedback_haiku_dev", "repairs without the error / diff text"),
        ("v1-nodiff_haiku_dev", "without data-diff repairs (errors only)"),
        ("v1-noescalate_haiku_dev", "without escalation to Sonnet"),
        ("v0_dev", "v0 sqlglot (reference)"),
        ("v0-unquoted_dev", "v0+ sqlglot unquoted (reference)"),
        ("v0-llm_haiku_dev", "v0-LLM Haiku (reference)"),
        ("v0-llm_opus_dev", "v0-LLM Opus (reference)"),
    ]
    lines = [
        "# Ablation - dev split (one run each)",
        "",
        "| Variant | Same result | Wrong but claimed migrated | Handed to a human | Calls/object | $/object |",
        "|---|---|---|---|---|---|",
    ]
    for name, label in variants:
        r = _load(name)
        if r:
            a = r["aggregate"]
            lines.append(
                f"| {label} | {a['matches_pct']}% | {a['wrong_claimed_migrated']} | "
                f"{a['needs_human_pct']}% | {a['calls_per_object']} | ${a['usd_per_object']:.4f} |"
            )
    return "\n".join(lines) + "\n"


if __name__ == "__main__":
    (RESULTS / "FINAL_test.md").write_text(final(), encoding="utf-8")
    (RESULTS / "ABLATION_dev.md").write_text(ablation(), encoding="utf-8")
    print((RESULTS / "ABLATION_dev.md").read_text(encoding="utf-8"))
    print((RESULTS / "FINAL_test.md").read_text(encoding="utf-8"))
