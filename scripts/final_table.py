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
    ("v1_haiku", "v1 - per-object agent, Haiku 5.5"),
    ("v2_haiku", "v2 - estate agent, Haiku 5.5"),
]
ROWS = [
    ("executes_pct", "Executes on Snowflake", "%"),
    ("matches_pct", "Same result as the source", "%"),
    ("verified_pct", "**Verified** (migrated, right, verifiable)", "%"),
    ("claimed_migrated_pct", "Claimed migrated", "%"),
    ("wrong_claimed_migrated", "**Wrong but claimed migrated**", "n"),
    ("needs_human_pct", "Handed to a human", "%"),
    ("needs_human_but_fine", "...of which were actually fine", "n"),
    ("triage_false_positives", "...of which refused at triage wrongly", "n"),
    ("hand_back_precision_pct", "Hand-back precision", "%"),
    ("hand_back_recall_pct", "Hand-back recall", "%"),
    ("hand_back_reason_correct_pct", "Hand-back reason correct", "%"),
    ("verified_with_mask", "Verified with clock columns masked", "n"),
    ("order_violations", "Dependency-order violations", "n"),
    ("calls_per_object", "Model calls / object", "x"),
    ("seconds_per_object", "Seconds / object", "s"),
    ("usd_per_object", "$ / object", "$"),
]


def wilson(p_pct: float, n: int, z: float = 1.96) -> tuple[float, float]:
    """95% Wilson score interval for a proportion given as a percentage."""
    if n <= 0:
        return (0.0, 100.0)
    p = p_pct / 100
    denom = 1 + z * z / n
    centre = (p + z * z / (2 * n)) / denom
    half = z * ((p * (1 - p) / n + z * z / (4 * n * n)) ** 0.5) / denom
    return (max(0.0, 100 * (centre - half)), min(100.0, 100 * (centre + half)))


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
        "model systems run 3 times independently; mean (range); [95% Wilson interval] on the "
        "headline rates. Source = local Spark SQL standing in for Hive; Hive write statements run "
        "through an in-memory emulator on the source (see README notes).",
        "",
        "| Metric | " + " | ".join(label for _, label in present) + " |",
        "|---|" + "---|" * len(present),
    ]
    for key, label, unit in ROWS:
        cells = []
        for k, _ in present:
            vals = [r["aggregate"][key] for r in runs[k] if r["aggregate"].get(key) is not None]
            cell = _cell(vals, unit)
            if unit == "%" and vals and key in ("verified_pct", "matches_pct"):
                lo, hi = wilson(statistics.mean(vals), runs[k][0]["aggregate"]["objects"])
                cell += f" [{lo:.0f}-{hi:.0f}]"
            cells.append(cell)
        lines.append(f"| {label} | " + " | ".join(cells) + " |")
    for group, title in (
        ("by_estate", "estate"),
        ("by_kind", "kind"),
        ("by_category", "truth category"),
    ):
        keys = sorted(
            {key for k, _ in present for r in runs[k] for key in r["aggregate"].get(group, {})}
        )
        if not keys:
            continue
        lines += [
            "",
            f"By {title}: verified % (wrong-but-claimed-migrated count):",
            "",
            f"| {title} | " + " | ".join(label for _, label in present) + " |",
            "|---|" + "---|" * len(present),
        ]
        for key in keys:
            cells = []
            for k, _ in present:
                groups = [r["aggregate"].get(group, {}).get(key) for r in runs[k]]
                groups = [g for g in groups if g]
                if not groups:
                    cells.append("-")
                    continue
                ver = _cell([g.get("verified_pct") or 0 for g in groups], "%")
                wrong = _cell([g.get("wrong_claimed_migrated", 0) for g in groups], "n")
                cells.append(f"{ver} ({wrong})")
            n_obj = next(
                (
                    g["objects"]
                    for k, _ in present
                    for g in [r["aggregate"].get(group, {}).get(key) for r in runs[k]]
                    if g
                ),
                "",
            )
            lines.append(f"| {key} (n={n_obj}) | " + " | ".join(cells) + " |")
    lines += ["", "Runs available: " + ", ".join(f"{k} x{len(runs[k])}" for k, _ in present)]
    return "\n".join(lines) + "\n"


def ablation() -> str:
    variants = [
        ("v2_haiku_dev", "**v2 (all steps)**"),
        ("v2-notriage_haiku_dev", "v2 without triage (every object converted)"),
        (
            "v2-nodoublerun_haiku_dev",
            "v2 without source probes (rules only, no tie / double-run checks)",
        ),
        ("v2-noorder_haiku_dev", "v2 without dependency ordering (file order, no re-verification)"),
        ("v2-noreverify_haiku_dev", "v2 without dependency ordering, with re-verification"),
        ("v1_haiku_dev", "v1 per-object agent (reference)"),
        ("v0_dev", "v0 sqlglot (reference)"),
        ("v0-unquoted_dev", "v0+ sqlglot unquoted (reference)"),
        ("v0-llm_haiku_dev", "v0-LLM Haiku (reference)"),
        ("v0-llm_opus_dev", "v0-LLM Opus (reference)"),
    ]
    lines = [
        "# Ablation - dev split (one run each)",
        "",
        "| Variant | Verified | Wrong but claimed migrated | Handed to a human | Hand-back precision / recall | Order violations | Calls/object | $/object |",
        "|---|---|---|---|---|---|---|---|",
    ]
    for name, label in variants:
        r = _load(name)
        if r:
            a = r["aggregate"]
            lines.append(
                f"| {label} | {a.get('verified_pct', a['matches_pct'])}% | {a['wrong_claimed_migrated']} | "
                f"{a['needs_human_pct']}% | {a.get('hand_back_precision_pct')}% / {a.get('hand_back_recall_pct')}% | "
                f"{a.get('order_violations', 0)} | {a['calls_per_object']} | ${a['usd_per_object']:.4f} |"
            )
    return "\n".join(lines) + "\n"


if __name__ == "__main__":
    (RESULTS / "FINAL_test.md").write_text(final(), encoding="utf-8")
    (RESULTS / "ABLATION_dev.md").write_text(ablation(), encoding="utf-8")
    print((RESULTS / "ABLATION_dev.md").read_text(encoding="utf-8"))
    print((RESULTS / "FINAL_test.md").read_text(encoding="utf-8"))
