"""Hive -> Snowflake Migration Agent: demo.

Mode A: the estate migration report of the held-out test objects, pre-computed (no key
needed): per-estate metrics, the dependency graph, and every object's Hive SQL, Snowflake SQL,
verdict, evidence and repair trace.
Mode B: convert your own Hive SQL with your own Anthropic key (conversion only: visitors have
no Snowflake attached, so the result is unverified and says so).
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import streamlit as st

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from hive2snow.systems import sqlglot_transpile  # noqa: E402

REPO_URL = "https://github.com/Kummaarrr/Snowflake-migrator"
RESULTS = ROOT / "app" / "samples" / "results.json"
OBJECTS = ROOT / "app" / "samples" / "objects.json"
ESTATES = {
    "A": "Retail analytics pipeline (layered, scale 1)",
    "B": "Legacy ETL traps",
    "C": "TPC-DS reports in legacy Hive style",
}
CATEGORY = {
    "convertible": "convertible",
    "maskable": "convertible, clock columns masked",
    "unconvertible": "needs porting by a person",
    "needs_config": "needs an environment mapping",
    "nondeterministic": "result not verifiable",
    "blocked": "blocked by an unconvertible upstream",
}


def badge(r: dict) -> str:
    claimed, status, cat = r["verdict"], r["status"], r.get("category", "convertible")
    convertible = cat in ("convertible", "maskable")
    masked = r.get("masked") or []
    if claimed == "migrated" and status == "match" and convertible:
        return "✅ migrated, verified" + (f" (masked: {', '.join(masked)})" if masked else "")
    if claimed == "migrated":
        why = "it must be handed back" if not convertible else status
        return "❌ WRONG: claimed migrated but " + why
    if not convertible:
        return "\U0001f6d1 handed back (correct): " + CATEGORY.get(cat, cat)
    return "⚠️ handed back: " + ("result differs" if status == "mismatch" else status)


@st.cache_data
def load() -> tuple[dict, dict[str, dict]]:
    results = json.loads(RESULTS.read_text(encoding="utf-8"))
    objects = {o["id"]: o for o in json.loads(OBJECTS.read_text(encoding="utf-8"))}
    return results, objects


def render_object(obj: dict, res: dict | None) -> None:
    st.subheader(f"{obj['id']} · {obj['title']}")
    deps = ", ".join(obj.get("depends_on") or []) or "none"
    st.caption(
        f"estate {obj.get('estate', 'C')} · {obj['kind']} · category: "
        f"{CATEGORY.get(obj.get('category', 'convertible'))} · depends on: {deps} · "
        f"features: {', '.join(obj['features'])}"
    )
    if res:
        b = badge(res)
        text = (
            f"**{b}** · {res['attempts']} attempt(s), {res['calls']} model call(s), "
            f"${res['cost_usd']:.4f}"
        )
        box = st.success if b.startswith("✅") else st.error if "WRONG" in b else st.warning
        box(text)
        with st.expander("Evidence and trace", expanded=not b.startswith("✅")):
            st.code((res.get("reason") or "") + "\n" + (res.get("evidence") or ""), language="text")
            for step in res.get("trace", []):
                detail = ", ".join(f"{k}: {str(v)[:160]}" for k, v in step.items() if k != "step")
                st.markdown(f"- `{step.get('step')}` {detail}")
    left, right = st.columns(2)
    with left:
        st.markdown("**Hive / Spark SQL (source)**")
        st.code(obj["hive_sql"], language="sql")
    with right:
        st.markdown("**Snowflake SQL (agent)**")
        st.code((res or {}).get("sql") or "-- handed back before conversion", language="sql")


def graph(objects: dict[str, dict], results: dict[str, dict], estate: str) -> str:
    lines = ["graph LR"]
    for o in objects.values():
        if o.get("estate") != estate:
            continue
        r = results.get(o["id"])
        if not r:
            mark = ""
        else:
            b = badge(r)
            mark = ":::ok" if b.startswith("✅") else ":::hb" if "handed back" in b else ":::bad"
        lines.append(f'  {o["id"]}["{o["id"]}"]{mark}')
        for d in o.get("depends_on") or []:
            lines.append(f"  {d} --> {o['id']}")
    lines += [
        "  classDef ok fill:#d4edda,stroke:#28a745",
        "  classDef hb fill:#fff3cd,stroke:#ffc107",
        "  classDef bad fill:#f8d7da,stroke:#dc3545",
    ]
    return "\n".join(lines)


def mode_report() -> None:
    results, objects = load()
    agg = results["aggregate"]
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Objects", agg["objects"])
    c2.metric("Verified", f"{agg.get('verified_pct', agg['matches_pct'])}%")
    c3.metric("Wrong but claimed migrated", agg["wrong_claimed_migrated"])
    c4.metric(
        "Handed to a human",
        f"{agg['needs_human_pct']}%",
        help=f"hand-back precision (really needed a person): {agg.get('hand_back_precision_pct')}%",
    )
    c5.metric("$ per object", f"{agg['usd_per_object']:.4f}")
    st.caption(
        "Held-out test objects, estate agent on Claude Haiku 5.5. Verified = converted, run on "
        "Snowflake and equal to the Hive (Spark) result. Hand-backs are correct when the object "
        "has no Snowflake equivalent or its result cannot be verified."
    )
    by_estate = agg.get("by_estate", {})
    if by_estate:
        st.markdown("**Per estate**")
        st.dataframe(
            [{"estate": f"{k} - {ESTATES.get(k, k)}", **v} for k, v in by_estate.items()],
            width="stretch",
            hide_index=True,
        )
    by_id = {r["id"]: r for r in results["objects"]}
    estate = st.radio(
        "Estate", list(ESTATES), format_func=lambda k: f"{k}: {ESTATES[k]}", horizontal=True
    )
    ids = [r["id"] for r in results["objects"] if objects[r["id"]].get("estate") == estate]
    if estate == "A":
        st.markdown(
            "**Dependency graph** (green verified, amber handed back, red wrong claim; "
            "uncoloured objects are dev-split, not in the test run)"
        )
        st.markdown(f"```mermaid\n{graph(objects, by_id, estate)}\n```")
    rows = [
        {
            "id": i,
            "kind": objects[i]["kind"],
            "category": CATEGORY.get(objects[i].get("category", "convertible")),
            "outcome": badge(by_id[i]),
            "attempts": by_id[i]["attempts"],
            "calls": by_id[i]["calls"],
            "title": objects[i]["title"],
        }
        for i in ids
    ]
    st.dataframe(rows, width="stretch", hide_index=True, height=min(480, 38 * (len(rows) + 1)))
    if ids:
        choice = st.selectbox(
            "Open an object", ids, format_func=lambda i: f"{i} · {objects[i]['title']}"
        )
        render_object(objects[choice], by_id[choice])


def mode_convert() -> None:
    st.markdown(
        "Paste Hive / Spark SQL. You get a triage verdict and a Snowflake draft from sqlglot "
        "(rule-based, free) and, with your own Anthropic key, the agent's model conversion. "
        "**Unverified:** the demo has no Snowflake attached to run it, so treat the output as a "
        "draft. In the real pipeline every object is run on both sides and compared before it is "
        "called migrated."
    )
    key = st.text_input(
        "Anthropic API key (optional; used for this request only, never stored)", type="password"
    )
    sql = st.text_area(
        "Hive SQL", height=220, placeholder="SELECT nvl(a, 0) ... LATERAL VIEW explode(...) ..."
    )
    if st.button("Convert", type="primary", disabled=not sql.strip()):
        from hive2snow.estate_agent import triage
        from hive2snow.objects import MigrationObject

        t = triage(MigrationObject("X", "query", "pasted", sql), source_checks=False)
        if t.category not in ("convertible", "maskable"):
            st.warning(
                f"**Triage: {CATEGORY[t.category]}.** {t.reason}. In the pipeline this object is "
                "handed to a person instead of converted."
            )
        draft, err = sqlglot_transpile(sql)
        st.markdown("**sqlglot draft**")
        st.code(draft or f"-- sqlglot could not transpile: {err}", language="sql")
        if key.strip():
            try:
                from hive2snow.agent import DRAFT_PROMPT, Repair
                from hive2snow.llm import parse_full
                from hive2snow.systems import SYSTEM

                out, info = parse_full(
                    "claude-haiku-5-5",
                    DRAFT_PROMPT.format(hive=sql),
                    Repair,
                    system=SYSTEM,
                    max_tokens=8000,
                    api_key=key.strip(),
                )
                st.markdown("**Agent conversion (Claude Haiku 5.5)**")
                st.code(out.snowflake_sql, language="sql")
                st.caption(f"{out.what_changed} · ${info.cost_usd:.4f}")
            except Exception as e:  # shown to the visitor, key never logged
                st.error(f"Model call failed: {type(e).__name__}. Check the key and try again.")


def main() -> None:
    st.set_page_config(page_title="Hive -> Snowflake Migration Agent", layout="wide")
    st.title("Hive → Snowflake Migration Agent")
    st.markdown(
        "Migrates a Hive / Spark SQL **estate** (queries, ETL scripts, DDL, with their "
        "dependencies) to Snowflake and **proves each object** by running both sides on the same "
        "data. Objects with no Snowflake equivalent or an unverifiable result are handed back "
        f"with the reason. Public TPC-DS data, no client code. [Source on GitHub]({REPO_URL})"
    )
    mode = st.radio(
        "Mode",
        ["Estate migration report (test objects)", "Convert your own Hive SQL"],
        horizontal=True,
        label_visibility="collapsed",
    )
    if mode.startswith("Estate"):
        mode_report()
    else:
        mode_convert()


main()
