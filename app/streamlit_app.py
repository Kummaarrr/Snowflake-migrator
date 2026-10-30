"""Hive -> Snowflake Migration Agent: demo.

Mode A: the migration report of the held-out test objects, pre-computed (no key needed).
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
BADGE = {
    "match": "✅ migrated, result verified",
    "mismatch": "⚠️ needs a human: result differs",
    "error": "❌ needs a human: Snowflake error",
}


@st.cache_data
def load() -> tuple[dict, dict[str, dict]]:
    results = json.loads(RESULTS.read_text(encoding="utf-8"))
    objects = {o["id"]: o for o in json.loads(OBJECTS.read_text(encoding="utf-8"))}
    return results, objects


def render_object(obj: dict, res: dict | None) -> None:
    st.subheader(f"{obj['id']} · {obj['title']}")
    st.caption(f"kind: {obj['kind']} · features: {', '.join(obj['features'])}")
    if res:
        status = res["status"]
        (st.success if status == "match" else st.warning)(
            f"**{BADGE[status]}** · claimed: {res['verdict'].replace('_', ' ')} · "
            f"{res['attempts']} attempt(s), {res['calls']} model call(s), ${res['cost_usd']:.4f}"
        )
        with st.expander("Evidence (what was run and compared)", expanded=status != "match"):
            st.code(res["evidence"], language="text")
            if res.get("trace"):
                st.markdown("**Trace**")
                for step in res["trace"]:
                    st.markdown(
                        f"- `{step.get('step')}` "
                        + ", ".join(f"{k}: {str(v)[:160]}" for k, v in step.items() if k != "step")
                    )
    left, right = st.columns(2)
    with left:
        st.markdown("**Hive / Spark SQL (source)**")
        st.code(obj["hive_sql"], language="sql")
    with right:
        st.markdown("**Snowflake SQL (agent)**")
        st.code(res["sql"] if res else "", language="sql")


def mode_samples() -> None:
    results, objects = load()
    agg = results["aggregate"]
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Objects", agg["objects"])
    c2.metric("Same result as the source", f"{agg['matches_pct']}%")
    c3.metric("Wrong but claimed migrated", agg["wrong_claimed_migrated"])
    c4.metric("$ per object", f"{agg['usd_per_object']:.3f}")
    st.caption(
        "Held-out test objects, agent on Claude Haiku 5.5, verified by running the converted "
        "SQL on Snowflake and comparing with the Spark (Hive) result."
    )
    by_id = {r["id"]: r for r in results["objects"]}
    rows = [
        {
            "id": r["id"],
            "kind": r["kind"],
            "status": BADGE[r["status"]].split(":")[0],
            "attempts": r["attempts"],
            "calls": r["calls"],
            "title": objects[r["id"]]["title"],
        }
        for r in results["objects"]
    ]
    st.dataframe(rows, width="stretch", hide_index=True, height=min(420, 38 * (len(rows) + 1)))
    choice = st.selectbox(
        "Open an object",
        [r["id"] for r in results["objects"]],
        format_func=lambda i: f"{i} · {objects[i]['title']}",
    )
    render_object(objects[choice], by_id[choice])


def mode_convert() -> None:
    st.markdown(
        "Paste Hive / Spark SQL. You get a Snowflake draft from sqlglot (rule-based, free) and, "
        "with your own Anthropic key, the agent's model conversion. **Unverified:** the demo has "
        "no Snowflake attached to run it, so treat the output as a draft. In the real pipeline "
        "every object is run on both sides and compared before it is called migrated."
    )
    key = st.text_input(
        "Anthropic API key (optional; used for this request only, never stored)", type="password"
    )
    sql = st.text_area(
        "Hive SQL", height=220, placeholder="SELECT nvl(a, 0) ... LATERAL VIEW explode(...) ..."
    )
    if st.button("Convert", type="primary", disabled=not sql.strip()):
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
            except Exception as e:
                st.error(f"Model call failed: {type(e).__name__}. Check the key and try again.")


def main() -> None:
    st.set_page_config(page_title="Hive -> Snowflake Migration Agent", layout="wide")
    st.title("Hive → Snowflake Migration Agent")
    st.markdown(
        "Converts Hive / Spark SQL queries, ETL scripts and table DDL to Snowflake and **proves "
        "each one** by running both sides on the same data and comparing the results. "
        f"Public TPC-DS data, no client code. [Source on GitHub]({REPO_URL})"
    )
    mode = st.radio(
        "Mode",
        ["Migration report (test objects)", "Convert your own Hive SQL"],
        horizontal=True,
        label_visibility="collapsed",
    )
    if mode.startswith("Migration"):
        mode_samples()
    else:
        mode_convert()


if __name__ == "__main__":
    main()
else:
    main()
