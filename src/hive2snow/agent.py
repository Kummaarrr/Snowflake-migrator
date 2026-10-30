"""v1: the migration agent.

    draft (sqlglot) -> run on Snowflake -> [error? repair with the error] x N
                    -> compare with the source result -> [differs? repair with the diff] x M
                    -> verdict from evidence

The model only writes SQL. Whether the SQL is right is decided by running it: the target's
error text and the data diff are the feedback, and the verdict is "migrated" only when the
target result equals the source result. Anything else is "needs human", with the last error or
diff attached, so the client's time goes to those objects only. The last repair attempt can
escalate to a stronger model.
"""

from __future__ import annotations

from dataclasses import dataclass

from pydantic import BaseModel, Field

from hive2snow import harness
from hive2snow.engines import Snowflake
from hive2snow.llm import CallInfo, parse_full
from hive2snow.objects import MigrationObject
from hive2snow.systems import SYSTEM, Conversion, sqlglot_transpile

HAIKU, SONNET = "claude-haiku-5-5", "claude-sonnet-5-5"


@dataclass(frozen=True)
class Config:
    model: str = HAIKU
    escalate_model: str | None = SONNET  # for the final attempt
    use_sqlglot_draft: bool = True
    error_rounds: int = 3  # repairs driven by a Snowflake error
    diff_rounds: int = 2  # repairs driven by a data diff
    feedback: bool = True  # False: repairs see only "it failed", no error / diff text (ablation)


class Repair(BaseModel):
    snowflake_sql: str = Field(description="Complete corrected Snowflake SQL, all statements")
    what_changed: str = Field(description="One line: what was wrong and what you changed")
    give_up: bool = Field(description="True only if a faithful conversion is impossible")


GUIDE = """You are converting one Hive / Spark SQL object to Snowflake. The Snowflake schema has the
same tables and columns as the Hive source (TPC-DS). Rules:
- Same result: same columns in the same order, same rows and values.
- Drop Hive session statements (SET, USE, ADD JAR) and storage clauses (STORED AS, ROW FORMAT,
  LOCATION, TBLPROPERTIES, CLUSTERED BY); PARTITIONED BY columns become ordinary columns
  (declare them after the others). Keep every DDL, INSERT and SELECT statement otherwise.
- Hive INSERT OVERWRITE TABLE t PARTITION (k = v) SELECT ... replaces only that partition:
  in Snowflake, DELETE the matching rows then INSERT (or use MERGE); the partition value is an
  extra column. A dynamic PARTITION (k) takes k from the SELECT's trailing columns. INSERT
  OVERWRITE without a partition replaces the whole table.
- Function mapping is your job (nvl -> COALESCE/NVL, datediff(a, b) in Hive is DATEDIFF('day', b,
  a), from_unixtime / unix_timestamp, date_format patterns -> TO_CHAR formats, LATERAL VIEW
  explode -> LATERAL FLATTEN, collect_set -> ARRAY_UNIQUE_AGG / ARRAY_AGG(DISTINCT), str_to_map,
  get_json_object -> GET_PATH(PARSE_JSON(...)), percentile_approx, GROUPING__ID -> GROUPING_ID,
  WITH ROLLUP, Hive integer division and rounding semantics, ...).
- Hive's 0-based array index, map access, named_struct -> OBJECT_CONSTRUCT and so on. In
  Snowflake, LATERAL FLATTEN returns VARIANT values: cast them (::STRING, ::INT) so types match.
- Return the complete SQL every time (all statements, separated by ';')."""

DRAFT_PROMPT = (
    GUIDE
    + """

Hive SQL:
```sql
{hive}
```
Write the Snowflake SQL."""
)

REPAIR_PROMPT = (
    GUIDE
    + """

Hive SQL (the source of truth):
```sql
{hive}
```
Current Snowflake SQL (attempt {attempt}):
```sql
{current}
```
{feedback}
Fix the Snowflake SQL."""
)


def _feedback(kind: str, text: str, cfg: Config) -> str:
    if not cfg.feedback:
        return (
            "Result: it failed." if kind == "error" else "Result: the data differs from the source."
        )
    if kind == "error":
        return f"Snowflake rejected it with this error:\n{text}"
    return (
        "It ran, but the result differs from the source result (Hive / Spark):\n"
        + text
        + "\nNumbers are compared with a small tolerance; column names don't matter, "
        "order matters only if the query orders."
    )


def migrate(obj: MigrationObject, sf: Snowflake, cfg: Config | None = None) -> Conversion:
    cfg = cfg or Config()
    calls: list[CallInfo] = []
    trace: list[dict] = []
    current = ""
    if cfg.use_sqlglot_draft:
        current, err = sqlglot_transpile(obj.hive_sql)
        trace.append({"step": "sqlglot", "ok": not err, "error": err})
    if not current:
        out, info = parse_full(
            cfg.model,
            DRAFT_PROMPT.format(hive=obj.hive_sql),
            Repair,
            system=SYSTEM,
            max_tokens=8000,
        )
        calls.append(info)
        current = out.snowflake_sql
        trace.append({"step": "draft", "model": cfg.model, "note": out.what_changed})
        if out.give_up:
            return Conversion(
                current,
                "needs_human",
                f"model could not convert: {out.what_changed}",
                calls,
                1,
                trace,
            )
    attempt, error_used, diff_used = 1, 0, 0
    last_kind, last_text = "", ""
    while True:
        outcome = harness.evaluate(sf, obj, current)
        trace.append(
            {
                "step": "run",
                "attempt": attempt,
                "status": outcome.status,
                "evidence": outcome.evidence[:500],
            }
        )
        if outcome.status == "match":
            return Conversion(
                current,
                "migrated",
                f"target result equals the source result ({outcome.evidence})",
                calls,
                attempt,
                trace,
            )
        kind = "error" if outcome.status == "error" else "diff"
        budget_left = (
            (cfg.error_rounds - error_used) if kind == "error" else (cfg.diff_rounds - diff_used)
        )
        if budget_left <= 0:
            break
        if kind == "error":
            error_used += 1
        else:
            diff_used += 1
        last_kind, last_text = kind, outcome.evidence
        final = (error_used + diff_used) >= (cfg.error_rounds + cfg.diff_rounds) - 1
        model = cfg.escalate_model if (final and cfg.escalate_model) else cfg.model
        prompt = REPAIR_PROMPT.format(
            hive=obj.hive_sql,
            current=current,
            attempt=attempt,
            feedback=_feedback(kind, outcome.evidence, cfg),
        )
        out, info = parse_full(model, prompt, Repair, system=SYSTEM, max_tokens=8000)
        calls.append(info)
        attempt += 1
        trace.append(
            {"step": "repair", "model": model, "note": out.what_changed, "give_up": out.give_up}
        )
        if out.give_up:
            return Conversion(
                out.snowflake_sql or current,
                "needs_human",
                f"model gave up: {out.what_changed}",
                calls,
                attempt,
                trace,
            )
        current = out.snowflake_sql or current
    why = "Snowflake error" if last_kind == "error" else "result differs from the source"
    return Conversion(
        current,
        "needs_human",
        f"{why} after {attempt} attempt(s): {last_text[:400]}",
        calls,
        attempt,
        trace,
    )
