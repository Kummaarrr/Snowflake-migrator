"""The systems compared.

- v0 (sqlglot):  rule-based transpile hive -> snowflake. It has no way to know whether it is
                 right, so it claims "migrated" whenever it produced SQL.
- v0-llm:        one model call, "convert this Hive SQL to Snowflake"; the model may say it
                 cannot, which counts as "needs human".
- v1 (agent):    agent.py - draft, run on the target, repair from the error, run on both
                 sides, repair from the data diff, verdict from evidence.

Every system returns a Conversion: the Snowflake SQL, its verdict and an explanation.
"""

from __future__ import annotations

from dataclasses import dataclass, field

import sqlglot
from pydantic import BaseModel, Field
from sqlglot import exp

from hive2snow.llm import CallInfo, parse_full
from hive2snow.objects import MigrationObject


@dataclass
class Conversion:
    sql: str
    verdict: str  # migrated | needs_human
    reason: str
    calls: list[CallInfo] = field(default_factory=list)
    attempts: int = 1
    trace: list[dict] = field(default_factory=list)

    @property
    def cost_usd(self) -> float:
        return sum(c.cost_usd for c in self.calls)


def sqlglot_transpile(hive_sql: str) -> tuple[str, str]:
    """Rule-based conversion; returns (sql, error)."""
    try:
        out = sqlglot.transpile(hive_sql, read="hive", write="snowflake", pretty=True)
        statements = [
            s
            for s in out
            if s.strip() and not s.strip().upper().startswith(("SET ", "USE ", "ADD JAR"))
        ]
        return ";\n".join(statements), ""
    except Exception as e:
        return "", f"{type(e).__name__}: {str(e).splitlines()[0][:300]}"


def sqlglot_transpile_unquoted(hive_sql: str) -> tuple[str, str]:
    """The stronger rule-based baseline: sqlglot, with identifier quoting removed. Hive's
    backticks are case-insensitive quoting; carried into Snowflake as double quotes they make
    lower-case names case-sensitive and nothing resolves. A migration team's first fix."""
    try:
        out = []
        for tree in sqlglot.parse(hive_sql, read="hive"):
            if tree is None:
                continue
            for ident in tree.find_all(exp.Identifier):
                ident.set("quoted", False)
            text = tree.sql(dialect="snowflake", pretty=True)
            if text.strip() and not text.strip().upper().startswith(("SET ", "USE ", "ADD JAR")):
                out.append(text)
        return ";\n".join(out), ""
    except Exception as e:
        return "", f"{type(e).__name__}: {str(e).splitlines()[0][:300]}"


def v0_sqlglot(obj: MigrationObject, unquoted: bool = False) -> Conversion:
    sql, err = (sqlglot_transpile_unquoted if unquoted else sqlglot_transpile)(obj.hive_sql)
    if err:
        return Conversion("", "needs_human", f"sqlglot could not transpile: {err}")
    return Conversion(
        sql, "migrated", "transpiled by sqlglot" + (" (identifiers unquoted)" if unquoted else "")
    )


class ConvertedSQL(BaseModel):
    snowflake_sql: str = Field(
        description="The complete Snowflake SQL; statements separated by ';'"
    )
    can_convert: bool = Field(description="False if the SQL cannot be converted faithfully")
    notes: str = Field(description="Assumptions, or why it cannot be converted (one line)")


SYSTEM = "You are a senior data engineer migrating a Hive / Spark SQL estate to Snowflake."
NAIVE_PROMPT = """Convert this Hive SQL to Snowflake SQL. Keep the same result (same columns in the \
same order, same rows). Drop Hive session settings (SET, USE, ADD JAR). Hive table properties \
(STORED AS, ROW FORMAT, LOCATION, TBLPROPERTIES, CLUSTERED BY) have no Snowflake equivalent: \
drop them; partition columns become ordinary columns. The tables exist in Snowflake with the \
same names and columns.

```sql
{sql}
```"""


def v0_llm(obj: MigrationObject, model: str) -> Conversion:
    out, info = parse_full(
        model, NAIVE_PROMPT.format(sql=obj.hive_sql), ConvertedSQL, system=SYSTEM, max_tokens=8000
    )
    verdict = "migrated" if out.can_convert and out.snowflake_sql.strip() else "needs_human"
    return Conversion(out.snowflake_sql, verdict, out.notes, [info])
