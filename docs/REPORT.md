# Hive → Snowflake Migration Agent: results report

## 1. Summary

Moving a Hive / Spark SQL estate to Snowflake means converting hundreds of queries, ETL scripts
and table definitions, and then proving each one still gives the same answer. Conversion tools
do the first part; the second part is where the weeks go.

This prototype does both for one workflow: give it a Hive object, get Snowflake SQL **with
proof**: the object was run on both sides on the same data and the results compared. Three
systems were measured on 70 objects over public TPC-DS data:

| | v0: sqlglot (rule-based) | v0-LLM: one call, Claude Haiku 5.5 | **v1: agent, Claude Haiku 5.5** |
|---|---|---|---|
| Same result as the source (held-out objects) | 26% | 82% (81-84%) | **100%** |
| Wrong but claimed migrated (safety) | 23 | 5.7 (5-6) | **0** |
| Handed to a human | 0% | 0% | 0% |
| $ per object | $0 | $0.0009 | $0.0008 |

**In one line:** the transpiler and the single model call each leave a share of silently wrong
conversions that a client would discover in production; the agent, by running both sides and
comparing, converts more of the objects *and* knows which ones it did not, so the review effort
goes only there.

## 2. Setup

**Source and target.** The source is local Spark SQL over TPC-DS data at scale factor 0.1
(24 tables, ~1M rows), standing in for a Hive estate: Spark SQL runs HiveQL (the same functions,
`LATERAL VIEW`, complex types, `INSERT OVERWRITE ... PARTITION`). The target is a Snowflake trial
loaded from the same Parquet files. One caveat about the build machine: it has no Hadoop native
binaries (Windows), so Spark cannot write table files; the write statements of ETL objects run
through a small in-memory emulator (`hive_writes.py`, tested), and every SELECT runs on Spark.

**Objects (70).**
- 40 TPC-DS queries rewritten the way legacy Hive code is written (`nvl`, `substr`,
  `date_add`, `cast(... as string)`, backticks, session `SET` preambles). Each rewrite is accepted
  only if its Spark result equals DuckDB's result for the original query: an independent engine
  proves the rewrite faithful. 7 of the 99 queries were skipped (empty results at this scale, one
  rewrite that differed).
- 20 hand-written legacy scripts: `LATERAL VIEW explode`, `collect_set` / `concat_ws`, Hive
  date functions (`from_unixtime`, `datediff` argument order, `add_months`), string functions
  (`instr`, `locate`, `regexp_extract`, `rlike`, `translate`), `if` / `nvl` / `isnull`, `WITH
  ROLLUP` and `GROUPING__ID`, window functions, `DISTRIBUTE BY` / `SORT BY`, `named_struct` /
  maps / arrays, `get_json_object`, timestamps, `percentile_approx`, reserved words in backticks,
  `LEFT SEMI JOIN`, `str_to_map`, and four ETL scripts (static and dynamic `INSERT OVERWRITE
  ... PARTITION`, CTAS, `INSERT INTO`, self-overwrite).
- 10 DDLs: partitioned Parquet / ORC tables, complex and nested types, an external delimited
  table, buckets, every primitive type, reserved-word columns, and two views compared by content.

**Ground truth** is the object's source result (or, for a DDL table, the columns it declares).
Exact by construction; nothing to label. **Split:** frozen, stratified by kind: 39 dev
objects for building, 31 test objects run once at the end (3 repeats for model systems).

**Metrics**, as a client reads them:
- executes on Snowflake; **same result as the source** (shape, cells within a numeric tolerance,
  order-insensitive unless the query orders; DDL by column names and type classes);
- claimed migrated, and **wrong but claimed migrated** (the safety metric, target 0);
- handed to a human, and how many of those were actually fine (wasted review);
- model calls, seconds and $ per object.

**Systems.**
- **v0 (sqlglot):** `transpile(read="hive", write="snowflake")`. It cannot know whether it is
  right, so it claims "migrated" whenever it produced SQL.
- **v0-LLM:** one model call with the conversion rules (Haiku 5.5; Opus 5.5 where run).
- **v1 (agent):** sqlglot draft → run on Snowflake → repair with the error (up to 3) → compare
  with the source result → repair with the diff (up to 2) → *migrated* only on a match. Haiku
  5.5, escalation to Sonnet 5.5 on the final attempt ([architecture.md](architecture.md)).

## 3. Building v1 (dev split)

| Step | Same result | Wrong but claimed migrated | Handed to a human | What changed |
|---|---|---|---|---|
| v0 sqlglot | 36% | 25 of 39 | 0% | Rule-based transpile. Quoted lower-case identifiers (`"customer_address"`) don't resolve in Snowflake; `LATERAL VIEW`, `WITH ROLLUP`, `STORED AS` -> `FILE_FORMAT`, Hive date/number functions fail |
| v0+ sqlglot, unquoted | 64% | 14 of 39 | 0% | The first fix a team makes (drop the quotes) doubles the hit rate; every remaining failure is still claimed migrated |
| v0-LLM Haiku | 77% | 9 of 39 | 0% | Syntax mostly right; wrong on semantics it cannot see: NULLs sort first in Hive and last in Snowflake, so every `ORDER BY` over a nullable column reorders |
| v1 agent, first run | 97% | **0** | 1 (2.6%) | Running both sides turns those semantic errors into visible diffs the model fixes (`NULLS FIRST`, casts out of `FLATTEN`, `INSERT OVERWRITE PARTITION` -> `DELETE` + `INSERT`). The one hand-back was a comparison artefact: Snowflake prints arrays as multi-line JSON |
| + JSON-normalised diff cells | **100%** | **0** | 0 | A generic fix to the comparison (parse and re-serialise JSON-looking cells), not to the agent |

Ablation ([ABLATION_dev.md](../evals/results/ABLATION_dev.md)):

| Variant | Same result | Wrong but claimed migrated | Handed to a human | Calls / object | $ / object |
|---|---|---|---|---|---|
| v1 (all steps) | 100% | 0 | 0% | 0.74 | $0.0007 |
| without the sqlglot draft (model drafts) | 100% | 0 | 0% | 1.15 | $0.0009 |
| repairs without the error / diff text | 92% | 0 | 8% | 0.82 | $0.0007 |
| without data-diff repairs (errors only) | 95% | 0 | 5% | 0.69 | $0.0012 |
| without escalation to Sonnet | 100% | 0 | 0% | 0.74 | $0.0007 |
| v0-LLM Opus 5.5, one call (reference) | 95% | 2 | 0% | 1.0 | $0.0199 |

What the ablation says:

- **The feedback is the agent.** Take away the error and diff text (the model is only told "it failed") and three objects are handed back that the full agent converts. Take away the diff-driven repairs and two semantic mismatches are handed back: without a data comparison nothing can tell the model that `nvl` on a `MAP` column or `percentile_approx` on an array came back in a different shape.
- **The sqlglot draft is a cost lever, not an accuracy lever.** The model drafting from scratch reaches the same 100% with 55% more calls. Free rule-based drafts cover most objects outright (26 of 39 needed no model call at all).
- **Escalation to Sonnet was never needed** on this set: Haiku fixed every object within the repair budget. It stays as a safety valve.
- **Safety is a property of the method, not of the model.** Every v1 variant, including the weakened ones, reports 0 wrong-but-claimed-migrated: whatever the model gets wrong is caught by running it. The one-call systems have no such check, so their wrong conversions are silent.

## 4. Final results (held-out test split)

| Metric | v0 - sqlglot (rule-based) | v0+ - sqlglot, identifiers unquoted | v0-LLM - Haiku 5.5, one call | v0-LLM - Opus 5.5, one call | v1 - agent, Haiku 5.5 |
|---|---|---|---|---|---|
| Executes on Snowflake | 29% | 68% | 100% | 99% (97%-100%) | 100% |
| **Same result as the source** | 26% | 64% | 82% (81%-84%) | 95% (94%-97%) | 100% |
| Claimed migrated | 100% | 100% | 100% | 99% (97%-100%) | 100% |
| **Wrong but claimed migrated** | 23 | 11 | 5.7 (5-6) | 1.3 (1-2) | 0 |
| Handed to a human | 0% | 0% | 0% | 1% (0%-3%) | 0% |
| ...of which were actually fine | 0 | 0 | 0 | 0 | 0 |
| Model calls / object | 0.0 | 0.0 | 1.0 | 1.0 (1.0-1.0) | 0.8 (0.7-0.9) |
| Seconds / object | 0 | 0 | 8 (8-8) | 10 (10-10) | 7 (7-8) |
| $ / object | $0.0000 | $0.0000 | $0.0009 | $0.0215 ($0.0209-$0.0219) | $0.0008 ($0.0008-$0.0009) |

- **v1 is at the ceiling on this set**: all 31 held-out objects verified equal to the source in all three runs, including the four ETL scripts with partition overwrites and the two views. Nothing was handed back, so a client would have had zero review work and zero silent errors. Cost: about $0.0008 per object; 0.8 model calls per object (18-21 of the 31 objects needed none: the sqlglot draft already matched).
- **The one-call systems' errors are semantic, not syntactic.** They executed on 99-100% of objects; the failures are results that differ: Hive sorts NULLs first and Snowflake last (Q15, Q34, L18, L20 in every Haiku run; L20 in two Opus runs), `add_months` on a month-end date (Hive keeps the day, Snowflake snaps to the month end: L03, Opus), decimal division in a `ROLLUP` (Q36). A transpiler and a model both produce plausible SQL here; only running it shows the difference.
- **The frontier model is not the fix.** Opus converts 95% in one call but still claims 1-2 wrong objects per run as migrated, at 27x v1's cost. The verification loop on the cheap model beats it on both accuracy and safety.
- **Determinism:** v1's results were identical across the three runs; only the number of repair calls varied (23-28 in total).

## 5. Failure analysis and limitations

- **No v1 failures on dev or test.** The residual risk is in the comparison itself: it is only as good as the normalisation rules (numeric tolerance 1e-6 relative / 0.005 absolute, dates as ISO strings, JSON-looking cells re-serialised, order-insensitive unless the query orders). A rule that is too lenient could hide a real difference; the rules are small, tested and listed in `diff.py`.
- **The object set is the limit.** 70 objects over one schema cover the common Hive patterns, but a real estate has UDFs, Hive-specific serdes, multi-insert statements, `TABLESAMPLE`, locale-dependent string collation and much larger result sets, where a cell-by-cell comparison needs sampling or checksums. The method carries over; the coverage does not.
- **`percentile_approx` is approximate by definition.** The agent converted L13 to `APPROX_PERCENTILE` / `MEDIAN` forms that happened to match on this data; on other data an exact match would not be guaranteed and the object would correctly be handed to a human.
- **Result-set sizes.** Comparisons pull both result sets to the client. TPC-DS queries return at most a few hundred rows; ETL tables a few hundred rows. For large tables the comparison would move to Snowflake / Spark side checksums.

- **Prototype scope.** One workflow: SQL objects over a loaded schema. No data movement at
  scale, cut-over, orchestration, Java UDFs or performance work. Real estates also have
  dialect features TPC-DS and 30 hand-written objects don't cover.
- **Emulated writes on the source.** Hive write statements run through an in-memory emulator
  on the source side (the Windows machine lacks Hadoop natives). Its semantics (static and
  dynamic partition overwrite, append, CTAS, drop) are unit-tested, and only the 4 ETL objects
  and 2 views depend on it.
- **Small test split.** 31 objects; one object is about 3 points.
- **Verification needs the source.** The agent's proof relies on running the original on the
  source system. That is the normal situation in a migration (the Hive cluster is still there),
  and it is why the method works: the data, not the model, decides.

## 6. Reproduce

```bash
uv sync --group dev --extra agent --extra app
uv run python scripts/build_data.py            # DuckDB dsdgen -> Parquet -> Spark + Snowflake
uv run python scripts/build_objects.py         # verify and freeze objects + truths
uv run pytest -q
uv run python scripts/run_eval.py --system v0 --split test
uv run python scripts/run_eval.py --system v1 --model haiku --split test --tag r1
uv run python scripts/final_table.py
```
