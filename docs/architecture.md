# Architecture

## The idea in one sentence

Conversion is cheap; proof is what costs a migration team its weeks. So the agent runs every
converted object on Snowflake, compares the result with the source's result, and only calls an
object "migrated" when the two agree. The model writes SQL; running it decides.

## v1 pipeline

```
Hive object --> sqlglot draft --> run on Snowflake --error--> repair (model, sees the error)  x3
                                       |                          ^
                                       ok                         |
                                       v                          |
                              compare with source result --differs--> repair (model, sees the diff) x2
                                       |
                                     match
                                       v
                          verdict: migrated (+ evidence) | needs human (+ last error / diff)
```

| Step | Module | Model? | What it does |
|---|---|---|---|
| draft | `systems.sqlglot_transpile` | no | Rule-based Hive -> Snowflake transpile. Free, instant, right for the easy majority. Session statements (`SET`, `USE`, `ADD JAR`) dropped. |
| run | `harness.evaluate` -> `engines.Snowflake` | no | Executes the SQL in a per-object clean state (the object's own tables and views dropped first). |
| compare | `harness.evaluate` -> `diff.compare` | no | Source result (Spark, cached) vs target result: same shape, cells by position with numeric tolerance, order-insensitive unless the query orders. For a DDL table: `DESCRIBE TABLE` vs the columns the Hive DDL declares (partition columns included, complex types as VARIANT / ARRAY / OBJECT). The diff text names the row, column and values. |
| repair | `agent.migrate` | Haiku 5.5; Sonnet 5.5 on the final attempt | Gets the Hive SQL, the current Snowflake SQL and the feedback (error text or data diff); returns complete SQL. May give up, which hands the object to a human. |
| verdict | `agent.migrate` | no | *migrated* only on a data match. Otherwise *needs human* with the evidence, so the team's time goes to the objects that need it, with the diff already in hand. |

## Baselines

- **v0 (sqlglot):** the transpile alone. It cannot know whether it is right, so it claims
  "migrated" whenever it produced SQL. The gap between its claims and its matches is what a
  team pays for in silent errors.
- **v0-LLM:** one model call, "convert this". The model may decline (needs human).

## Source and target

- **Source:** local Spark SQL over TPC-DS Parquet files (Spark SQL runs HiveQL: the same
  functions, `LATERAL VIEW`, complex types, `INSERT OVERWRITE ... PARTITION`). No cluster. On
  this Windows machine Spark cannot write table files (no Hadoop native binaries), so the few
  write statements of ETL objects are executed by a small emulator over in-memory views
  (`hive_writes.py`); every SELECT runs on Spark itself.
- **Target:** a Snowflake trial, loaded from the same Parquet files through an internal stage.
  A key-pair service user; nothing is cached on the target side (its runs are the measurement).

## Objects and ground truth

- 40 TPC-DS queries rewritten into legacy Hive style by `hiveify.py` (nvl, substr, date_add,
  backticks, session preambles, cast-as-string). Each rewrite is accepted only if its Spark
  result equals DuckDB's result for the original query: an independent engine proves the
  rewrite faithful.
- 20 hand-written legacy scripts (Hive functions, complex types, JSON, `WITH ROLLUP`,
  `DISTRIBUTE BY`, `LEFT SEMI JOIN`, `INSERT OVERWRITE` static and dynamic partitions, CTAS and
  appends) and 10 DDLs (partitions, complex and nested types, external text tables, buckets,
  reserved words, views).
- Truth = the object's source result; frozen split, test objects run only at the end.

## Demo

`app/streamlit_app.py`: the per-object migration report (pre-computed), and "convert your own
Hive SQL" with the visitor's own Anthropic key (conversion only, unverified: visitors have no
Snowflake attached).
