# Architecture

## The idea in one sentence

Conversion is cheap; proof is what costs a migration team its weeks. So the agent runs every
converted object on Snowflake, compares the result with the source's result, and only calls an
object "migrated" when the two agree. The model writes SQL; running it decides. And an estate
contains objects that must *not* be converted: the agent's job includes saying so, with the
reason.

## v2: the estate agent

```
estate (Hive files)
  |
  v
triage (estate_agent.triage)  no model calls
  |  needs porting?        jar / metastore UDF, TRANSFORM script, non-JDK reflect -> hand back: unconvertible (+ fix hint)
  |  needs environment?    LOAD DATA INPATH, INSERT OVERWRITE DIRECTORY, LOCATION   -> hand back: needs_config (+ what is needed)
  |  not verifiable?       rand, sampling, collect_list, first(), LIMIT w/o ORDER BY,
  |                        ranking / LIMIT over ties (probed on the source), clock in a
  |                        filter, two source runs differ                           -> hand back: nondeterministic
  |  clock in the output?  load_ts = current_timestamp()                            -> mask those columns, continue
  v
dependency order (deps.migration_order)   upstream handed back? -> hand back: blocked
  |
  v
per object (agent.migrate, the v1 loop)
  sqlglot draft -> run on Snowflake --error--> repair with the error (model) x3
                        | ok
                        v
             compare with the source result --differs--> repair with the diff (model) x2
             (small: cell by cell; large: in-warehouse aggregates + ordered sample)
                        | match
                        v
             migrated (+ evidence, + "masked: load_ts" when masked)  |  needs human (+ last error / diff)
  |
  v
re-verify downstream objects whose upstream changed after they were checked -> estate report
```

| Step | Module | Model? | What it does |
|---|---|---|---|
| triage | `estate_agent.triage` | no | Rules over the SQL (comments and string literals stripped), unknown-function detection against Spark's built-in list (`hive_builtins.py`, macros defined in the script excepted), clock-column mapping to output columns (through the table DDL for INSERTs), `LIMIT` without `ORDER BY` anywhere, tie-sensitive window functions without an order; then source probes: duplicate (partition, order) keys under `row_number`-like functions, ties at a top-level `LIMIT` cut, and a second run with a different shuffle layout. |
| order | `deps` | no | Tables read / written extracted with sqlglot; topological order; an object whose upstream was handed back is handed back as *blocked* rather than repaired against a missing table. |
| draft | `systems.sqlglot_transpile` | no | Rule-based Hive -> Snowflake transpile. Free, instant, right for the easy majority. |
| run | `harness.evaluate` -> `engines.Snowflake` | no | Executes the SQL in a per-object clean state (the object's own tables and views dropped first). |
| compare | `harness.evaluate` -> `diff.compare` | no | Source result (Spark, cached, with upstream objects replayed) vs target result: same shape, cells by position with numeric tolerance; order-insensitive unless the query orders, and when the ORDER BY keys are output columns the key sequence is checked row by row while the rows themselves are compared as a multiset (tied rows may come in either order). Masked columns are dropped from both sides. Large results: per-column aggregates computed inside each engine plus a 500-row ordered sample. DDL tables: `DESCRIBE TABLE` vs the declared columns. |
| repair | `agent.migrate` | Haiku 5.5; Sonnet 5.5 on the final attempt | Gets the Hive SQL, the current Snowflake SQL and the feedback (error text or data diff); returns complete SQL. May give up, which hands the object to a human. |
| verdict | `agent.migrate` | no | *migrated* only on a data match (with the mask named when one was used). Otherwise *needs human* with the evidence. |
| re-verify | `estate_agent._reverify` | no | Objects verified before an upstream object was (re)converted are evaluated again; a failure turns the verdict into a hand-back. |

## Scoring (`scoring.py`)

Every object has a ground-truth category: *convertible*, *maskable* (convertible once clock
columns are masked), *unconvertible*, *needs_config*, *nondeterministic*, *blocked*. The system's
decision is *migrated* or a hand-back of some kind (read from its reason). From the two:
**verified** (migrated, right, truly convertible, nothing real masked), **wrong but claimed
migrated** (a wrong result, a lucky match on an object that must be handed back, or a real
column masked away), hand-back **precision** and **recall**, whether the hand-back **reason** was
the right kind, **triage false positives**, **wasted hand-backs** (the conversion was right),
**dependency-order violations**, cost - overall, per estate, per kind, per category, plus the
truth x decision confusion matrix. The verifier itself is measured by `scripts/mutation_test.py`:
known semantic bugs are injected into verified conversions and the catch rate reported.

## Baselines

- **v0 (sqlglot):** the transpile alone. It cannot know whether it is right, so it claims
  "migrated" whenever it produced SQL. **v0+:** the same with identifier quoting removed.
- **v0-LLM:** one model call, "convert this" (Haiku 5.5; Opus 5.5 as the expensive reference).
  The model may decline (needs human).
- **v1:** the per-object loop above without triage, ordering or re-verification.

## Source and target

- **Source:** local Spark SQL over TPC-DS Parquet files, standing in for Hive (Spark SQL runs
  HiveQL: the same functions, `LATERAL VIEW`, complex types, `INSERT OVERWRITE ... PARTITION`,
  multi-insert). Two datasets: scale 0.1 for estates B and C, scale 1 for estate A. On this
  Windows machine Spark cannot write table files (no Hadoop native binaries), so write
  statements of ETL objects are executed by a small emulator over in-memory views
  (`hive_writes.py`: partition-level overwrite semantics, multi-insert, CTAS, `hivevar`
  substitution and `TEMPORARY MACRO` expansion); every SELECT runs on Spark itself. Objects
  whose Hive and Spark 3 semantics differ are tagged `spark3_semantics`.
- **Target:** a Snowflake trial, loaded from the same Parquet files through an internal stage
  (chunked for the scale-1 tables). A key-pair service user; nothing is cached on the target
  side (its runs are the measurement). Both sessions run in UTC.
- **Upstream for a split run:** a test report may depend on a dev aggregate. `run_eval`
  provisions such upstream objects on Snowflake from `data/reference_conversions.json`
  (verified dev conversions), so every system is measured against the same correct upstream.

## Objects and ground truth

Three estates, frozen in `data/objects.json` with category, dependencies, split and the
`source_stable` flag from a double run (see the report for counts): **A** a layered pipeline at
scale 1 (staging views -> partitioned aggregates -> reports, with one chain blocked by a metastore
UDF), **B** hand-written legacy traps across all six categories, **C** TPC-DS queries rewritten
into legacy Hive style, each accepted only if its Spark result equals DuckDB's result for the
original. Truth = the source result (or declared columns for a DDL table) plus the category.

## Demo

`app/streamlit_app.py`: the estate report (pre-computed from a test run: per-estate table,
dependency graph, per-object verdict with evidence), and "convert your own Hive SQL": triage
plus a sqlglot draft, and a model conversion with the visitor's own Anthropic key (unverified:
visitors have no Snowflake attached).
