# Hive → Snowflake Migration Agent: results report (v2)

## 1. Summary

Moving a Hive / Spark SQL estate to Snowflake means converting hundreds of queries, ETL scripts
and table definitions, and then proving each one still gives the same answer. Conversion tools
do the first part; the second part is where the weeks go. And a real estate contains objects
that *cannot* be converted (custom UDFs, scripts) or *cannot be verified* (results that depend
on random numbers, the clock, or an undefined order): the right output for those is a hand-back
with the reason, not a plausible-looking conversion.

This project migrates **estates**: objects with dependencies, in order, each run on both sides
and compared, each handed back when it should be. Five systems were measured on
182 objects in three estates; the held-out results:

| | v0: sqlglot | v0+: sqlglot, unquoted | v0-LLM: one Opus 5.5 call | v1: per-object agent, Haiku 5.5 | **v2: estate agent, Haiku 5.5** |
|---|---|---|---|---|---|
| **Verified** (migrated, right, verifiable) | 29% | 48% | 80% (78-81%) | 80% (73-84%) | **87% (86-87%)** |
| **Wrong but claimed migrated** | 60 | 44 | 14.7 (13-16) | 2.7 (2-3) | **0** |
| Hand-back precision / recall | n/a / 0% | n/a / 0% | 78% (67-100%) / 18% | 65% (45-75%) / 82% | **97% (92-100%) / 100%** |
| $ per object | $0 | $0 | $0.0197 | $0.0008 | $0.0006 |

On 85 held-out objects the estate agent reached the verified ceiling in two of three runs (87%: 11 of the 85 must be handed back, and it handed back all 11 with the right reason) and missed it by one object in the third; it claimed nothing it could not prove in any run, at $0.0006 per object. The transpiler verified 29% and shipped 60 wrong objects as done; one Opus 5.5 call per object verified 80% and still shipped about 15 wrong objects per run, at 33 times the cost.

## 2. Setup

**Source and target.** The source is local Spark SQL over TPC-DS, standing in for a Hive
estate (Spark SQL runs HiveQL: the same functions, `LATERAL VIEW`, complex types, `INSERT
OVERWRITE ... PARTITION`, multi-insert). Two datasets: scale 0.1 (~1M rows) for estates B and
C, scale 1 (store_sales 2.9M rows) for the layered estate A. The target is a Snowflake trial
loaded from the same Parquet files. One caveat about the build machine: it has no Hadoop
native binaries (Windows), so Spark cannot write table files; Hive write statements (`CREATE
TABLE`, `INSERT OVERWRITE ... PARTITION`, `INSERT INTO`, CTAS, multi-insert) run through a
small in-memory emulator (`hive_writes.py`, unit-tested), and every SELECT runs on Spark.

**Estates (182 objects).**
- **A - retail analytics, layered (30 objects, scale 1):** 5 staging views → 7 aggregate
  tables (`INSERT OVERWRITE ... PARTITION`, CTAS + append, an ETL over another ETL) → 18
  reports. Dependencies are extracted from the SQL; a report is only right if its aggregate
  is. One staging view calls a metastore UDF nobody has the source of: it and the aggregate and
  two reports downstream of it must come back as *blocked*, not converted. Large results are
  verified in-warehouse (counts and per-column aggregates computed inside each engine, plus a
  500-row ordered sample compared cell by cell).
- **B - legacy ETL traps (70 objects):** 50 convertible objects that exercise
  known semantic differences (`size(NULL)`, `split` as a regex, casts that fail in Snowflake,
  integer division, NULL ordering, month-end `add_months`, `datediff` order, decimal and
  integer overflow, `GROUPING SETS`, window frames, multi-insert, `LATERAL VIEW OUTER`,
  `posexplode`, `json_tuple`, `stack`, maps and arrays of structs, reserved-word identifiers,
  regex back-references, the `YYYY` week-year pattern, `decode` as a charset decode,
  `TEMPORARY MACRO`, `hivevar` parameters, `LEFT SEMI JOIN`, `xpath`, partition overwrite over
  pre-loaded partitions), 2 **maskable** objects (right figures plus a `load_ts`
  clock column: convert, verify with the clock column masked, say so), 4
  **unconvertible** objects that need porting by a person (jar UDFs and UDAFs, a metastore UDF
  called without `ADD JAR`, `TRANSFORM` scripts; engine-specific `hash()` / `xxhash64` and
  `reflect` on JDK methods were first labelled unconvertible and relabelled convertible when v1
  reproduced Spark's hash with a UDF - see section 4), 3 **needs-config** objects that need an environment mapping (`LOAD
  DATA INPATH`, `INSERT OVERWRITE DIRECTORY`, an external table `LOCATION` + `MSCK REPAIR`) and
  11 **non-deterministic** ones (`rand()`, `collect_list` / `collect_set` /
  `first()`, `LIMIT` without `ORDER BY`, `TABLESAMPLE`, `row_number` and `LIMIT` over ties,
  `first_value` without an order, generated ids, `current_date` in a filter, approximate
  functions).
- **C - reporting (82 objects):** the TPC-DS queries that run on the source, rewritten into
  legacy Hive style (`nvl`, `substr`, `date_add`, backticks, session preambles). Each rewrite
  is accepted only if its Spark result equals DuckDB's result for the original query.

**Ground truth:** the source result (or, for a DDL table, its declared columns), the object's
category, its dependencies, and for maskable objects the columns that may be masked. For
objects that must be handed back the truth is the category itself: a claimed match is counted
as wrong, and so is a verification that masked a real column. The source is Spark 3.5: the few
objects whose Hive and Spark semantics differ (`add_months` at month end, `GROUPING__ID`,
overflow) are tagged `spark3_semantics` and the truth is Spark's behaviour. **Split:** frozen; estate A at the layer level (staging
views dev, aggregates and their reports split as chains), B stratified by category, C by kind:
97 dev objects for building, 85 test objects run at the end (3 repeats for
model systems).

**Metrics**, as a client reads them:
- **verified %**: migrated, equal to the source, and verifiable (the headline);
- **wrong but claimed migrated** (target 0): mismatches, errors, and unverifiable objects
  claimed as done;
- **hand-back precision / recall**: were the hand-backs necessary, and were the objects that
  needed a person all handed back; and was the *reason* the right one (a hand-back that says
  "Snowflake error" for a jar UDF sends the reader down the wrong path);
- triage false positives (convertible objects refused before any attempt), dependency-order
  violations; model calls, seconds and $ per object; all per estate and per category.

**Systems.** v0 sqlglot; v0+ sqlglot with identifier quoting removed; v0-LLM one call (Haiku
5.5 and Opus 5.5); v1, the shipped per-object agent (sqlglot draft → run → repair from the
error → compare → repair from the diff); **v2**, the estate agent: triage (constructs that
need porting or an environment mapping, and results that cannot be verified, are handed back
before anything is spent - detected from the SQL, by probing the source for ties in rankings
and at `LIMIT` cuts, and by running the source twice with a different shuffle layout; clock
columns in the output are masked rather than refused) → dependency order →
the v1 loop per object, with in-warehouse validation for large results → objects whose
upstream failed are handed back as blocked → downstream re-verification → estate report
([architecture.md](architecture.md)).

## 3. Building v2 (dev split)

| System (dev split, one run) | Verified | Wrong but claimed migrated | Hand-back precision / recall | $ / object |
|---|---|---|---|---|
| v0 - sqlglot | 31% | 67 | - / 0% | $0.0000 |
| v0+ - sqlglot, unquoted | 56% | 43 | - / 0% | $0.0000 |
| v0-LLM - Haiku 5.5, one call | 64% | 30 | 80% / 36% | $0.0008 |
| v0-LLM - Opus 5.5, one call | 80% | 15 | 100% / 36% | $0.0196 |
| v1 - per-object agent | 84% | 2 | 69% / 82% | $0.0008 |
| **v2 - estate agent** | 88% | 0 | 92% / 100% | $0.0006 |

Ablation ([ABLATION_dev.md](../evals/results/ABLATION_dev.md)):

| Variant | Verified | Wrong but claimed migrated | Handed to a human | Hand-back precision / recall | Order violations | Calls/object | $/object |
|---|---|---|---|---|---|---|---|
| **v2 (all steps)** | 87.6% | 0 | 12.4% | 91.7% / 100.0% | 0 | 0.64 | $0.0006 |
| v2 without triage (every object converted) | 86.6% | 2 | 11.3% | 81.8% / 81.8% | 0 | 0.87 | $0.0010 |
| v2 without source probes (rules only, no tie / double-run checks) | 86.6% | 0 | 13.4% | 84.6% / 100.0% | 0 | 0.64 | $0.0006 |
| v2 without dependency ordering (file order, no re-verification) | 82.5% | 0 | 17.5% | 64.7% / 100.0% | 5 | 0.79 | $0.0008 |
| v2 without dependency ordering, with re-verification | 82.5% | 0 | 17.5% | 64.7% / 100.0% | 5 | 0.8 | $0.0008 |
| v1 per-object agent (reference) | 84.5% | 2 | 13.4% | 69.2% / 81.8% | 0 | 0.88 | $0.0008 |
| v0 sqlglot (reference) | 30.9% | 67 | 0.0% | None% / 0.0% | 0 | 0 | $0.0000 |
| v0+ sqlglot unquoted (reference) | 55.7% | 43 | 0.0% | None% / 0.0% | 0 | 0 | $0.0000 |
| v0-LLM Haiku (reference) | 63.9% | 30 | 5.2% | 80.0% / 36.4% | 0 | 1 | $0.0008 |
| v0-LLM Opus (reference) | 80.4% | 15 | 4.1% | 100.0% / 36.4% | 0 | 1 | $0.0196 |

What building on the dev split changed, in order. (1) The first baseline pass exposed two problems in the pre-registered set, both fixed and committed before any v2 run: dev objects depended on test aggregates (the dev set was made upstream-closed), and v1 reproduced Spark's Murmur3 `hash()` with a JavaScript UDF, matching all 50 values - so the hash / xxhash64 / JDK-reflect objects were relabelled convertible (the information needed is in the SQL). (2) A triage dry run against the truth categories found four false alarms on TPC-DS queries: TPC-DS's habitual `LIMIT 100` on a one-row aggregate (it cuts nothing; the rule now checks the result size) and ties at a `LIMIT` cut between identical duplicate rows (whichever make the cut, the result is the same). (3) The harness learnt three things: in-warehouse validation addresses target columns by position (converted SQL may name or case them differently), a failed upstream replay makes the double run inconclusive rather than a non-determinism verdict, and every run first drops the tables its objects create (stale tables from earlier runs had hidden ordering mistakes). (4) The no-ordering ablation first showed nothing, because the frozen set happened to be stored in dependency order; it now takes objects in file-name order, as a folder lists them.

The ablation reads as follows. Without triage, every object is converted: two non-deterministic objects come back as lucky matches and are claimed migrated, and no hand-back carries the right reason. Without the source probes (rules only), precision drops to 85%: objects whose ties or instability only show when the source is run are converted, fail to match, and come back as generic failures. Without dependency ordering, estate A falls from 92% to 54% verified: reports are converted before the aggregates they read exist, burn their repair budget on 'table does not exist', and are handed back - five ordering violations, each a wasted hand-back. Re-verification does not rescue that case (the objects were never verified to begin with); it matters when an upstream object changes after its consumers were checked, which dependency order prevents.

## 4. Review round

Before the object set was frozen, the estate design and the metric definitions went through a blind review round, as in P2: two AI reviewers acting as a buyer (the person who signs off a migration) and as a Hive-to-Snowflake migration consultant, each given the code and the plan but not my conclusions. Adopted from the buyer: a mutation test of the verifier (a verifier that lets bugs through is not a verifier), Wilson intervals on every headline rate, a review-hours model with stated assumptions, hand-backs that carry a fix hint rather than a bare refusal, the Spark-not-Hive and emulator disclosures in the result tables, pre-registration of the object set before any v2 run, and a note that SnowConvert (the incumbent tool) was not run here. Adopted from the consultant: three more truth categories - *needs-config* (environment mapping, not porting), *maskable* (clock columns verified by masking rather than refused) and *blocked* (downstream of an unconvertible object); tie detection (ranking ties and ties at a `LIMIT` cut probed on the source, a second source run with a different shuffle layout, and a tie-tolerant ordered compare); metastore-UDF detection against the source engine's built-in list; triage rules evaluated on SQL with comments and string literals stripped; whether a hand-back gives the *right reason* and triage false positives as metrics, with a truth x decision confusion matrix; estate A grown from 19 to 30 objects with a blocked chain and an ETL-over-ETL chain; and `spark3_semantics` tags on the objects whose Hive and Spark 3 behaviour differ. Declined: a real Hive cluster as the source (not available; disclosed instead). Two corrections were made after the first baseline pass and before any v2 run, both committed with the reason: the first split let dev objects depend on test aggregates (the dev set was made upstream-closed), and v1 reproduced Spark's Murmur3 `hash()` with a JavaScript UDF, matching all 50 values, which overturned the pre-registered 'unconvertible' label on the three hash / JDK-reflect objects (relabelled convertible: the information needed is in the SQL). The consultant's review also caught a real bug in the write emulator: a mixed static / dynamic `PARTITION (y = 2001, m)` spec swapped the partition columns, so v1's B34 truth had been wrong.

## 5. Final results (held-out test split)

| Metric | v0 - sqlglot (rule-based) | v0+ - sqlglot, identifiers unquoted | v0-LLM - Haiku 5.5, one call | v0-LLM - Opus 5.5, one call | v1 - per-object agent, Haiku 5.5 | v2 - estate agent, Haiku 5.5 |
|---|---|---|---|---|---|---|
| Executes on Snowflake | 40% | 59% | 84% (81%-89%) | 92% (91%-93%) | 91% (89%-92%) | 87% (86%-87%) |
| Same result as the source | 31% [22-41] | 49% [39-60] | 63% (58%-67%) [52-72] | 81% (79%-82%) [71-88] | 84% (78%-87%) [75-90] | 87% (86%-87%) [78-92] |
| **Verified** (migrated, right, verifiable) | 29% [21-40] | 48% [38-59] | 62% (56%-66%) [51-71] | 80% (78%-81%) [70-87] | 80% (73%-84%) [70-87] | 87% (86%-87%) [78-92] |
| Claimed migrated | 100% | 100% | 96% | 97% (96%-98%) | 83% (76%-86%) | 87% (86%-87%) |
| **Wrong but claimed migrated** | 60 | 44 | 29.7 (26-34) | 14.7 (13-16) | 2.7 (2-3) | 0 |
| Handed to a human | 0% | 0% | 4% | 3% (2%-4%) | 17% (14%-24%) | 13% (13%-14%) |
| ...of which were actually fine | 0 | 0 | 0 | 0 | 1.7 (1-2) | 0 |
| ...of which refused at triage wrongly | 0 | 0 | 0 | 0 | 0 | 0 |
| Hand-back precision | - | - | 67% | 78% (67%-100%) | 65% (45%-75%) | 97% (92%-100%) |
| Hand-back recall | 0% | 0% | 18% | 18% | 82% | 100% |
| Hand-back reason correct | - | - | 0% | 0% | 0% | 100% |
| Verified with clock columns masked | 0 | 0 | 0 | 0 | 0 | 2 |
| Dependency-order violations | 0 | 0 | 0 | 0 | 0 | 0 |
| Model calls / object | 0.0 | 0.0 | 1.0 | 1.0 | 0.8 (0.7-0.9) | 0.5 (0.5-0.5) |
| Seconds / object | 3 | 3 | 8 (5-10) | 12 (12-12) | 20 (20-21) | 48 (33-79) |
| $ / object | $0.0000 | $0.0000 | $0.0008 | $0.0197 ($0.0195-$0.0202) | $0.0008 ($0.0007-$0.0009) | $0.0006 |

**Reading the table.** The headline is the pair verified / wrong-but-claimed-migrated. v2 is at the verified ceiling in two of three runs (all convertible and maskable objects verified) and one object short in the third (B43, below); it makes 0 wrong claims in every run and is the only system whose hand-backs are both complete (recall 100%) and explained (reason correct 100%). v1, the same conversion loop without triage or ordering, verifies nearly as many convertible objects (94%) but has no way to recognise an unverifiable object: a lucky match on `collect_list` or `first_value` without an order is claimed migrated (2-3 per run), and its hand-backs are 'Snowflake error after 4 attempts' for a jar UDF - true, and useless to the person who gets it. v1 was also the least stable system: in one run its estate A collapsed to 29% when an early aggregate failed and everything downstream burnt its budget against a missing table; v2 reports those as blocked by name. The one-call baselines are fast, but claim almost everything: Opus 5.5 verifies 92% of the convertible objects - impressive syntax - and still ships 13-16 wrong objects per run, because nothing checks its answer; Haiku in one call ships 26-34.

**By estate.** C (TPC-DS reporting) is solved by any careful system (v1 and v2 99-100%; the unquoted transpiler 84%). The differences are in A (the pipeline: ordering, partition semantics, scale-1 validation) and B (the traps), which is where a real estate's cost is.

**Cost and time.** v2 makes fewer model calls than v1 (0.5 vs 0.8 per object: triage spends nothing on objects that must be handed back) and costs $0.0006 per object; its seconds per object (33-79) are dominated by the source probes and the double run on Spark, which a cluster would make negligible.

**Disclosures.** Test run r1 hit one Snowflake transport error (a result chunk that failed to download) while re-checking A_T01, which the agent had already verified in-warehouse; the object's evaluation was re-run with `scripts/reevaluate.py`, which leaves the conversion untouched and records the re-run in the object's trace (109,722 rows, all aggregates and the sample match). The v2 test runs were otherwise uninterrupted; earlier attempts that were cut short by the machine sleeping were discarded and re-run whole.

## 6. What it means for a migration team

A rough effort model, with its assumptions stated so a reader can substitute their own. Assume a 500-object estate with the same mix as the three estates here, a reviewer who needs about 15 minutes to re-check a verified object (read the diff evidence, spot-check), about 2 hours to convert and verify an object by hand, and about 30 minutes to act on a hand-back that already names the construct and the fix. With a transpiler alone every object needs the full manual check because the tool cannot tell right from wrong, so the 500 objects cost about 500 x 2 h = 1,000 reviewer-hours. With the estate agent the verified share needs the 15-minute re-check and the handed-back share the 30-minute action, so at the rates measured on the held-out split the estate costs roughly (verified% x 500 x 0.25 h) + (handed-back% x 500 x 0.5 h) plus the model and warehouse cost, which at the measured $/object is small change. Wrong-but-claimed-migrated objects are the term that dominates when it is not zero: each one is a production incident, not a review item. At v2's measured rates (87% verified, 13% handed back, 0 wrong claims) that is about 435 x 0.25 h + 65 x 0.5 h = roughly 140 reviewer-hours instead of 1,000, plus about $0.30 of model calls for the whole estate. The numbers are illustrative; the measured rates are in the table above, and a reader's own review times can be substituted.

## 7. Failure analysis and limitations

**Where the verifier is weaker than it looks** (from the mutation test, [MUTATION_test.md](../evals/results/MUTATION_test.md)). Of 189 injected bugs, 70 did not change the result on this data (dropping `NULLS FIRST` where the sort key has no NULLs, `DISTINCT` where the values are already distinct, an off-by-one on a boundary with no rows on it): no verifier could see them with this data, which is an argument for test data that exercises the edges, not for a different comparison. Of the 119 that did change the result, 115 were caught (97%). The four misses are two design choices worth stating plainly: (a) numbers are compared with a relative tolerance of 1e-6 (needed for DECIMAL vs DOUBLE noise), so rounding a total of 97 million to one decimal instead of two passes - for money columns an absolute tolerance of half a cent is the right rule, and it is the first change for the next version; (b) the comparison is type-insensitive for numbers held as text, so a JSON value cast to INT instead of STRING passes when the digits agree.

**What v2 still hands back that it should not.** On dev, one convertible object (`xxhash64`): Spark's 64-bit xxHash can be reproduced with a UDF, as v1 did for Murmur3, but Haiku did not manage it in five attempts. It came back as a failure with the last Snowflake error, not as a wrong claim. On the test split, one object in one run: B43 (`hash()` reproduced as a Murmur3 UDF) was verified in runs 1 and 3, but in run 2 the escalation model's repair - a long UDF body - ran out of output tokens, and the object was handed back as a system error rather than claimed. Hand-backs are safe; this one was wasted.

**How the baselines fail**, for a reader deciding which of these problems they have: the transpiler quotes every identifier (Snowflake then looks for lower-case tables) and converts Hive semantics literally (NULL ordering, `split` as a regex, casts that fail instead of returning NULL, `INSERT OVERWRITE ... PARTITION`); one-call models fix the syntax and miss the semantics silently (month-end `add_months`, decimal overflow, integer wrap-around, partition overwrite keeping the other partitions), and convert objects that should be refused (random sampling, generated ids, `current_date` in a filter).

- **Still a stand-in for Hive.** Spark SQL is Hive-compatible, not Hive; a real estate adds
  serdes, Hive-specific UDF libraries and cluster behaviour the source here doesn't have, and
  write statements run through an emulator on this machine.
- **Verification needs the source.** The proof relies on running the original on the source
  system. That is the normal situation in a migration (the Hive cluster is still there), and
  it is why the method works: the data, not the model, decides.
- **Scale.** Estate A's aggregates are hundreds of thousands of rows; a production estate has
  tables of billions. The in-warehouse validation (aggregates inside each engine + an ordered
  sample) is the pattern that scales; the row-pull comparison used for small results does not.
- **85 test objects**; one object is about 1 points.

## 8. The v1 prototype

v2 supersedes the per-object prototype shipped on 30 Oct 2026 (70 independent objects, all
convertible; 100% verified on its 31 test objects, which is what showed the set was too easy).
Its results are kept in `evals/results/*_test_r*.json` from that date; v1 itself is one of the
systems compared above.

## 9. Reproduce

```bash
uv sync --group dev --extra agent --extra app
uv run python scripts/build_data.py                       # TPC-DS 0.1 -> Parquet -> Snowflake
uv run python scripts/build_data.py --sf 1 --dataset sf1  # scale 1 for estate A
uv run python scripts/build_objects.py                    # estates, truths, stability flags
uv run pytest -q
uv run python scripts/run_eval.py --system v2 --model haiku --split test --tag r1
uv run python scripts/final_table.py
```
