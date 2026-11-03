# Hive → Snowflake Migration Agent

Migrates a Hive / Spark SQL **estate** (queries, ETL scripts, table DDL, with their
dependencies) to Snowflake and **proves each object**: every conversion is run on both sides on
the same data and the results are compared. Objects with no Snowflake equivalent or an
unverifiable result are handed back with the reason and a suggested fix, so the team's time
goes only there. Public TPC-DS data; every claim measured on held-out objects frozen before the
agent was run, with confidence intervals.

**Write-up:** [docs/REPORT.md](docs/REPORT.md) · **Architecture:** [docs/architecture.md](docs/architecture.md) · **Live demo:** https://app-migrator-fadef9kndmndt43cqxet4g.streamlit.app/

## The story

1. **Transpilers convert the easy majority and don't know when they're wrong.** sqlglot (`hive`
   → `snowflake`) gives the source's answer for 29% of the held-out objects while
   claiming every one it produced is fine; unquoting identifiers (the first fix every team
   makes) lifts it to 48%. The rest are silent errors a client finds in production.
2. **A frontier model in one call is better at syntax, not at truth.** One "convert this" call
   to Claude Opus 5.5 reaches 80% (78-81%) verified, still ships 14.7 (13-16) wrong objects as
   done, and converts objects that should have been refused (random, clock-dependent, custom
   UDFs).
3. **The estate agent runs both sides, lets the data decide, and knows what to refuse.** v2
   triages each object (no Snowflake equivalent → hand back; result not verifiable → hand
   back, detected from the SQL and by running the source twice), orders the rest by
   dependencies, converts with a cheap model in a draft → run → repair-from-error → compare →
   repair-from-diff loop, verifies large results inside the warehouses, and re-verifies
   downstream objects. Held-out: **87% (86-87%) verified (the ceiling is 87%: 11 of the 85 objects
   must be handed back), 0 wrong-but-claimed-migrated, hand-back precision 97% (92-100%)**,
   at $0.0006 per object. The verifier itself was
   mutation-tested: 97% of the injected semantic bugs that change the result were caught (the rest change nothing on this data).

## Results

**Held-out test split:** 85 of 182 migration objects never used while building,
across three estates: a layered retail-analytics pipeline at TPC-DS scale 1 (staging views →
partitioned aggregates → reports), 70 hand-written legacy-ETL traps (including objects that
cannot be converted or verified), and TPC-DS reports in legacy Hive style. Model systems run 3
times with fresh model calls; mean (range); 95% Wilson intervals in the full table.

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

**In one line:** On 85 held-out objects the estate agent reached the verified ceiling in two of three runs (87%: 11 of the 85 must be handed back, and it handed back all 11 with the right reason) and missed it by one object in the third; it claimed nothing it could not prove in any run, at $0.0006 per object. The transpiler verified 29% and shipped 60 wrong objects as done; one Opus 5.5 call per object verified 80% and still shipped about 15 wrong objects per run, at 33 times the cost.

Ablation on the dev split: [evals/results/ABLATION_dev.md](evals/results/ABLATION_dev.md) ·
All runs: [evals/results/FINAL_test.md](evals/results/FINAL_test.md) · Verifier mutation test:
[evals/results/MUTATION_test.md](evals/results/MUTATION_test.md) · Review round, effort model
and failure analysis: [docs/REPORT.md](docs/REPORT.md)

## How it works

```
estate (Hive files)
  -> triage: needs porting?      (jar / metastore UDF, TRANSFORM, non-JDK reflect)      -> hand back + fix hint
             needs environment?  (LOAD DATA INPATH, OVERWRITE DIRECTORY, LOCATION)       -> hand back + what's needed
             unverifiable?       (rand, collect_list, LIMIT w/o ORDER BY, ties in a ranking
                                  or at a LIMIT cut, clock in a filter, 2 source runs differ) -> hand back
             clock column in the output? (load_ts)                                        -> mask it, continue
  -> dependency graph -> migration order (upstream handed back? -> hand back as blocked)
  -> per object: sqlglot draft -> run on Snowflake --error--> repair with the error (x3)
                                       | ok
                                       v
                        compare with the source result --differs--> repair with the diff (x2)
                        (small: cell by cell; large: in-warehouse aggregates + ordered sample)
                                       | match
                                       v
                           migrated (verified)  |  needs a human (+ last error / diff)
  -> re-verify downstream objects -> estate report
```

- **Source:** local Spark SQL over TPC-DS Parquet (Spark SQL runs HiveQL); no cluster. Scale
  0.1 for the trap and reporting estates, scale 1 for the pipeline. It is Spark standing in for
  Hive: the report says so wherever it matters.
- **Target:** a Snowflake trial loaded from the same Parquet files.
- **Proof:** results compared cell by cell with numeric tolerance, order-insensitive unless the
  query orders; large results by per-column aggregates computed inside each engine plus a
  500-row ordered sample; DDL tables by their resulting columns. The verifier's strength is
  measured by a mutation test, not assumed.
- **Honest ground truth:** for non-deterministic objects the truth is "cannot be verified": a
  lucky match counts as wrong.

## Repository

```
src/hive2snow/
  engines.py       Spark source (HiveQL, two datasets) and Snowflake target; hive_writes.py emulates Hive writes
  estate_a.py      layered pipeline; estate_b.py legacy traps with categories; hiveify.py TPC-DS -> Hive style
  deps.py          tables read / written, dependency graph, migration order
  harness.py       truths with upstream replay, double-run stability, diff / in-warehouse validation
  estate_agent.py  v2: triage -> order -> per-object loop -> re-verify; agent.py the per-object loop (v1)
  systems.py       v0 sqlglot, v0+ unquoted, v0-LLM; scoring.py the client-facing metrics
scripts/           build_data (--dataset sf01|sf1), build_objects, run_eval, mutation_test, final_table
app/               Streamlit demo: estate report with dependency graph; convert your own Hive SQL (your key)
data/objects.json  the frozen object set (estate, category, dependencies, split); evals/results/ every run
```

## Run it

```bash
uv sync --group dev --extra agent --extra app
# Snowflake: a trial and a key-pair service user (see snowflake_setup.sql), then .env
uv run python scripts/build_data.py                        # TPC-DS 0.1 -> Parquet -> Spark + Snowflake
uv run python scripts/build_data.py --sf 1 --dataset sf1   # scale 1 for the pipeline estate
uv run python scripts/build_objects.py                     # estates, truths, stability flags
uv run python scripts/run_eval.py --system v2 --model haiku --split dev
uv run python scripts/mutation_test.py evals/results/v2_haiku_dev.json
uv run streamlit run app/streamlit_app.py
```

The demo holds no keys. "Convert your own Hive SQL" uses the visitor's own Anthropic key, passed
per call and never stored; its output is a triaged draft (visitors have no Snowflake attached to
verify it). In a real deployment the agent runs inside the client's environment against their
own Snowflake account; what leaves it are SQL text, error messages and small result diffs sent
to the model API (or a private model endpoint), never bulk data.

## Notes

Public TPC-DS data and the author's own design; no client code or employer material. The
Windows build machine has no Hadoop native binaries, so Hive write statements run on the source
through a small, unit-tested in-memory emulator (`hive_writes.py`); every SELECT runs on Spark.
The first version of this project (per-object, 70 convertible objects, 100% on its too-easy
test set) is kept as a chapter in the report, not as a result.

## License

MIT
