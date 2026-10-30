# Hive → Snowflake Migration Agent

Converts Hive / Spark SQL queries, ETL scripts and table DDL to Snowflake and **proves each
conversion**: every object is run on both sides on the same data and the results are compared.
The report says, per object, *migrated (verified)* or *needs a human*, with the diff or error
attached. A prototype on public TPC-DS data, with every claim measured.

**Write-up:** [docs/REPORT.md](docs/REPORT.md) · **Architecture:** [docs/architecture.md](docs/architecture.md) · **Demo:** `streamlit run app/streamlit_app.py` (live link once deployed)

## The story

1. **Rule-based transpilers get the easy majority and don't know when they're wrong.** sqlglot
   (`hive` → `snowflake`) converts a legacy Hive estate in seconds; on our 70 objects it gives
   the source's answer for 26% of them while claiming every one it produced is fine
   (the first fix a team makes, unquoting identifiers, gets it to 64%). Those silent errors are what a migration team spends its weeks finding.
2. **A model alone is better at the syntax, not at the truth.** One "convert this" call to
   Claude Haiku 5.5 reaches 82% (81-84%); it still marks wrong conversions as done.
3. **The agent runs both sides and lets the data decide.** v1 drafts with sqlglot, runs the
   draft on Snowflake, repairs from the error text, compares the result with the Hive (Spark)
   result, repairs from the diff, and calls an object migrated **only when the results match**.
   Everything else is handed back with the evidence. Held-out result: 100% of objects
   verified equal, 0 wrong-but-claimed-migrated, at $0.0008 per object.

## Results

**Held-out test split:** 31 migration objects never used while building (TPC-DS queries in
legacy Hive style, hand-written legacy scripts with `LATERAL VIEW`, complex types, Hive date and
string functions, `INSERT OVERWRITE ... PARTITION`, and DDLs). Model systems run 3 times with
fresh model calls; mean (range).

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

**In one line:** the agent turns "converted, hopefully right" into "converted and verified, or
flagged with the diff", and the client's review time goes to the flagged objects only.

Ablation on the dev split (what each step buys): [evals/results/ABLATION_dev.md](evals/results/ABLATION_dev.md) ·
All test runs: [evals/results/FINAL_test.md](evals/results/FINAL_test.md)

## How it works

```
Hive object -> sqlglot draft -> run on Snowflake --error--> repair with the error (x3)
                                     | ok
                                     v
                           compare with the source result --differs--> repair with the diff (x2)
                                     | match
                                     v
                     migrated (verified) | needs a human (+ last error / diff)
```

- **Source:** local Spark SQL over TPC-DS Parquet (Spark SQL runs HiveQL); no cluster.
- **Target:** a Snowflake trial loaded from the same Parquet files.
- **Proof:** results compared cell by cell with numeric tolerance, order-insensitive unless the
  query orders; DDL tables compared by their resulting columns (partition columns included,
  complex types as VARIANT / ARRAY / OBJECT).
- **Objects:** 40 TPC-DS queries rewritten into legacy Hive style (each rewrite proven equal to
  DuckDB's independent result), 20 hand-written legacy scripts, 10 DDLs.

## Repository

```
src/hive2snow/
  engines.py      Spark source (HiveQL) and Snowflake target; hive_writes.py emulates Hive writes
  objects.py      hand-written legacy scripts and DDLs; hiveify.py rewrites TPC-DS to Hive style
  harness.py      run on the target in a clean state, diff against the source truth
  diff.py         the comparison rules; scoring.py the client-facing metrics
  systems.py      v0 sqlglot and v0-LLM; agent.py the v1 agent
scripts/          build_data, build_objects, run_eval, final_table, build_demo_samples
app/              Streamlit demo: migration report + convert your own Hive SQL (your own key)
data/objects.json the frozen object set with its split; evals/results/ every run
```

## Run it

```bash
uv sync --group dev --extra agent --extra app
# Snowflake: a trial account and a key-pair service user (see snowflake_setup.sql), then .env
uv run python scripts/build_data.py        # TPC-DS 0.1 -> Parquet -> Spark views + Snowflake
uv run python scripts/build_objects.py     # verify and freeze the 70 objects + source truths
uv run python scripts/run_eval.py --system v0 --split dev           # free
uv run python scripts/run_eval.py --system v1 --model haiku --split dev
uv run streamlit run app/streamlit_app.py
```

The demo holds no keys. "Convert your own Hive SQL" uses the visitor's own Anthropic key, passed
per call and never stored; its output is a draft (visitors have no Snowflake attached to verify
it).

## Notes

Public TPC-DS data and the author's own design; no client code or employer material. The
Windows build machine has no Hadoop native binaries, so Hive write statements run on the source
through a small in-memory emulator (`hive_writes.py`); every SELECT runs on Spark itself.

## License

MIT
