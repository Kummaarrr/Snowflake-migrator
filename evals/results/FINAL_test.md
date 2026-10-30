# Final results - test split

31 held-out migration objects (queries, ETL scripts, DDL) never used while building; model systems run 3 times independently; mean (range).

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

By kind, % with the same result as the source:

| Kind | v0 - sqlglot (rule-based) | v0+ - sqlglot, identifiers unquoted | v0-LLM - Haiku 5.5, one call | v0-LLM - Opus 5.5, one call | v1 - agent, Haiku 5.5 |
|---|---|---|---|---|---|
| query | 28% | 76% | 81% (80%-84%) | 93% (92%-96%) | 100% |
| etl | 0% | 0% | 50% | 100% | 100% |
| ddl | 25% | 25% | 100% | 100% | 100% |

Runs available: v0 x1, v0-unquoted x1, v0-llm_haiku x3, v0-llm_opus x3, v1_haiku x3
