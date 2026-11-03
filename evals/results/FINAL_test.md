# Final results - test split

85 held-out migration objects (queries, ETL scripts, DDL) never used while building; model systems run 3 times independently; mean (range); [95% Wilson interval] on the headline rates. Source = local Spark SQL standing in for Hive; Hive write statements run through an in-memory emulator on the source (see README notes).

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

By estate: verified % (wrong-but-claimed-migrated count):

| estate | v0 - sqlglot (rule-based) | v0+ - sqlglot, identifiers unquoted | v0-LLM - Haiku 5.5, one call | v0-LLM - Opus 5.5, one call | v1 - per-object agent, Haiku 5.5 | v2 - estate agent, Haiku 5.5 |
|---|---|---|---|---|---|---|
| A (n=17) | 0% (17) | 0% (17) | 29% (12%-59%) (12 (7-15)) | 63% (59%-71%) (6.3 (5-7)) | 61% (29%-76%) (0.3 (0-1)) | 82% (0) |
| B (n=31) | 32% (21) | 32% (21) | 53% (48%-58%) (11.7 (10-13)) | 67% (64%-71%) (7.7 (7-8)) | 67% (64%-68%) (2.3 (2-3)) | 73% (71%-74%) (0) |
| C (n=37) | 40% (22) | 84% (6) | 84% (81%-86%) (6 (5-7)) | 98% (97%-100%) (0.7 (0-1)) | 99% (97%-100%) (0) | 100% (0) |

By kind: verified % (wrong-but-claimed-migrated count):

| kind | v0 - sqlglot (rule-based) | v0+ - sqlglot, identifiers unquoted | v0-LLM - Haiku 5.5, one call | v0-LLM - Opus 5.5, one call | v1 - per-object agent, Haiku 5.5 | v2 - estate agent, Haiku 5.5 |
|---|---|---|---|---|---|---|
| ddl (n=1) | 0% (1) | 0% (1) | 0% (0.7 (0-1)) | 0% (1) | 0% (0) | 0% (0) |
| etl (n=6) | 0% (6) | 0% (6) | 17% (0%-33%) (5 (4-6)) | 61% (50%-67%) (2.3 (2-3)) | 50% (33%-67%) (0.7 (0-1)) | 83% (0) |
| query (n=78) | 32% (53) | 53% (37) | 66% (62%-69%) (24 (21-28)) | 82% (81%-83%) (11.3 (10-12)) | 83% (77%-86%) (2) | 88% (87%-88%) (0) |

By truth category: verified % (wrong-but-claimed-migrated count):

| truth category | v0 - sqlglot (rule-based) | v0+ - sqlglot, identifiers unquoted | v0-LLM - Haiku 5.5, one call | v0-LLM - Opus 5.5, one call | v1 - per-object agent, Haiku 5.5 | v2 - estate agent, Haiku 5.5 |
|---|---|---|---|---|---|---|
| blocked (n=3) | 0% (3) | 0% (3) | 0% (3) | 0% (3) | 0% (0) | 0% (0) |
| convertible (n=72) | 35% (47) | 57% (31) | 71% (65%-76%) (19.7 (16-24)) | 92% (90%-94%) (5 (3-6)) | 94% (86%-99%) (0.7 (0-1)) | 100% (99%-100%) (0) |
| maskable (n=2) | 0% (2) | 0% (2) | 50% (1) | 67% (50%-100%) (0.7 (0-1)) | 0% (0) | 100% (0) |
| needs_config (n=1) | 0% (1) | 0% (1) | 0% (0.7 (0-1)) | 0% (1) | 0% (0) | 0% (0) |
| nondeterministic (n=5) | 0% (5) | 0% (5) | 0% (5) | 0% (5) | 0% (2) | 0% (0) |
| unconvertible (n=2) | 0% (2) | 0% (2) | 0% (0.3 (0-1)) | 0% (0) | 0% (0) | 0% (0) |

Runs available: v0 x1, v0-unquoted x1, v0-llm_haiku x3, v0-llm_opus x3, v1_haiku x3, v2_haiku x3
