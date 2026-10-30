# Ablation - dev split (one run each)

| Variant | Same result | Wrong but claimed migrated | Handed to a human | Calls/object | $/object |
|---|---|---|---|---|---|
| v1 (all steps) | 100.0% | 0 | 0.0% | 0.74 | $0.0007 |
| without the sqlglot draft (model drafts) | 100.0% | 0 | 0.0% | 1.15 | $0.0009 |
| repairs without the error / diff text | 92.3% | 0 | 7.7% | 0.82 | $0.0007 |
| without data-diff repairs (errors only) | 94.9% | 0 | 5.1% | 0.69 | $0.0012 |
| without escalation to Sonnet | 100.0% | 0 | 0.0% | 0.74 | $0.0007 |
| v0 sqlglot (reference) | 35.9% | 25 | 0.0% | 0 | $0.0000 |
| v0+ sqlglot unquoted (reference) | 64.1% | 14 | 0.0% | 0 | $0.0000 |
| v0-LLM Haiku (reference) | 76.9% | 9 | 0.0% | 1 | $0.0008 |
| v0-LLM Opus (reference) | 94.9% | 2 | 0.0% | 1 | $0.0199 |
