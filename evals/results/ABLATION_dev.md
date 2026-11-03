# Ablation - dev split (one run each)

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
