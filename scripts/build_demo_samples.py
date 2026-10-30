"""Copy what the demo shows into app/samples/ (no model or Snowflake calls).

uv run python scripts/build_demo_samples.py [evals/results/v1_haiku_test_r1.json]
"""

from __future__ import annotations

import json
import shutil
import sys
from pathlib import Path

from hive2snow.config import PROJECT_ROOT

SAMPLES = PROJECT_ROOT / "app" / "samples"


def main(result_file: str) -> int:
    SAMPLES.mkdir(parents=True, exist_ok=True)
    src = Path(result_file)
    data = json.loads(src.read_text(encoding="utf-8"))
    for o in data["objects"]:  # keep the demo file small
        o["trace"] = [
            {k: (v if k != "evidence" else str(v)[:300]) for k, v in step.items()}
            for step in o.get("trace", [])
        ]
        o["evidence"] = o["evidence"][:1200]
    (SAMPLES / "results.json").write_text(json.dumps(data, indent=1), encoding="utf-8")
    shutil.copy(PROJECT_ROOT / "data" / "objects.json", SAMPLES / "objects.json")
    print(f"{len(data['objects'])} objects -> {SAMPLES}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1] if len(sys.argv) > 1 else "evals/results/v1_haiku_test_r1.json"))
