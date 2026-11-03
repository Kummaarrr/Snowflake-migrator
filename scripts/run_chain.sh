#!/usr/bin/env bash
# The remaining P3 v2 measurement chain, run detached from any editor session (see
# scripts/start_chain.ps1). Each step is committed when it lands; progress goes to
# evals/results/chain.log and a chain.done marker is written at the end.
cd "$(dirname "$0")/.." || exit 1
unset VIRTUAL_ENV
export PYSPARK_PYTHON="$(cygpath -w "$PWD/.venv/Scripts/python.exe")"
LOG=evals/results/chain.log
F='^ *$\|it/s\]\|s/it\]\|Stage \|WARN\|FutureWarning\|options = self\|ERROR Executor\|^\s*at \|^Caused by\|^java\.\|\.\.\. [0-9]+ more\|ERROR TaskSetManager\|Setting default\|To adjust\|SUCCESS: The\|unsupported syntax\|Traceback\|File "\|^\s*\^\|ConnectionReset\|SystemExit\|During handling\|pyspark.errors\|Driver stacktrace\|^org.apache'

log() { echo "[$(date +%H:%M)] $*" >>"$LOG"; }
commit() {
  (cd .. && git add -A -- p3-hive-snowflake-migrator/evals && git commit -q -m "P3 v2: $1

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>" >/dev/null 2>&1)
  log "committed: $1"
}
run() {
  log "######## $*"
  timeout 9000 uv run python scripts/run_eval.py "$@" 2>&1 | tr '\r' '\n' | grep -v "$F" \
    | grep -v "^\(columns\|only in\|row [0-9]\)" | grep -v "^  Q[0-9]" >>"$LOG"
  log "exit=${PIPESTATUS[0]}"
}

log "chain start"
for t in r2 r3; do run --system v2 --model haiku --split test --tag $t; commit "test run v2 haiku $t"; done
for t in r2 r3; do run --system v0-llm --model haiku --split test --tag $t; commit "test run v0-llm haiku $t"; done
for t in r1 r2 r3; do run --system v0-llm --model opus --split test --tag $t; commit "test run v0-llm opus $t"; done
run --system v0 --split test
run --system v0-unquoted --split test
commit "test runs v0 and v0+"
log "######## reevaluate r1 A_T01 (transport error)"
timeout 1800 uv run python scripts/reevaluate.py evals/results/v2_haiku_test_r1.json A_T01 2>&1 | grep -v "$F" >>"$LOG"
commit "v2 test r1: A_T01 re-measured after a Snowflake transport error"
log "######## mutation"
timeout 5400 uv run python scripts/mutation_test.py evals/results/v2_haiku_test_r1.json 2>&1 | grep -v "$F" | tail -30 >>"$LOG"
uv run python scripts/final_table.py 2>&1 | grep -v "$F" | tail -80 >>"$LOG"
commit "mutation test and final tables"
log "chain done"
touch evals/results/chain.done
