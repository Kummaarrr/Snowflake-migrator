"""v2 pieces that need no engines: dependency extraction and ordering, triage rules, scoring,
the Hive CLI preprocessing, order-key positions and the tie-tolerant ordered compare."""

import pandas as pd

from hive2snow import deps, diff
from hive2snow.estate_a import ESTATE_A
from hive2snow.estate_agent import _clock_mask, triage, unknown_functions
from hive2snow.estate_b import ESTATE_B
from hive2snow.harness import Outcome, order_positions
from hive2snow.hive_writes import preprocess
from hive2snow.objects import MigrationObject
from hive2snow.scoring import aggregate, score
from hive2snow.systems import Conversion


def test_dependencies_follow_created_tables():
    objs = [MigrationObject(**{**o.__dict__}) for o in ESTATE_A]
    deps.annotate(objs)
    by = {o.id: o for o in objs}
    assert by["A_T01"].depends_on == ["A_S01", "A_S02"]
    assert "A_T01" in by["A_R01"].depends_on and "A_T03" in by["A_R07"].depends_on
    assert by["A_T07"].depends_on == ["A_T01"] and by["A_R13"].depends_on == ["A_T07"]
    assert "A_S05" in by["A_T06"].depends_on and by["A_R11"].depends_on == ["A_T06"]
    order = [o.id for o in deps.migration_order(objs)]
    for o in objs:
        for d in o.depends_on:
            assert order.index(d) < order.index(o.id)


def test_triage_rules_match_the_estate_categories():
    """Rules only (no source probes): every unconvertible / needs_config object is caught with
    the right category, no convertible or maskable object is refused, maskable objects get the
    right mask, and the rules alone see most of the non-deterministic ones."""
    misses = []
    for o in ESTATE_B + ESTATE_A:
        t = triage(o, source_checks=False)
        if o.category in ("unconvertible", "needs_config") and t.category != o.category:
            misses.append((o.id, f"{o.category} seen as {t.category}: {t.reason[:60]}"))
        if o.category in ("convertible", "blocked") and t.category != "convertible":
            misses.append((o.id, f"false alarm: {t.reason[:80]}"))
        if o.category == "maskable" and (
            t.category != "maskable" or sorted(t.mask) != sorted(o.mask_columns)
        ):
            misses.append((o.id, f"mask: {t.category} {t.mask}"))
    assert not misses, misses
    nondet = [o for o in ESTATE_B if o.category == "nondeterministic"]
    seen = [o.id for o in nondet if triage(o, source_checks=False).category == "nondeterministic"]
    assert len(seen) >= len(nondet) - 2, (len(seen), len(nondet))


def test_unknown_functions_and_macros():
    assert unknown_functions("SELECT fiscal_period(d_date) FROM date_dim") == ["fiscal_period"]
    assert (
        unknown_functions("SELECT pmod(-5, 3), bround(1.25, 1), xpath_string('<a/>', '/a')") == []
    )
    sql = "CREATE TEMPORARY MACRO clean(s STRING) upper(trim(s));\nSELECT clean(c_last_name) FROM customer"
    assert unknown_functions(sql) == []


def test_clock_mask_finds_output_columns_only():
    q = MigrationObject(
        "X",
        "query",
        "t",
        "SELECT a, count(*) AS n, current_timestamp() AS load_ts FROM t GROUP BY a",
    )
    assert _clock_mask(q) == (["load_ts"], "")
    etl = MigrationObject(
        "Y", "etl", "t",
        "CREATE TABLE d (k INT, paid DECIMAL(10,2), load_ts TIMESTAMP) STORED AS PARQUET;\n"
        "INSERT OVERWRITE TABLE d SELECT k, sum(p), current_timestamp() FROM s GROUP BY k;\nSELECT * FROM d ORDER BY k",
        creates=["d"],
    )  # fmt: skip
    assert _clock_mask(etl) == (["load_ts"], "")
    f = MigrationObject(
        "Z", "query", "t", "SELECT count(*) FROM t WHERE d > date_sub(current_date(), 30)"
    )
    mask, why = _clock_mask(f)
    assert mask is None and "filter" in why


def test_preprocess_substitutes_hivevars_and_expands_macros():
    out = preprocess(
        [
            "SET hivevar:yr=2001",
            "CREATE TEMPORARY MACRO clean_name(s STRING) upper(trim(s))",
            "SELECT clean_name(c_last_name) AS ln FROM customer WHERE d_year = ${hivevar:yr}",
        ]
    )
    assert out == ["SELECT (upper(trim((c_last_name)))) AS ln FROM customer WHERE d_year = 2001"]


def test_order_positions_and_tie_tolerant_compare():
    obj = MigrationObject(
        "X", "query", "t", "SELECT a, b, sum(c) AS s FROM t GROUP BY a, b ORDER BY s DESC, a"
    )
    assert order_positions(obj) == [2, 0]
    assert order_positions(
        MigrationObject("Y", "query", "t", "SELECT a, b FROM t ORDER BY a, b")
    ) == [0, 1]
    assert (
        order_positions(MigrationObject("Z", "query", "t", "SELECT a, b FROM t ORDER BY c")) is None
    )
    # rows tied on the sort key may come in either order; a real order difference is caught
    src = pd.DataFrame({"a": [1, 1, 2], "b": ["x", "y", "z"]})
    tgt = pd.DataFrame({"a": [1, 1, 2], "b": ["y", "x", "z"]})
    assert diff.compare(src, tgt, ordered=True, order_positions=[0]).match
    assert not diff.compare(src, tgt, ordered=True).match
    bad = pd.DataFrame({"a": [2, 1, 1], "b": ["z", "x", "y"]})
    assert not diff.compare(src, bad, ordered=True, order_positions=[0]).match


def _score(
    category: str,
    verdict: str,
    status: str,
    reason: str = "r",
    masked=None,
    mask_cols=None,
    trace=None,
):
    obj = MigrationObject(
        "X", "query", "t", "SELECT 1", category=category, mask_columns=mask_cols or []
    )
    conv = Conversion("SELECT 1", verdict, reason, trace=trace or [])
    return score(obj, conv, Outcome(status, ""), 1.0, masked=masked)


def test_scoring_rewards_correct_hand_backs_and_punishes_lucky_matches():
    s = _score("nondeterministic", "migrated", "match")
    assert s.wrong_claim and not s.verified
    s = _score("nondeterministic", "needs_human", "match", "nondeterministic: rand()")
    assert s.correct_hand_back and s.reason_ok and not s.wasted_hand_back
    s = _score("nondeterministic", "needs_human", "error", "Snowflake error after 4 attempts")
    assert s.correct_hand_back and not s.reason_ok and s.decision == "hb_failed"
    s = _score("convertible", "needs_human", "match")
    assert s.wasted_hand_back and not s.triage_false_positive
    s = _score(
        "convertible", "needs_human", "error", "unconvertible: x", trace=[{"step": "triage"}]
    )
    assert s.triage_false_positive
    # masking: allowed columns are fine, a real column masked away is a wrong claim
    s = _score("maskable", "migrated", "match", masked=["load_ts"], mask_cols=["load_ts"])
    assert s.verified and not s.wrong_claim
    s = _score("maskable", "migrated", "match", masked=["load_ts", "paid"], mask_cols=["load_ts"])
    assert s.wrong_claim and not s.verified
    s = _score("blocked", "needs_human", "error", "blocked: upstream A_S05 not migrated")
    assert s.correct_hand_back and s.reason_ok
    agg = aggregate(
        [
            _score("convertible", "migrated", "match"),
            _score("unconvertible", "needs_human", "error", "unconvertible: jar"),
            _score("nondeterministic", "migrated", "match"),
            _score("needs_config", "needs_human", "error", "result differs"),
        ]
    )
    assert agg["verified_pct"] == 25.0 and agg["wrong_claimed_migrated"] == 1
    assert agg["hand_back_precision_pct"] == 100.0 and agg["hand_back_recall_pct"] == 66.7
    assert agg["hand_back_reason_correct_pct"] == 50.0
    assert agg["confusion"]["nondeterministic"]["migrated"] == 1
    assert agg["confusion"]["needs_config"]["hb_failed"] == 1
