"""The code the verdicts rest on: the diff, the DDL column check, statement splitting."""

import datetime as dt
from decimal import Decimal

import pandas as pd

from hive2snow import diff
from hive2snow.engines import split_statements
from hive2snow.harness import expected_columns
from hive2snow.hiveify import hiveify
from hive2snow.objects import DDL, LEGACY
from hive2snow.systems import sqlglot_transpile


def test_diff_tolerates_engine_representation_differences():
    a = pd.DataFrame({"x": [Decimal("1.10"), None], "d": [dt.date(2001, 1, 1), dt.date(2001, 1, 2)],
                      "s": ["abc ", "d"], "b": [True, False]})  # fmt: skip
    b = pd.DataFrame({"X": [1.1000001, None], "D": [pd.Timestamp("2001-01-01"), pd.Timestamp("2001-01-02")],
                      "S": ["abc", "d"], "B": [1, 0]})  # fmt: skip
    assert diff.compare(a, b, ordered=True).match


def test_diff_catches_real_differences_and_explains_them():
    a = pd.DataFrame({"x": [1.0, 2.0, 3.0]})
    assert not diff.compare(a, pd.DataFrame({"x": [1.0, 2.0]}), False).match
    d = diff.compare(a, pd.DataFrame({"x": [1.0, 2.0, 3.5]}), ordered=True)
    assert not d.match and "row 3" in d.details[0]
    # order matters only when asked
    assert diff.compare(a, pd.DataFrame({"x": [3.0, 2.0, 1.0]}), ordered=False).match
    assert not diff.compare(a, pd.DataFrame({"x": [3.0, 2.0, 1.0]}), ordered=True).match


def test_has_order_by_looks_at_the_outer_query_only():
    assert diff.has_order_by("SELECT * FROM t ORDER BY 1")
    assert not diff.has_order_by("SELECT * FROM (SELECT * FROM t ORDER BY 1) x")
    assert not diff.has_order_by("SELECT row_number() OVER (ORDER BY a) FROM t")


def test_split_statements_respects_quotes_and_comments():
    parts = split_statements("SET a=1; -- x; y\nSELECT ';' AS s; SELECT 2")
    assert len(parts) == 3 and parts[1].strip().startswith("SELECT ';'")


def test_expected_columns_include_partitions_and_map_types():
    cols = expected_columns(DDL[0].hive_sql)  # D01: 6 columns + 2 partition columns
    assert [c for c, _ in cols][-2:] == ["sale_date", "store_sk"]
    assert dict(cols)["net_paid"] == "number" and dict(cols)["sale_date"] == "date"
    cols2 = dict(expected_columns(DDL[1].hive_sql))  # D02 complex types
    assert (
        cols2["tags"] == "array"
        and cols2["attributes"] == "object"
        and cols2["address"] == "object"
    )


def test_hiveify_is_deterministic_and_parses():
    sql = "SELECT COALESCE(a, 0) AS a, SUBSTRING(b, 1, 3) FROM t WHERE d > CAST('2000-01-01' AS DATE) + INTERVAL 30 DAY"
    one, two = hiveify(sql, 7), hiveify(sql, 7)
    assert one == two and "nvl(" in one.lower() and "date_add(" in one.lower()


def test_hand_written_objects_have_unique_ids_and_declared_outputs():
    ids = [o.id for o in LEGACY + DDL]
    assert len(ids) == len(set(ids))
    for o in LEGACY + DDL:
        if o.kind == "etl":
            assert o.creates and split_statements(o.hive_sql)[-1].upper().startswith("SELECT")
        if o.kind == "ddl":
            assert o.creates and (o.ddl_table or o.check_sql)


def test_sqlglot_baseline_never_raises():
    for o in LEGACY + DDL:
        sql, err = sqlglot_transpile(o.hive_sql)
        assert sql or err
