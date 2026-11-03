"""Source-side guarantees (need the TPC-DS Parquet files; slow: starts Spark once)."""

import pytest

from hive2snow.engines import PARQUET_DIR, spark

pytestmark = pytest.mark.skipif(
    not (PARQUET_DIR / "store.parquet").exists(), reason="run scripts/build_data.py first"
)


@pytest.fixture(scope="module")
def s():
    engine = spark()
    yield engine
    engine.reset()


def test_hive_functions_run_on_the_source(s):
    r = s.run(
        "SELECT nvl(NULL, 1) a, size(array(1,2)) b, date_add(cast('2000-01-01' as date), 30) c"
    )
    assert r.ok and r.frame is not None and list(r.frame.iloc[0]) == [1, 2, r.frame.iloc[0, 2]]


def test_insert_overwrite_static_partition_replaces_only_that_partition(s):
    s.reset()
    r = s.run(
        """
        CREATE TABLE t_part (k INT, v STRING) PARTITIONED BY (y INT) STORED AS PARQUET;
        INSERT OVERWRITE TABLE t_part PARTITION (y = 2000) SELECT 1, 'a';
        INSERT OVERWRITE TABLE t_part PARTITION (y = 2001) SELECT 2, 'b';
        INSERT OVERWRITE TABLE t_part PARTITION (y = 2000) SELECT 3, 'c';
        SELECT k, v, y FROM t_part ORDER BY y, k
        """
    )
    assert r.ok, r.error
    assert r.frame is not None and r.frame.values.tolist() == [[3, "c", 2000], [2, "b", 2001]]


def test_mixed_static_and_dynamic_partition_spec_maps_columns_by_position(s):
    """PARTITION (y = 2001, m): the SELECT supplies the data columns then m; y comes from the
    clause; only the (y, m) partitions written are replaced (Hive semantics)."""
    s.reset()
    r = s.run(
        """
        SET hive.exec.dynamic.partition.mode=nonstrict;
        CREATE TABLE t_mixed (k INT, q BIGINT) PARTITIONED BY (y INT, m INT) STORED AS PARQUET;
        INSERT OVERWRITE TABLE t_mixed PARTITION (y = 2000, m) SELECT 1, 10, 12;
        INSERT OVERWRITE TABLE t_mixed PARTITION (y = 2001, m)
          SELECT 2, 20, 1 UNION ALL SELECT 3, 30, 2;
        INSERT OVERWRITE TABLE t_mixed PARTITION (y = 2001, m) SELECT 4, 40, 2;
        SELECT k, q, y, m FROM t_mixed ORDER BY y, m, k
        """
    )
    assert r.ok, r.error
    assert r.frame is not None
    assert r.frame.values.tolist() == [[1, 10, 2000, 12], [2, 20, 2001, 1], [4, 40, 2001, 2]]


def test_hivevar_substitution_and_temporary_macro_on_the_source(s):
    s.reset()
    r = s.run(
        """
        SET hivevar:lim=2;
        CREATE TEMPORARY MACRO twice(x INT) x * 2;
        SELECT twice(x) AS d FROM (SELECT 1 AS x UNION ALL SELECT 2 UNION ALL SELECT 3) t
        WHERE x <= ${hivevar:lim} ORDER BY d
        """
    )
    assert r.ok, r.error
    assert r.frame is not None and r.frame["d"].tolist() == [2, 4]


def test_dynamic_partitions_and_insert_into(s):
    s.reset()
    r = s.run(
        """
        SET hive.exec.dynamic.partition.mode=nonstrict;
        CREATE TABLE t_dyn (k INT) PARTITIONED BY (y INT);
        INSERT OVERWRITE TABLE t_dyn PARTITION (y) SELECT 1, 2000 UNION ALL SELECT 2, 2001;
        INSERT INTO TABLE t_dyn SELECT 3, 2001;
        INSERT OVERWRITE TABLE t_dyn PARTITION (y) SELECT 9, 2000;
        SELECT k, y FROM t_dyn ORDER BY y, k
        """
    )
    assert r.ok, r.error
    assert r.frame is not None and r.frame.values.tolist() == [[9, 2000], [2, 2001], [3, 2001]]


def test_ctas_and_drop(s):
    s.reset()
    r = s.run("CREATE TABLE t_ctas STORED AS PARQUET AS SELECT 1 AS a; SELECT * FROM t_ctas")
    assert r.ok and r.frame is not None and r.frame.iloc[0, 0] == 1
    assert s.run("DROP TABLE IF EXISTS t_ctas; SELECT 1 AS ok").ok
    assert not s.run("SELECT * FROM t_ctas").ok
