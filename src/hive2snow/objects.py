"""The migration objects: what a client hands over.

Three kinds, all over the TPC-DS tables:
- query:  a SELECT (TPC-DS queries rewritten the way legacy Hive code is written, and
          hand-written scripts with Hive-specific functions and syntax); compared by result;
- etl:    a script that creates / overwrites tables (`INSERT OVERWRITE ... PARTITION`, CTAS,
          `INSERT INTO`) and ends with a SELECT of the result table; compared by that result;
- ddl:    a CREATE TABLE / VIEW; a table is compared by its resulting columns (names and type
          classes, partition columns included), a view by its content.

`LEGACY` and `DDL` below are hand-written. `scripts/build_objects.py` adds the TPC-DS queries,
verifies every object on the source and freezes the set with its split in data/objects.json.
"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class MigrationObject:
    id: str
    kind: str  # query | etl | ddl
    title: str
    hive_sql: str
    features: list[str] = field(default_factory=list)
    creates: list[str] = field(default_factory=list)  # tables / views the script creates
    ddl_table: str = ""  # ddl: the table to DESCRIBE (empty for views, compared by content)
    check_sql: str = ""  # ddl view: the SELECT that compares content
    split: str = "dev"


LEGACY: list[MigrationObject] = [
    MigrationObject(
        "L01",
        "query",
        "Explode a delimited list: LATERAL VIEW explode(split())",
        """\
-- top items per store as a delimited string, then exploded back to rows (legacy report feed)
SELECT t.ss_store_sk, item_id, size(t.items) AS n_items
FROM (
  SELECT ss_store_sk, collect_set(cast(ss_item_sk AS string)) AS items
  FROM store_sales
  WHERE ss_sold_date_sk = 2451545 AND ss_item_sk <= 60
  GROUP BY ss_store_sk
) t
LATERAL VIEW explode(t.items) e AS item_id
ORDER BY t.ss_store_sk, cast(item_id AS int)""",
        ["lateral_view", "explode", "collect_set", "size", "cast_string"],
    ),
    MigrationObject(
        "L02",
        "query",
        "collect_set + sort_array + concat_ws per customer",
        """\
SELECT c.c_customer_id,
       concat_ws('|', sort_array(collect_set(i.i_category))) AS categories,
       count(DISTINCT i.i_category) AS n_cat
FROM store_sales ss
JOIN customer c ON c.c_customer_sk = ss.ss_customer_sk
JOIN item i ON i.i_item_sk = ss.ss_item_sk
WHERE ss.ss_sold_date_sk BETWEEN 2451545 AND 2451560 AND c.c_customer_sk < 400
GROUP BY c.c_customer_id
HAVING count(DISTINCT i.i_category) >= 3
ORDER BY n_cat DESC, c.c_customer_id
LIMIT 50""",
        ["collect_set", "sort_array", "concat_ws", "having"],
    ),
    MigrationObject(
        "L03",
        "query",
        "Hive date functions: from_unixtime / unix_timestamp / datediff order",
        """\
SELECT d_date_sk,
       from_unixtime(unix_timestamp(cast(d_date AS string), 'yyyy-MM-dd'), 'yyyyMM') AS yyyymm,
       datediff(d_date, cast('2000-01-01' AS date)) AS days_since_y2k,
       add_months(d_date, 3) AS plus_q,
       last_day(d_date) AS month_end,
       date_format(d_date, 'EEE') AS dow,
       weekofyear(d_date) AS iso_week,
       months_between(cast('2001-01-01' AS date), d_date) AS mb
FROM date_dim
WHERE d_year = 2000 AND d_moy = 2
ORDER BY d_date_sk""",
        [
            "from_unixtime",
            "unix_timestamp",
            "datediff",
            "add_months",
            "last_day",
            "date_format",
            "weekofyear",
            "months_between",
        ],
    ),
    MigrationObject(
        "L04",
        "query",
        "Hive string functions: instr, locate, lpad, regexp_extract, rlike, translate",
        """\
SELECT i_item_id,
       instr(i_item_desc, 'the') AS pos_the,
       locate('a', i_item_desc, 5) AS pos_a,
       lpad(cast(i_item_sk AS string), 8, '0') AS padded,
       regexp_extract(i_item_desc, '^(\\\\w+)', 1) AS first_word,
       regexp_replace(i_item_desc, '[aeiou]', '') AS no_vowels,
       translate(i_item_id, 'A', 'a') AS tr,
       substr(i_item_desc, -5) AS tail5,
       length(trim(i_item_desc)) AS len,
       reverse(i_item_id) AS rev
FROM item
WHERE i_item_desc rlike '^[A-Z][a-z]+ ' AND i_item_sk <= 300
ORDER BY i_item_sk""",
        [
            "instr",
            "locate",
            "lpad",
            "regexp_extract",
            "regexp_replace",
            "translate",
            "substr_negative",
            "rlike",
            "reverse",
        ],
    ),
    MigrationObject(
        "L05",
        "query",
        "if() / nvl / isnull in a margin report",
        """\
SELECT ss_store_sk,
       sum(if(ss_net_profit > 0, ss_net_profit, 0)) AS profit,
       sum(if(ss_net_profit < 0, -ss_net_profit, 0)) AS loss,
       sum(nvl(ss_coupon_amt, 0)) AS coupons,
       count(if(isnull(ss_customer_sk), 1, NULL)) AS anon_sales,
       round(avg(nvl(ss_ext_discount_amt, 0)), 2) AS avg_disc
FROM store_sales
WHERE ss_sold_date_sk BETWEEN 2451545 AND 2451575
GROUP BY ss_store_sk
ORDER BY ss_store_sk""",
        ["if", "nvl", "isnull", "round"],
    ),
    MigrationObject(
        "L06",
        "query",
        "GROUP BY ... WITH ROLLUP and GROUPING__ID",
        """\
SELECT i_category, i_class, GROUPING__ID AS gid, sum(ss_ext_sales_price) AS sales,
       count(*) AS n
FROM store_sales ss JOIN item i ON i.i_item_sk = ss.ss_item_sk
WHERE ss.ss_sold_date_sk BETWEEN 2451545 AND 2451560 AND i_category IN ('Books', 'Music')
GROUP BY i_category, i_class WITH ROLLUP
ORDER BY gid, i_category, i_class""",
        ["with_rollup", "grouping__id"],
    ),
    MigrationObject(
        "L07",
        "query",
        "Window functions: lag, running sum, ntile, dense_rank",
        """\
SELECT d_date_sk, daily,
       daily - lag(daily, 1) OVER (ORDER BY d_date_sk) AS delta,
       sum(daily) OVER (ORDER BY d_date_sk ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS wk_sum,
       ntile(4) OVER (ORDER BY daily) AS quartile,
       dense_rank() OVER (ORDER BY daily DESC) AS rnk
FROM (
  SELECT d_date_sk, sum(ss_net_paid) AS daily
  FROM store_sales ss JOIN date_dim d ON d.d_date_sk = ss.ss_sold_date_sk
  WHERE d_year = 2001 AND d_moy = 3
  GROUP BY d_date_sk
) x
ORDER BY d_date_sk""",
        ["lag", "rows_between", "ntile", "dense_rank"],
    ),
    MigrationObject(
        "L08",
        "query",
        "DISTRIBUTE BY / SORT BY in a legacy extract (order-insensitive)",
        """\
SELECT ss_store_sk, ss_item_sk, sum(ss_quantity) AS qty
FROM store_sales
WHERE ss_sold_date_sk BETWEEN 2451545 AND 2451550
GROUP BY ss_store_sk, ss_item_sk
HAVING sum(ss_quantity) > 50
DISTRIBUTE BY ss_store_sk SORT BY ss_store_sk, qty DESC""",
        ["distribute_by", "sort_by", "having"],
    ),
    MigrationObject(
        "L09",
        "query",
        "Complex types: named_struct, map access, array index, array_contains",
        """\
SELECT c_customer_id,
       named_struct('first', c_first_name, 'last', c_last_name).last AS last_name,
       map('M', 'Male', 'F', 'Female')[nvl(cd_gender, 'M')] AS gender,
       array(c_birth_day, c_birth_month, c_birth_year)[2] AS birth_year,
       array_contains(array('Primary', 'Secondary'), cd_education_status) AS basic_edu,
       size(map('a', 1, 'b', 2)) AS map_size
FROM customer c
JOIN customer_demographics cd ON cd.cd_demo_sk = c.c_current_cdemo_sk
WHERE c_customer_sk <= 200
ORDER BY c_customer_id""",
        ["named_struct", "map_literal", "array_index", "array_contains", "size"],
    ),
    MigrationObject(
        "L10",
        "query",
        "get_json_object over a JSON string built with concat",
        """\
SELECT i_item_sk,
       get_json_object(js, '$.brand') AS brand,
       cast(get_json_object(js, '$.price') AS double) AS price,
       get_json_object(js, '$.tags[0]') AS first_tag
FROM (
  SELECT i_item_sk,
         concat('{"brand":"', i_brand, '","price":', cast(i_current_price AS string),
                ',"tags":["', i_class, '","', i_category, '"]}') AS js
  FROM item
  WHERE i_item_sk <= 150 AND i_current_price IS NOT NULL AND i_brand IS NOT NULL
) j
ORDER BY i_item_sk""",
        ["get_json_object", "concat", "cast_string"],
    ),
    MigrationObject(
        "L11",
        "query",
        "Timestamps: cast, hour/minute, to_date, INTERVAL arithmetic",
        """\
SELECT t_time_sk, t_time,
       cast(concat('2001-03-15 ', t_hour, ':', t_minute, ':', t_second) AS timestamp) AS ts,
       hour(cast(concat('2001-03-15 ', t_hour, ':', t_minute, ':', t_second) AS timestamp)) AS h,
       to_date(cast(concat('2001-03-15 ', t_hour, ':', t_minute, ':', t_second) AS timestamp)) AS dt,
       cast(concat('2001-03-15 ', t_hour, ':', t_minute, ':', t_second) AS timestamp)
         + INTERVAL 90 MINUTE AS later,
       unix_timestamp(cast(concat('2001-03-15 ', t_hour, ':', t_minute, ':', t_second) AS timestamp)) AS epoch
FROM time_dim
WHERE t_time_sk % 3600 = 0
ORDER BY t_time_sk""",
        ["cast_timestamp", "hour", "to_date", "interval", "unix_timestamp"],
    ),
    MigrationObject(
        "L12",
        "query",
        "Numeric: integer division, pmod, bround vs round, cast decimal",
        """\
SELECT ss_item_sk,
       sum(ss_quantity) / count(*) AS avg_qty_div,
       pmod(sum(ss_quantity), 7) AS m7,
       round(sum(ss_net_paid) / count(*), 2) AS avg_paid,
       bround(avg(ss_list_price), 1) AS banker,
       cast(sum(ss_ext_tax) AS decimal(12, 2)) AS tax,
       floor(avg(ss_wholesale_cost)) AS fl, ceil(avg(ss_wholesale_cost)) AS ce,
       abs(min(ss_net_profit)) AS worst
FROM store_sales
WHERE ss_sold_date_sk BETWEEN 2451545 AND 2451560 AND ss_item_sk <= 100
GROUP BY ss_item_sk
ORDER BY ss_item_sk""",
        ["int_division", "pmod", "bround", "cast_decimal", "floor_ceil"],
    ),
    MigrationObject(
        "L13",
        "query",
        "percentile_approx and stddev per store",
        """\
SELECT ss_store_sk,
       percentile_approx(ss_net_paid, 0.5, 10000) AS median_paid,
       percentile_approx(ss_net_paid, array(0.25, 0.75), 10000) AS quartiles,
       round(stddev_pop(ss_net_paid), 3) AS sd,
       count(*) AS n
FROM store_sales
WHERE ss_sold_date_sk BETWEEN 2451545 AND 2451560 AND ss_net_paid IS NOT NULL
GROUP BY ss_store_sk
ORDER BY ss_store_sk""",
        ["percentile_approx", "array_percentiles", "stddev_pop"],
    ),
    MigrationObject(
        "L14",
        "query",
        "Reserved words and backticks, ORDER BY position, UNION (distinct)",
        """\
SELECT `d`.`d_year` AS `year`, 'store' AS `channel`, count(*) AS `count`
FROM `store_sales` `s` JOIN `date_dim` `d` ON `d`.`d_date_sk` = `s`.`ss_sold_date_sk`
WHERE `d`.`d_year` IN (2000, 2001) AND s.ss_item_sk < 50
GROUP BY `d`.`d_year`
UNION
SELECT d.d_year, 'web', count(*)
FROM web_sales w JOIN date_dim d ON d.d_date_sk = w.ws_sold_date_sk
WHERE d.d_year IN (2000, 2001) AND w.ws_item_sk < 50
GROUP BY d.d_year
ORDER BY 1, 2""",
        ["backticks", "reserved_words", "union_distinct", "order_by_position"],
    ),
    MigrationObject(
        "L15",
        "query",
        "Semi/anti joins the Hive way: LEFT SEMI JOIN, NOT EXISTS, IN subquery",
        """\
SELECT c.c_customer_sk, c.c_last_name
FROM customer c
LEFT SEMI JOIN store_sales ss ON ss.ss_customer_sk = c.c_customer_sk
  AND ss.ss_sold_date_sk BETWEEN 2451545 AND 2451600
WHERE NOT EXISTS (SELECT 1 FROM catalog_returns r WHERE r.cr_returning_customer_sk = c.c_customer_sk)
  AND c.c_birth_country IN (SELECT DISTINCT c2.c_birth_country FROM customer c2 WHERE c2.c_preferred_cust_flag = 'Y')
ORDER BY c.c_customer_sk
LIMIT 100""",
        ["left_semi_join", "not_exists", "in_subquery"],
    ),
    MigrationObject(
        "L16",
        "etl",
        "INSERT OVERWRITE a static partition, then read the table",
        """\
SET hive.exec.compress.output=true;
CREATE TABLE IF NOT EXISTS l16_store_daily (
  ss_store_sk INT, sales_date DATE, net_paid DECIMAL(17,2), tickets BIGINT
) PARTITIONED BY (d_year INT) STORED AS PARQUET;
INSERT OVERWRITE TABLE l16_store_daily PARTITION (d_year = 2001)
SELECT ss_store_sk, d.d_date, sum(ss_net_paid), count(DISTINCT ss_ticket_number)
FROM store_sales ss JOIN date_dim d ON d.d_date_sk = ss.ss_sold_date_sk
WHERE d.d_year = 2001 AND d.d_moy = 1
GROUP BY ss_store_sk, d.d_date;
SELECT * FROM l16_store_daily ORDER BY d_year, sales_date, ss_store_sk""",
        ["set_hive", "create_table_partitioned", "insert_overwrite_static_partition"],
        creates=["l16_store_daily"],
    ),
    MigrationObject(
        "L17",
        "etl",
        "Dynamic partitions: INSERT OVERWRITE ... PARTITION (d_year, d_moy)",
        """\
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
CREATE TABLE l17_cat_month (i_category STRING, sales DECIMAL(17,2), qty BIGINT)
PARTITIONED BY (d_year INT, d_moy INT) STORED AS ORC;
INSERT OVERWRITE TABLE l17_cat_month PARTITION (d_year, d_moy)
SELECT i.i_category, sum(ss.ss_ext_sales_price), sum(ss.ss_quantity), d.d_year, d.d_moy
FROM store_sales ss
JOIN date_dim d ON d.d_date_sk = ss.ss_sold_date_sk
JOIN item i ON i.i_item_sk = ss.ss_item_sk
WHERE d.d_year = 2002 AND d.d_moy IN (1, 2)
GROUP BY i.i_category, d.d_year, d.d_moy;
SELECT * FROM l17_cat_month ORDER BY d_year, d_moy, i_category""",
        ["set_hive", "dynamic_partitions", "insert_overwrite"],
        creates=["l17_cat_month"],
    ),
    MigrationObject(
        "L18",
        "etl",
        "CTAS, INSERT INTO append, overwrite one partition of several",
        """\
CREATE TABLE l18_returns STORED AS PARQUET AS
SELECT sr_store_sk, d.d_year, sum(sr_return_amt) AS returned
FROM store_returns sr JOIN date_dim d ON d.d_date_sk = sr.sr_returned_date_sk
WHERE d.d_year IN (2000, 2001)
GROUP BY sr_store_sk, d.d_year;
INSERT INTO TABLE l18_returns
SELECT sr_store_sk, 2002, sum(sr_return_amt)
FROM store_returns sr JOIN date_dim d ON d.d_date_sk = sr.sr_returned_date_sk
WHERE d.d_year = 2002
GROUP BY sr_store_sk;
SELECT sr_store_sk, d_year, round(returned, 2) AS returned FROM l18_returns
ORDER BY sr_store_sk, d_year""",
        ["ctas", "insert_into", "stored_as"],
        creates=["l18_returns"],
    ),
    MigrationObject(
        "L19",
        "etl",
        "Staging table rebuilt with INSERT OVERWRITE (no partition) and a lookup",
        """\
DROP TABLE IF EXISTS l19_stage;
CREATE TABLE l19_stage (i_item_sk INT, i_brand STRING, first_sold INT, n BIGINT) STORED AS PARQUET;
INSERT OVERWRITE TABLE l19_stage
SELECT i.i_item_sk, i.i_brand, min(ss.ss_sold_date_sk), count(*)
FROM store_sales ss JOIN item i ON i.i_item_sk = ss.ss_item_sk
WHERE i.i_item_sk <= 80
GROUP BY i.i_item_sk, i.i_brand;
INSERT OVERWRITE TABLE l19_stage
SELECT i_item_sk, upper(i_brand), first_sold, n FROM l19_stage WHERE n >= 100;
SELECT s.*, nvl(d.d_date, cast('1900-01-01' AS date)) AS first_date
FROM l19_stage s LEFT JOIN date_dim d ON d.d_date_sk = s.first_sold
ORDER BY s.i_item_sk""",
        ["drop_table", "insert_overwrite_full", "self_overwrite", "nvl"],
        creates=["l19_stage"],
    ),
    MigrationObject(
        "L20",
        "query",
        "Hive-style JSON-ish map aggregation with str_to_map and explode of a map",
        """\
SELECT ss_store_sk, k AS metric, v AS value
FROM (
  SELECT ss_store_sk,
         str_to_map(concat('tickets:', count(DISTINCT ss_ticket_number),
                           ',items:', sum(ss_quantity),
                           ',customers:', count(DISTINCT ss_customer_sk)), ',', ':') AS m
  FROM store_sales
  WHERE ss_sold_date_sk BETWEEN 2451545 AND 2451550
  GROUP BY ss_store_sk
) t
LATERAL VIEW explode(m) e AS k, v
ORDER BY ss_store_sk, metric""",
        ["str_to_map", "lateral_view_map", "explode_map"],
    ),
]

DDL: list[MigrationObject] = [
    MigrationObject(
        "D01",
        "ddl",
        "Partitioned Parquet fact table with TBLPROPERTIES",
        """\
CREATE TABLE IF NOT EXISTS d01_sales_fact (
  ticket_number BIGINT COMMENT 'store ticket',
  item_sk INT,
  customer_sk INT,
  quantity INT,
  net_paid DECIMAL(17,2),
  sold_ts TIMESTAMP
)
PARTITIONED BY (sale_date DATE, store_sk INT)
STORED AS PARQUET
TBLPROPERTIES ('parquet.compression'='SNAPPY', 'owner'='finance')""",
        ["partitioned_by", "stored_as_parquet", "tblproperties", "column_comment"],
        creates=["d01_sales_fact"],
        ddl_table="d01_sales_fact",
    ),
    MigrationObject(
        "D02",
        "ddl",
        "Complex types: array, map, struct",
        """\
CREATE TABLE d02_customer_profile (
  customer_id STRING,
  tags ARRAY<STRING>,
  attributes MAP<STRING, INT>,
  address STRUCT<street:STRING, city:STRING, zip:STRING>,
  scores ARRAY<DOUBLE>,
  updated TIMESTAMP
) STORED AS ORC""",
        ["array_type", "map_type", "struct_type", "stored_as_orc"],
        creates=["d02_customer_profile"],
        ddl_table="d02_customer_profile",
    ),
    MigrationObject(
        "D03",
        "ddl",
        "External delimited text table with LOCATION",
        """\
CREATE EXTERNAL TABLE d03_raw_events (
  event_id STRING,
  event_ts STRING,
  user_id BIGINT,
  payload STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t' LINES TERMINATED BY '\\n'
STORED AS TEXTFILE
LOCATION '/user/hive/warehouse/raw/events'""",
        ["external", "row_format_delimited", "textfile", "location"],
        creates=["d03_raw_events"],
        ddl_table="d03_raw_events",
    ),
    MigrationObject(
        "D04",
        "ddl",
        "Bucketed and sorted table",
        """\
CREATE TABLE d04_inventory_snap (
  item_sk INT, warehouse_sk INT, qty_on_hand INT, snap_date DATE
)
CLUSTERED BY (item_sk) SORTED BY (snap_date DESC) INTO 32 BUCKETS
STORED AS ORC TBLPROPERTIES ('orc.compress'='ZLIB', 'transactional'='false')""",
        ["clustered_by", "sorted_by", "buckets"],
        creates=["d04_inventory_snap"],
        ddl_table="d04_inventory_snap",
    ),
    MigrationObject(
        "D05",
        "ddl",
        "Every Hive primitive type",
        """\
CREATE TABLE d05_types (
  c_tinyint TINYINT, c_smallint SMALLINT, c_int INT, c_bigint BIGINT,
  c_float FLOAT, c_double DOUBLE, c_decimal DECIMAL(20,4),
  c_string STRING, c_varchar VARCHAR(50), c_char CHAR(10),
  c_boolean BOOLEAN, c_binary BINARY, c_date DATE, c_timestamp TIMESTAMP
) STORED AS PARQUET""",
        ["primitive_types", "char", "binary"],
        creates=["d05_types"],
        ddl_table="d05_types",
    ),
    MigrationObject(
        "D06",
        "ddl",
        "Reserved words as column names, database-qualified name, IF NOT EXISTS",
        """\
CREATE TABLE IF NOT EXISTS `tpcds`.`d06_orders` (
  `order` BIGINT,
  `date` DATE,
  `user` STRING,
  `group` STRING,
  `value` DOUBLE,
  `comment` STRING
) PARTITIONED BY (`year` INT) STORED AS PARQUET""",
        ["backticked_reserved", "db_qualified", "if_not_exists"],
        creates=["d06_orders"],
        ddl_table="d06_orders",
    ),
    MigrationObject(
        "D07",
        "ddl",
        "Partition by a string date key with ORC and SKEWED hint-free props",
        """\
CREATE TABLE d07_web_clicks (
  click_id STRING, page_sk INT, user_sk INT, dwell_ms INT, referrer STRING
)
PARTITIONED BY (dt STRING)
STORED AS ORC
TBLPROPERTIES ('orc.compress'='SNAPPY', 'orc.bloom.filter.columns'='user_sk')""",
        ["partition_string_key", "orc_props"],
        creates=["d07_web_clicks"],
        ddl_table="d07_web_clicks",
    ),
    MigrationObject(
        "D08",
        "ddl",
        "Temporary-style table with nested complex types",
        """\
CREATE TABLE d08_sessions (
  session_id STRING,
  events ARRAY<STRUCT<ts:TIMESTAMP, name:STRING, props:MAP<STRING,STRING>>>,
  device STRUCT<os:STRING, version:STRING, ids:ARRAY<STRING>>,
  started TIMESTAMP, ended TIMESTAMP
) STORED AS PARQUET""",
        ["nested_complex_types"],
        creates=["d08_sessions"],
        ddl_table="d08_sessions",
    ),
    MigrationObject(
        "D09",
        "ddl",
        "View with Hive functions over TPC-DS (compared by content)",
        """\
CREATE VIEW d09_v_store_month AS
SELECT ss_store_sk, d.d_year, d.d_moy,
       sum(ss_net_paid) AS net_paid,
       nvl(sum(ss_coupon_amt), 0) AS coupons,
       concat_ws('-', cast(d.d_year AS string), lpad(cast(d.d_moy AS string), 2, '0')) AS ym
FROM store_sales ss JOIN date_dim d ON d.d_date_sk = ss.ss_sold_date_sk
WHERE d.d_year = 2001
GROUP BY ss_store_sk, d.d_year, d.d_moy""",
        ["create_view", "nvl", "concat_ws", "lpad"],
        creates=["d09_v_store_month"],
        check_sql="SELECT * FROM d09_v_store_month ORDER BY ss_store_sk, d_year, d_moy",
    ),
    MigrationObject(
        "D10",
        "ddl",
        "View with a window function and a Hive date expression",
        """\
CREATE VIEW IF NOT EXISTS d10_v_item_rank AS
SELECT i_item_sk, i_brand, i_current_price,
       row_number() OVER (PARTITION BY i_brand ORDER BY i_current_price DESC, i_item_sk) AS rn,
       date_format(i_rec_start_date, 'yyyy-MM') AS since
FROM item
WHERE i_rec_start_date IS NOT NULL AND i_current_price IS NOT NULL""",
        ["create_view", "row_number", "date_format"],
        creates=["d10_v_item_rank"],
        check_sql="SELECT * FROM d10_v_item_rank WHERE rn <= 3 ORDER BY i_brand, rn, i_item_sk",
    ),
]
