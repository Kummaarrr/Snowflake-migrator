"""Estate A - "retail analytics": a layered pipeline over TPC-DS at scale 1.

    raw TPC-DS tables
      -> staging views   (A_S*: cleaning, type fixes, business keys)      [ddl views]
      -> aggregate tables (A_T*: INSERT OVERWRITE ... PARTITION by year)  [etl]
      -> reports          (A_R*: queries over the aggregates)             [query]

Objects depend on each other: a report is only right if its aggregate is, and the aggregate
only if its staging view is. The agent must migrate in dependency order and re-verify
downstream objects when an upstream conversion changes. Results of the aggregate tables are
large (hundreds of thousands of rows), so they are verified in-warehouse.
"""

from __future__ import annotations

from hive2snow.objects import MigrationObject

DS = "sf1"
YEARS = "(2000, 2001)"


def _view(
    id_: str,
    title: str,
    name: str,
    body: str,
    features: list[str],
    check: str,
    category: str = "convertible",
    reason: str = "",
) -> MigrationObject:
    return MigrationObject(
        id_,
        "ddl",
        title,
        f"CREATE VIEW {name} AS\n{body}",
        features,
        [name],
        check_sql=check,
        estate="A",
        dataset=DS,
        category=category,
        expected_reason=reason,
    )


def _etl(
    id_: str,
    title: str,
    sql: str,
    features: list[str],
    creates: list[str],
    large: bool = True,
    category: str = "convertible",
    reason: str = "",
) -> MigrationObject:
    return MigrationObject(
        id_,
        "etl",
        title,
        sql,
        features,
        creates,
        estate="A",
        dataset=DS,
        large=large,
        category=category,
        expected_reason=reason,
    )


def _q(
    id_: str,
    title: str,
    sql: str,
    features: list[str],
    category: str = "convertible",
    reason: str = "",
    mask: list[str] | None = None,
) -> MigrationObject:
    return MigrationObject(
        id_,
        "query",
        title,
        sql,
        features,
        estate="A",
        dataset=DS,
        category=category,
        expected_reason=reason,
        mask_columns=mask or [],
    )


BLOCKED = "blocked: reads a_stg_store (A_S05), which cannot be converted until the region_code UDF is ported"


STAGING: list[MigrationObject] = [
    _view(
        "A_S01",
        "Staging: store sales with business keys and cleaned amounts",
        "a_stg_store_sales",
        """SELECT ss.ss_ticket_number, ss.ss_item_sk, ss.ss_customer_sk, ss.ss_store_sk, ss.ss_promo_sk,
       d.d_date AS sale_date, d.d_year, d.d_moy, d.d_qoy,
       nvl(ss.ss_quantity, 0) AS quantity,
       nvl(ss.ss_net_paid, 0) AS net_paid,
       nvl(ss.ss_net_paid_inc_tax, 0) AS net_paid_inc_tax,
       nvl(ss.ss_net_profit, 0) AS net_profit,
       nvl(ss.ss_coupon_amt, 0) AS coupon_amt,
       if(ss.ss_customer_sk IS NULL, 'anonymous', 'known') AS customer_kind
FROM store_sales ss JOIN date_dim d ON d.d_date_sk = ss.ss_sold_date_sk
WHERE d.d_year IN """
        + YEARS,
        ["staging_view", "nvl", "if"],
        "SELECT d_year, d_moy, customer_kind, count(*) AS n, sum(quantity) AS q, round(sum(net_paid), 2) AS paid FROM a_stg_store_sales GROUP BY d_year, d_moy, customer_kind ORDER BY d_year, d_moy, customer_kind",
    ),
    _view(
        "A_S02",
        "Staging: items with normalised category and a price band",
        "a_stg_item",
        """SELECT i_item_sk, i_item_id, upper(trim(i_category)) AS category, upper(trim(i_class)) AS class,
       nvl(i_brand, 'UNBRANDED') AS brand, i_current_price,
       CASE WHEN i_current_price < 10 THEN 'low' WHEN i_current_price < 50 THEN 'mid' ELSE 'high' END AS price_band,
       cast(i_manufact_id AS string) AS manufact_id
FROM item WHERE i_current_price IS NOT NULL""",
        ["staging_view", "case_bands", "cast_string"],
        "SELECT category, price_band, count(*) AS n, round(avg(i_current_price), 2) AS avg_price FROM a_stg_item GROUP BY category, price_band ORDER BY category, price_band",
    ),
    _view(
        "A_S03",
        "Staging: customers with demographics and age at 2001",
        "a_stg_customer",
        """SELECT c.c_customer_sk, c.c_customer_id, concat_ws(' ', c.c_first_name, c.c_last_name) AS full_name,
       cd.cd_gender AS gender, cd.cd_marital_status AS marital, cd.cd_education_status AS education,
       2001 - c.c_birth_year AS age_2001,
       CASE WHEN 2001 - c.c_birth_year < 30 THEN 'under30' WHEN 2001 - c.c_birth_year < 50 THEN '30to49' ELSE '50plus' END AS age_band,
       ca.ca_state AS state, ca.ca_country AS country
FROM customer c
LEFT JOIN customer_demographics cd ON cd.cd_demo_sk = c.c_current_cdemo_sk
LEFT JOIN customer_address ca ON ca.ca_address_sk = c.c_current_addr_sk""",
        ["staging_view", "concat_ws", "left_join"],
        "SELECT age_band, gender, count(*) AS n FROM a_stg_customer GROUP BY age_band, gender ORDER BY age_band, gender",
    ),
    _view(
        "A_S04",
        "Staging: store returns joined to the original sale",
        "a_stg_returns",
        """SELECT sr.sr_ticket_number, sr.sr_item_sk, sr.sr_customer_sk, sr.sr_store_sk,
       d.d_year, d.d_moy, nvl(sr.sr_return_quantity, 0) AS return_qty, nvl(sr.sr_return_amt, 0) AS return_amt,
       datediff(d.d_date, d0.d_date) AS days_to_return
FROM store_returns sr
JOIN date_dim d ON d.d_date_sk = sr.sr_returned_date_sk
JOIN store_sales ss ON ss.ss_ticket_number = sr.sr_ticket_number AND ss.ss_item_sk = sr.sr_item_sk
JOIN date_dim d0 ON d0.d_date_sk = ss.ss_sold_date_sk
WHERE d.d_year IN """
        + YEARS,
        ["staging_view", "datediff"],
        "SELECT d_year, d_moy, count(*) AS n, sum(return_qty) AS q, round(avg(days_to_return), 2) AS avg_days FROM a_stg_returns GROUP BY d_year, d_moy ORDER BY d_year, d_moy",
    ),
    _view(
        "A_S05",
        "Staging: stores with a region from a metastore UDF (the one nobody has the source of)",
        "a_stg_store",
        """SELECT s_store_sk, s_store_id, s_store_name, region_code(s_state) AS region, s_number_employees, s_floor_space
FROM store""",
        ["staging_view", "metastore_udf"],
        "SELECT region, count(*) AS stores FROM a_stg_store GROUP BY region ORDER BY region",
        "unconvertible",
        "needs porting: region_code is a metastore UDF (not a built-in); locate its source and rewrite it as a Snowflake UDF",
    ),
]

AGGREGATES: list[MigrationObject] = [
    _etl(
        "A_T01",
        "Aggregate: daily store sales by category and price band (partitioned by year)",
        """SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
CREATE TABLE a_agg_daily_sales (sale_date DATE, ss_store_sk INT, category STRING, price_band STRING,
  tickets BIGINT, quantity BIGINT, net_paid DECIMAL(17,2), net_profit DECIMAL(17,2)) PARTITIONED BY (d_year INT) STORED AS PARQUET;
INSERT OVERWRITE TABLE a_agg_daily_sales PARTITION (d_year)
SELECT s.sale_date, s.ss_store_sk, i.category, i.price_band, count(DISTINCT s.ss_ticket_number), sum(s.quantity), sum(s.net_paid), sum(s.net_profit), s.d_year
FROM a_stg_store_sales s JOIN a_stg_item i ON i.i_item_sk = s.ss_item_sk
GROUP BY s.sale_date, s.ss_store_sk, i.category, i.price_band, s.d_year;
SELECT * FROM a_agg_daily_sales""",
        ["dynamic_partitions", "depends_on_views"],
        ["a_agg_daily_sales"],
    ),
    _etl(
        "A_T02",
        "Aggregate: monthly customer segment performance",
        """CREATE TABLE a_agg_segment_month (d_year INT, d_moy INT, age_band STRING, gender STRING, customers BIGINT,
  tickets BIGINT, net_paid DECIMAL(17,2), coupon_amt DECIMAL(17,2), avg_ticket DECIMAL(17,2)) STORED AS PARQUET;
INSERT OVERWRITE TABLE a_agg_segment_month
SELECT s.d_year, s.d_moy, c.age_band, c.gender, count(DISTINCT s.ss_customer_sk), count(DISTINCT s.ss_ticket_number),
       sum(s.net_paid), sum(s.coupon_amt), sum(s.net_paid) / count(DISTINCT s.ss_ticket_number)
FROM a_stg_store_sales s JOIN a_stg_customer c ON c.c_customer_sk = s.ss_customer_sk
WHERE s.customer_kind = 'known'
GROUP BY s.d_year, s.d_moy, c.age_band, c.gender;
SELECT * FROM a_agg_segment_month ORDER BY d_year, d_moy, age_band, gender""",
        ["count_distinct", "division", "depends_on_views"],
        ["a_agg_segment_month"],
        large=False,
    ),
    _etl(
        "A_T03",
        "Aggregate: item return rates (static partition per year, two loads)",
        """CREATE TABLE a_agg_item_returns (ss_item_sk INT, category STRING, sold_qty BIGINT, returned_qty BIGINT,
  return_rate DOUBLE, avg_days_to_return DOUBLE) PARTITIONED BY (d_year INT) STORED AS PARQUET;
INSERT OVERWRITE TABLE a_agg_item_returns PARTITION (d_year = 2000)
SELECT s.ss_item_sk, i.category, sum(s.quantity), nvl(sum(r.return_qty), 0),
       nvl(sum(r.return_qty), 0) / sum(s.quantity), avg(r.days_to_return)
FROM a_stg_store_sales s JOIN a_stg_item i ON i.i_item_sk = s.ss_item_sk
LEFT JOIN a_stg_returns r ON r.sr_ticket_number = s.ss_ticket_number AND r.sr_item_sk = s.ss_item_sk
WHERE s.d_year = 2000 GROUP BY s.ss_item_sk, i.category;
INSERT OVERWRITE TABLE a_agg_item_returns PARTITION (d_year = 2001)
SELECT s.ss_item_sk, i.category, sum(s.quantity), nvl(sum(r.return_qty), 0),
       nvl(sum(r.return_qty), 0) / sum(s.quantity), avg(r.days_to_return)
FROM a_stg_store_sales s JOIN a_stg_item i ON i.i_item_sk = s.ss_item_sk
LEFT JOIN a_stg_returns r ON r.sr_ticket_number = s.ss_ticket_number AND r.sr_item_sk = s.ss_item_sk
WHERE s.d_year = 2001 GROUP BY s.ss_item_sk, i.category;
SELECT * FROM a_agg_item_returns""",
        ["static_partitions", "left_join_nvl", "depends_on_views"],
        ["a_agg_item_returns"],
    ),
    _etl(
        "A_T04",
        "Aggregate: store monthly scorecard (CTAS, then an appended month)",
        """CREATE TABLE a_agg_store_month STORED AS ORC AS
SELECT ss_store_sk, d_year, d_moy, count(DISTINCT ss_ticket_number) AS tickets, sum(net_paid) AS net_paid,
       sum(net_profit) AS net_profit, sum(coupon_amt) AS coupon_amt, count(DISTINCT ss_customer_sk) AS customers
FROM a_stg_store_sales WHERE NOT (d_year = 2001 AND d_moy = 12) GROUP BY ss_store_sk, d_year, d_moy;
INSERT INTO TABLE a_agg_store_month
SELECT ss_store_sk, d_year, d_moy, count(DISTINCT ss_ticket_number), sum(net_paid), sum(net_profit), sum(coupon_amt), count(DISTINCT ss_customer_sk)
FROM a_stg_store_sales WHERE d_year = 2001 AND d_moy = 12 GROUP BY ss_store_sk, d_year, d_moy;
SELECT * FROM a_agg_store_month ORDER BY ss_store_sk, d_year, d_moy""",
        ["ctas", "insert_into", "depends_on_views"],
        ["a_agg_store_month"],
        large=False,
    ),
    _etl(
        "A_T05",
        "Aggregate: promotion lift by category",
        """CREATE TABLE a_agg_promo_lift (d_year INT, category STRING, promo_tickets BIGINT, promo_paid DECIMAL(17,2),
  base_tickets BIGINT, base_paid DECIMAL(17,2), lift_pct DOUBLE) STORED AS PARQUET;
INSERT OVERWRITE TABLE a_agg_promo_lift
SELECT s.d_year, i.category,
       count(DISTINCT if(s.ss_promo_sk IS NOT NULL AND s.ss_promo_sk > 0, s.ss_ticket_number, NULL)),
       sum(if(s.ss_promo_sk IS NOT NULL AND s.ss_promo_sk > 0, s.net_paid, 0)),
       count(DISTINCT if(s.ss_promo_sk IS NULL OR s.ss_promo_sk = 0, s.ss_ticket_number, NULL)),
       sum(if(s.ss_promo_sk IS NULL OR s.ss_promo_sk = 0, s.net_paid, 0)),
       round(100.0 * sum(if(s.ss_promo_sk IS NOT NULL AND s.ss_promo_sk > 0, s.net_paid, 0)) / sum(s.net_paid), 3)
FROM a_stg_store_sales s JOIN a_stg_item i ON i.i_item_sk = s.ss_item_sk
GROUP BY s.d_year, i.category;
SELECT * FROM a_agg_promo_lift ORDER BY d_year, category""",
        ["conditional_aggregates", "depends_on_views"],
        ["a_agg_promo_lift"],
        large=False,
    ),
    _etl(
        "A_T06",
        "Aggregate: monthly sales by store region (over the UDF staging view)",
        """CREATE TABLE a_agg_region_month (region STRING, d_year INT, d_moy INT, tickets BIGINT, net_paid DECIMAL(17,2)) STORED AS PARQUET;
INSERT OVERWRITE TABLE a_agg_region_month
SELECT st.region, s.d_year, s.d_moy, count(DISTINCT s.ss_ticket_number), sum(s.net_paid)
FROM a_stg_store_sales s JOIN a_stg_store st ON st.s_store_sk = s.ss_store_sk
GROUP BY st.region, s.d_year, s.d_moy;
SELECT * FROM a_agg_region_month ORDER BY region, d_year, d_moy""",
        ["depends_on_views", "blocked_chain"],
        ["a_agg_region_month"],
        large=False,
        category="blocked",
        reason=BLOCKED,
    ),
    _etl(
        "A_T07",
        "Aggregate: yearly category totals rolled up from the daily aggregate (ETL over ETL)",
        """CREATE TABLE a_agg_category_year (d_year INT, category STRING, net_paid DECIMAL(17,2), net_profit DECIMAL(17,2), tickets BIGINT) STORED AS PARQUET;
INSERT OVERWRITE TABLE a_agg_category_year
SELECT d_year, category, sum(net_paid), sum(net_profit), sum(tickets) FROM a_agg_daily_sales GROUP BY d_year, category;
SELECT * FROM a_agg_category_year ORDER BY d_year, category""",
        ["depends_on_agg", "two_level_chain"],
        ["a_agg_category_year"],
        large=False,
    ),
]

REPORTS: list[MigrationObject] = [
    _q(
        "A_R01",
        "Report: top categories by net paid per year, with share",
        "SELECT d_year, category, round(sum(net_paid), 2) AS paid, round(100 * sum(net_paid) / sum(sum(net_paid)) OVER (PARTITION BY d_year), 2) AS share_pct FROM a_agg_daily_sales GROUP BY d_year, category ORDER BY d_year, paid DESC",
        ["window_share", "depends_on_agg"],
    ),
    _q(
        "A_R02",
        "Report: month-over-month ticket growth per store",
        "SELECT ss_store_sk, d_year, d_moy, tickets, tickets - lag(tickets) OVER (PARTITION BY ss_store_sk ORDER BY d_year, d_moy) AS delta, round(net_profit / nullif(net_paid, 0), 4) AS margin FROM a_agg_store_month ORDER BY ss_store_sk, d_year, d_moy",
        ["lag", "nullif", "depends_on_agg"],
    ),
    _q(
        "A_R03",
        "Report: worst return rates among items with volume",
        "SELECT d_year, category, ss_item_sk, sold_qty, returned_qty, round(return_rate, 4) AS rr FROM a_agg_item_returns WHERE sold_qty >= 200 AND return_rate > 0 ORDER BY d_year, rr DESC, ss_item_sk LIMIT 50",
        ["filter_order_limit", "depends_on_agg"],
    ),
    _q(
        "A_R04",
        "Report: segment ranking by average ticket within each month",
        "SELECT d_year, d_moy, age_band, gender, avg_ticket, dense_rank() OVER (PARTITION BY d_year, d_moy ORDER BY avg_ticket DESC) AS rnk FROM a_agg_segment_month ORDER BY d_year, d_moy, rnk, age_band, gender",
        ["dense_rank", "depends_on_agg"],
    ),
    _q(
        "A_R05",
        "Report: promo lift vs base, categories where promotions beat 30% of sales",
        "SELECT d_year, category, promo_tickets, base_tickets, lift_pct, if(lift_pct > 30, 'promo-heavy', 'organic') AS kind FROM a_agg_promo_lift ORDER BY d_year, lift_pct DESC, category",
        ["if", "depends_on_agg"],
    ),
    _q(
        "A_R06",
        "Report: weekday pattern of sales by price band",
        "SELECT date_format(sale_date, 'EEEE') AS weekday, price_band, round(sum(net_paid) / count(DISTINCT sale_date), 2) AS paid_per_day FROM a_agg_daily_sales GROUP BY date_format(sale_date, 'EEEE'), price_band ORDER BY weekday, price_band",
        ["date_format_weekday", "depends_on_agg"],
    ),
    _q(
        "A_R07",
        "Report: cross-aggregate join - returns vs sales per category and year",
        "SELECT s.d_year, s.category, round(sum(s.net_paid), 2) AS paid, sum(r.returned_qty) AS returned, round(sum(r.returned_qty) / sum(s.quantity), 5) AS return_share FROM (SELECT d_year, category, sum(net_paid) AS net_paid, sum(quantity) AS quantity FROM a_agg_daily_sales GROUP BY d_year, category) s JOIN (SELECT d_year, category, sum(returned_qty) AS returned_qty FROM a_agg_item_returns GROUP BY d_year, category) r ON r.d_year = s.d_year AND r.category = s.category GROUP BY s.d_year, s.category ORDER BY s.d_year, s.category",
        ["two_aggregates", "depends_on_agg"],
    ),
    _q(
        "A_R08",
        "Report: customers per state and age band from staging (bypasses aggregates)",
        "SELECT state, age_band, count(*) AS customers, round(avg(age_2001), 1) AS avg_age FROM a_stg_customer WHERE state IS NOT NULL GROUP BY state, age_band ORDER BY state, age_band",
        ["depends_on_view"],
    ),
    _q(
        "A_R09",
        "Report: quarterly totals with ROLLUP over the daily aggregate",
        "SELECT d_year, quarter(sale_date) AS q, category, round(sum(net_paid), 2) AS paid FROM a_agg_daily_sales GROUP BY d_year, quarter(sale_date), category WITH ROLLUP ORDER BY d_year, q, category",
        ["with_rollup", "quarter", "depends_on_agg"],
    ),
    _q(
        "A_R10",
        "Report: store scorecard percentile bands",
        "SELECT ss_store_sk, d_year, round(sum(net_paid), 2) AS paid, ntile(4) OVER (PARTITION BY d_year ORDER BY sum(net_paid)) AS quartile FROM a_agg_store_month GROUP BY ss_store_sk, d_year ORDER BY d_year, quartile, ss_store_sk",
        ["ntile_over_aggregate", "depends_on_agg"],
    ),
    _q(
        "A_R11",
        "Report: region share of monthly sales (over the blocked aggregate)",
        "SELECT d_year, d_moy, region, round(100 * net_paid / sum(net_paid) OVER (PARTITION BY d_year, d_moy), 2) AS share_pct FROM a_agg_region_month ORDER BY d_year, d_moy, region",
        ["window_share", "blocked_chain"],
        "blocked",
        BLOCKED,
    ),
    _q(
        "A_R12",
        "Report: region vs store scorecard join (over the blocked aggregate)",
        "SELECT r.d_year, r.region, round(sum(r.net_paid), 2) AS region_paid, count(DISTINCT m.ss_store_sk) AS stores FROM a_agg_region_month r JOIN a_agg_store_month m ON m.d_year = r.d_year AND m.d_moy = r.d_moy GROUP BY r.d_year, r.region ORDER BY r.d_year, r.region",
        ["two_aggregates", "blocked_chain"],
        "blocked",
        BLOCKED,
    ),
    _q(
        "A_R13",
        "Report: year-over-year growth per category (self join on the yearly rollup)",
        "SELECT cur.category, cur.net_paid AS paid_2001, prev.net_paid AS paid_2000, round(100 * (cur.net_paid - prev.net_paid) / prev.net_paid, 2) AS growth_pct FROM a_agg_category_year cur JOIN a_agg_category_year prev ON prev.category = cur.category AND prev.d_year = cur.d_year - 1 WHERE cur.d_year = 2001 ORDER BY growth_pct DESC, cur.category",
        ["self_join", "two_level_chain"],
    ),
    _q(
        "A_R14",
        "Report: most profitable store per month (row_number with a tiebreaker)",
        "SELECT d_year, d_moy, ss_store_sk, net_profit FROM (SELECT d_year, d_moy, ss_store_sk, net_profit, row_number() OVER (PARTITION BY d_year, d_moy ORDER BY net_profit DESC, ss_store_sk) AS rn FROM a_agg_store_month) x WHERE rn = 1 ORDER BY d_year, d_moy",
        ["row_number_tiebreaker", "depends_on_agg"],
    ),
    _q(
        "A_R15",
        "Report: items never returned per category in 2001",
        "SELECT category, count(*) AS items, sum(CASE WHEN returned_qty = 0 THEN 1 ELSE 0 END) AS never_returned, round(100.0 * sum(CASE WHEN returned_qty = 0 THEN 1 ELSE 0 END) / count(*), 2) AS pct FROM a_agg_item_returns WHERE d_year = 2001 GROUP BY category ORDER BY category",
        ["conditional_aggregates", "depends_on_agg"],
    ),
    _q(
        "A_R16",
        "Report: coupon intensity bands per store and year",
        "SELECT ss_store_sk, d_year, round(sum(coupon_amt) / sum(net_paid) * 100, 3) AS coupon_pct, CASE WHEN sum(coupon_amt) / sum(net_paid) > 0.05 THEN 'high' WHEN sum(coupon_amt) / sum(net_paid) > 0.02 THEN 'mid' ELSE 'low' END AS band FROM a_agg_store_month GROUP BY ss_store_sk, d_year ORDER BY ss_store_sk, d_year",
        ["case_bands", "depends_on_agg"],
    ),
    _q(
        "A_R17",
        "Report: paid per ticket vs promo lift per category (joins two aggregates, one two levels deep)",
        "SELECT c.d_year, c.category, c.tickets, round(c.net_paid / c.tickets, 2) AS paid_per_ticket, p.lift_pct FROM a_agg_category_year c JOIN a_agg_promo_lift p ON p.d_year = c.d_year AND p.category = c.category ORDER BY c.d_year, c.category",
        ["two_aggregates", "two_level_chain"],
    ),
    _q(
        "A_R18",
        "Report: monthly totals stamped with a report timestamp (audit column)",
        "SELECT d_year, d_moy, round(sum(net_paid), 2) AS paid, count(DISTINCT ss_store_sk) AS stores, current_timestamp() AS report_ts FROM a_agg_store_month GROUP BY d_year, d_moy ORDER BY d_year, d_moy",
        ["audit_clock_columns", "depends_on_agg"],
        "maskable",
        "verify with the clock column report_ts masked",
        ["report_ts"],
    ),
]

ESTATE_A: list[MigrationObject] = STAGING + AGGREGATES + REPORTS
