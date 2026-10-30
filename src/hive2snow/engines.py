"""The two sides of the migration.

Source: local Spark SQL over the TPC-DS Parquet files (Spark SQL runs HiveQL: the same
functions, `LATERAL VIEW`, complex types, `INSERT OVERWRITE ... PARTITION`). It stands in for
the client's Hive / Spark estate; no cluster is needed.

Target: Snowflake, through the Python connector with key-pair authentication (a service user
created once by `snowflake_setup.sql`).

Both return pandas DataFrames with lower-case column names, so results can be compared.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from functools import cache
from pathlib import Path
from typing import Any

import pandas as pd

from hive2snow import hive_writes
from hive2snow.config import GENERATED, Env, prepare_spark_env

PARQUET_DIR = GENERATED / "tpcds_parquet"
SPARK_WAREHOUSE = GENERATED / "spark_warehouse"


@dataclass
class RunResult:
    ok: bool
    frame: pd.DataFrame | None
    error: str
    seconds: float
    statements: int = 1


def _lower(frame: pd.DataFrame) -> pd.DataFrame:
    frame.columns = [str(c).lower() for c in frame.columns]
    return frame


def split_statements(sql: str) -> list[str]:
    """Split a script on ';' outside quotes and comments (good enough for the eval objects)."""
    out, buf, quote, i = [], [], None, 0
    while i < len(sql):
        ch = sql[i]
        if quote:
            buf.append(ch)
            if ch == quote:
                quote = None
        elif ch in ("'", '"', "`"):
            quote = ch
            buf.append(ch)
        elif sql.startswith("--", i):
            j = sql.find("\n", i)
            i = len(sql) if j < 0 else j
            buf.append("\n")
            continue
        elif ch == ";":
            out.append("".join(buf))
            buf = []
        else:
            buf.append(ch)
        i += 1
    out.append("".join(buf))
    return [s.strip() for s in out if s.strip()]


class Spark:
    """Source engine (singleton session)."""

    def __init__(self) -> None:
        prepare_spark_env()
        from pyspark.sql import SparkSession

        SPARK_WAREHOUSE.mkdir(parents=True, exist_ok=True)
        self.session = (
            SparkSession.builder.master("local[4]")
            .appName("hive2snow-source")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "8")
            .config("spark.driver.memory", "3g")
            .config("spark.sql.warehouse.dir", SPARK_WAREHOUSE.as_posix())
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
            .getOrCreate()
        )
        self._register_views()

    def _register_views(self) -> None:
        for path in sorted(PARQUET_DIR.glob("*.parquet")):
            self.session.read.parquet(path.as_posix()).createOrReplaceTempView(path.stem)

    def run(self, sql: str) -> RunResult:
        start = time.perf_counter()
        statements = split_statements(sql)
        try:
            frame = None
            for statement in statements:
                if hive_writes.is_write(statement):  # no Hadoop natives here: emulated
                    df = hive_writes.execute(self.session, statement)
                    frame = df.toPandas() if df is not None else pd.DataFrame()
                    continue
                frame = self.session.sql(statement).toPandas()
            assert frame is not None
            return RunResult(True, _lower(frame), "", time.perf_counter() - start, len(statements))
        except Exception as e:
            return RunResult(
                False, None, _spark_error(e), time.perf_counter() - start, len(statements)
            )

    def reset(self) -> None:
        """Drop the views an ETL object created, keep the TPC-DS tables."""
        base = {p.stem for p in PARQUET_DIR.glob("*.parquet")}
        for t in self.session.catalog.listTables():
            if t.name not in base:
                self.session.catalog.dropTempView(t.name)


def _spark_error(e: Exception) -> str:
    text = str(e)
    for marker in ("AnalysisException", "ParseException"):
        if marker in text:
            text = text.split(marker, 1)[1]
    return text.strip().splitlines()[0][:600] if text.strip() else type(e).__name__


@cache
def spark() -> Spark:
    return Spark()


class Snowflake:
    """Target engine (one connection, reconnects when it drops)."""

    def __init__(self, schema: str | None = None) -> None:
        self.env = Env()
        self.schema = schema or self.env.snowflake_schema
        self._con: Any = None

    def connect(self) -> Any:
        if self._con is not None:
            return self._con
        import snowflake.connector
        from cryptography.hazmat.primitives import serialization

        key = serialization.load_pem_private_key(
            Path(self.env.snowflake_private_key_path).read_bytes(), password=None
        )
        pkb = key.private_bytes(
            serialization.Encoding.DER,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        )
        self._con = snowflake.connector.connect(
            account=self.env.snowflake_account,
            user=self.env.snowflake_user,
            private_key=pkb,
            warehouse=self.env.snowflake_warehouse,
            database=self.env.snowflake_database,
            role=self.env.snowflake_role,
            session_parameters={"QUERY_TAG": "hive2snow", "TIMEZONE": "UTC"},
            client_session_keep_alive=True,
        )
        cur = self._con.cursor()
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {self.env.snowflake_database}.{self.schema}")
        cur.execute(f"USE SCHEMA {self.env.snowflake_database}.{self.schema}")
        return self._con

    def run(self, sql: str, timeout_s: int = 120) -> RunResult:
        start = time.perf_counter()
        statements = split_statements(sql)
        try:
            cur = self.connect().cursor()
            frame = None
            for statement in statements:
                cur.execute(statement, timeout=timeout_s)
                frame = cur.fetch_pandas_all() if cur.description else pd.DataFrame()
            assert frame is not None
            return RunResult(True, _lower(frame), "", time.perf_counter() - start, len(statements))
        except Exception as e:
            # Snowflake errors span lines ("SQL compilation error:" / detail): keep the detail
            text = " ".join(str(e).split())[:700] if str(e).strip() else type(e).__name__
            if "connection" in text.lower() or "closed" in text.lower():
                self._con = None
            return RunResult(False, None, text, time.perf_counter() - start, len(statements))

    def execute(self, sql: str) -> None:
        r = self.run(sql)
        if not r.ok:
            raise RuntimeError(r.error)
