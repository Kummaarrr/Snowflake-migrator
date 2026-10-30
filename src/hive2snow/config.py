"""Paths and settings: secrets from the environment / .env, nothing else machine-specific."""

from __future__ import annotations

import os
import sys
from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict

PROJECT_ROOT = Path(__file__).resolve().parents[2]
CONFIG_DIR = PROJECT_ROOT / "config"
DATA_DIR = PROJECT_ROOT / "data"
GENERATED = DATA_DIR / "generated"


def project_path(relative: str) -> Path:
    return PROJECT_ROOT / relative


class Env(BaseSettings):
    """Values read from environment variables / .env (the shell wins)."""

    model_config = SettingsConfigDict(
        env_file=PROJECT_ROOT / ".env", env_file_encoding="utf-8", extra="ignore"
    )

    anthropic_api_key: str = ""
    max_run_cost_usd: float = 5.0
    snowflake_account: str = ""
    snowflake_user: str = ""
    snowflake_private_key_path: str = ""
    snowflake_warehouse: str = "COMPUTE_WH"
    snowflake_database: str = "MIGRATION_EVAL"
    snowflake_schema: str = "TPCDS"
    snowflake_role: str = "ACCOUNTADMIN"


def prepare_spark_env() -> None:
    """Windows: PySpark needs the interpreter path and a Hadoop home with winutils."""
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    hadoop = Path(r"C:\hadoop")
    if sys.platform == "win32" and (hadoop / "bin" / "winutils.exe").exists():
        os.environ.setdefault("HADOOP_HOME", str(hadoop))
        os.environ["PATH"] = str(hadoop / "bin") + os.pathsep + os.environ.get("PATH", "")
