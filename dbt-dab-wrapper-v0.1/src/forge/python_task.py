# =============================================
# src/forge/python_task.py
# =============================================
# THE CODE IS THE DOCUMENTATION
#
# Provides the bridge between Python code and
# Databricks tables, using forge.yml for config.
#
# Two execution modes:
#   1. ON-CLUSTER (PySpark) — full SparkSession, DataFrames
#   2. LOCAL (databricks-sql-connector) — lightweight SQL
#
# Usage (on-cluster — inside a Databricks job):
#   from forge.python_task import ForgeTask
#
#   task = ForgeTask()
#   df = task.read_table("stg_customers")
#   df = df.filter(df.email.isNotNull())
#   task.write_table("customer_clean", df)
#
# Usage (local — from your laptop):
#   task = ForgeTask(profile="dev")
#   rows = task.sql("SELECT * FROM dev_fd_silver.stg_customers LIMIT 10")
#   for row in rows:
#       print(row)
#
# Usage (with type-safe models):
#   from forge.python_task import ForgeTask
#   from forge.type_safe import build_models
#
#   task = ForgeTask()
#   models = build_models()
#   df = task.read_table("stg_orders")
#   errors = models.StgOrders.validate_dataframe(df)
#   if errors:
#       print(f"{len(errors)} bad rows")
#
# Orchestration:
#   Declared in forge.yml under python_tasks:
#   Then included in `forge workflow` as spark_python_task entries.

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import yaml

from forge.compute_resolver import (
    ConnectionInfo,
    resolve_connection,
    resolve_model_schema,
    resolve_profile,
    _expand_env_prefix,
)


# =============================================
# FORGE TASK — the main entry point for Python
# =============================================

class ForgeTask:
    """
    Context manager for Python tasks in a forge pipeline.

    Reads forge.yml, resolves the active profile, and provides
    helpers to read/write tables with correct catalog.schema names.

    Usage:
        task = ForgeTask()                    # uses active_profile
        task = ForgeTask(profile="dev")       # explicit profile
        task = ForgeTask(config_path="path/to/forge.yml")

    Table resolution uses the same naming patterns as dbt:
        task.table("stg_customers")
        # → "dev_fd_silver.dev_demo.stg_customers"  (catalog.schema.table)
    """

    def __init__(
        self,
        profile: str | None = None,
        config_path: str | Path | None = None,
    ):
        self._config_path = Path(config_path) if config_path else self._find_config()
        self._config = yaml.safe_load(self._config_path.read_text())
        self._profile = resolve_profile(self._config, profile_name=profile)
        self._expanded = _expand_env_prefix(self._profile, self._config)
        self._connection: ConnectionInfo | None = None
        self._spark: Any = None
        self._sql_conn: Any = None

    @staticmethod
    def _find_config() -> Path:
        """Walk up from cwd to find forge.yml."""
        current = Path.cwd()
        for parent in [current, *current.parents]:
            candidate = parent / "forge.yml"
            if candidate.exists():
                return candidate
        raise FileNotFoundError(
            "No forge.yml found. Run from your project directory "
            "or pass config_path explicitly."
        )

    @property
    def config(self) -> dict:
        """The full forge.yml config."""
        return self._config

    @property
    def profile_name(self) -> str:
        """Name of the active profile."""
        return self._expanded.get("_name", "dev")

    @property
    def platform(self) -> str:
        return self._expanded.get("platform", "databricks")

    @property
    def connection(self) -> ConnectionInfo:
        """Lazily resolved connection details."""
        if self._connection is None:
            self._connection = resolve_connection(self._profile)
        return self._connection

    # ── Table name resolution ─────────────────────────────

    def table(self, model_name: str, model_def: dict | None = None) -> str:
        """
        Resolve the fully-qualified table name for a model.

        Uses the same naming patterns as the dbt compilation:
            task.table("stg_customers")
            # → "dev_fd_silver.dev_demo.stg_customers"

            task.table("customer_summary")
            # → "dev_fd_silver.dev_demo.customer_summary"

        Pass model_def to override layer/schema (from models.yml).
        """
        model_def = model_def or {}
        catalog, schema = resolve_model_schema(
            model_name, model_def, self._profile, self._config,
        )
        return f"{catalog}.{schema}.{model_name}"

    def catalog(self, logical_name: str = "bronze") -> str:
        """Resolve a logical catalog name → concrete name."""
        catalogs = self._expanded.get("catalogs", {})
        return catalogs.get(logical_name, logical_name)

    def schema(self, logical_name: str = "silver") -> str:
        """Resolve a logical schema name → concrete name."""
        schemas = self._expanded.get("schemas", {})
        return schemas.get(logical_name, logical_name)

    # ── PySpark (on-cluster) ──────────────────────────────

    def spark_session(self) -> Any:
        """
        Get or create a SparkSession.

        On Databricks: returns the pre-existing session.
        Locally: creates a local session (requires pyspark installed).

            spark = task.spark_session()
            df = spark.sql("SELECT 1")
        """
        if self._spark is not None:
            return self._spark

        try:
            # On Databricks, the session is pre-created
            from pyspark.sql import SparkSession
            self._spark = SparkSession.builder.getOrCreate()
        except ImportError:
            raise ImportError(
                "PySpark not available. Install it with: pip install pyspark\n"
                "Or use task.sql_connect() for lightweight SQL access."
            )
        return self._spark

    def read_table(self, model_name: str, model_def: dict | None = None) -> Any:
        """
        Read a table as a PySpark DataFrame.

            df = task.read_table("stg_customers")
            df.show()

            # With filter:
            df = task.read_table("stg_orders").filter("quantity > 5")
        """
        fqn = self.table(model_name, model_def)
        spark = self.spark_session()
        return spark.table(fqn)

    def write_table(
        self,
        model_name: str,
        df: Any,
        mode: str = "overwrite",
        model_def: dict | None = None,
    ) -> None:
        """
        Write a PySpark DataFrame to a table.

            task.write_table("customer_enriched", df)
            task.write_table("customer_enriched", df, mode="append")

        The table name is resolved through forge naming patterns.
        """
        fqn = self.table(model_name, model_def)
        df.write.mode(mode).saveAsTable(fqn)

    def run_sql(self, query: str) -> Any:
        """
        Execute a SQL statement via SparkSession.

            df = task.run_sql("SELECT * FROM dev_fd_silver.dev_demo.stg_orders")
            task.run_sql("CREATE TABLE IF NOT EXISTS ...")
        """
        spark = self.spark_session()
        return spark.sql(query)

    # ── Databricks SQL Connector (local / lightweight) ────

    def sql_connect(self) -> Any:
        """
        Connect via databricks-sql-connector (lightweight, no Spark).

        Requires: pip install databricks-sql-connector

            conn = task.sql_connect()
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM dev_fd_silver.dev_demo.stg_customers LIMIT 5")
            rows = cursor.fetchall()
        """
        if self._sql_conn is not None:
            return self._sql_conn

        conn_info = self.connection
        if not conn_info.is_databricks:
            raise ValueError(
                f"sql_connect() only works with Databricks. "
                f"Current platform: {conn_info.platform}"
            )

        if not conn_info.host or not conn_info.token:
            raise ValueError(
                "Missing host or token. Configure ~/.databrickscfg or set "
                "DBT_DATABRICKS_HOST / DBT_DATABRICKS_TOKEN env vars."
            )

        try:
            from databricks import sql as dbsql
        except ImportError:
            raise ImportError(
                "Install databricks-sql-connector:\n"
                "  pip install databricks-sql-connector"
            )

        http_path = conn_info.http_path or os.environ.get("DBT_DATABRICKS_HTTP_PATH", "")
        if not http_path:
            raise ValueError(
                "Missing http_path. Add it to ~/.databrickscfg or set "
                "DBT_DATABRICKS_HTTP_PATH."
            )

        self._sql_conn = dbsql.connect(
            server_hostname=conn_info.host.replace("https://", ""),
            http_path=http_path,
            access_token=conn_info.token,
        )
        return self._sql_conn

    def sql(self, query: str) -> list[dict]:
        """
        Run a SQL query via the lightweight connector and return rows as dicts.

            rows = task.sql("SELECT * FROM dev_fd_silver.dev_demo.stg_customers LIMIT 5")
            for row in rows:
                print(row["email"])

        Uses databricks-sql-connector (no Spark needed).
        """
        conn = self.sql_connect()
        cursor = conn.cursor()
        try:
            cursor.execute(query)
            columns = [desc[0] for desc in cursor.description]
            return [dict(zip(columns, row)) for row in cursor.fetchall()]
        finally:
            cursor.close()

    def close(self) -> None:
        """Close any open connections."""
        if self._sql_conn is not None:
            self._sql_conn.close()
            self._sql_conn = None

    # ── Volume helpers ────────────────────────────────

    def list_volume(self, volume_name: str, catalog: str | None = None, schema: str | None = None) -> list[dict]:
        """List files in a Unity Catalog Volume.

        On Databricks: executes ``LIST '/Volumes/{catalog}/{schema}/{volume}'``.
        Locally: falls back to ``os.listdir()`` for the path.

        Returns a list of dicts with keys: path, name, size.

            files = task.list_volume("landing")
            for f in files:
                print(f["name"], f["size"])
        """
        cat = catalog or self.catalog("bronze")
        sch = schema or self.schema("bronze")
        volume_path = f"/Volumes/{cat}/{sch}/{volume_name}"

        if self.platform == "databricks":
            try:
                spark = self.spark_session()
                rows = spark.sql(f"LIST '{volume_path}'").collect()
                return [
                    {"path": r.path, "name": r.name, "size": r.size}
                    for r in rows
                ]
            except Exception:
                pass

        # Local fallback
        local_path = Path(volume_path)
        if local_path.is_dir():
            return [
                {"path": str(f), "name": f.name, "size": f.stat().st_size}
                for f in local_path.iterdir()
                if f.is_file()
            ]
        return []

    def read_volume_file(self, volume_name: str, file_name: str, file_format: str = "csv", **read_options: Any) -> Any:
        """Read a single file from a Volume as a Spark DataFrame.

            df = task.read_volume_file("landing", "customers_2024.csv", "csv", header=True)
        """
        cat = self.catalog("bronze")
        sch = self.schema("bronze")
        file_path = f"/Volumes/{cat}/{sch}/{volume_name}/{file_name}"
        spark = self.spark_session()
        return spark.read.format(file_format).options(**read_options).load(file_path)

    def write_table_with_lineage(
        self,
        model_name: str,
        df: Any,
        source_path: str = "",
        source_type: str = "volume",
        mode: str = "append",
        model_def: dict | None = None,
    ) -> None:
        """Write a DataFrame to a table with a _lineage STRUCT column.

        Adds provenance metadata so forge explain can trace data origin
        back to the source file / API / event stream.

            task.write_table_with_lineage(
                "raw_customers", df,
                source_path="/Volumes/.../customers_2024.csv",
                source_type="volume",
            )
        """
        from datetime import datetime, timezone
        from pyspark.sql import functions as F

        now = datetime.now(timezone.utc).isoformat()
        lineage_col = F.struct(
            F.lit("3").alias("schema_version"),
            F.lit(model_name).alias("model"),
            F.lit(source_path).alias("source_path"),
            F.lit(source_type).alias("source_type"),
            F.lit(now).alias("ingested_at"),
        ).alias("_lineage")

        df_with_lineage = df.withColumn("_lineage", lineage_col)
        fqn = self.table(model_name, model_def)
        df_with_lineage.write.mode(mode).saveAsTable(fqn)

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()


# =============================================
# SCAFFOLD — generate a Python task file
# =============================================

_TASK_TEMPLATE = '''\
"""
{description}

forge python task: {name}
Stage: {stage}

This file is executed as a Databricks spark_python_task.
It reads/writes tables using the same naming patterns as dbt.
"""
from forge.python_task import ForgeTask


def main():
    task = ForgeTask()

    # ── Read input tables ─────────────────────────────
    # df = task.read_table("stg_customers")

    # ── Transform ─────────────────────────────────────
    # df = df.filter(df.email.isNotNull())

    # ── Write output ──────────────────────────────────
    # task.write_table("customer_enriched", df)

    print(f"✅ {{task.profile_name}}: {name} complete")


if __name__ == "__main__":
    main()
'''


def scaffold_python_task(
    name: str,
    output_dir: Path | None = None,
    stage: str = "enrich",
    description: str = "",
) -> Path:
    """
    Generate a scaffold Python task file.

        scaffold_python_task("enrich_customers")
        # → python/enrich_customers.py
    """
    out_dir = output_dir or Path("python")
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{name}.py"

    desc = description or f"Python task: {name}"
    content = _TASK_TEMPLATE.format(
        name=name,
        stage=stage,
        description=desc,
    )

    out_path.write_text(content)
    return out_path


# =============================================
# LOAD PYTHON TASKS FROM forge.yml
# =============================================

def load_python_tasks(forge_config: dict) -> list[dict]:
    """
    Read python_tasks from forge.yml.

    forge.yml:
        python_tasks:
          - name: enrich_customers
            file: python/enrich_customers.py
            stage: enrich
          - name: export_report
            file: python/export_report.py
            stage: serve
            depends_on: [customer_summary]

    Returns list of task dicts with keys: name, file, stage, depends_on.
    """
    tasks = forge_config.get("python_tasks", [])
    result = []
    for task in tasks:
        result.append({
            "name": task["name"],
            "file": task.get("file", f"python/{task['name']}.py"),
            "stage": task.get("stage", "enrich"),
            "depends_on": task.get("depends_on", []),
        })
    return result
