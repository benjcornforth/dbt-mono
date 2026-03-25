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
from forge.project_paths import PROJECT_PYTHON_DIR


# =============================================
# FORGE TASK — the main entry point for Python
# =============================================

class ForgeTask:
    """
    Context manager for Python tasks in a forge pipeline.

    Reads forge.yml, resolves the selected profile, and provides
    helpers to read/write tables with correct catalog.schema names.

    Usage:
        task = ForgeTask()                    # uses the default resolved profile
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
        """Find forge.yml — checks CWD parents, then relative to this module."""
        # 1. Walk up from CWD (works locally)
        current = Path.cwd()
        for parent in [current, *current.parents]:
            candidate = parent / "forge.yml"
            if candidate.exists():
                return candidate

        # 2. On Databricks, CWD is not the project root.
        #    This module lives at files/forge/python_task.py,
        #    so forge.yml is at files/forge.yml (one level up from this file).
        module_dir = Path(__file__).resolve().parent   # forge/
        files_root = module_dir.parent                  # files/
        candidate = files_root / "forge.yml"
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

        Pass model_def to override layer/schema (from dbt/ddl).
        """
        model_def = model_def or {}
        catalog, schema = resolve_model_schema(
            model_name, model_def, self._expanded, self._config,
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
        validate: bool = False,
    ) -> None:
        """Write a DataFrame to a table with a _lineage STRUCT column.

        Adds provenance metadata so forge explain can trace data origin
        back to the source file / API / event stream.

        When ``validate=True``, every row is checked against the Pydantic
        model generated by ``forge codegen`` before writing.  Bad rows
        raise ``ValueError`` with details.

            task.write_table_with_lineage(
                "raw_customers", df,
                source_path="/Volumes/.../customers_2024.csv",
                source_type="volume",
                validate=True,
            )
        """
        if validate:
            self._validate_df(model_name, df)

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

    def log_lineage(
        self,
        model_name: str,
        row_count: int,
        sources: list[str] | None = None,
        model_def: dict | None = None,
    ) -> None:
        """Write a lineage_log entry for a python-managed model.

        Mirrors the INSERT INTO lineage_log that SQL models get at compile time.
        Call after write_table_with_lineage() to close the lineage loop.

            task.log_lineage("raw_customers", df.count(), sources=["landing/customers.csv"])
        """
        import os
        from datetime import datetime, timezone

        model_def = model_def or {}
        catalog, schema = resolve_model_schema(
            model_name, model_def, self._profile, self._config,
        )
        meta_cat = self.catalog("meta")
        meta_sch = self.schema("meta")
        fq_log = f"{meta_cat}.{meta_sch}.lineage_log"

        run_id = os.environ.get("DBX_JOB_RUN_ID", "local")
        git_commit = os.environ.get("FORGE_GIT_COMMIT", "unknown")
        sources_str = ", ".join(sources) if sources else ""

        insert_sql = (
            f"INSERT INTO {fq_log} "
            f"(run_id, model, materialized, rows_created, catalog, schema, sources, git_commit) "
            f"VALUES ('{run_id}', '{model_name}', 'managed_by_python', "
            f"{row_count}, '{catalog}', '{schema}', '{sources_str}', '{git_commit}')"
        )

        # Prefer SparkSQL (on-cluster), fall back to SQL connector (local)
        if self._spark is not None:
            self._spark.sql(insert_sql)
        else:
            self.sql(insert_sql)

    def _validate_df(self, model_name: str, df: Any) -> None:
        """Validate a DataFrame against the Pydantic model from type_safe."""
        from forge.type_safe import build_models

        models = build_models(profile_name=self.profile_name)
        class_name = "".join(p.capitalize() for p in model_name.split("_"))
        model_cls = getattr(models, class_name, None)
        if model_cls is None:
            return  # no model found — skip validation silently
        errors = model_cls.validate_dataframe(df)
        if errors:
            msg_lines = [f"{len(errors)} validation errors in '{model_name}':"]
            for idx, err in errors[:10]:  # show first 10
                msg_lines.append(f"  Row {idx}: {err}")
            if len(errors) > 10:
                msg_lines.append(f"  ... and {len(errors) - 10} more")
            raise ValueError("\n".join(msg_lines))

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
from __future__ import annotations

import os
import sys

# Databricks runs python_task scripts via exec(compile(...)), so add the
# staged bundle root explicitly for reliable imports.
_script_path = os.path.abspath(sys._getframe().f_code.co_filename)
_files_dir = os.path.dirname(os.path.dirname(_script_path))
if _files_dir not in sys.path:
    sys.path.insert(0, _files_dir)

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


_INGEST_TEMPLATE = '''\
"""
{description}

forge python task: {name}
Stage: ingest
Template: ingest_from_volume

Ingests files from a Unity Catalog Volume into raw tables.
Driven by the ingestion_config seed (dbt/ddl/meta/seeds/) compiled
into the meta catalog during setup.
Tracks every file in the file_manifest table (per-domain, managed_by: python)
defined in dbt/ddl/bronze/models/.
Pydantic validation is provided by forge codegen (dbt/generated/sdk/models.py).
"""
from __future__ import annotations

import hashlib
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

# Databricks runs python_task scripts via exec(compile(...)), so locate the
# staged bundle root dynamically instead of assuming a fixed script folder.
_script_path = Path(os.path.abspath(sys._getframe().f_code.co_filename))
_files_dir = next(
    (
        parent
        for parent in (_script_path.parent, *_script_path.parent.parents)
        if (parent / "forge.yml").exists() and (parent / "forge").is_dir()
    ),
    _script_path.parent.parent,
)
if str(_files_dir) not in sys.path:
    sys.path.insert(0, str(_files_dir))

from forge.python_task import ForgeTask
from forge.type_safe import build_models


def main():
    task = ForgeTask()
    spark = task.spark_session()
    models = build_models(profile_name=task.profile_name)
    domain = task.config.get("domain", spark.conf.get("forge.domain", ""))

    # ── 1. Read ingestion config table from meta catalog ─────
    config_table = task.table(
        "ingestion_config", {{"catalog": "meta", "schema": "config"}}
    )
    configs = [
        models.IngestionConfig(**r.asDict())
        for r in spark.table(config_table).collect()
        if r.active and r.ingest_type == "volume"
    ]

    # ── 2. Read file manifest for dedup ─────────────────────
    manifest_table = task.table("file_manifest")
    try:
        existing = set(
            r.file_path
            for r in spark.table(manifest_table).select("file_path").collect()
        )
    except Exception:
        existing = set()

    ingested = 0

    # ── 3. For each config, list Volume + ingest new files ──
    for cfg in configs:
        files = task.list_volume(cfg.volume_name)
        pattern = re.compile(cfg.file_regex)

        for f in files:
            if f["path"] in existing or not pattern.search(f["name"]):
                continue

            # ── 4. Read file as DataFrame ───────────────────
            read_opts = {{}}
            if cfg.has_header is not None:
                read_opts["header"] = str(cfg.has_header).lower()
            if cfg.delimiter:
                read_opts["sep"] = cfg.delimiter

            df = spark.read.format(cfg.file_format or "csv") \\
                     .options(**read_opts) \\
                     .load(f["path"])

            # ── 5. Write with lineage + validation ──────────
            task.write_table_with_lineage(
                cfg.target_model, df,
                source_path=f["path"],
                source_type="volume",
                validate=True,
            )

            # ── 6. Append manifest entry ────────────────────
            now = datetime.now(timezone.utc)
            entry = models.FileManifest(
                file_path=f["path"],
                file_name=f["name"],
                model_name=cfg.target_model,
                domain=domain or None,
                file_format=cfg.file_format,
                row_count=df.count(),
                file_size_bytes=f["size"],
                checksum=hashlib.md5(f["path"].encode()).hexdigest(),
                ingested_at=now,
                status="SUCCESS",
            )
            manifest_df = spark.createDataFrame(
                [entry.to_spark_row()],
                schema=models.FileManifest.spark_schema(),
            )
            manifest_df.write.mode("append").saveAsTable(manifest_table)
            ingested += 1

    print(f"✅ {{task.profile_name}}: {name} — ingested {{ingested}} files")


if __name__ == "__main__":
    main()
'''


_TEMPLATES: dict[str, str] = {
    "default": _TASK_TEMPLATE,
    "ingest": _INGEST_TEMPLATE,
}


def scaffold_python_task(
    name: str,
    output_dir: Path | None = None,
    stage: str = "enrich",
    description: str = "",
    template: str = "default",
) -> Path:
    """
    Generate a scaffold Python task file.

        scaffold_python_task("enrich_customers")
        # → dbt/project/python/enrich_customers.py

        scaffold_python_task("ingest_orders", template="ingest")
        # → dbt/project/python/ingest_orders.py  (with Volume ingestion logic)
    """
    tmpl = _TEMPLATES.get(template)
    if tmpl is None:
        raise ValueError(
            f"Unknown template '{template}'. "
            f"Available: {', '.join(sorted(_TEMPLATES))}"
        )

    out_dir = output_dir or PROJECT_PYTHON_DIR
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{name}.py"

    desc = description or f"Python task: {name}"
    if template == "ingest":
        stage = "ingest"

    content = tmpl.format(
        name=name,
        stage=stage,
        description=desc,
    )

    out_path.write_text(content)
    return out_path


# =============================================
# LOAD PYTHON TASKS — auto-discovered from dbt/project/python/
# =============================================

def load_python_tasks(_forge_config: dict | None = None) -> list[dict]:
    """
    Auto-discover python tasks from the project-code directory.

    Each .py file in dbt/project/python/ becomes a task. No forge.yml config needed.
    Placement (SETUP vs PROCESS) is DDL-driven via managed_by headers.

    Returns list of task dicts with keys: name, file.
    """
    if not PROJECT_PYTHON_DIR.is_dir():
        return []
    return [
        {"name": file_path.stem, "file": file_path.as_posix()}
        for file_path in sorted(PROJECT_PYTHON_DIR.iterdir())
        if file_path.suffix == ".py" and not file_path.name.startswith("_")
    ]
