"""
Volume ingestion — reads config, lists files, writes raw tables

forge python task: ingest_from_volume
Stage: ingest
Template: ingest_from_volume

Ingests files from a Unity Catalog Volume into raw tables.
Driven by the ingestion_config seed (dbt/ddl/meta/seeds/) loaded
into the meta catalog by `dbt seed`.
Tracks every file in the file_manifest table (managed_by: python)
defined in dbt/ddl/bronze/models/.
Pydantic validation is provided by forge codegen (sdk/models.py).
"""
from __future__ import annotations

import hashlib
import os
import sys
import re
from datetime import datetime, timezone

# Databricks runs python_task scripts via exec(compile(f.read(), filename, 'exec')).
# This means __file__ points to the ipykernel WRAPPER, not our script.
# The correct path lives in the code object's co_filename (set by compile()).
_script_path = os.path.abspath(sys._getframe().f_code.co_filename)
_files_dir = os.path.dirname(os.path.dirname(_script_path))  # python/ → files/
if _files_dir not in sys.path:
    sys.path.insert(0, _files_dir)

from forge.type_safe import build_models


def _parse_runtime_params() -> dict[str, str]:
    """Parse --key=value params injected by forge workflow at deploy time."""
    params: dict[str, str] = {}
    for arg in sys.argv[1:]:
        if arg.startswith("--") and "=" in arg:
            key, _, val = arg[2:].partition("=")
            params[key] = val
    return params


def main():
    from pyspark.sql import SparkSession

    params = _parse_runtime_params()
    spark = SparkSession.builder.getOrCreate()
    models = build_models()

    # Resolved catalog/schema names — baked into the task YAML at deploy time
    catalog_meta = params["catalog_meta"]
    catalog_bronze = params["catalog_bronze"]
    schema = params["schema_bronze"]  # all layers share the same schema

    # ── 1. Read ingestion config seed from meta catalog ─────
    config_table = f"{catalog_meta}.{schema}.ingestion_config"
    configs = [
        r
        for r in spark.table(config_table).collect()
        if r.active and r.ingest_type == "volume"
    ]

    # ── 2. Read file manifest for dedup ─────────────────────
    manifest_table = f"{catalog_bronze}.{schema}.file_manifest"
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
        vol_path = f"/Volumes/{catalog_bronze}/{schema}/{cfg.volume_name}"
        try:
            files = [
                {"path": r.path, "name": r.name, "size": r.size}
                for r in spark.sql(f"LIST '{vol_path}'").collect()
            ]
        except Exception:
            files = []
        pattern = re.compile(cfg.file_regex)

        for f in files:
            if f["path"] in existing or not pattern.search(f["name"]):
                continue

            # ── 4. Read file as DataFrame ───────────────────
            read_opts = {}
            if cfg.has_header is not None:
                read_opts["header"] = str(cfg.has_header).lower()
            if cfg.delimiter:
                read_opts["sep"] = cfg.delimiter

            df = spark.read.format(cfg.file_format or "csv") \
                     .options(**read_opts) \
                     .load(f["path"])

            # ── 5. Write to target table ────────────────────
            target = f"{catalog_bronze}.{schema}.{cfg.target_model}"
            df.write.mode("append").saveAsTable(target)

            # ── 6. Append manifest entry ────────────────────
            now = datetime.now(timezone.utc)
            entry = models.FileManifest(
                file_path=f["path"],
                file_name=f["name"],
                model_name=cfg.target_model,
                domain=None,
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

    print(f"✅ ingest_from_volume — ingested {ingested} files")


if __name__ == "__main__":
    main()
