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
import re
from datetime import datetime, timezone

from forge.python_task import ForgeTask
from forge.type_safe import build_models


def main():
    task = ForgeTask()
    spark = task.spark_session()
    models = build_models()
    domain = task.config.get("domain", spark.conf.get("forge.domain", ""))

    # ── 1. Read ingestion config seed from meta catalog ─────
    config_table = task.table(
        "ingestion_config", {"catalog": "meta", "schema": "config"}
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
            read_opts = {}
            if cfg.has_header is not None:
                read_opts["header"] = str(cfg.has_header).lower()
            if cfg.delimiter:
                read_opts["sep"] = cfg.delimiter

            df = spark.read.format(cfg.file_format or "csv") \
                     .options(**read_opts) \
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

    print(f"✅ {task.profile_name}: ingest_from_volume — ingested {ingested} files")


if __name__ == "__main__":
    main()
