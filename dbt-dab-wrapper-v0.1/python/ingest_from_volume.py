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


def _build_schema(params: dict[str, str], model_name: str):
    """Build a StructType from --schema_{model} runtime param.

    Param format: 'col1:type1|col2:type2|...'
    Falls back to None if no schema param exists.
    """
    raw = params.get(f"schema_{model_name}")
    if not raw:
        return None

    from pyspark.sql.types import (
        StructType, StructField,
        StringType, IntegerType, LongType, DoubleType, FloatType,
        BooleanType, DateType, TimestampType, DecimalType, ShortType,
    )

    type_map = {
        "string": StringType(),
        "int": IntegerType(),
        "integer": IntegerType(),
        "bigint": LongType(),
        "long": LongType(),
        "smallint": ShortType(),
        "short": ShortType(),
        "double": DoubleType(),
        "float": FloatType(),
        "boolean": BooleanType(),
        "date": DateType(),
        "timestamp": TimestampType(),
    }

    fields = []
    for part in raw.split("|"):
        col_name, _, col_type = part.partition(":")
        col_type_lower = col_type.strip().lower()
        if col_type_lower in type_map:
            spark_type = type_map[col_type_lower]
        elif col_type_lower.startswith("decimal"):
            # e.g. decimal(10,2)
            import re as _re
            m = _re.match(r"decimal\((\d+),\s*(\d+)\)", col_type_lower)
            if m:
                spark_type = DecimalType(int(m.group(1)), int(m.group(2)))
            else:
                spark_type = DecimalType(10, 0)
        else:
            spark_type = StringType()
        fields.append(StructField(col_name.strip(), spark_type, True))

    return StructType(fields)


def main():
    from pyspark.sql import SparkSession

    params = _parse_runtime_params()
    spark = SparkSession.builder.getOrCreate()

    # Resolved catalog/schema names — baked into the task YAML at deploy time
    catalog_meta = params["catalog_meta"]
    catalog_bronze = params["catalog_bronze"]
    schema_bronze = params["schema_bronze"]
    schema_meta = params["schema_meta"]

    print(
        f"📋 Runtime params: catalog_meta={catalog_meta}, catalog_bronze={catalog_bronze}, "
        f"schema_bronze={schema_bronze}, schema_meta={schema_meta}"
    )

    # ── 1. Read ingestion config seed from meta catalog ─────
    config_table = f"{catalog_meta}.{schema_meta}.ingestion_config"
    print(f"📖 Reading ingestion config from {config_table}")
    all_rows = spark.table(config_table).collect()
    print(f"   Found {len(all_rows)} total config rows")
    configs = [r for r in all_rows if r.active and r.ingest_type == "volume"]
    print(f"   {len(configs)} active volume configs: {[r.source_name for r in configs]}")

    # ── 2. Read file manifest for dedup ─────────────────────
    manifest_table = f"{catalog_bronze}.{schema_bronze}.file_manifest"
    try:
        existing = set(
            r.file_path
            for r in spark.table(manifest_table).select("file_path").collect()
        )
        print(f"📂 File manifest: {len(existing)} previously ingested files")
    except Exception as e:
        existing = set()
        print(f"📂 File manifest: empty or not found ({e})")

    ingested = 0

    # ── 3. For each config, list Volume + ingest new files ──
    for cfg in configs:
        vol_path = f"/Volumes/{catalog_bronze}/{schema_bronze}/{cfg.volume_name}"
        print(f"\n🔍 [{cfg.source_name}] Listing volume: {vol_path}")
        try:
            files = [
                {"path": r.path, "name": r.name, "size": r.size}
                for r in spark.sql(f"LIST '{vol_path}'").collect()
            ]
            print(f"   Found {len(files)} files in volume")
        except Exception as e:
            files = []
            print(f"   ⚠️ Failed to list volume: {e}")
        pattern = re.compile(cfg.file_regex)
        if files:
            print(f"   Files found: {[f['name'] for f in files]}")
        matched = [f for f in files if pattern.search(f["name"])]
        skipped = [f for f in matched if f["path"] in existing]
        new_files = [f for f in matched if f["path"] not in existing]
        print(f"   Regex /{cfg.file_regex}/: {len(matched)} matched, {len(skipped)} already ingested, {len(new_files)} new")

        for f in new_files:
            print(f"   📥 Ingesting: {f['name']} ({f['size']} bytes)")

            try:
                # ── 4. Read file as DataFrame ───────────────────
                target = f"{catalog_bronze}.{schema_bronze}.{cfg.target_model}"
                ddl_schema = _build_schema(params, cfg.target_model)

                read_opts = {}
                if cfg.has_header is not None:
                    read_opts["header"] = str(cfg.has_header).lower()
                if cfg.delimiter:
                    read_opts["sep"] = cfg.delimiter

                reader = spark.read.format(cfg.file_format or "csv") \
                         .options(**read_opts)
                if ddl_schema:
                    reader = reader.schema(ddl_schema)
                df = reader.load(f["path"])

                row_count = df.count()
                print(f"      Read {row_count} rows, columns: {df.columns}")

                # ── 5. Tag each row with ingestion metadata ────
                from pyspark.sql import functions as F
                df = df.withColumn("_origin_file", F.lit(f["path"]))
                df = df.withColumn("_inserted_at", F.current_timestamp())

                # ── 6. Write to target table ────────────────────
                df.write.mode("append").saveAsTable(target)
                print(f"      ✅ Written to {target}")

                # ── 6. Append manifest entry ────────────────────
                now = datetime.now(timezone.utc)
                manifest_df = spark.createDataFrame(
                    [{
                        "file_path": f["path"],
                        "file_name": f["name"],
                        "model_name": cfg.target_model,
                        "domain": None,
                        "file_format": cfg.file_format,
                        "row_count": row_count,
                        "file_size_bytes": f["size"],
                        "checksum": hashlib.md5(f["path"].encode()).hexdigest(),
                        "ingested_at": now,
                        "status": "SUCCESS",
                    }],
                    schema=spark.table(manifest_table).schema,
                )
                manifest_df.write.mode("append").saveAsTable(manifest_table)
                print(f"      ✅ Manifest entry recorded")
                ingested += 1

            except Exception as exc:
                print(f"      ⚠️ QUARANTINED: {f['name']} — {exc}")
                now = datetime.now(timezone.utc)

                # ── Record in ingest_quarantine (central table) ──
                ingest_q_table = f"{catalog_meta}.{schema_meta}.ingest_quarantine"
                q_df = spark.createDataFrame(
                    [{
                        "file_path": f["path"],
                        "file_name": f["name"],
                        "target_model": cfg.target_model,
                        "file_format": cfg.file_format,
                        "file_size_bytes": f["size"],
                        "error_message": str(exc)[:4000],
                        "git_commit": params.get("git_commit", "unknown"),
                        "detected_at": now,
                    }],
                    schema=spark.table(ingest_q_table).schema,
                )
                q_df.write.mode("append").saveAsTable(ingest_q_table)
                print(f"      ✅ Ingest quarantine entry recorded")

                # ── Also mark in file manifest ──
                manifest_df = spark.createDataFrame(
                    [{
                        "file_path": f["path"],
                        "file_name": f["name"],
                        "model_name": cfg.target_model,
                        "domain": None,
                        "file_format": cfg.file_format,
                        "row_count": 0,
                        "file_size_bytes": f["size"],
                        "checksum": hashlib.md5(f["path"].encode()).hexdigest(),
                        "ingested_at": now,
                        "status": "QUARANTINED",
                    }],
                    schema=spark.table(manifest_table).schema,
                )
                manifest_df.write.mode("append").saveAsTable(manifest_table)
                print(f"      ✅ Manifest entry recorded")

    print(f"\n{'='*50}")
    print(f"✅ ingest_from_volume — ingested {ingested} files")


if __name__ == "__main__":
    main()
