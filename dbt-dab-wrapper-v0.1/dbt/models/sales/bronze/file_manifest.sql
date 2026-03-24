{{ config(materialized='table', schema='ben_sales', database='dev_fd_bronze') }}

-- managed_by: ingest_from_volume
-- This table is populated by an external process.
-- Forge generates CREATE TABLE to keep it in the graph with tests.

SELECT
    CAST(NULL AS string) AS file_path,
    CAST(NULL AS string) AS file_name,
    CAST(NULL AS string) AS model_name,
    CAST(NULL AS string) AS domain,
    CAST(NULL AS string) AS file_format,
    CAST(NULL AS int) AS row_count,
    CAST(NULL AS int) AS file_size_bytes,
    CAST(NULL AS string) AS checksum,
    CAST(NULL AS timestamp) AS ingested_at,
    CAST(NULL AS string) AS status
WHERE 1 = 0
