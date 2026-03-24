{{ config(materialized='table', schema='ben_sales', database='dev_fd_bronze') }}

-- managed_by: ingest_from_volume
-- This table is populated by an external process.
-- Forge generates CREATE TABLE to keep it in the graph with tests.

SELECT
    CAST(NULL AS int) AS customer_id,
    CAST(NULL AS string) AS first_name,
    CAST(NULL AS string) AS last_name,
    CAST(NULL AS string) AS email,
    CAST(NULL AS date) AS signup_date,
    CAST(NULL AS string) AS country,
    CAST(NULL AS decimal(10,2)) AS revenue
WHERE 1 = 0
