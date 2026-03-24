{{ config(materialized='table', schema=var("schema_bronze"), database=var("catalog_bronze")) }}

-- managed_by: python
-- This table is populated by an external process.
-- Forge generates CREATE TABLE to keep it in the graph with tests.

SELECT
    CAST(NULL AS int) AS order_id,
    CAST(NULL AS int) AS customer_id,
    CAST(NULL AS string) AS product,
    CAST(NULL AS int) AS quantity,
    CAST(NULL AS decimal(10,2)) AS unit_price,
    CAST(NULL AS date) AS order_date
WHERE 1 = 0
