-- =============================================
-- RECONCILE — post-clean row-count audit
-- =============================================
-- Runs after customer_clean to verify row counts match between
-- staging and clean layers. Inserts a reconciliation record.
-- Referenced by custom_task: run_reconciliation (stage: enrich)

-- Compare row counts between stg_customers → customer_clean
-- If mismatch exceeds threshold, log a warning row.

CREATE TABLE IF NOT EXISTS {{ catalog }}.{{ schema }}.reconciliation_log (
  check_name        STRING,
  source_table      STRING,
  target_table      STRING,
  source_count      BIGINT,
  target_count      BIGINT,
  delta             BIGINT,
  status            STRING,
  checked_at        TIMESTAMP
) USING DELTA;

INSERT INTO {{ catalog }}.{{ schema }}.reconciliation_log
WITH counts AS (
  SELECT
    (SELECT COUNT(*) FROM {{ catalog }}.{{ schema }}.stg_customers) AS src,
    (SELECT COUNT(*) FROM {{ catalog }}.{{ schema }}.customer_clean) AS tgt
)
SELECT
  'stg_to_clean_customers'  AS check_name,
  'stg_customers'           AS source_table,
  'customer_clean'          AS target_table,
  src                       AS source_count,
  tgt                       AS target_count,
  src - tgt                 AS delta,
  CASE
    WHEN src = tgt THEN 'PASS'
    WHEN src - tgt <= 5 THEN 'WARN'
    ELSE 'FAIL'
  END                       AS status,
  current_timestamp()       AS checked_at
FROM counts;
