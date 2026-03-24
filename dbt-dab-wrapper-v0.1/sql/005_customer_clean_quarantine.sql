-- Quarantine: customer_clean
CREATE OR REPLACE TABLE `dev_fd_silver`.`ben_sales`.`customer_clean_quarantine`
USING DELTA
AS
SELECT *,
    'customer_clean' AS _quarantine_source,
    'unknown' AS _quarantine_git_commit,
    '2026-03-24T11:41:41.828284+00:00' AS _quarantine_detected_at
FROM `dev_fd_silver`.`ben_sales`.`customer_clean`
WHERE email IS NULL OR revenue < 0;

-- Execution summary
SELECT
    'customer_clean_quarantine' AS model,
    'quarantine' AS materialized,
    (SELECT COUNT(*) FROM `dev_fd_silver`.`ben_sales`.`customer_clean_quarantine`) AS rows_quarantined,
    (SELECT COUNT(*) FROM `dev_fd_silver`.`ben_sales`.`customer_clean`) AS rows_passed,
    current_timestamp() AS completed_at;
