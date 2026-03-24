-- Quarantine: customer_clean
CREATE SCHEMA IF NOT EXISTS `dev_fd_silver`.`ben_sales`;

CREATE OR REPLACE TABLE `dev_fd_silver`.`ben_sales`.`customer_clean_quarantine`
USING DELTA
AS
SELECT *,
    'customer_clean' AS _quarantine_source,
    'unknown' AS _quarantine_git_commit,
    '2026-03-24T18:49:47.139643+00:00' AS _quarantine_detected_at
FROM `dev_fd_silver`.`ben_sales`.`customer_clean`
WHERE email IS NULL OR revenue < 0;

-- Execution summary
SELECT
    'customer_clean_quarantine' AS model,
    'quarantine' AS materialized,
    (SELECT COUNT(*) FROM `dev_fd_silver`.`ben_sales`.`customer_clean_quarantine`) AS rows_quarantined,
    (SELECT COUNT(*) FROM `dev_fd_silver`.`ben_sales`.`customer_clean`) AS rows_passed,
    current_timestamp() AS completed_at;
