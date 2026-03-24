-- Quarantine: customer_clean
INSERT INTO `dev_fd_meta`.`ben_sales`.`transform_quarantine`
SELECT
    'customer_clean' AS model_name,
    '`dev_fd_bronze`.`ben_sales`.`stg_customers`' AS source_table,
    'email IS NULL OR revenue < 0' AS quarantine_rule,
    to_json(struct(*)) AS row_data,
    'unknown' AS git_commit,
    current_timestamp() AS detected_at
FROM `dev_fd_bronze`.`ben_sales`.`stg_customers`
WHERE email IS NULL OR revenue < 0;

-- Execution summary
SELECT
    'customer_clean' AS quarantine_source,
    (SELECT COUNT(*) FROM `dev_fd_meta`.`ben_sales`.`transform_quarantine` WHERE model_name = 'customer_clean') AS rows_quarantined,
    (SELECT COUNT(*) FROM `dev_fd_silver`.`ben_sales`.`customer_clean`) AS rows_passed,
    current_timestamp() AS completed_at;
