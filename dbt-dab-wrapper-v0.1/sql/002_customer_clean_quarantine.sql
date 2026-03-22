-- Quarantine: customer_clean
CREATE OR REPLACE TABLE `public`.`silver`.`customer_clean_quarantine`
USING DELTA
AS
SELECT *,
    'customer_clean' AS _quarantine_source,
    'unknown' AS _quarantine_git_commit,
    '2026-03-22T20:51:23.367834+00:00' AS _quarantine_detected_at
FROM `public`.`silver`.`customer_clean`
WHERE email IS NULL OR revenue < 0;
