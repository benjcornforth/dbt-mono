-- dbt/models/customer_clean.sql
-- Pure business logic. SQL only.
-- Lineage columns auto-embed provenance for every row.
SELECT
  email,
  {{ dbt_dab_tools.python_udf('clean_email') }}(email) AS clean_email,
  {{ dbt_dab_tools.lineage_columns() }}
FROM {{ ref('raw_customers') }}
{{ dbt_dab_tools.quarantine('clean_email IS NULL') }}
