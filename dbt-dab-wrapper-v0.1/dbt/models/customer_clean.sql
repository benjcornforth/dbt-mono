-- =============================================
-- dbt/models/customer_clean.sql
-- =============================================
-- Clean customer model with quarantine.
-- Bad rows (null email, negative revenue) get moved
-- to customer_clean_quarantine table automatically.
--
-- Methodology: v2 (added revenue filter in Mar 2026)

{{ config(
    materialized='table',
    meta={'version': 'v2'},
    post_hook="{{ dbt_dab_tools.quarantine('email IS NULL OR revenue < 0') }}"
) }}

select
    customer_id,
    first_name,
    last_name,
    email,
    signup_date,
    country,
    revenue,
    {{ dbt_dab_tools.lineage_columns() }}
from {{ ref('stg_customers') }}
