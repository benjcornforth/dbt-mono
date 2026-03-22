-- =============================================
-- dbt/models/stg_customers.sql
-- =============================================
-- Staging layer: light cleaning, typed, with lineage.
-- Business logic only - all tooling is in dbt-tools/.

{{ config(materialized='view') }}

select
    customer_id,
    first_name,
    last_name,
    email,
    cast(signup_date as date) as signup_date,
    country,
    cast(revenue as decimal(10,2)) as revenue,
    {{ dbt_dab_tools.lineage_columns() }}
from {{ ref('raw_customers') }}
