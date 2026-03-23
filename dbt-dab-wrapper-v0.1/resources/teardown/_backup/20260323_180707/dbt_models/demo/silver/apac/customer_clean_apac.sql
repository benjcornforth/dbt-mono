{{ config(
    materialized='table', schema='ben_demo_apac', database=var("catalog_silver"), tags=['apac'], meta={'version': 'v2'}, post_hook="{{ dbt_dab_tools.quarantine('email IS NULL OR revenue < 0') }}"
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
from {{ ref('stg_customers_apac') }}
