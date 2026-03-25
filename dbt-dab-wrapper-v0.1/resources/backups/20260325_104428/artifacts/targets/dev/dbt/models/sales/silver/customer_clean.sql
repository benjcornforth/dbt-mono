{{ config(
    materialized='table', schema='ben_sales', database='dev_fd_silver', meta={'version': 'v2'}, post_hook="{{ quarantine('email IS NULL OR revenue < 0') }}"
) }}

select
    customer_id,
    first_name,
    last_name,
    email,
    signup_date,
    country,
    revenue,
    {{ lineage_columns() }}
from {{ ref('stg_customers') }}
