{{ config(
    materialized='view', schema='ben_sales', database='dev_fd_silver'
) }}

select
    customer_id,
    first_name,
    last_name,
    email,
    cast(signup_date as date) as signup_date,
    country,
    cast(revenue as decimal(10,2)) as revenue,
    {{ dbt_dab_tools.lineage_columns(columns=[{'name': 'signup_date', 'expr': 'cast(signup_date as date)', 'op': 'CAST', 'inputs': ['signup_date']}, {'name': 'revenue', 'expr': 'cast(revenue as decimal(10,2))', 'op': 'CAST', 'inputs': ['revenue']}]) }}
from {{ ref('raw_customers') }}
