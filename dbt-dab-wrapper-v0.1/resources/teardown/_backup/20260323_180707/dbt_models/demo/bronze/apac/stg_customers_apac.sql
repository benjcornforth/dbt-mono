{{ config(
    materialized='view', schema='ben_demo_apac', database=var("catalog_bronze"), tags=['apac']
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
from {{ source('seed', 'raw_customers') }}
