-- =============================================
-- dbt/models/customer_summary.sql
-- =============================================
-- Gold layer: aggregated customer metrics.
-- Full lineage chain: raw -> staging -> clean -> summary.

{{ config(
    materialized='table',
    meta={'version': 'v1'}
) }}

select
    customer_id,
    first_name,
    last_name,
    email,
    country,
    count(order_id) as total_orders,
    sum(line_total) as total_revenue,
    min(order_date) as first_order_date,
    max(order_date) as last_order_date,
    {{ dbt_dab_tools.lineage_columns() }}
from {{ ref('customer_orders') }}
group by
    customer_id,
    first_name,
    last_name,
    email,
    country
