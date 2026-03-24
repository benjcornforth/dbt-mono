{{ config(
    materialized='table', schema='ben_sales', database='dev_fd_silver', meta={'version': 'v1'}
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
    loyalty_tier(sum(line_total)) as tier,
    {{ dbt_dab_tools.lineage_columns(columns=[{'name': 'total_orders', 'expr': 'count(order_id)', 'op': 'AGGREGATION'}, {'name': 'total_revenue', 'expr': 'sum(line_total)', 'op': 'AGGREGATION'}, {'name': 'first_order_date', 'expr': 'min(order_date)', 'op': 'AGGREGATION'}, {'name': 'last_order_date', 'expr': 'max(order_date)', 'op': 'AGGREGATION'}, {'name': 'tier', 'expr': 'loyalty_tier(total_revenue)', 'udf_name': 'loyalty_tier', 'op': 'SQL_UDF', 'inputs': ['total_revenue']}]) }}
from {{ ref('customer_orders') }}
group by
    customer_id, first_name, last_name, email, country
