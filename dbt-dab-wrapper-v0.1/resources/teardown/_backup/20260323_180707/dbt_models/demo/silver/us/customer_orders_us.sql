{{ config(
    materialized='table', schema='ben_demo_us', database=var("catalog_silver"), tags=['us'], meta={'version': 'v1'}
) }}

select /*+ BROADCAST(c) */
    c.customer_id,
    c.first_name,
    c.last_name,
    c.email,
    c.country,
    o.order_id,
    o.product,
    o.quantity,
    o.unit_price,
    o.line_total,
    o.order_date,
    {{ dbt_dab_tools.lineage_columns() }}
from {{ ref('customer_clean_us') }} c
inner join {{ ref('stg_orders_us') }} o
    on c.customer_id = o.customer_id
