{{ config(
    materialized='table', schema='ben_sales', database='dev_fd_silver', meta={'version': 'v1'}
) }}

select /*+ BROADCAST(c) */
    c.customer_id as customer_id,
    c.first_name as first_name,
    c.last_name as last_name,
    c.email as email,
    c.country as country,
    o.order_id as order_id,
    o.product as product,
    o.quantity as quantity,
    o.unit_price as unit_price,
    o.line_total as line_total,
    o.order_date as order_date,
    {{ lineage_columns() }}
from {{ ref('customer_clean') }} c
inner join {{ ref('stg_orders') }} o
    on c.customer_id = o.customer_id
