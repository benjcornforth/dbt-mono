{{ config(
    materialized='table', schema='ben_sales', database='dev_fd_silver', meta={'version': 'v1'}
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
    {{ lineage_columns() }}
from {{ ref('customer_clean') }} c
inner join {{ ref('stg_orders') }} o
    on c.customer_id = o.customer_id
