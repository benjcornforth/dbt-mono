{{ config(
    materialized='view', schema='ben_sales', database='dev_fd_silver'
) }}

select
    order_id,
    customer_id,
    product,
    quantity,
    cast(unit_price as decimal(10,2)) as unit_price,
    (quantity) * (cast(unit_price as decimal(10,2))) as line_total,
    cast(order_date as date) as order_date,
    {{ lineage_columns(columns=[{'name': 'unit_price', 'expr': 'cast(unit_price as decimal(10,2))', 'op': 'CAST', 'inputs': ['unit_price']}, {'name': 'line_total', 'expr': 'quantity * unit_price', 'op': 'EXPRESSION'}, {'name': 'order_date', 'expr': 'cast(order_date as date)', 'op': 'CAST', 'inputs': ['order_date']}]) }}
from {{ ref('raw_orders') }}
