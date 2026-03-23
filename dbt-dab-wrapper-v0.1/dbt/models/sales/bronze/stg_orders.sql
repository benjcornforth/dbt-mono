{{ config(
    materialized='view', schema=var("schema_bronze"), database=var("catalog_bronze")
) }}

select
    order_id,
    customer_id,
    product,
    quantity,
    cast(unit_price as decimal(10,2)) as unit_price,
    quantity * unit_price as line_total,
    cast(order_date as date) as order_date,
    {{ dbt_dab_tools.lineage_columns(columns=[{'name': 'unit_price', 'expr': 'cast(unit_price as decimal(10,2))', 'op': 'CAST', 'inputs': ['unit_price']}, {'name': 'line_total', 'expr': 'quantity * unit_price', 'op': 'EXPRESSION'}, {'name': 'order_date', 'expr': 'cast(order_date as date)', 'op': 'CAST', 'inputs': ['order_date']}]) }}
from {{ ref('raw_orders') }}
