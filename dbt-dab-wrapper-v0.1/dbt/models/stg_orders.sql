-- =============================================
-- dbt/models/stg_orders.sql
-- =============================================
-- Staging layer for orders.
-- Verbose lineage: every column's inputs + expression
-- are captured at runtime in _lineage.columns.

{{ config(materialized='view') }}

select
    order_id,
    customer_id,
    product,
    quantity,
    cast(unit_price as decimal(10,2)) as unit_price,
    quantity * unit_price as line_total,
    cast(order_date as date) as order_date,
    {{ dbt_dab_tools.lineage_columns(columns=[
        {'name': 'order_id'},
        {'name': 'customer_id'},
        {'name': 'product'},
        {'name': 'quantity'},
        {'name': 'unit_price',  'expr': 'cast(unit_price as decimal(10,2))', 'inputs': ['unit_price'], 'op': 'CAST'},
        {'name': 'line_total',  'expr': 'quantity * unit_price',             'inputs': ['quantity', 'unit_price']},
        {'name': 'order_date',  'expr': 'cast(order_date as date)',          'inputs': ['order_date'], 'op': 'CAST'},
    ]) }}
from {{ ref('raw_orders') }}
