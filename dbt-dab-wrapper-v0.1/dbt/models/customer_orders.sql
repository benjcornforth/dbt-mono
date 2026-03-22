-- =============================================
-- dbt/models/customer_orders.sql
-- =============================================
-- Joins clean customers with their orders.
-- Multi-source lineage: stg_orders + customer_clean.
-- This model demonstrates lineage across multiple refs.

{{ config(
    materialized='table',
    meta={'version': 'v1'}
) }}

select
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
from {{ ref('customer_clean') }} c
inner join {{ ref('stg_orders') }} o
    on c.customer_id = o.customer_id
