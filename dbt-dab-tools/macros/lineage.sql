-- =============================================
-- dbt-tools/macros/lineage.sql
-- =============================================
-- THE CODE IS THE DOCUMENTATION
--
-- Embeds provenance + lineage metadata into every model
-- as a single STRUCT column called `_lineage`.
-- Aligned with ODCS (Open Data Contract Standard) so every
-- row in every table carries its own audit trail.
--
-- Usage in a model:
--   SELECT
--     col_a,
--     col_b,
--     {{ lineage_columns() }}
--   FROM {{ ref('upstream_model') }}
--
-- What you get (single _lineage column, struct fields):
--   _lineage.schema_version → struct shape version (for migrations)
--   _lineage.model          → which dbt model produced this row
--   _lineage.sources        → upstream refs that fed this model
--   _lineage.git_commit     → exact git commit at deploy time
--   _lineage.deployed_at    → timestamp of the deploy
--   _lineage.compute_type   → serverless / dedicated / auto
--   _lineage.contract_id    → ODCS contract identifier
--   _lineage.version        → methodology version (from model meta)
--
-- Access a field:
--   Databricks / Spark: SELECT _lineage.model FROM my_table
--   Postgres:           SELECT _lineage->>'model' FROM my_table
--
-- A data challenge can be made at any stage:
--   "Show me exactly where this value came from,
--    which code produced it, and when."
--
-- Works on: Databricks, Spark, Postgres (adapter-aware)
-- =============================================

{#- Bump this constant whenever the struct shape changes.
    The lineage_migrate() macro uses it to detect stale rows. -#}
{% set LINEAGE_SCHEMA_VERSION = '3' %}

-- =============================================
-- VERBOSE COLUMN LINEAGE
-- =============================================
-- Pass `columns` to capture runtime input values
-- alongside the expression that produced each output.
--
-- Supported op types (covers all SQL statement patterns):
--   PASSTHROUGH  → column passed through unchanged
--   CAST         → type conversion (cast, ::)
--   EXPRESSION   → arithmetic / string ops (a * b, concat)
--   CASE         → conditional logic (CASE WHEN ... END)
--   AGGREGATION  → aggregate functions (SUM, COUNT, MIN, MAX, AVG)
--   WINDOW       → window functions (ROW_NUMBER, RANK, LAG, LEAD)
--   FUNCTION     → scalar function or UDF call
--   JOIN         → column sourced from a joined table
--   CONSTANT     → hard-coded literal value
--
-- Usage:
--   {{ dbt_dab_tools.lineage_columns(columns=[
--       {'name': 'order_id'},
--       {'name': 'unit_price', 'expr': 'cast(unit_price as decimal(10,2))', 'inputs': ['unit_price'], 'op': 'CAST'},
--       {'name': 'line_total', 'expr': 'quantity * unit_price', 'inputs': ['quantity', 'unit_price']},
--       {'name': 'total_rev',  'expr': 'sum(line_total)', 'inputs': ['line_total'], 'op': 'AGGREGATION'},
--       {'name': 'tier',       'expr': "case when revenue>1000 then 'gold' else 'silver' end", 'inputs': ['revenue'], 'op': 'CASE'},
--   ]) }}
--
-- Each row then carries the actual input VALUES at query time:
--   _lineage.columns[0].inputs  →  {'quantity': '5', 'unit_price': '10.00'}
-- =============================================

{% macro lineage_columns(columns=none) %}
    {%- set base_fields = {
        'schema_version': dbt_dab_tools._lineage_schema_version(),
        'model':          this.name,
        'sources':        dbt_dab_tools._lineage_upstream_refs(),
        'git_commit':     var("git_commit", "unknown"),
        'deployed_at':    run_started_at | string,
        'compute_type':   var("compute_type", "serverless"),
        'contract_id':    this.schema ~ '.' ~ this.name,
        'version':        dbt_dab_tools._lineage_methodology_version(),
    } -%}

    {%- if target.type in ('databricks', 'spark') -%}
    named_struct(
        {%- for key, val in base_fields.items() %}
        '{{ key }}', '{{ val }}',
        {%- endfor %}
        'columns',
        {%- if columns %}
        array(
            {%- for col in columns %}
            {%- set col_name = col['name'] -%}
            {%- set col_expr = col['expr'] if 'expr' in col else col_name -%}
            {%- set col_op = col['op'] if 'op' in col else ('EXPRESSION' if 'expr' in col or 'inputs' in col else 'PASSTHROUGH') -%}
            {%- set col_inputs = col['inputs'] if 'inputs' in col else [col_name] -%}
            named_struct(
                'name', '{{ col_name }}',
                'expression', '{{ col_expr }}',
                'op', '{{ col_op }}',
                'inputs', map(
                    {%- for inp in col_inputs %}
                    '{{ inp }}', cast({{ inp }} as string){{ ',' if not loop.last }}
                    {%- endfor %}
                )
            ){{ ',' if not loop.last }}
            {%- endfor %}
        )
        {%- else %}
        cast(null as array<struct<name:string, expression:string, op:string, inputs:map<string,string>>>)
        {%- endif %}
    ) AS _lineage

    {%- elif target.type == 'postgres' -%}
    jsonb_build_object(
        {%- for key, val in base_fields.items() %}
        '{{ key }}', '{{ val }}',
        {%- endfor %}
        'columns',
        {%- if columns %}
        jsonb_build_array(
            {%- for col in columns %}
            {%- set col_name = col['name'] -%}
            {%- set col_expr = col['expr'] if 'expr' in col else col_name -%}
            {%- set col_op = col['op'] if 'op' in col else ('EXPRESSION' if 'expr' in col or 'inputs' in col else 'PASSTHROUGH') -%}
            {%- set col_inputs = col['inputs'] if 'inputs' in col else [col_name] -%}
            jsonb_build_object(
                'name', '{{ col_name }}',
                'expression', '{{ col_expr }}',
                'op', '{{ col_op }}',
                'inputs', jsonb_build_object(
                    {%- for inp in col_inputs %}
                    '{{ inp }}', {{ inp }}::text{{ ',' if not loop.last }}
                    {%- endfor %}
                )
            ){{ ',' if not loop.last }}
            {%- endfor %}
        )
        {%- else %}
        'null'::jsonb
        {%- endif %}
    ) AS _lineage

    {%- else -%}
    {#- Fallback: JSON string for Redshift / others -#}
    cast(
        '{{ tojson(base_fields) }}'
        as varchar(8000)
    ) AS _lineage
    {%- endif -%}
{% endmacro %}

{#- Returns the current struct schema version constant -#}
{% macro _lineage_schema_version() %}{{ dbt_dab_tools.LINEAGE_SCHEMA_VERSION }}{% endmacro %}


-- =============================================
-- Helper: collect upstream refs for this model
-- =============================================
{% macro _lineage_upstream_refs() %}
    {%- set upstream = [] -%}
    {%- if model.depends_on is defined and model.depends_on.nodes is defined -%}
        {%- for node_id in model.depends_on.nodes -%}
            {%- set parts = node_id.split('.') -%}
            {%- do upstream.append(parts[-1]) -%}
        {%- endfor -%}
    {%- endif -%}
    {{ upstream | join(', ') }}
{%- endmacro %}


-- =============================================
-- Helper: methodology version from model meta
-- =============================================
{% macro _lineage_methodology_version() %}
    {%- if model.config is defined and model.config.meta is defined and model.config.meta.version is defined -%}
        {{ model.config.meta.version }}
    {%- elif model.meta is defined and model.meta is mapping and model.meta.get('version') -%}
        {{ model.meta['version'] }}
    {%- else -%}
        {{ var("methodology_version", "v1") }}
    {%- endif -%}
{%- endmacro %}


-- =============================================
-- FULL PROVENANCE MACRO
-- =============================================
-- For models that want row-level provenance on EVERY column:
--   SELECT
--     {{ provenance_wrap('col_a', 'upstream_model') }},
--     {{ provenance_wrap('col_b', 'upstream_model') }}
--   FROM {{ ref('upstream_model') }}
--
-- Produces: col_a (unchanged) but registers the lineage in
-- the _provenance_registry audit table.
-- =============================================

{% macro provenance_wrap(column_name, source_model) %}
    {#- The column value passes through unchanged -#}
    {{ column_name }}
    {#- But we register it in the provenance audit log -#}
    {%- if execute -%}
        {%- do dbt_dab_tools._register_column_provenance(column_name, source_model) -%}
    {%- endif -%}
{% endmacro %}


{% macro _register_column_provenance(column_name, source_model) %}
    {#-
      This writes to an in-memory registry that the wrapper's
      graph.py picks up after compile. No runtime cost.
      The registry becomes part of the ODCS contract's lineage section.
    -#}
    {%- set registry = var('_provenance_registry', []) -%}
    {%- do registry.append({
        'target_model': this.name,
        'target_column': column_name,
        'source_model': source_model,
        'source_column': column_name,
        'git_commit': var('git_commit', 'unknown'),
        'deployed_at': run_started_at | string,
    }) -%}
{%- endmacro %}


-- =============================================
-- DATA CHALLENGE MACRO
-- =============================================
-- When a user challenges a value, this macro generates
-- the audit query to trace it back to source.
--
-- Usage:
--   {{ data_challenge('customers', 'revenue', 'customer_id = 42') }}
--
-- Generates a query that joins the model → lineage columns
-- → upstream sources, giving full traceability.
-- =============================================

{% macro data_challenge(model_name, column_name, filter_condition) %}
    SELECT
        '{{ model_name }}' AS challenged_model,
        '{{ column_name }}' AS challenged_column,
        {%- if target.type in ('databricks', 'spark') %}
        t._lineage.model          AS _lineage_model,
        t._lineage.sources        AS _lineage_sources,
        t._lineage.git_commit     AS _lineage_git_commit,
        t._lineage.deployed_at    AS _lineage_deployed_at,
        t._lineage.compute_type   AS _lineage_compute_type,
        t._lineage.contract_id    AS _lineage_contract_id,
        t._lineage.version        AS _lineage_version,
        t._lineage.schema_version AS _lineage_schema_version,
        {%- elif target.type == 'postgres' %}
        t._lineage->>'model'          AS _lineage_model,
        t._lineage->>'sources'        AS _lineage_sources,
        t._lineage->>'git_commit'     AS _lineage_git_commit,
        t._lineage->>'deployed_at'    AS _lineage_deployed_at,
        t._lineage->>'compute_type'   AS _lineage_compute_type,
        t._lineage->>'contract_id'    AS _lineage_contract_id,
        t._lineage->>'version'        AS _lineage_version,
        t._lineage->>'schema_version' AS _lineage_schema_version,
        {%- else %}
        t._lineage,
        {%- endif %}
        t.{{ column_name }} AS challenged_value
    FROM {{ ref(model_name) }} t
    WHERE {{ filter_condition }}
{% endmacro %}


-- =============================================
-- LINEAGE STRUCT MIGRATION
-- =============================================
-- When the struct shape changes between versions, materialized
-- tables still hold rows with the old struct schema. This macro
-- detects stale rows and rebuilds the _lineage column in-place.
--
-- Usage (as a pre_hook on any materialized table):
--   {{ config(
--       pre_hook="{{ dbt_dab_tools.lineage_migrate() }}"
--   ) }}
--
-- Or run standalone after a dbt-dab-tools version bump:
--   dbt run-operation lineage_migrate --args '{model_name: customer_clean}'
--
-- How it works:
--   1. Reads schema_version from existing _lineage column
--   2. If it differs from LINEAGE_SCHEMA_VERSION, rebuilds
--      the table with the new struct shape (adding new fields
--      with defaults, preserving existing values)
--   3. Rows are never lost — only the _lineage column is re-cast
-- =============================================

{% macro lineage_migrate(model_name=none) %}
    {%- set current_version = dbt_dab_tools.LINEAGE_SCHEMA_VERSION -%}
    {%- set target_relation = ref(model_name) if model_name else this -%}

    {%- if target.type in ('databricks', 'spark') -%}
    {#- On Databricks: rebuild _lineage struct for stale rows,
        preserving existing values and adding new fields with defaults. -#}
    MERGE INTO {{ target_relation }} AS t
    USING (
        SELECT *,
            named_struct(
                'schema_version', '{{ current_version }}',
                'model',          coalesce(_lineage.model, ''),
                'sources',        coalesce(_lineage.sources, ''),
                'git_commit',     coalesce(_lineage.git_commit, 'unknown'),
                'deployed_at',    coalesce(_lineage.deployed_at, ''),
                'compute_type',   coalesce(_lineage.compute_type, 'serverless'),
                'contract_id',    coalesce(_lineage.contract_id, ''),
                'version',        coalesce(_lineage.version, 'v1'),
                'columns',        coalesce(_lineage.columns,
                    cast(null as array<struct<name:string, expression:string, op:string, inputs:map<string,string>>>)
                )
            ) AS _lineage_new
        FROM {{ target_relation }}
        WHERE _lineage.schema_version != '{{ current_version }}'
           OR _lineage.schema_version IS NULL
    ) AS src
    ON t._lineage = src._lineage
    WHEN MATCHED THEN UPDATE SET t._lineage = src._lineage_new;

    {%- elif target.type == 'postgres' -%}
    {#- On Postgres (JSONB): merge in missing keys + bump schema_version. -#}
    UPDATE {{ target_relation }}
    SET _lineage = jsonb_build_object('columns', 'null'::jsonb)
        || _lineage
        || jsonb_build_object('schema_version', '{{ current_version }}')
    WHERE _lineage->>'schema_version' IS DISTINCT FROM '{{ current_version }}';

    {%- endif -%}
{% endmacro %}
