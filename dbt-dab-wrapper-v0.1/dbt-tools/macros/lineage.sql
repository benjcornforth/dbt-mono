-- =============================================
-- dbt-tools/macros/lineage.sql
-- =============================================
-- THE CODE IS THE DOCUMENTATION
--
-- Embeds provenance + lineage metadata into every model.
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
-- What you get:
--   _lineage_model        → which dbt model produced this row
--   _lineage_sources      → upstream refs that fed this model
--   _lineage_git_commit   → exact git commit at deploy time
--   _lineage_deployed_at  → timestamp of the deploy
--   _lineage_compute_type → serverless / dedicated / auto
--   _lineage_contract_id  → ODCS contract identifier
--   _lineage_version      → methodology version (from model meta)
--
-- A data challenge can be made at any stage:
--   "Show me exactly where this value came from,
--    which code produced it, and when."
--
-- Works on: Databricks, Postgres, Redshift (portable SQL)
-- =============================================

{% macro lineage_columns() %}
    {#- Model identity -#}
    '{{ this.name }}' AS _lineage_model,

    {#- Upstream sources (all refs this model depends on) -#}
    '{{ dbt_dab_tools._lineage_upstream_refs() }}' AS _lineage_sources,

    {#- Git commit at deploy time (set by wrapper) -#}
    '{{ var("git_commit", "unknown") }}' AS _lineage_git_commit,

    {#- Deploy timestamp -#}
    '{{ run_started_at }}' AS _lineage_deployed_at,

    {#- Compute type from wrapper config -#}
    '{{ var("compute_type", "serverless") }}' AS _lineage_compute_type,

    {#- ODCS contract identifier -#}
    '{{ this.schema }}.{{ this.name }}' AS _lineage_contract_id,

    {#- Methodology version from model meta or project var -#}
    '{{ dbt_dab_tools._lineage_methodology_version() }}' AS _lineage_version
{% endmacro %}


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
        t._lineage_model,
        t._lineage_sources,
        t._lineage_git_commit,
        t._lineage_deployed_at,
        t._lineage_compute_type,
        t._lineage_contract_id,
        t._lineage_version,
        t.{{ column_name }} AS challenged_value
    FROM {{ ref(model_name) }} t
    WHERE {{ filter_condition }}
{% endmacro %}
