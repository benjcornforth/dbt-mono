-- =============================================
-- macros/lineage.sql
-- =============================================
-- Local lineage/provenance macros for Forge.

{% set LINEAGE_SCHEMA_VERSION = '4' %}

{% macro lineage_columns(columns=none, origin=none, lineage_mode='full') %}
    {%- set base_fields = {
        'schema_version': _lineage_schema_version(),
        'model':          this.name,
        'sources':        _lineage_upstream_refs(),
        'git_commit':     var("git_commit", "unknown"),
        'deployed_at':    run_started_at | string,
        'compute_type':   var("compute_type", "serverless"),
        'contract_id':    this.schema ~ '.' ~ this.name,
        'version':        _lineage_methodology_version(),
        'lineage_mode':   lineage_mode,
    } -%}

    {%- set is_full = (lineage_mode == 'full') -%}

    {%- if target.type in ('databricks', 'spark') -%}
    named_struct(
        {%- for key, val in base_fields.items() %}
        '{{ key }}', '{{ val }}',
        {%- endfor %}
        'origin',
        {%- if origin %}
        named_struct(
            'type', '{{ origin.type | default("unknown") }}',
            'path', '{{ origin.path | default("") }}',
            'endpoint', '{{ origin.endpoint | default("") }}',
            'format', '{{ origin.format | default("") }}',
            'loaded_at', current_timestamp()
        ),
        {%- else %}
        cast(null as struct<type:string, path:string, endpoint:string, format:string, loaded_at:timestamp>),
        {%- endif %}
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
                    {%- if is_full %}
                    '{{ inp }}', cast({{ inp }} as string){{ ',' if not loop.last }}
                    {%- else %}
                    '{{ inp }}', '{{ inp }}'{{ ',' if not loop.last }}
                    {%- endif %}
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
        'origin',
        {%- if origin %}
        jsonb_build_object(
            'type', '{{ origin.type | default("unknown") }}',
            'path', '{{ origin.path | default("") }}',
            'endpoint', '{{ origin.endpoint | default("") }}',
            'format', '{{ origin.format | default("") }}',
            'loaded_at', now()
        ),
        {%- else %}
        'null'::jsonb,
        {%- endif %}
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
                    {%- if is_full %}
                    '{{ inp }}', {{ inp }}::text{{ ',' if not loop.last }}
                    {%- else %}
                    '{{ inp }}', '{{ inp }}'{{ ',' if not loop.last }}
                    {%- endif %}
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
    cast(
        '{{ tojson(base_fields) }}'
        as varchar(8000)
    ) AS _lineage
    {%- endif -%}
{% endmacro %}

{% macro _lineage_schema_version() %}{{ LINEAGE_SCHEMA_VERSION }}{% endmacro %}

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

{% macro _lineage_methodology_version() %}
    {%- if model.config is defined and model.config.meta is defined and model.config.meta.version is defined -%}
        {{ model.config.meta.version }}
    {%- elif model.meta is defined and model.meta is mapping and model.meta.get('version') -%}
        {{ model.meta['version'] }}
    {%- else -%}
        {{ var("methodology_version", "v1") }}
    {%- endif -%}
{%- endmacro %}

{% macro provenance_wrap(column_name, source_model) %}
    {{ column_name }}
    {%- if execute -%}
        {%- do _register_column_provenance(column_name, source_model) -%}
    {%- endif -%}
{% endmacro %}

{% macro _register_column_provenance(column_name, source_model) %}
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

{% macro lineage_migrate(model_name=none) %}
    {%- set current_version = LINEAGE_SCHEMA_VERSION -%}
    {%- set target_relation = ref(model_name) if model_name else this -%}

    {%- if target.type in ('databricks', 'spark') -%}
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
    UPDATE {{ target_relation }}
    SET _lineage = jsonb_build_object('columns', 'null'::jsonb)
        || _lineage
        || jsonb_build_object('schema_version', '{{ current_version }}')
    WHERE _lineage->>'schema_version' IS DISTINCT FROM '{{ current_version }}';

    {%- endif -%}
{% endmacro %}