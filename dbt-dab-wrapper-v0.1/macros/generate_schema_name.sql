{% macro generate_schema_name(custom_schema_name, node) -%}
    {#  Forge-managed: use the custom schema directly.
        dbt's default concatenates default_schema + custom_schema,
        which doubles the name when both resolve to the same value.
        Since forge resolves full schema names via var("schema_*"),
        we use the custom schema as-is.  #}
    {%- if custom_schema_name is not none -%}
        {{ custom_schema_name | trim }}
    {%- else -%}
        {{ target.schema }}
    {%- endif -%}
{%- endmacro %}
