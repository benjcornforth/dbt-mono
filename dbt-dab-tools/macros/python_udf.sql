-- dbt-tools/macros/python_udf.sql
-- =============================================
-- DEPLOY UDFs: run-operation macro (dbt best practice)
-- =============================================
-- Usage:
--   dbt run-operation deploy_udfs --args '{"udfs_sql": "CREATE FUNCTION ..."}'
--
-- This is the dbt-native way to deploy UDFs before models run.
-- Called by `forge deploy` automatically.
-- Can also be used as a pre-hook on the first model that uses a UDF.
--
-- For pure-SQL mode (no dbt), forge compile --pure-sql emits
-- UDF CREATE statements directly — this macro is not needed.

{% macro deploy_udfs(udfs_sql) %}
  {% if udfs_sql %}
    {% for statement in udfs_sql.split(';') %}
      {% if statement.strip() %}
        {% do run_query(statement ~ ';') %}
      {% endif %}
    {% endfor %}
    {{ log("✅ UDFs deployed via run-operation", info=True) }}
  {% else %}
    {{ log("⚠ No UDF SQL provided — skipping", info=True) }}
  {% endif %}
{% endmacro %}
