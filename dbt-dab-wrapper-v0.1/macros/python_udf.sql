-- Local run-operation macro for Forge UDF deployment.
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