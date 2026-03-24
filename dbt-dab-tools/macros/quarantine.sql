-- dbt-tools/macros/quarantine.sql
-- COMMENT: Quarantines bad rows to the central transform_quarantine table.
-- Used as a post_hook: post_hook=quarantine('revenue < 0')
-- Requires dbt var: meta_catalog (e.g. dev_fd_meta)
{% macro quarantine(failing_condition) %}
  {% set meta_catalog = var("meta_catalog", this.database) %}
  {% set quarantine_table = '`' ~ meta_catalog ~ '`.`' ~ this.schema ~ '`.`transform_quarantine`' %}
  INSERT INTO {{ quarantine_table }}
  SELECT
    '{{ this.name }}' AS model_name,
    '{{ this }}' AS source_table,
    '{{ failing_condition }}' AS quarantine_rule,
    to_json(struct(*)) AS row_data,
    '{{ var("git_commit", "local") }}' AS git_commit,
    current_timestamp() AS detected_at
  FROM {{ this }}
  WHERE {{ failing_condition }}
{% endmacro %}
