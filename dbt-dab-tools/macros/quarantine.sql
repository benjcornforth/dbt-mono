-- dbt-tools/macros/quarantine.sql
-- COMMENT: Quarantines bad rows without failing the model.
-- Used as a post_hook: post_hook=quarantine('revenue < 0')
-- Works on Databricks AND Postgres.
{% macro quarantine(failing_condition) %}
  {% set quarantine_table = '`' ~ this.database ~ '`.`' ~ this.schema ~ '`.`' ~ this.name ~ '_quarantine`' %}
  CREATE OR REPLACE TABLE {{ quarantine_table }} AS
  SELECT *,
    '{{ this.name }}' AS _quarantine_source,
    '{{ var("git_commit", "local") }}' AS _quarantine_git_commit,
    '{{ run_started_at }}' AS _quarantine_detected_at
  FROM {{ this }}
  WHERE {{ failing_condition }}
{% endmacro %}
