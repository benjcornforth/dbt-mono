-- Local quarantine macro for Forge.
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