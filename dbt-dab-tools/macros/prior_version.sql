-- dbt-tools/macros/prior_version.sql
-- COMMENT: Safe rollback to previous deployment version.
-- Child uses {{ ref('model', version='prior') }}
{% macro prior_version(ref_name) %}
  {{ ref(ref_name) }}_v_previous
{% endmacro %}
