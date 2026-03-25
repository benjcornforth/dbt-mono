-- Local prior-version macro for Forge.
{% macro prior_version(ref_name) %}
  {{ ref(ref_name) }}_v_previous
{% endmacro %}