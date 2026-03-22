-- dbt-tools/macros/python_udf.sql
-- COMMENT: Creates Python UDFs but keeps everything SQL-visible for lineage.
-- Child calls {{ python_udf('clean_email') }} in any SQL model.
{% macro python_udf(name, return_type='string') %}
  CREATE OR REPLACE FUNCTION {{ target.schema }}.{{ name }} (input STRING)
  RETURNS {{ return_type }}
  LANGUAGE PYTHON
  AS $$
    def {{ name }}(input):
        # Python code loaded from external udfs/ folder (versioned in dbt-tools)
        pass  # placeholder - real code in dbt-tools/udfs/
    return {{ name }}(input)
  $$;
{% endmacro %}
