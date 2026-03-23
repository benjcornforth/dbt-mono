-- dbt-dab-tools/macros/forge_udfs.sql
-- =============================================
-- FORGE OPERATIONAL UDFs — cross-project functions
-- =============================================
-- These UDFs are managed by the forge wrapper (dbt-dab-tools),
-- not by individual projects. They query the shared _backup_archive
-- table in the operations catalog.
--
-- Usage:
--   dbt run-operation deploy_forge_udfs
--
-- Prerequisites:
--   profiles.yml must have var("archive_table") set (auto-injected by forge).

{% macro deploy_forge_udfs() %}
  {% set archive_table = var("archive_table") %}
  {% set catalog = target.catalog %}
  {% set schema = target.schema %}

  {% set diff_sql %}
    CREATE OR REPLACE FUNCTION {{ catalog }}.{{ schema }}.diff_backup_history(
      ref_a STRING,
      ref_b STRING
    )
    RETURNS TABLE (
      asset_name      STRING,
      asset_type      STRING,
      change_type     STRING,
      ref_a_value     STRING,
      ref_b_value     STRING,
      ref_a_row_count BIGINT,
      ref_b_row_count BIGINT,
      ref_a_timestamp TIMESTAMP,
      ref_b_timestamp TIMESTAMP
    )
    RETURN
      WITH snap_a AS (
        SELECT asset_name, asset_type, row_count, archived_at,
               git_sha, git_branch
        FROM {{ archive_table }}
        WHERE git_sha = ref_a OR git_branch = ref_a
      ),
      snap_b AS (
        SELECT asset_name, asset_type, row_count, archived_at,
               git_sha, git_branch
        FROM {{ archive_table }}
        WHERE git_sha = ref_b OR git_branch = ref_b
      ),
      latest_a AS (
        SELECT asset_name, asset_type, row_count, archived_at,
               COALESCE(git_sha, git_branch) AS ref_val,
               ROW_NUMBER() OVER (PARTITION BY asset_name ORDER BY archived_at DESC) AS rn
        FROM snap_a
      ),
      latest_b AS (
        SELECT asset_name, asset_type, row_count, archived_at,
               COALESCE(git_sha, git_branch) AS ref_val,
               ROW_NUMBER() OVER (PARTITION BY asset_name ORDER BY archived_at DESC) AS rn
        FROM snap_b
      ),
      a AS (SELECT * FROM latest_a WHERE rn = 1),
      b AS (SELECT * FROM latest_b WHERE rn = 1)
      SELECT
        COALESCE(a.asset_name, b.asset_name)   AS asset_name,
        COALESCE(a.asset_type, b.asset_type)   AS asset_type,
        CASE
          WHEN a.asset_name IS NULL THEN 'ADDED'
          WHEN b.asset_name IS NULL THEN 'REMOVED'
          WHEN a.row_count != b.row_count THEN 'CHANGED'
          ELSE 'UNCHANGED'
        END                                     AS change_type,
        a.ref_val                               AS ref_a_value,
        b.ref_val                               AS ref_b_value,
        a.row_count                             AS ref_a_row_count,
        b.row_count                             AS ref_b_row_count,
        a.archived_at                           AS ref_a_timestamp,
        b.archived_at                           AS ref_b_timestamp
      FROM a
      FULL OUTER JOIN b ON a.asset_name = b.asset_name
      WHERE a.asset_name IS NULL
         OR b.asset_name IS NULL
         OR a.row_count != b.row_count
  {% endset %}

  {% set deploy_sql %}
    CREATE OR REPLACE FUNCTION {{ catalog }}.{{ schema }}.fetch_last_deployments(
      n INT
    )
    RETURNS TABLE (
      deployment_id   BIGINT,
      git_sha         STRING,
      git_branch      STRING,
      environment     STRING,
      performed_by    STRING,
      deployed_at     TIMESTAMP,
      asset_count     BIGINT,
      total_rows      BIGINT,
      assets          STRING
    )
    RETURN
      WITH deployments AS (
        SELECT
          git_sha,
          git_branch,
          environment,
          performed_by,
          MIN(archived_at) AS deployed_at,
          COUNT(*)         AS asset_count,
          SUM(row_count)   AS total_rows,
          CONCAT_WS(', ', COLLECT_SET(asset_name)) AS assets
        FROM {{ archive_table }}
        GROUP BY git_sha, git_branch, environment, performed_by
      ),
      ranked AS (
        SELECT *,
          ROW_NUMBER() OVER (ORDER BY deployed_at DESC) AS rn
        FROM deployments
      )
      SELECT
        rn              AS deployment_id,
        git_sha,
        git_branch,
        environment,
        performed_by,
        deployed_at,
        asset_count,
        total_rows,
        assets
      FROM ranked
      WHERE rn <= n
  {% endset %}

  {% do run_query(diff_sql) %}
  {{ log("✅ diff_backup_history() deployed", info=True) }}

  {% do run_query(deploy_sql) %}
  {{ log("✅ fetch_last_deployments() deployed", info=True) }}

{% endmacro %}
