CREATE SCHEMA IF NOT EXISTS `dev_fd_bronze`.`ben_sales`;
CREATE SCHEMA IF NOT EXISTS `dev_fd_meta`.`ben_lineage`;
CREATE SCHEMA IF NOT EXISTS `dev_fd_silver`.`ben_sales`;

-- Table stubs for lineage UDFs (forge-internal)
CREATE TABLE IF NOT EXISTS `dev_fd_meta`.`ben_lineage`.`lineage_graph` (
    target_model STRING NOT NULL,
    source_model STRING NOT NULL,
    target_column STRING,
    source_column STRING,
    join_key STRING,
    source_key STRING,
    transform_type STRING NOT NULL,
    expression STRING,
    target_catalog STRING NOT NULL,
    target_schema STRING NOT NULL,
    source_catalog STRING NOT NULL,
    source_schema STRING NOT NULL,
    git_commit STRING DEFAULT 'unknown',
    updated_at TIMESTAMP DEFAULT current_timestamp()
)
USING DELTA
TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported');

CREATE TABLE IF NOT EXISTS `dev_fd_meta`.`ben_lineage`.`lineage_log` (
    run_id STRING NOT NULL,
    model STRING NOT NULL,
    materialized STRING,
    rows_created BIGINT,
    catalog STRING,
    schema STRING,
    sources STRING,
    git_commit STRING,
    completed_at TIMESTAMP DEFAULT current_timestamp()
)
USING DELTA
TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported');

-- UDF: average_score
-- Pandas vectorized UDF
-- Vectorized average using pandas — fast on large batches
DROP FUNCTION IF EXISTS `dev_fd_silver`.`ben_sales`.average_score;
CREATE FUNCTION `dev_fd_silver`.`ben_sales`.average_score(score_a double, score_b double)
RETURNS double
LANGUAGE PYTHON
AS $$
import pandas as pd
def average_score(score_a: pd.Series, score_b: pd.Series) -> pd.Series:
    return (score_a + score_b) / 2.0
$$;

-- UDF: clean_email
-- Lowercase and trim email addresses
DROP FUNCTION IF EXISTS `dev_fd_silver`.`ben_sales`.clean_email;
CREATE FUNCTION `dev_fd_silver`.`ben_sales`.clean_email(raw_email string)
RETURNS string
LANGUAGE PYTHON
AS $$
def clean_email(raw_email):
    if raw_email is None:
        return None
    return raw_email.strip().lower()
$$;

-- UDF: loyalty_tier
-- Assigns GOLD/SILVER/BRONZE based on revenue
DROP FUNCTION IF EXISTS `dev_fd_silver`.`ben_sales`.loyalty_tier;
CREATE FUNCTION `dev_fd_silver`.`ben_sales`.loyalty_tier(revenue decimal(18,2))
RETURNS string
RETURN (CASE
  WHEN revenue >= 1000 THEN 'GOLD'
  WHEN revenue >=  500 THEN 'SILVER'
  ELSE 'BRONZE'
END);

-- UDF: trace_lineage
-- Trace any value back through the full DAG (max depth 10)
DROP FUNCTION IF EXISTS `dev_fd_meta`.`ben_lineage`.trace_lineage;
CREATE FUNCTION `dev_fd_meta`.`ben_lineage`.trace_lineage(
    start_model STRING, key_col STRING, key_val STRING
)
RETURNS STRING
RETURN (
  SELECT concat_ws('\n', collect_list(line))
  FROM (
    SELECT
      concat(
        '[', CAST(depth AS STRING), '] ',
        model, '.', key_column, ' = ', key_value,
        CASE WHEN transform_type IS NOT NULL
             THEN concat(' (', transform_type,
                  CASE WHEN expression IS NOT NULL AND expression != ''
                       THEN concat(': ', expression) ELSE '' END, ')')
             ELSE '' END
      ) AS line
    FROM (
      SELECT 0 AS depth,
             start_model AS model,
             key_col AS key_column,
             key_val AS key_value,
             start_model AS path,
             CAST(NULL AS STRING) AS transform_type,
             CAST(NULL AS STRING) AS expression,
             CAST(NULL AS STRING) AS source_catalog,
             CAST(NULL AS STRING) AS source_schema
      UNION ALL
      SELECT 1 AS depth,
             g0.source_model AS model,
             COALESCE(g0.source_key, g0.join_key, key_col) AS key_column,
             key_val AS key_value,
             concat(start_model, ' -> ', g0.source_model) AS path,
             g0.transform_type,
             g0.expression,
             g0.source_catalog,
             g0.source_schema
      FROM `dev_fd_meta`.`ben_lineage`.lineage_graph g0
      WHERE g0.target_model = start_model
      UNION ALL
      SELECT 2 AS depth,
             g1.source_model AS model,
             COALESCE(g1.source_key, g1.join_key, COALESCE(g0.source_key, g0.join_key, key_col)) AS key_column,
             key_val AS key_value,
             concat(start_model, ' -> ', g0.source_model, ' -> ', g1.source_model) AS path,
             g1.transform_type,
             g1.expression,
             g1.source_catalog,
             g1.source_schema
      FROM `dev_fd_meta`.`ben_lineage`.lineage_graph g0
      JOIN `dev_fd_meta`.`ben_lineage`.lineage_graph g1 ON g0.source_model = g1.target_model
      WHERE g0.target_model = start_model
      UNION ALL
      SELECT 3 AS depth,
             g2.source_model AS model,
             COALESCE(g2.source_key, g2.join_key, COALESCE(g1.source_key, g1.join_key, COALESCE(g0.source_key, g0.join_key, key_col))) AS key_column,
             key_val AS key_value,
             concat(start_model, ' -> ', g0.source_model, ' -> ', g1.source_model, ' -> ', g2.source_model) AS path,
             g2.transform_type,
             g2.expression,
             g2.source_catalog,
             g2.source_schema
      FROM `dev_fd_meta`.`ben_lineage`.lineage_graph g0
      JOIN `dev_fd_meta`.`ben_lineage`.lineage_graph g1 ON g0.source_model = g1.target_model
      JOIN `dev_fd_meta`.`ben_lineage`.lineage_graph g2 ON g1.source_model = g2.target_model
      WHERE g0.target_model = start_model
      UNION ALL
      SELECT 4 AS depth,
             g3.source_model AS model,
             COALESCE(g3.source_key, g3.join_key, COALESCE(g2.source_key, g2.join_key, COALESCE(g1.source_key, g1.join_key, COALESCE(g0.source_key, g0.join_key, key_col)))) AS key_column,
             key_val AS key_value,
             concat(start_model, ' -> ', g0.source_model, ' -> ', g1.source_model, ' -> ', g2.source_model, ' -> ', g3.source_model) AS path,
             g3.transform_type,
             g3.expression,
             g3.source_catalog,
             g3.source_schema
      FROM `dev_fd_meta`.`ben_lineage`.lineage_graph g0
      JOIN `dev_fd_meta`.`ben_lineage`.lineage_graph g1 ON g0.source_model = g1.target_model
      JOIN `dev_fd_meta`.`ben_lineage`.lineage_graph g2 ON g1.source_model = g2.target_model
      JOIN `dev_fd_meta`.`ben_lineage`.lineage_graph g3 ON g2.source_model = g3.target_model
      WHERE g0.target_model = start_model
      UNION ALL
      SELECT 5 AS depth,
             g4.source_model AS model,
             COALESCE(g4.source_key, g4.join_key, COALESCE(g3.source_key, g3.join_key, COALESCE(g2.source_key, g2.join_key, COALESCE(g1.source_key, g1.join_key, COALESCE(g0.source_key, g0.join_key, key_col))))) AS key_column,
             key_val AS key_value,
             concat(start_model, ' -> ', g0.source_model, ' -> ', g1.source_model, ' -> ', g2.source_model, ' -> ', g3.source_model, ' -> ', g4.source_model) AS path,
             g4.transform_type,
             g4.expression,
             g4.source_catalog,
             g4.source_schema
      FROM `dev_fd_meta`.`ben_lineage`.lineage_graph g0
      JOIN `dev_fd_meta`.`ben_lineage`.lineage_graph g1 ON g0.source_model = g1.target_model
      JOIN `dev_fd_meta`.`ben_lineage`.lineage_graph g2 ON g1.source_model = g2.target_model
      JOIN `dev_fd_meta`.`ben_lineage`.lineage_graph g3 ON g2.source_model = g3.target_model
      JOIN `dev_fd_meta`.`ben_lineage`.lineage_graph g4 ON g3.source_model = g4.target_model
      WHERE g0.target_model = start_model
      UNION ALL
      SELECT 6 AS depth,
             g5.source_model AS model,
             COALESCE(g5.source_key, g5.join_key, COALESCE(g4.source_key, g4.join_key, COALESCE(g3.source_key, g3.join_key, COALESCE(g2.source_key, g2.join_key, COALESCE(g1.source_key, g1.join_key, COALESCE(g0.source_key, g0.join_key, key_col)))))) AS key_column,
             key_val AS key_value,
             concat(start_model, ' -> ', g0.source_model, ' -> ', g1.source_model, ' -> ', g2.source_model, ' -> ', g3.source_model, ' -> ', g4.source_model, ' -> ', g5.source_model) AS path,
             g5.transform_type,
             g5.expression,
             g5.source_catalog,
             g5.source_schema
      FROM `dev_fd_meta`.`ben_lineage`.lineage_graph g0
      JOIN `dev_fd_meta`.`ben_lineage`.lineage_graph g1 ON g0.source_model = g1.target_model
      JOIN `dev_fd_meta`.`ben_lineage`.lineage_graph g2 ON g1.source_model = g2.target_model
      JOIN `dev_fd_meta`.`ben_lineage`.lineage_graph g3 ON g2.source_model = g3.target_model
      JOIN `dev_fd_meta`.`ben_lineage`.lineage_graph g4 ON g3.source_model = g4.target_model
      JOIN `dev_fd_meta`.`ben_lineage`.lineage_graph g5 ON g4.source_model = g5.target_model
      WHERE g0.target_model = start_model
      UNION ALL
      SELECT 7 AS depth,
             g6.source_model AS model,
             COALESCE(g6.source_key, g6.join_key, COALESCE(g5.source_key, g5.join_key, COALESCE(g4.source_key, g4.join_key, COALESCE(g3.source_key, g3.join_key, COALESCE(g2.source_key, g2.join_key, COALESCE(g1.source_key, g1.join_key, COALESCE(g0.source_key, g0.join_key, key_col))))))) AS key_column,
             key_val AS key_value,
             concat(start_model, ' -> ', g0.source_model, ' -> ', g1.source_model, ' -> ', g2.source_model, ' -> ', g3.source_model, ' -> ', g4.source_model, ' -> ', g5.source_model, ' -> ', g6.source_model) AS path,
             g6.transform_type,
             g6.expression,
             g6.source_catalog,
             g6.source_schema
      FROM `dev_fd_meta`.`ben_lineage`.lineage_graph g0
      JOIN `dev_fd_meta`.`ben_lineage`.lineage_graph g1 ON g0.source_model = g1.target_model
      JOIN `dev_fd_meta`.`ben_lineage`.lineage_graph g2 ON g1.source_model = g2.target_model
      JOIN `dev_fd_meta`.`ben_lineage`.lineage_graph g3 ON g2.source_model = g3.target_model
      JOIN `dev_fd_meta`.`ben_lineage`.lineage_graph g4 ON g3.source_model = g4.target_model
      JOIN `dev_fd_meta`.`ben_lineage`.lineage_graph g5 ON g4.source_model = g5.target_model
      JOIN `dev_fd_meta`.`ben_lineage`.lineage_graph g6 ON g5.source_model = g6.target_model
      WHERE g0.target_model = start_model
      UNION ALL
      SELECT 8 AS depth,
             g7.source_model AS model,
             COALESCE(g7.source_key, g7.join_key, COALESCE(g6.source_key, g6.join_key, COALESCE(g5.source_key, g5.join_key, COALESCE(g4.source_key, g4.join_key, COALESCE(g3.source_key, g3.join_key, COALESCE(g2.source_key, g2.join_key, COALESCE(g1.source_key, g1.join_key, COALESCE(g0.source_key, g0.join_key, key_col)))))))) AS key_column,
             key_val AS key_value,
             concat(start_model, ' -> ', g0.source_model, ' -> ', g1.source_model, ' -> ', g2.source_model, ' -> ', g3.source_model, ' -> ', g4.source_model, ' -> ', g5.source_model, ' -> ', g6.source_model, ' -> ', g7.source_model) AS path,
             g7.transform_type,
             g7.expression,
             g7.source_catalog,
             g7.source_schema
      FROM `dev_fd_meta`.`ben_lineage`.lineage_graph g0
      JOIN `dev_fd_meta`.`ben_lineage`.lineage_graph g1 ON g0.source_model = g1.target_model
      JOIN `dev_fd_meta`.`ben_lineage`.lineage_graph g2 ON g1.source_model = g2.target_model
      JOIN `dev_fd_meta`.`ben_lineage`.lineage_graph g3 ON g2.source_model = g3.target_model
      JOIN `dev_fd_meta`.`ben_lineage`.lineage_graph g4 ON g3.source_model = g4.target_model
      JOIN `dev_fd_meta`.`ben_lineage`.lineage_graph g5 ON g4.source_model = g5.target_model
      JOIN `dev_fd_meta`.`ben_lineage`.lineage_graph g6 ON g5.source_model = g6.target_model
      JOIN `dev_fd_meta`.`ben_lineage`.lineage_graph g7 ON g6.source_model = g7.target_model
      WHERE g0.target_model = start_model
      UNION ALL
      SELECT 9 AS depth,
             g8.source_model AS model,
             COALESCE(g8.source_key, g8.join_key, COALESCE(g7.source_key, g7.join_key, COALESCE(g6.source_key, g6.join_key, COALESCE(g5.source_key, g5.join_key, COALESCE(g4.source_key, g4.join_key, COALESCE(g3.source_key, g3.join_key, COALESCE(g2.source_key, g2.join_key, COALESCE(g1.source_key, g1.join_key, COALESCE(g0.source_key, g0.join_key, key_col))))))))) AS key_column,
             key_val AS key_value,
             concat(start_model, ' -> ', g0.source_model, ' -> ', g1.source_model, ' -> ', g2.source_model, ' -> ', g3.source_model, ' -> ', g4.source_model, ' -> ', g5.source_model, ' -> ', g6.source_model, ' -> ', g7.source_model, ' -> ', g8.source_model) AS path,
             g8.transform_type,
             g8.expression,
             g8.source_catalog,
             g8.source_schema
      FROM `dev_fd_meta`.`ben_lineage`.lineage_graph g0
      JOIN `dev_fd_meta`.`ben_lineage`.lineage_graph g1 ON g0.source_model = g1.target_model
      JOIN `dev_fd_meta`.`ben_lineage`.lineage_graph g2 ON g1.source_model = g2.target_model
      JOIN `dev_fd_meta`.`ben_lineage`.lineage_graph g3 ON g2.source_model = g3.target_model
      JOIN `dev_fd_meta`.`ben_lineage`.lineage_graph g4 ON g3.source_model = g4.target_model
      JOIN `dev_fd_meta`.`ben_lineage`.lineage_graph g5 ON g4.source_model = g5.target_model
      JOIN `dev_fd_meta`.`ben_lineage`.lineage_graph g6 ON g5.source_model = g6.target_model
      JOIN `dev_fd_meta`.`ben_lineage`.lineage_graph g7 ON g6.source_model = g7.target_model
      JOIN `dev_fd_meta`.`ben_lineage`.lineage_graph g8 ON g7.source_model = g8.target_model
      WHERE g0.target_model = start_model
    )
    ORDER BY depth
  )
);

-- UDF: trace_lineage_json
-- Same as trace_lineage but returns structured JSON (max depth 10)
DROP FUNCTION IF EXISTS `dev_fd_meta`.`ben_lineage`.trace_lineage_json;
CREATE FUNCTION `dev_fd_meta`.`ben_lineage`.trace_lineage_json(
    start_model STRING, key_col STRING, key_val STRING
)
RETURNS STRING
RETURN (
  SELECT to_json(collect_list(node))
  FROM (
    SELECT named_struct(
      'depth', depth,
      'model', model,
      'key_column', key_column,
      'key_value', key_value,
      'transform_type', transform_type,
      'expression', expression,
      'catalog', source_catalog,
      'schema', source_schema
    ) AS node
    FROM (
      SELECT 0 AS depth,
             start_model AS model,
             key_col AS key_column,
             key_val AS key_value,
             start_model AS path,
             CAST(NULL AS STRING) AS transform_type,
             CAST(NULL AS STRING) AS expression,
             CAST(NULL AS STRING) AS source_catalog,
             CAST(NULL AS STRING) AS source_schema
      UNION ALL
      SELECT 1 AS depth,
             g0.source_model AS model,
             COALESCE(g0.source_key, g0.join_key, key_col) AS key_column,
             key_val AS key_value,
             concat(start_model, ' -> ', g0.source_model) AS path,
             g0.transform_type,
             g0.expression,
             g0.source_catalog,
             g0.source_schema
      FROM `dev_fd_meta`.`ben_lineage`.lineage_graph g0
      WHERE g0.target_model = start_model
      UNION ALL
      SELECT 2 AS depth,
             g1.source_model AS model,
             COALESCE(g1.source_key, g1.join_key, COALESCE(g0.source_key, g0.join_key, key_col)) AS key_column,
             key_val AS key_value,
             concat(start_model, ' -> ', g0.source_model, ' -> ', g1.source_model) AS path,
             g1.transform_type,
             g1.expression,
             g1.source_catalog,
             g1.source_schema
      FROM `dev_fd_meta`.`ben_lineage`.lineage_graph g0
      JOIN `dev_fd_meta`.`ben_lineage`.lineage_graph g1 ON g0.source_model = g1.target_model
      WHERE g0.target_model = start_model
      UNION ALL
      SELECT 3 AS depth,
             g2.source_model AS model,
             COALESCE(g2.source_key, g2.join_key, COALESCE(g1.source_key, g1.join_key, COALESCE(g0.source_key, g0.join_key, key_col))) AS key_column,
             key_val AS key_value,
             concat(start_model, ' -> ', g0.source_model, ' -> ', g1.source_model, ' -> ', g2.source_model) AS path,
             g2.transform_type,
             g2.expression,
             g2.source_catalog,
             g2.source_schema
      FROM `dev_fd_meta`.`ben_lineage`.lineage_graph g0
      JOIN `dev_fd_meta`.`ben_lineage`.lineage_graph g1 ON g0.source_model = g1.target_model
      JOIN `dev_fd_meta`.`ben_lineage`.lineage_graph g2 ON g1.source_model = g2.target_model
      WHERE g0.target_model = start_model
      UNION ALL
      SELECT 4 AS depth,
             g3.source_model AS model,
             COALESCE(g3.source_key, g3.join_key, COALESCE(g2.source_key, g2.join_key, COALESCE(g1.source_key, g1.join_key, COALESCE(g0.source_key, g0.join_key, key_col)))) AS key_column,
             key_val AS key_value,
             concat(start_model, ' -> ', g0.source_model, ' -> ', g1.source_model, ' -> ', g2.source_model, ' -> ', g3.source_model) AS path,
             g3.transform_type,
             g3.expression,
             g3.source_catalog,
             g3.source_schema
      FROM `dev_fd_meta`.`ben_lineage`.lineage_graph g0
      JOIN `dev_fd_meta`.`ben_lineage`.lineage_graph g1 ON g0.source_model = g1.target_model
      JOIN `dev_fd_meta`.`ben_lineage`.lineage_graph g2 ON g1.source_model = g2.target_model
      JOIN `dev_fd_meta`.`ben_lineage`.lineage_graph g3 ON g2.source_model = g3.target_model
      WHERE g0.target_model = start_model
      UNION ALL
      SELECT 5 AS depth,
             g4.source_model AS model,
             COALESCE(g4.source_key, g4.join_key, COALESCE(g3.source_key, g3.join_key, COALESCE(g2.source_key, g2.join_key, COALESCE(g1.source_key, g1.join_key, COALESCE(g0.source_key, g0.join_key, key_col))))) AS key_column,
             key_val AS key_value,
             concat(start_model, ' -> ', g0.source_model, ' -> ', g1.source_model, ' -> ', g2.source_model, ' -> ', g3.source_model, ' -> ', g4.source_model) AS path,
             g4.transform_type,
             g4.expression,
             g4.source_catalog,
             g4.source_schema
      FROM `dev_fd_meta`.`ben_lineage`.lineage_graph g0
      JOIN `dev_fd_meta`.`ben_lineage`.lineage_graph g1 ON g0.source_model = g1.target_model
      JOIN `dev_fd_meta`.`ben_lineage`.lineage_graph g2 ON g1.source_model = g2.target_model
      JOIN `dev_fd_meta`.`ben_lineage`.lineage_graph g3 ON g2.source_model = g3.target_model
      JOIN `dev_fd_meta`.`ben_lineage`.lineage_graph g4 ON g3.source_model = g4.target_model
      WHERE g0.target_model = start_model
      UNION ALL
      SELECT 6 AS depth,
             g5.source_model AS model,
             COALESCE(g5.source_key, g5.join_key, COALESCE(g4.source_key, g4.join_key, COALESCE(g3.source_key, g3.join_key, COALESCE(g2.source_key, g2.join_key, COALESCE(g1.source_key, g1.join_key, COALESCE(g0.source_key, g0.join_key, key_col)))))) AS key_column,
             key_val AS key_value,
             concat(start_model, ' -> ', g0.source_model, ' -> ', g1.source_model, ' -> ', g2.source_model, ' -> ', g3.source_model, ' -> ', g4.source_model, ' -> ', g5.source_model) AS path,
             g5.transform_type,
             g5.expression,
             g5.source_catalog,
             g5.source_schema
      FROM `dev_fd_meta`.`ben_lineage`.lineage_graph g0
      JOIN `dev_fd_meta`.`ben_lineage`.lineage_graph g1 ON g0.source_model = g1.target_model
      JOIN `dev_fd_meta`.`ben_lineage`.lineage_graph g2 ON g1.source_model = g2.target_model
      JOIN `dev_fd_meta`.`ben_lineage`.lineage_graph g3 ON g2.source_model = g3.target_model
      JOIN `dev_fd_meta`.`ben_lineage`.lineage_graph g4 ON g3.source_model = g4.target_model
      JOIN `dev_fd_meta`.`ben_lineage`.lineage_graph g5 ON g4.source_model = g5.target_model
      WHERE g0.target_model = start_model
      UNION ALL
      SELECT 7 AS depth,
             g6.source_model AS model,
             COALESCE(g6.source_key, g6.join_key, COALESCE(g5.source_key, g5.join_key, COALESCE(g4.source_key, g4.join_key, COALESCE(g3.source_key, g3.join_key, COALESCE(g2.source_key, g2.join_key, COALESCE(g1.source_key, g1.join_key, COALESCE(g0.source_key, g0.join_key, key_col))))))) AS key_column,
             key_val AS key_value,
             concat(start_model, ' -> ', g0.source_model, ' -> ', g1.source_model, ' -> ', g2.source_model, ' -> ', g3.source_model, ' -> ', g4.source_model, ' -> ', g5.source_model, ' -> ', g6.source_model) AS path,
             g6.transform_type,
             g6.expression,
             g6.source_catalog,
             g6.source_schema
      FROM `dev_fd_meta`.`ben_lineage`.lineage_graph g0
      JOIN `dev_fd_meta`.`ben_lineage`.lineage_graph g1 ON g0.source_model = g1.target_model
      JOIN `dev_fd_meta`.`ben_lineage`.lineage_graph g2 ON g1.source_model = g2.target_model
      JOIN `dev_fd_meta`.`ben_lineage`.lineage_graph g3 ON g2.source_model = g3.target_model
      JOIN `dev_fd_meta`.`ben_lineage`.lineage_graph g4 ON g3.source_model = g4.target_model
      JOIN `dev_fd_meta`.`ben_lineage`.lineage_graph g5 ON g4.source_model = g5.target_model
      JOIN `dev_fd_meta`.`ben_lineage`.lineage_graph g6 ON g5.source_model = g6.target_model
      WHERE g0.target_model = start_model
      UNION ALL
      SELECT 8 AS depth,
             g7.source_model AS model,
             COALESCE(g7.source_key, g7.join_key, COALESCE(g6.source_key, g6.join_key, COALESCE(g5.source_key, g5.join_key, COALESCE(g4.source_key, g4.join_key, COALESCE(g3.source_key, g3.join_key, COALESCE(g2.source_key, g2.join_key, COALESCE(g1.source_key, g1.join_key, COALESCE(g0.source_key, g0.join_key, key_col)))))))) AS key_column,
             key_val AS key_value,
             concat(start_model, ' -> ', g0.source_model, ' -> ', g1.source_model, ' -> ', g2.source_model, ' -> ', g3.source_model, ' -> ', g4.source_model, ' -> ', g5.source_model, ' -> ', g6.source_model, ' -> ', g7.source_model) AS path,
             g7.transform_type,
             g7.expression,
             g7.source_catalog,
             g7.source_schema
      FROM `dev_fd_meta`.`ben_lineage`.lineage_graph g0
      JOIN `dev_fd_meta`.`ben_lineage`.lineage_graph g1 ON g0.source_model = g1.target_model
      JOIN `dev_fd_meta`.`ben_lineage`.lineage_graph g2 ON g1.source_model = g2.target_model
      JOIN `dev_fd_meta`.`ben_lineage`.lineage_graph g3 ON g2.source_model = g3.target_model
      JOIN `dev_fd_meta`.`ben_lineage`.lineage_graph g4 ON g3.source_model = g4.target_model
      JOIN `dev_fd_meta`.`ben_lineage`.lineage_graph g5 ON g4.source_model = g5.target_model
      JOIN `dev_fd_meta`.`ben_lineage`.lineage_graph g6 ON g5.source_model = g6.target_model
      JOIN `dev_fd_meta`.`ben_lineage`.lineage_graph g7 ON g6.source_model = g7.target_model
      WHERE g0.target_model = start_model
      UNION ALL
      SELECT 9 AS depth,
             g8.source_model AS model,
             COALESCE(g8.source_key, g8.join_key, COALESCE(g7.source_key, g7.join_key, COALESCE(g6.source_key, g6.join_key, COALESCE(g5.source_key, g5.join_key, COALESCE(g4.source_key, g4.join_key, COALESCE(g3.source_key, g3.join_key, COALESCE(g2.source_key, g2.join_key, COALESCE(g1.source_key, g1.join_key, COALESCE(g0.source_key, g0.join_key, key_col))))))))) AS key_column,
             key_val AS key_value,
             concat(start_model, ' -> ', g0.source_model, ' -> ', g1.source_model, ' -> ', g2.source_model, ' -> ', g3.source_model, ' -> ', g4.source_model, ' -> ', g5.source_model, ' -> ', g6.source_model, ' -> ', g7.source_model, ' -> ', g8.source_model) AS path,
             g8.transform_type,
             g8.expression,
             g8.source_catalog,
             g8.source_schema
      FROM `dev_fd_meta`.`ben_lineage`.lineage_graph g0
      JOIN `dev_fd_meta`.`ben_lineage`.lineage_graph g1 ON g0.source_model = g1.target_model
      JOIN `dev_fd_meta`.`ben_lineage`.lineage_graph g2 ON g1.source_model = g2.target_model
      JOIN `dev_fd_meta`.`ben_lineage`.lineage_graph g3 ON g2.source_model = g3.target_model
      JOIN `dev_fd_meta`.`ben_lineage`.lineage_graph g4 ON g3.source_model = g4.target_model
      JOIN `dev_fd_meta`.`ben_lineage`.lineage_graph g5 ON g4.source_model = g5.target_model
      JOIN `dev_fd_meta`.`ben_lineage`.lineage_graph g6 ON g5.source_model = g6.target_model
      JOIN `dev_fd_meta`.`ben_lineage`.lineage_graph g7 ON g6.source_model = g7.target_model
      JOIN `dev_fd_meta`.`ben_lineage`.lineage_graph g8 ON g7.source_model = g8.target_model
      WHERE g0.target_model = start_model
    )
    ORDER BY depth
  )
);

-- UDF: last_run_id
-- Returns the most recent run_id for a model
DROP FUNCTION IF EXISTS `dev_fd_meta`.`ben_lineage`.last_run_id;
CREATE FUNCTION `dev_fd_meta`.`ben_lineage`.last_run_id(model_name STRING)
RETURNS STRING
RETURN (
  SELECT run_id FROM `dev_fd_meta`.`ben_lineage`.lineage_log
  WHERE model = model_name
  ORDER BY completed_at DESC
  LIMIT 1
);
