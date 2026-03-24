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

-- UDF: trace_lineage
-- Trace any value back through the full DAG
DROP FUNCTION IF EXISTS `dev_fd_meta`.`ben_sales`.trace_lineage;
CREATE FUNCTION `dev_fd_meta`.`ben_sales`.trace_lineage(
    start_model STRING, key_col STRING, key_val STRING
)
RETURNS STRING
RETURN (
  WITH RECURSIVE lineage AS (
    SELECT
      start_model AS model,
      key_col     AS key_column,
      key_val     AS key_value,
      0           AS depth,
      start_model AS path,
      CAST(NULL AS STRING) AS transform_type,
      CAST(NULL AS STRING) AS expression,
      CAST(NULL AS STRING) AS source_catalog,
      CAST(NULL AS STRING) AS source_schema
    UNION ALL
    SELECT
      g.source_model,
      COALESCE(g.source_key, g.join_key, l.key_column),
      l.key_value,
      l.depth + 1,
      concat(l.path, ' -> ', g.source_model),
      g.transform_type,
      g.expression,
      g.source_catalog,
      g.source_schema
    FROM lineage l
    JOIN `dev_fd_meta`.`ben_sales`.lineage_graph g
      ON l.model = g.target_model
    WHERE l.depth < 10
  )
  SELECT concat_ws('\n', collect_list(
    concat(
      '[', CAST(depth AS STRING), '] ',
      model, '.', key_column, ' = ', key_value,
      CASE WHEN transform_type IS NOT NULL
           THEN concat(' (', transform_type,
                CASE WHEN expression IS NOT NULL AND expression != ''
                     THEN concat(': ', expression) ELSE '' END, ')')
           ELSE '' END
    )
  ))
  FROM lineage
  ORDER BY depth
);

-- UDF: trace_lineage_json
-- Same as trace_lineage but returns structured JSON
DROP FUNCTION IF EXISTS `dev_fd_meta`.`ben_sales`.trace_lineage_json;
CREATE FUNCTION `dev_fd_meta`.`ben_sales`.trace_lineage_json(
    start_model STRING, key_col STRING, key_val STRING
)
RETURNS STRING
RETURN (
  WITH RECURSIVE lineage AS (
    SELECT
      start_model AS model,
      key_col     AS key_column,
      key_val     AS key_value,
      0           AS depth,
      CAST(NULL AS STRING) AS transform_type,
      CAST(NULL AS STRING) AS expression,
      CAST(NULL AS STRING) AS source_catalog,
      CAST(NULL AS STRING) AS source_schema
    UNION ALL
    SELECT
      g.source_model,
      COALESCE(g.source_key, g.join_key, l.key_column),
      l.key_value,
      l.depth + 1,
      g.transform_type,
      g.expression,
      g.source_catalog,
      g.source_schema
    FROM lineage l
    JOIN `dev_fd_meta`.`ben_sales`.lineage_graph g
      ON l.model = g.target_model
    WHERE l.depth < 10
  )
  SELECT to_json(collect_list(named_struct(
    'depth', depth,
    'model', model,
    'key_column', key_column,
    'key_value', key_value,
    'transform_type', transform_type,
    'expression', expression,
    'catalog', source_catalog,
    'schema', source_schema
  )))
  FROM lineage
  ORDER BY depth
);

-- UDF: last_run_id
-- Returns the most recent run_id for a model
DROP FUNCTION IF EXISTS `dev_fd_meta`.`ben_sales`.last_run_id;
CREATE FUNCTION `dev_fd_meta`.`ben_sales`.last_run_id(model_name STRING)
RETURNS STRING
RETURN (
  SELECT run_id FROM `dev_fd_meta`.`ben_sales`.lineage_log
  WHERE model = model_name
  ORDER BY completed_at DESC
  LIMIT 1
);
