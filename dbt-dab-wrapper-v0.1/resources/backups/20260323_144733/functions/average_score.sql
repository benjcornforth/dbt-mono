-- UDF: average_score
-- Pandas vectorized UDF
-- Vectorized average using pandas — fast on large batches
DROP FUNCTION IF EXISTS {{ target.catalog }}.{{ target.schema }}.average_score;
CREATE FUNCTION {{ target.catalog }}.{{ target.schema }}.average_score(score_a double, score_b double)
RETURNS double
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES ('pandas', 'numpy')
HANDLER = 'compute'
AS $$
import pandas as pd
def compute(score_a: pd.Series, score_b: pd.Series) -> pd.Series:
    return (score_a + score_b) / 2.0
$$;
