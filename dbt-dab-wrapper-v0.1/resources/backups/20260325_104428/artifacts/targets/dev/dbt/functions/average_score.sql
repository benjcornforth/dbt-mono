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
