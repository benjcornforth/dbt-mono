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
