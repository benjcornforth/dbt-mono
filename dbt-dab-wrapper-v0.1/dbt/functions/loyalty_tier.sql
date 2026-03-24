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
