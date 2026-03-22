-- UDF: loyalty_tier
-- Assigns GOLD/SILVER/BRONZE based on revenue
CREATE OR REPLACE FUNCTION `public`.`silver`.loyalty_tier(revenue decimal(18,2))
RETURNS string
RETURN (CASE
  WHEN revenue >= 1000 THEN 'GOLD'
  WHEN revenue >=  500 THEN 'SILVER'
  ELSE 'BRONZE'
END);
