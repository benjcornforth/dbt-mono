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
