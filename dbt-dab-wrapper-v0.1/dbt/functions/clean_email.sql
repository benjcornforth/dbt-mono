-- UDF: clean_email
-- Lowercase and trim email addresses
CREATE OR REPLACE FUNCTION {{ target.schema }}.clean_email(raw_email string)
RETURNS string
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
HANDLER = 'clean'
AS $$
def clean(raw_email):
    if raw_email is None:
        return None
    return raw_email.strip().lower()
$$;
