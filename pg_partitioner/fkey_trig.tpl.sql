
CREATE OR REPLACE FUNCTION %(fkey_name)s_fkey_trig()
    RETURNS trigger AS $$
DECLARE

BEGIN
    EXECUTE 'SELECT 1 FROM %(ref_table_name)s
             WHERE (%(fields)s) = (' || %(new_vals)s || ') LIMIT 1;';
    IF FOUND THEN
        RETURN NEW;
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;