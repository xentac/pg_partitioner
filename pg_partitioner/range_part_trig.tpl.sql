
CREATE OR REPLACE FUNCTION quote_nullable(val anyelement)
    RETURNS text AS $$
    SELECT COALESCE(quote_literal($1), 'NULL');
$$ LANGUAGE sql;

CREATE OR REPLACE FUNCTION %(table_name)s_ins_func(rec %(table_name)s)
    RETURNS %(table_name)s AS $$
DECLARE
    partition varchar;
    name_parts varchar[];
    upper_dim integer;
    ins_sql varchar;
BEGIN
    FOR partition IN
        SELECT relname
        FROM pg_class t, pg_namespace n
        WHERE n.oid=t.relnamespace 
            AND nspname || '.' || relname ~ ('^%(table_name)s_[0-9]+_[0-9]+$')
        ORDER BY relname
    LOOP
        name_parts := string_to_array(partition, '_');
        upper_dim := array_upper(name_parts, 1);
        IF rec.%(ts_column)s >= name_parts[upper_dim-1]::%(col_type)s 
                AND rec.%(ts_column)s < name_parts[upper_dim]::%(col_type)s THEN
            ins_sql := 'INSERT INTO %(table_name)s_' || name_parts[upper_dim-1] || '_' || 
                        name_parts[upper_dim] || ' (%(table_atts)s) VALUES (' || %(atts_vals)s || ');';
            EXECUTE ins_sql;
            RETURN NULL;
        END IF;
    END LOOP;
    RAISE WARNING 'No partition created for %(table_name)s to hold value %(col_type)s %%, leaving data in parent table.', rec.%(ts_column)s;
    RETURN rec;
END;
$$ language plpgsql;


CREATE OR REPLACE FUNCTION %(table_name)s_ins_trig()
    RETURNS trigger AS $$
DECLARE
    res %(table_name)s;
    null_rec %(table_name)s;
BEGIN
    SELECT INTO res * FROM %(table_name)s_ins_func(NEW) as g;
    IF row(res.*) IS DISTINCT FROM row(null_rec.*) THEN
        RETURN NEW;
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;
