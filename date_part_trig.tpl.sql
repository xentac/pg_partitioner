
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
        FROM pg_class
        WHERE relname ~ ('^%(table_name)s_[0-9]{8}_[0-9]{8}$')
        ORDER BY relname
    LOOP
        name_parts := string_to_array(partition, '_');
        upper_dim := array_upper(name_parts, 1);
        IF rec.%(ts_column)s >= name_parts[upper_dim-1]::timestamp 
                AND rec.%(ts_column)s < name_parts[upper_dim]::timestamp THEN
            ins_sql := 'INSERT INTO %(table_name)s_' || name_parts[upper_dim-1] || '_' || 
                        name_parts[upper_dim] || ' (%(table_atts)s) VALUES (' || %(atts_vals)s || ');';
            EXECUTE ins_sql;
            RETURN NULL;
        END IF;
    END LOOP;
    RAISE WARNING 'No partition created for %(table_name)s to hold timestamp value %%, leaving data in parent table.', rec.%(ts_column)s;
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

CREATE OR REPLACE FUNCTION create_part_ins_trig(table_name varchar)
    RETURNS void AS $$
BEGIN
    EXECUTE 'CREATE TRIGGER ' || table_name || '_partition_trigger BEFORE INSERT ON '
            || table_name || ' FOR EACH ROW EXECUTE PROCEDURE ' || table_name || '_ins_trig();';
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION drop_part_ins_trig(table_name varchar)
    RETURNS void AS $$
DECLARE
    str varchar;
BEGIN
    EXECUTE 'DROP TRIGGER IF EXISTS ' || table_name || '_partition_trigger ON ' || quote_ident(table_name) || ';';
END;
$$ LANGUAGE plpgsql;
