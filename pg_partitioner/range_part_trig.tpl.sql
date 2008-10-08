
CREATE OR REPLACE FUNCTION quote_nullable(val anyelement)
    RETURNS text AS $$
    SELECT COALESCE(quote_literal($1), 'NULL');
$$ LANGUAGE sql;

CREATE OR REPLACE FUNCTION get_table_partitions(text)
    RETURNS SETOF text AS $$
    SELECT n.nspname || '.' || t.relname::text
    FROM pg_class t, pg_namespace n, pg_inherits i
    WHERE n.oid=t.relnamespace 
        AND nspname || '.' || relname ~ ('^' || $1 || '_[0-9]+_[0-9]+$')
        AND t.oid=i.inhrelid AND i.inhparent = $1::regclass
    ORDER BY relname
$$ LANGUAGE sql;

CREATE OR REPLACE FUNCTION get_table_pkey_fields(table_name text)
    RETURNS text[] AS $$
    SELECT ARRAY(SELECT a.attname::text
                 FROM pg_constraint c, pg_attribute a
                 WHERE c.conrelid=a.attrelid AND a.attnum = any(c.conkey)
                    AND c.contype='p' AND c.conrelid=$1::regclass)
$$ LANGUAGE sql;

CREATE OR REPLACE FUNCTION move_partition_data(table_name text, part_col text, count integer)
    RETURNS void AS $$
DECLARE
    bounds text[];
    partition text;
    pkey_fields text[];
    pkey_fields_conv text[];
    pkey_vals text;
    move_data_sql text;
    moved_data_pkey text;
    delete_sql text;
    offset integer;
    moved integer;
BEGIN
    SELECT * FROM get_table_pkey_fields(table_name) INTO pkey_fields;
    
    -- make the returning clause
    FOR i IN 1 .. array_upper(pkey_fields, 1)
    LOOP
        pkey_fields_conv[i] := pkey_fields[i] || '::text';
    END LOOP;
    
    FOR partition IN
        SELECT * FROM get_table_partitions(table_name)
    LOOP
        raise notice 'moving data for partition: %%', partition;
        SELECT array[substring(partition from '^'||table_name||'_([0-9]*)_[0-9]*$'),
               substring(partition from '^'||table_name||'_[0-9]*_([0-9]*)$')]
        INTO bounds;
        offset := 0;
        LOOP
            pkey_vals := '';
            move_data_sql := 'INSERT INTO ' || partition || '
                              SELECT *
                              FROM ' || table_name || '
                              WHERE ' || part_col || ' >= ' || quote_literal(bounds[1]) || ' AND ' || 
                                part_col || ' < ' || quote_literal(bounds[2]) || '
                              OFFSET ' || offset::text || ' LIMIT ' || count::text || '
                              RETURNING ''(''||' || array_to_string(pkey_fields_conv, ',') || '||'')'';';
            -- RAISE NOTICE 'move data sql: %%', move_data_sql;
            FOR moved_data_pkey IN EXECUTE move_data_sql
            LOOP
                -- RAISE NOTICE 'move data pkey: %%', moved_data_pkey;
                pkey_vals := pkey_vals || moved_data_pkey || ',';
            END LOOP;
            
            EXIT WHEN NOT FOUND;
            -- GET DIAGNOSTICS moved := ROW_COUNT;
            -- raise notice 'row count: %%, pkey_vals %%', moved, pkey_vals;
                        
            pkey_vals := substring(pkey_vals from 1 for char_length(pkey_vals)-1);
            delete_sql := 'DELETE FROM ONLY ' || table_name || '
                           WHERE (' || array_to_string(pkey_fields, ',') || ') = any(ARRAY[' || pkey_vals ||']);';
            RAISE NOTICE 'delete data sql: %%', delete_sql;
            EXECUTE delete_sql;
            
            GET DIAGNOSTICS moved := ROW_COUNT;
            
            EXIT WHEN moved < count;
            offset := offset + count;
        END LOOP;
    END LOOP;
END;
$$ language plpgsql;       

CREATE OR REPLACE FUNCTION %(table_name)s_ins_func(rec %(table_name)s)
    RETURNS %(table_name)s AS $$
DECLARE
    partition varchar;
    name_parts varchar[];
    upper_dim integer;
    ins_sql varchar;
BEGIN
    -- FOR partition IN
    --     SELECT relname
    --     FROM pg_class t, pg_namespace n
    --     WHERE n.oid=t.relnamespace 
    --         AND nspname || '.' || relname ~ ('^%(table_name)s_[0-9]+_[0-9]+$')
    --     ORDER BY relname
    FOR partition IN
        SELECT * FROM get_table_partitions('%(table_name)s')
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
