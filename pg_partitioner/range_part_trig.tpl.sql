
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

CREATE OR REPLACE FUNCTION move_partition_data(table_name text, part_col text, count integer, max real)
    RETURNS integer AS $$
DECLARE
    bounds text[];
    partition text;
    pkey_fields text[];
    pkey_fields_conv text[];
    pkeys_str text;
    move_data_sql text;
    moved_data_pkey text;
    delete_sql text;
    moved integer;
    to_move integer DEFAULT count;
    total_moved integer DEFAULT 0;
BEGIN
    SELECT * FROM get_table_pkey_fields(table_name) INTO pkey_fields;
    
    -- make the returning clause
    FOR i IN 1 .. array_upper(pkey_fields, 1)
    LOOP
        pkey_fields_conv[i] := pkey_fields[i] || '::text';
    END LOOP;
    
    <<main_loop>>
    FOR partition IN
        SELECT * FROM get_table_partitions(table_name)
    LOOP
        raise notice 'moving data for partition: %%', partition;
        SELECT array[substring(partition from '^'||table_name||'_([0-9]*)_[0-9]*$'),
               substring(partition from '^'||table_name||'_[0-9]*_([0-9]*)$')]
        INTO bounds;
        <<partition_loop>>
        LOOP
            -- ensure we don't pass the max rows to be moved if it's set
            IF total_moved + to_move > max THEN
                to_move := max - total_moved;
            END IF;
            
            pkeys_str := '';
            move_data_sql := 'INSERT INTO ' || partition || '
                              SELECT *
                              FROM ONLY ' || table_name || '
                              WHERE ' || part_col || ' >= ' || quote_literal(bounds[1]) || ' AND ' || 
                                part_col || ' < ' || quote_literal(bounds[2]) || '
                              LIMIT ' || quote_literal(to_move) || '
                              RETURNING ''(''||' || array_to_string(pkey_fields_conv, ',') || '||'')'';';
            -- RAISE NOTICE 'move data sql: %%', move_data_sql;
            FOR moved_data_pkey IN EXECUTE move_data_sql
            LOOP
                -- RAISE NOTICE 'move data pkey: %%', moved_data_pkey;
                pkeys_str := pkeys_str || moved_data_pkey || ',';
            END LOOP;
            
            EXIT WHEN NOT FOUND;
            GET DIAGNOSTICS moved := ROW_COUNT;
            -- raise notice 'inserted count: %%', moved;
            -- raise notice 'pkeys: %%', pkeys_str;
                        
            pkeys_str := substring(pkeys_str from 1 for char_length(pkeys_str)-1);
            delete_sql := 'DELETE FROM ONLY ' || table_name || '
                           WHERE (' || array_to_string(pkey_fields, ',') || ') = any(ARRAY[' || pkeys_str ||']);';
            -- RAISE NOTICE 'delete data sql: %%', delete_sql;
            EXECUTE delete_sql;
            
            GET DIAGNOSTICS moved := ROW_COUNT;
            -- raise notice 'deleted count: %%', moved;
            
            total_moved := total_moved + moved;
            EXIT main_loop WHEN total_moved = max;
            EXIT partition_loop WHEN moved < count;
        END LOOP;
    END LOOP;
    RETURN total_moved;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION move_partition_data(table_name text, part_col text, count integer)
    RETURNS integer AS $$
    SELECT move_partition_data($1, $2, $3, 'Infinity')
$$ LANGUAGE sql;

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
