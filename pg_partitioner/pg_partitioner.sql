
CREATE OR REPLACE FUNCTION quote_nullable(val anyelement)
    RETURNS text AS $$
    SELECT COALESCE(quote_literal($1), 'NULL');
$$ LANGUAGE sql;

CREATE OR REPLACE FUNCTION quote_array_literals(arr anyarray)
    RETURNS text[] AS $$
DECLARE
    i int;
    ret text[];
BEGIN
    FOR i IN 1 .. array_upper(arr, 1)
    LOOP
        ret[i] := quote_literal(arr[i]);
    END LOOP;
    RETURN ret;
END;
$$ LANGUAGE plpgsql;

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
    offset integer;
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
        -- raise notice 'moving data for partition: %%', partition;
        SELECT array[substring(partition from '^'||table_name||'_([0-9]*)_[0-9]*$'),
               substring(partition from '^'||table_name||'_[0-9]*_([0-9]*)$')]
        INTO bounds;
        offset := 0;
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
                              WHERE ' || quote_ident(part_col) || ' >= ' || quote_literal(bounds[1]) || ' AND ' || 
                                quote_ident(part_col) || ' < ' || quote_literal(bounds[2]) || '
                              ORDER BY ' || quote_ident(part_col) || '
                              LIMIT ' || quote_literal(to_move) || ' ';
            -- RAISE NOTICE 'move data sql: %%', move_data_sql;
            IF max != 'Infinity' THEN
                move_data_sql := move_data_sql || ' RETURNING ''('' || array_to_string(quote_array_literals(array[' || array_to_string(pkey_fields_conv, ',') || ']), '','') || '')'';';
                FOR moved_data_pkey IN EXECUTE move_data_sql
                LOOP
                    -- RAISE NOTICE 'move data pkey: %%', moved_data_pkey;
                    pkeys_str := pkeys_str || moved_data_pkey || ',';
                END LOOP;
            
                EXIT WHEN NOT FOUND;
                GET DIAGNOSTICS moved := ROW_COUNT;
                -- raise notice 'inserted count: %%', moved;
                        
                pkeys_str := substring(pkeys_str from 1 for char_length(pkeys_str)-1);
                delete_sql := 'DELETE FROM ONLY ' || table_name || '
                               WHERE (' || array_to_string(pkey_fields, ',') || ') IN (' || pkeys_str ||');';
                -- RAISE NOTICE 'delete data sql: %%', delete_sql;
                EXECUTE delete_sql;
            ELSE
                move_data_sql := move_data_sql || ' OFFSET ' || quote_literal(offset);
                EXECUTE move_data_sql;
                offset := offset + to_move;
            END IF;
            
            GET DIAGNOSTICS moved := ROW_COUNT;
            -- raise notice 'deleted count: %%', moved;
            
            total_moved := total_moved + moved;
            EXIT main_loop WHEN total_moved = max;
            EXIT partition_loop WHEN moved < count;
        END LOOP;
        IF max = 'Infinity' THEN
            EXECUTE 'DELETE FROM ONLY ' || table_name || '
                     WHERE ' || quote_ident(part_col) || ' >= ' || quote_literal(bounds[1]) || ' AND ' ||
                        quote_ident(part_col) || ' < ' || quote_literal(bounds[2]) || ';';
        END IF;
    END LOOP;
    RETURN total_moved;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION move_partition_data(table_name text, part_col text, count integer)
    RETURNS integer AS $$
    SELECT move_partition_data($1, $2, $3, 'Infinity')
$$ LANGUAGE sql;