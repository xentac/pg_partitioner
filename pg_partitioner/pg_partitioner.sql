DROP SCHEMA IF EXISTS partitioner CASCADE;
CREATE SCHEMA partitioner;

CREATE OR REPLACE FUNCTION partitioner.quote_nullable(val anyelement)
    RETURNS text AS $$
    SELECT COALESCE(quote_literal($1), 'NULL');
$$ LANGUAGE sql;

CREATE OR REPLACE FUNCTION partitioner.quote_array_literals(arr anyarray)
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

CREATE OR REPLACE FUNCTION partitioner.table_exists(table_name text)
    RETURNS text AS $$
DECLARE
    dot_pos int;
    check_sql text;
    schema_name text;
    table_name2 text;
BEGIN
    check_sql := 'SELECT n.nspname || ''.'' || t.relname
                  FROM pg_class t, pg_namespace n
                  WHERE t.relkind=''r'' AND t.relnamespace=n.oid
                  ';
    SELECT position('.' in table_name) INTO dot_pos;
    IF dot_pos = 0 THEN
        check_sql := check_sql || ' AND relname=' || quote_literal(table_name) || ' AND pg_table_is_visible(t.oid);';
    ELSE
        SELECT split_part(table_name, '.', 1) INTO schema_name;
        SELECT split_part(table_name, '.', 2) INTO table_name2;
        check_sql := check_sql || ' AND n.nspname=' || quote_literal(schema_name) || '
                                   AND t.relname=' || quote_literal(table_name2) || ';';
    END IF;
    EXECUTE check_sql INTO table_name2;
    RETURN table_name2;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION partitioner.get_column_type(table_name text, column_name text)
    RETURNS text AS $$
DECLARE
    q_table_name text;
    column_type text;
    check_sql text;
BEGIN
    SELECT table_exists(table_name) INTO q_table_name;
    IF q_table_name IS NULL THEN
        RAISE EXCEPTION 'Table % does not exist (at least not in the current search path).', table_name;
    END IF;
    
    check_sql := 'SELECT pg_catalog.format_type(a.atttypid, a.atttypmod)
                  FROM pg_attribute a
                  WHERE a.attrelid = ' || quote_literal(q_table_name) || '::regclass
                    AND a.attname=' || quote_literal(column_name) || ';';
    EXECUTE check_sql
    INTO column_type;
    
    IF column_type IS NULL THEN
        RAISE EXCEPTION 'Could not find column % on table %.', column_name, q_table_name;
    END IF;
    
    RETURN column_type;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION partitioner.get_table_constraint_defs(table_name text)
    RETURNS SETOF text AS $$
    SELECT pg_get_constraintdef(c.oid)
    FROM pg_constraint c
    WHERE c.conrelid=$1::regclass
$$ LANGUAGE sql;

CREATE OR REPLACE FUNCTION partitioner.get_table_index_defs(table_name text)
    RETURNS SETOF text AS $$
    SELECT pg_get_indexdef(i.indexrelid) as def
    FROM pg_index i
    WHERE i.indrelid=$1::regclass
        -- AND i.indisprimary IS NOT TRUE AND i.indisunique IS NOT TRUE
$$ LANGUAGE sql;

CREATE OR REPLACE FUNCTION partitioner.get_table_attributes(table_name text)
    RETURNS SETOF text AS $$
    SELECT a.attname::text
    FROM pg_attribute a
    WHERE a.attrelid=$1::regclass
        AND a.attnum > 0 AND NOT a.attisdropped
    ORDER BY a.attnum
$$ LANGUAGE sql;

CREATE OR REPLACE FUNCTION partitioner.column_is_indexed(column_name text, table_name text)
    RETURNS boolean AS $$
DECLARE
    tmp record;
BEGIN
    PERFORM 1
    FROM pg_attribute a, pg_index i
    WHERE a.attname=column_name AND a.attrelid=table_name::regclass
        AND a.attrelid=i.indrelid AND i.indkey[0]=a.attnum;
        
    IF FOUND THEN
        RETURN TRUE;
    END IF;
    RETURN FALSE;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION partitioner.get_table_partitions(text)
    RETURNS SETOF text AS $$
    SELECT n.nspname || '.' || t.relname::text
    FROM pg_class t, pg_namespace n, pg_inherits i
    WHERE n.oid=t.relnamespace 
        AND nspname || '.' || relname ~ ('^' || $1 || '_[0-9]+_[0-9]+$')
        AND t.oid=i.inhrelid AND i.inhparent = $1::regclass
    ORDER BY relname
$$ LANGUAGE sql;

CREATE OR REPLACE FUNCTION partitioner.get_table_pkey_fields(table_name text)
    RETURNS text[] AS $$
    SELECT ARRAY(SELECT a.attname::text
                 FROM pg_constraint c, pg_attribute a
                 WHERE c.conrelid=a.attrelid AND a.attnum = any(c.conkey)
                    AND c.contype='p' AND c.conrelid=$1::regclass)
$$ LANGUAGE sql;

CREATE OR REPLACE FUNCTION partitioner.get_attributes_str_by_attnums(table_name text, attnums int[])
    RETURNS text AS $$
DECLARE
    attname_sql text;
    column_name text;
    attnames text[];
    i int;
BEGIN
    FOR i IN 1 .. array_upper(attnums, 1)
    LOOP
        attname_sql := 'SELECT attname
                        FROM pg_attribute
                        WHERE attnum=' || quote_literal(attnums[i]) || '
                        AND attrelid=''' || table_name || '''::regclass;';
        EXECUTE attname_sql
        INTO column_name;
        attnames[i] := column_name;
    END LOOP;
    RETURN array_to_string(attnames, ',');
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION partitioner.move_partition_data(table_name text, part_col text, count integer, max real)
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
    SELECT * FROM partitioner.get_table_pkey_fields(table_name) INTO pkey_fields;
    
    -- make the returning clause
    FOR i IN 1 .. array_upper(pkey_fields, 1)
    LOOP
        pkey_fields_conv[i] := pkey_fields[i] || '::text';
    END LOOP;
    
    <<main_loop>>
    FOR partition IN
        SELECT * FROM partitioner.get_table_partitions(table_name)
    LOOP
        -- raise notice 'moving data for partition: %', partition;
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
            -- RAISE NOTICE 'move data sql: %', move_data_sql;
            IF max != 'Infinity' THEN
                move_data_sql := move_data_sql || ' RETURNING ''('' || array_to_string(quote_array_literals(array[' || array_to_string(pkey_fields_conv, ',') || ']), '','') || '')'';';
                FOR moved_data_pkey IN EXECUTE move_data_sql
                LOOP
                    -- RAISE NOTICE 'move data pkey: %', moved_data_pkey;
                    pkeys_str := pkeys_str || moved_data_pkey || ',';
                END LOOP;
            
                EXIT WHEN NOT FOUND;
                GET DIAGNOSTICS moved := ROW_COUNT;
                -- raise notice 'inserted count: %', moved;
                        
                pkeys_str := substring(pkeys_str from 1 for char_length(pkeys_str)-1);
                delete_sql := 'DELETE FROM ONLY ' || table_name || '
                               WHERE (' || array_to_string(pkey_fields, ',') || ') IN (' || pkeys_str ||');';
                -- RAISE NOTICE 'delete data sql: %', delete_sql;
                EXECUTE delete_sql;
            ELSE
                move_data_sql := move_data_sql || ' OFFSET ' || quote_literal(offset);
                EXECUTE move_data_sql;
                offset := offset + to_move;
            END IF;
            
            GET DIAGNOSTICS moved := ROW_COUNT;
            -- raise notice 'deleted count: %', moved;
            
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

CREATE OR REPLACE FUNCTION partitioner.move_partition_data(table_name text, part_col text, count integer)
    RETURNS integer AS $$
    SELECT partitioner.move_partition_data($1, $2, $3, 'Infinity'::real)
$$ LANGUAGE sql;