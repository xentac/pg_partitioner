DROP SCHEMA IF EXISTS pgpartitioner CASCADE;
CREATE SCHEMA pgpartitioner;

CREATE OR REPLACE FUNCTION pgpartitioner.quote_nullable(val anyelement)
    RETURNS text AS $$
    SELECT COALESCE(quote_literal($1), 'NULL');
$$ LANGUAGE sql;
COMMENT ON FUNCTION pgpartitioner.quote_nullable (arr anyelement) IS 'Quotes NULL values: ''NULL''';

CREATE OR REPLACE FUNCTION pgpartitioner.quote_array_literals(arr anyarray)
    RETURNS text[] AS $$
DECLARE
    i int;
    ret text[];
BEGIN
    FOR i IN 1 .. array_upper(arr, 1)
    LOOP
        ret[i] := quote_nullable(arr[i]);
    END LOOP;
    RETURN ret;
END;
$$ LANGUAGE plpgsql;
COMMENT ON FUNCTION pgpartitioner.quote_array_literals (arr anyarray) IS 'Quotes all values in array with quote_nullable';

CREATE OR REPLACE FUNCTION pgpartitioner.table_exists(table_name text)
    RETURNS text AS $$
DECLARE
    dot_pos int;
    check_sql text;
    schema_name text;
    table_name2 text;
BEGIN
    check_sql := 'SELECT n.nspname || ''.'' || t.relname
                  FROM pg_class t, pg_namespace n
                  WHERE t.relkind=''r'' AND t.relnamespace=n.oid';
                  
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
COMMENT ON FUNCTION pgpartitioner.table_exists (table_name text) IS 'Checks if a table exists, returns fully qualified name or NULL.';

CREATE OR REPLACE FUNCTION pgpartitioner.get_column_type(table_name text, column_name text)
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
COMMENT ON FUNCTION pgpartitioner.get_column_type (table_name text, column_name text) IS 'Returns the type of the specified column on the specified table';

CREATE OR REPLACE FUNCTION pgpartitioner.get_table_constraint_defs(table_name text)
    RETURNS SETOF text AS $$
    SELECT pg_get_constraintdef(c.oid)
    FROM pg_constraint c
    WHERE c.conrelid=$1::regclass
$$ LANGUAGE sql;
COMMENT ON FUNCTION pgpartitioner.get_table_constraint_defs (table_name text) IS 'Returns pg_get_constraintdef() string for each constraint on the specified table';

CREATE OR REPLACE FUNCTION pgpartitioner.get_table_index_defs(table_name text)
    RETURNS SETOF text AS $$
    SELECT pg_get_indexdef(i.indexrelid) as def
    FROM pg_index i
    WHERE i.indrelid=$1::regclass
        -- AND i.indisprimary IS NOT TRUE AND i.indisunique IS NOT TRUE
$$ LANGUAGE sql;
COMMENT ON FUNCTION pgpartitioner.get_table_index_defs (table_name text) IS 'Returns pg_get_indexdef() strings for each index on the specified table.';

CREATE OR REPLACE FUNCTION pgpartitioner.get_table_attributes(table_name text)
    RETURNS SETOF text AS $$
    SELECT a.attname::text
    FROM pg_attribute a
    WHERE a.attrelid=$1::regclass
        AND a.attnum > 0 AND NOT a.attisdropped
    ORDER BY a.attnum
$$ LANGUAGE sql;
COMMENT ON FUNCTION pgpartitioner.get_table_attributes (table_name text) IS 'Returns the all the attribute names on the specified table as text';

CREATE OR REPLACE FUNCTION pgpartitioner.column_is_indexed(column_name text, table_name text)
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
COMMENT ON FUNCTION pgpartitioner.column_is_indexed (column_name text, table_name text) IS 'Checks if column is in the first position of any index on the specified table.';

CREATE OR REPLACE FUNCTION pgpartitioner.get_partitions(text)
    RETURNS SETOF text AS $$
    SELECT n.nspname || '.' || t.relname::text
    FROM pg_class t, pg_namespace n, pg_inherits i
    WHERE n.oid=t.relnamespace 
        AND nspname || '.' || relname ~ ('^' || $1 || '_[0-9]+$')
        AND t.oid=i.inhrelid AND i.inhparent = $1::regclass
    ORDER BY relname
$$ LANGUAGE sql;
COMMENT ON FUNCTION pgpartitioner.get_partitions (text) IS 'Returns all partitions of the specified table as text.';

CREATE OR REPLACE FUNCTION pgpartitioner.get_partition_parent(text)
    RETURNS text AS $$
    SELECT n.nspname || '.' || t.relname::text
    FROM pg_class t, pg_namespace n, pg_inherits i
    WHERE n.oid=t.relnamespace
        AND t.oid=i.inhparent AND i.inhrelid = $1::regclass
$$ LANGUAGE sql;

CREATE OR REPLACE FUNCTION pgpartitioner.get_partition_points(table_name text)
    RETURNS SETOF text AS $$
    SELECT split_part(s.part, '_', array_upper(string_to_array(s.part, '_'), 1))
    FROM pgpartitioner.get_partitions($1) s(part)
$$ LANGUAGE sql;

CREATE OR REPLACE FUNCTION pgpartitioner.get_table_pkey_fields(table_name text)
    RETURNS text[] AS $$
    SELECT ARRAY(SELECT a.attname::text
                 FROM pg_constraint c, pg_attribute a
                 WHERE c.conrelid=a.attrelid AND a.attnum = any(c.conkey)
                    AND c.contype='p' AND c.conrelid=$1::regclass)
$$ LANGUAGE sql;
COMMENT ON FUNCTION pgpartitioner.get_table_pkey_fields (table_name text) IS 'Returns a text array of all of a tables primary key attributes';

CREATE OR REPLACE FUNCTION pgpartitioner.get_attributes_str_by_attnums(table_name text, attnums int[])
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
COMMENT ON FUNCTION pgpartitioner.get_attributes_str_by_attnums (table_name text, attnums int[]) IS 'Given an array of integers matching attnums on the specified table, returns a CSV string of the corresponding attribute names.';

CREATE OR REPLACE FUNCTION pgpartitioner.move_partition_data(src_tbl text, dst_tbl text, part_col text, count integer, max real)
    RETURNS integer AS $$
DECLARE
    partition_points text[];
    bounds text[];
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
    SELECT * FROM pgpartitioner.get_table_pkey_fields(src_tbl) INTO pkey_fields;
    SELECT ARRAY(SELECT * FROM pgpartitioner.get_partition_points(pgpartitioner.get_partition_parent(dst_tbl)))
        INTO partition_points;
    
    -- make the returning clause
    FOR i IN 1..array_upper(pkey_fields, 1)
    LOOP
        pkey_fields_conv[i] := pkey_fields[i] || '::text';
    END LOOP;
    
    bounds[1] := substring(dst_tbl from '_([0-9]*)$');
    raise notice 'bounds: %', bounds[1];
    FOR i IN 1..array_upper(partition_points, 1)-1
    LOOP
        IF bounds[1] = partition_points[i] THEN
            bounds[2] := partition_points[i+1];
            EXIT;
        END IF;
    END LOOP;
    
    offset := 0;
    LOOP
        -- ensure we don't pass the max rows to be moved if it's set
        IF total_moved + to_move > max THEN
            to_move := max - total_moved;
        END IF;
        
        pkeys_str := '';
        move_data_sql := 'INSERT INTO ' || dst_tbl || '
                          SELECT *
                          FROM ONLY ' || src_tbl || '
                          WHERE ' || quote_ident(part_col) || ' >= ' || quote_literal(bounds[1]);
        
        IF bounds[2] IS NOT NULL THEN
            move_data_sql := move_data_sql || ' AND ' || quote_ident(part_col) || ' < ' || quote_literal(bounds[2]);
        END IF;
        
        move_data_sql := move_data_sql || '
                          ORDER BY ' || quote_ident(part_col) || '
                          LIMIT ' || quote_literal(to_move) || ' ';
        RAISE NOTICE 'move data sql: %', move_data_sql;
        IF max != 'Infinity' THEN
            move_data_sql := move_data_sql || ' RETURNING ''('' || array_to_string(pgpartitioner.quote_array_literals(array[' || array_to_string(pkey_fields_conv, ',') || ']), '','') || '')'';';
            FOR moved_data_pkey IN EXECUTE move_data_sql
            LOOP
                -- RAISE NOTICE 'move data pkey: %', moved_data_pkey;
                pkeys_str := pkeys_str || moved_data_pkey || ',';
            END LOOP;
        
            EXIT WHEN NOT FOUND;
            GET DIAGNOSTICS moved := ROW_COUNT;
            -- raise notice 'inserted count: %', moved;
                    
            pkeys_str := substring(pkeys_str from 1 for char_length(pkeys_str)-1);
            delete_sql := 'DELETE FROM ONLY ' || src_tbl || '
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
        EXIT WHEN total_moved = max OR moved < count OR count = 0;
        -- EXIT main_loop WHEN total_moved = max OR moved < count;
        -- EXIT partition_loop WHEN moved < count;
    END LOOP;
    IF max = 'Infinity' THEN
        delete_sql := 'DELETE FROM ONLY ' || src_tbl || '
                 WHERE ' || quote_ident(part_col) || ' >= ' || quote_literal(bounds[1]);
        IF bounds[2] IS NOT NULL THEN
            delete_sql := delete_sql || ' AND ' || quote_ident(part_col) || ' < ' || quote_literal(bounds[2]) || ';';
        END IF;
        EXECUTE delete_sql;
    END IF;
    RETURN total_moved;
END;
$$ LANGUAGE plpgsql;
COMMENT ON FUNCTION pgpartitioner.move_partition_data(src_tbl text, dst_tbl text, part_col text, count integer, max real) IS 'Handles the actual moving of data using partioner schema functions.  Specifies the table to partition, the column to base partitioning on, the number of records to partition during each iteration, and the maximum amount of records to move.';

CREATE OR REPLACE FUNCTION pgpartitioner.move_partition_data(src_tbl text, dst_tbl text, part_col text, count integer)
    RETURNS integer AS $$
    SELECT pgpartitioner.move_partition_data($1, $2, $3, $4, 'Infinity'::real)
$$ LANGUAGE sql;
COMMENT ON FUNCTION pgpartitioner.move_partition_data(src_tbl text, dst_tbl text, part_col text, count integer) IS 'Override of pgpartitioner.move_partition_data(), moves all data at once.';

CREATE OR REPLACE FUNCTION pgpartitioner.partition_parent_data(table_name text, part_col text, count integer, max real)
    RETURNS integer AS $$
DECLARE
    q_table_name text;
    partition text;
    moved integer;
    total_moved integer;
    cur_max real DEFAULT max;
BEGIN
    SELECT pgpartitioner.table_exists(table_name) INTO q_table_name;
    IF q_table_name IS NULL THEN
        RAISE NOTICE '% does not exist in the current search path.';
    END IF;
    
    total_moved := 0;
    FOR partition IN
        SELECT * FROM pgpartitioner.get_partitions(q_table_name)
    LOOP
        SELECT pgpartitioner.move_partition_data(q_table_name, partition, part_col, count, cur_max) INTO moved;
        total_moved := total_moved + moved;
        cur_max := cur_max - moved;
        EXIT WHEN cur_max = 0;
    END LOOP;
    RETURN total_moved;
END
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION pgpartitioner.partition_parent_data(table_name text, part_col text, count integer)
    RETURNS integer AS $$
    SELECT pgpartitioner.partition_parent_data($1, $2, $3, 'Infinity'::real);
$$ LANGUAGE sql;