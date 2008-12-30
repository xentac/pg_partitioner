
CREATE OR REPLACE FUNCTION %(table_name)s_ins_func(rec %(table_name)s)
    RETURNS %(table_name)s AS $$
DECLARE
    partition varchar;
    partition_points text[];
    ins_sql varchar;
BEGIN    
    FOR partition IN SELECT * FROM pgpartitioner.get_partitions('%(table_name)s')
    LOOP
        SELECT * FROM pgpartitioner.get_partition_bounds(partition) INTO partition_points;
        IF rec.%(part_column)s >= partition_points[1]::%(col_type)s AND rec.%(part_column)s < partition_points[2]::%(col_type)s THEN
            ins_sql := 'INSERT INTO ' || partition || ' (%(table_atts)s) VALUES (' || %(atts_vals)s || ');';
            EXECUTE ins_sql;
            RETURN NULL;
        END IF;
    END LOOP;
    
    RAISE WARNING 'No partition created for %(table_name)s to hold value %(col_type)s %%, leaving data in parent table.', rec.%(part_column)s;
    RETURN rec;
END;
$$ language plpgsql;


CREATE OR REPLACE FUNCTION %(table_name)s_ins_trig()
    RETURNS trigger AS $$
DECLARE
    res %(table_name)s;
    null_rec %(table_name)s;
BEGIN
    SELECT INTO res * FROM %(table_name)s_ins_func(NEW);
    IF row(res.*) IS DISTINCT FROM row(null_rec.*) THEN
        RETURN NEW;
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;
