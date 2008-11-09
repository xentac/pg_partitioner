
CREATE OR REPLACE FUNCTION %(table_name)s_ins_func(rec %(table_name)s)
    RETURNS %(table_name)s AS $$
DECLARE
    partition varchar;
    name_parts varchar[];
    partition_points text[];
    upper_dim integer;
    i integer;
    ins_sql varchar;
BEGIN
    SELECT pgpartitioner.get_partition_points('%(table_name)s') INTO partition_points;
    upper_dim := array_upper(parition_points, 1);
    
    partition := %(table_name)s || '_';
    IF rec.%(part_column)s < partition_points[1] THEN
        RAISE WARNING 'No partition created for %(table_name)s to hold value %(col_type)s %%, leaving data in parent table.', rec.%(part_column)s;
        RETURN rec;
    ELSIF rec.%(part_column)s > partition_points[upper_dim] THEN
        partition := partition || partition_points[uppder_dim];
    ELSE
        FOR i IN 2 .. upper_dim-1
        LOOP
            IF rec.%(part_column)s >= partition_points[i] AND rec.%(part_column)s < partition_points[i+1] THEN
                partition := partition || partition_points[i];
                EXIT;
            END IF;
        END LOOP;
    END IF;
        
    ins_sql := 'INSERT INTO ' || partition || ' (%(table_atts)s) VALUES (' || %(atts_vals)s || ');';
    EXECUTE ins_sql;
    RETURN NULL;    
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
