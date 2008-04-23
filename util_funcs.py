
def table_exists(curs, table_name, schema_name='public'):
    '''
    Fetches a row if the given table exists.
    '''
    check_sql = \
    '''
    SELECT 1
    FROM pg_class t, pg_namespace n
    WHERE t.relname=%s AND n.nspname=%s
        AND t.relnamespace=n.oid;
    '''
    curs.execute(check_sql, (table_name, schema_name))
    return curs.fetchone()

def table_has_column(curs, table_name, column_name, schema_name='public'):
    '''
    Returns a result if the given table has the given column.
    '''
    check_sql = \
    '''
    SELECT 1
    FROM pg_class c, pg_attribute a, pg_type t, pg_namespace n
    WHERE c.oid=a.attrelid
        AND c.relname=%s AND a.attname=%s
        AND c.relnamespace=n.oid AND n.nspname=%s
        AND a.atttypid=t.oid AND t.typname ~ 'timestamp';
    '''
    curs.execute(check_sql, (table_name, column_name, schema_name))
    return curs.fetchone()
    
def get_constraint_defs(curs, table_name, schema_name='public'):
    '''
    Returns a list of constraint definition fragments suitable for use 
    in SQL create table or alter table statements.  This will include
    primary keys and unique indexes.
    '''
    constraints_sql = \
    '''
    SELECT pg_get_constraintdef(c.oid)
    FROM pg_class t, pg_constraint c, pg_namespace n
    WHERE t.relname=%s and n.nspname=%s
        AND t.oid=c.conrelid AND t.relnamespace=n.oid;
    '''
    curs.execute(constraints_sql, (table_name, schema_name))
    constraints = []
    for res in curs.fetchall():
        constraints.append(res[0])
    
    return constraints

def get_index_defs(curs, table_name, schema_name='public'):
    '''
    Returns a list of 2-tuples consisting of each index creation def  statement
    for any non-primary key or unique indexes on the given table and the 
    index name. 
    '''
    indexes_sql = \
    '''
    SELECT pg_get_indexdef(i.indexrelid) as def, ti.relname as name
    FROM pg_class t, pg_class ti, pg_index i, pg_namespace n
    WHERE t.relname=%s AND n.nspname=%s
        AND t.relnamespace=n.oid AND t.oid=i.indrelid
        AND i.indexrelid=ti.oid
        AND i.indisprimary IS NOT TRUE AND i.indisunique IS NOT TRUE;
    '''
    curs.execute(indexes_sql, (table_name, schema_name))
    return curs.fetchall()

def table_attributes(curs, table_name, schema_name='public'):
    '''
    Returns a tuple of the given table's attributes
    '''
    att_sql = \
    '''
    SELECT a.attname
    FROM pg_attribute a, pg_class t, pg_namespace n
    WHERE a.attrelid=t.oid AND t.relnamespace=n.oid
        AND t.relname=%s AND n.nspname=%s
        AND a.attnum > 0 AND NOT a.attisdropped
    ORDER BY a.attnum;
    '''
    atts = ()
    curs.execute(att_sql, (table_name, schema_name))
    for res in curs.fetchall():
        atts += (res[0],)
    return atts

def normalize_date(curs, date_str, fmt, diff='0 months'):
    '''
    Takes a valid date string in any format and formats it according to fmt.
    '''
    normalize_date_sql = \
    '''
    SELECT to_char(%s::timestamp + %s, %s);
    '''
    curs.execute(normalize_date_sql, (date_str, diff, fmt))
    return curs.fetchone()[0]

def get_dates_by_count(curs, start_ts, end_ts, count):
    dates_by_count_sql = \
    '''
    SELECT to_char(%s::timestamp + (%s::timestamp - %s::timestamp)*g.i/%s, 'YYYYMMDD') as ts
    FROM generate_series(0, %s) g(i)
    UNION
    SELECT %s as ts
    ORDER BY ts
    '''
    curs.execute(dates_by_count_sql, (start_ts, end_ts, start_ts, count, count, end_ts))
    for res in curs.fetchall():
        yield res[0]

def get_dates_by_unit(curs, start_ts, end_ts, unit):
    dates_by_unit_sql = \
    '''
    SELECT to_char(%s::timestamp + (%s * %s::interval)::interval, 'YYYYMMDD');
    '''
    i = 0
    while True:
        curs.execute(dates_by_unit_sql, (start_ts, i, unit))
        res = curs.fetchone()
        yield res[0]
        if res[0] >= end_ts:
            return
        i += 1
