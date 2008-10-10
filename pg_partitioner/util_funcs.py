
def table_exists(curs, table_name=''):
    '''
    Fetches a row if the given table exists.
    '''
    check_sql = \
    '''
    SELECT n.nspname || '.' || t.relname
    FROM pg_class t, pg_namespace n
    WHERE t.relname=%s AND t.relkind='r'
        AND t.relnamespace=n.oid
    '''
    if table_name.find('.') > -1:
        schema_name, table_name = table_name.split('.')
        check_sql += " AND n.nspname='%s'" % schema_name
    else:
        check_sql += " AND pg_table_is_visible(t.oid)"
    curs.execute(check_sql, (table_name, ))
    return curs.fetchone()

def get_column_type(curs, table_name, column_name):
    '''
    Returns a result if the given table has the given column.
    '''
    check_sql = \
    '''
    SELECT pg_catalog.format_type(a.atttypid, a.atttypmod)
    FROM pg_attribute a
    WHERE a.attrelid = %s::regclass AND a.attname=%s
    '''
    curs.execute(check_sql, (table_name, column_name))
    return curs.fetchone()[0]
    
def get_constraint_defs(curs, table_name):
    '''
    Returns a list of constraint definition fragments suitable for use 
    in SQL create table or alter table statements.  This will include
    primary keys and unique indexes but not fkeys.
    '''
    constraints_sql = \
    '''
    SELECT pg_get_constraintdef(c.oid)
    FROM pg_constraint c
    WHERE c.conrelid=%s::regclass
        AND c.contype!='f';
    '''
    curs.execute(constraints_sql, (table_name,))
    return [res[0] for res in curs.fetchall()]
    
def get_fkey_defs(curs, table_name):
    '''
    Returns a list of fkey definition fragments suitable for use in
    SQL create table or alter table statements.
    '''
    fkeys_sql = \
    '''
    SELECT pg_get_constraintdef(c.oid)
    FROM pg_constraint c
    WHERE c.conrelid=%s::regclass
        AND c.contype='f';
    '''
    curs.execute(fkeys_sql, (table_name,))
    return [res[0] for res in curs.fetchall()]

def get_index_defs(curs, table_name):
    '''
    Returns a list of 2-tuples consisting of each index creation def  statement
    for any non-primary key or unique indexes on the given table and the 
    index name. 
    '''
    indexes_sql = \
    '''
    SELECT pg_get_indexdef(i.indexrelid) as def
    FROM pg_index i
    WHERE i.indrelid=%s::regclass
        AND i.indisprimary IS NOT TRUE AND i.indisunique IS NOT TRUE;
    '''
    curs.execute(indexes_sql, (table_name,))
    return [res[0] for res in curs.fetchall()]

def table_attributes(curs, table_name):
    '''
    Returns a tuple of the given table's attributes
    '''
    att_sql = \
    '''
    SELECT a.attname
    FROM pg_attribute a
    WHERE a.attrelid=%s::regclass
        AND a.attnum > 0 AND NOT a.attisdropped
    ORDER BY a.attnum;
    '''
    curs.execute(att_sql, (table_name,))
    atts = tuple([res[0] for res in curs.fetchall()])
    return atts

def normalize_date(curs, date_str, fmt, units='month', diff='0 months'):
    '''
    Takes a valid date string in any format and formats it according to fmt.
    '''
    normalize_date_sql = \
    '''
    SELECT to_char(date_trunc(%s, %s::timestamp + %s), %s);
    '''
    curs.execute(normalize_date_sql, (units, date_str, diff, fmt))
    return curs.fetchone()[0]
