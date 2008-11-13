
def table_exists(curs, table_name=''):
    '''
    If table_name is a schema qualified table name and it exists it is returned,
    else if it is not schema qualified and the table name exists in the search 
    path then that schema qualified table name is returned, else None.
    '''
    curs.execute('SELECT pgpartitioner.table_exists(%s)', (table_name,))
    return curs.fetchone()

def get_column_type(curs, table_name, column_name):
    '''
    If column_name exists on table_name it's SQL type is returned.  Else an
    exception is raised.
    '''
    curs.execute('SELECT pgpartitioner.get_column_type(%s, %s);', (table_name, column_name))
    return curs.fetchone()[0]
    
def get_constraint_defs(curs, table_name, fkeys=True):
    '''
    Returns a list of constraint definition fragments suitable for use 
    in SQL create table or alter table statements.  fkeys are not included if
    fkeys is false
    '''
    curs.execute('SELECT * FROM pgpartitioner.get_table_constraint_defs(%s, %s);', (table_name, fkeys))
    return [res[0] for res in curs.fetchall()]

def get_index_defs(curs, table_name):
    '''
    Returns a list of 2-tuples consisting of each index creation def  statement
    for any non-primary key or unique indexes on the given table and the 
    index name. 
    '''
    curs.execute('SELECT * FROM pgpartitioner.get_table_index_defs(%s);', (table_name,))
    return [res[0] for res in curs.fetchall()]

def table_attributes(curs, table_name):
    '''
    Returns a tuple of the given table's attributes
    '''
    curs.execute('SELECT * FROM pgpartitioner.get_table_attributes(%s);', (table_name,))
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
