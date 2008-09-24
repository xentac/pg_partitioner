#!/usr/bin/env python

import sys, os, re
import psycopg2
from optparse import OptionGroup
from db_script import DBScript
from util_funcs import *

create_part_sql = \
'''
CREATE TABLE %s (
    %s
    CHECK (%s >= '%s' AND %s < '%s')
) INHERITS (%s);
'''

def_dates_sql = \
'''
SELECT to_char(date_trunc('%s', MIN(%s)), 'YYYYMMDD'), to_char(date_trunc('%s', MAX(%s)), 'YYYYMMDD')
FROM %s;
'''

def_ints_sql = \
'''
SELECT %s * (MIN(%s)/%s), %s * (MAX(%s)/%s) 
FROM %s;
'''

trig_check_sql = \
'''
SELECT 1
FROM pg_trigger t
WHERE t.tgrelid='%(table_name)s'::regclass
    AND t.tgname='%(base_table_name)s_partition_trigger';
'''

create_trig_sql = \
'''
CREATE TRIGGER %(base_table_name)s_partition_trigger BEFORE INSERT OR UPDATE
    ON %(table_name)s FOR EACH ROW
    EXECUTE PROCEDURE %(table_name)s_ins_trig();
'''

move_down_sql = \
'''
SELECT %(table_name)s_ins_func(t.*)
FROM (SELECT *
      FROM ONLY %(table_name)s ORDER BY %(ts_column)s OFFSET %(offset)s LIMIT %(limit)s) AS t;
'''

def_table_schema = \
'''
SELECT n.nspname
FROM pg_namespace n, pg_class t
WHERE t.relnamespace=n.oid
    AND t.relname=%s AND pg_table_is_visible(t.oid)
'''

class DatePartitioner(DBScript):
    def __init__(self, args):
        super(DatePartitioner, self).__init__('DatePartitioner', args)
        
        if self.args[0].find('.') > -1:
            self.qualified_table_name = self.args[0]
            self.table_name = self.args[0].split('.')[1]
        else:
            self.curs.execute(def_table_schema, (self.args[0],))
            self.qualified_table_name = self.curs.fetchone()[0]+'.'+self.args[0]
            self.table_name = self.args[0]
        self.ts_column = self.args[1]
    
    def init_optparse(self):
        usage = "%prog [options] TABLE PARTITION_FIELD"
        parser = super(DatePartitioner, self).init_optparse(usage)
        
        g = OptionGroup(parser, "Partitioning options", 
                        "Ways to customize the number of partitions and/or the range of each.  Nothing is created if none of these is used.  This is useful for making table paritioning a two step process: 1. Create the partitions using the below options.  2. Migrate the data from the parent table into the new partitions using the above -m option.")
        g.add_option('-u', '--units', dest="units", metavar='UNIT',
                     help="A valid PG unit for the column partitioned on.  Defaults to month for timestamp/date columns and 1 for integer column types")
        g.add_option('--scale', type='int', metavar='COUNT',
                     default=1,
                     help="A 'scale factor' for the units.  The resulting range of values per partition created is scale * units.  The default is 1.")
        g.add_option('-s', '--start', 
                     help="A valid date string for the start date for the partitions.  If used with --units this will default to the oldest DATE_FIELD value in the table truncated to --units, else will truncate to day.")
        g.add_option('-e', '--end', 
                     help="A valid date string for the end date of the partitions.  Using --unit will force this be rounded to the nearest --unit value based from --start after --end. Defaults to the current date.")
        g.add_option('-i', '--ignore_errors', action="store_true", default=False,
                     help="When creating tables, ignore any errors instead of rolling completely back. Default: False.")
        g.add_option('-m', '--migrate', action="callback", default=0, callback=self.migrate_opt_callback, dest="migrate",
                     help="Migrate any data in the parent table in to available partitions.  This is done by repeatedly moving X rows from the parent down until all rows have been processed where X is an optional argment to this option that defaults to 100.  Any rows for which no valid child table exists are left in the parent.")
        g.add_option('-f', '--fkeys', action="store_true", default=False,
                    help="Include building any fkeys present on the parent on the partitions.")
                     
        parser.add_option_group(g)
        
        return parser
    
    def migrate_opt_callback(self, option, opt_str, value, parser):
        assert value is None
        try:
            value = int(parser.rargs[0])
            del parser.rargs[0]
        except ValueError:
            value = 100
            
        setattr(parser.values, option.dest, value)
        
    
    def validate_opts(self):
        if len(self.args) < 2:
            self.parser.error("date_partitioner.py requires both a table name and timestamp field name on that table as arguments.")
            
        if not table_exists(self.curs, self.args[0]):
            self.parser.error("%s does not exist in the given database." % self.args[0])
        
        self.col_type = get_column_type(self.curs, self.args[0], self.args[1])
        if not self.col_type:
            self.parser.error("%s does not exist on %s." % (self.args[1], self.args[0]))
        self.set_range_vars()
    
    def set_range_vars(self):
        if self.col_type == 'date' or re.search('time[^\]]*$', self.col_type):
            self.short_type = 'ts'
            units = self.opts.units or 'month'
            self.curs.execute(def_dates_sql % (units, self.args[1], units, self.args[1], self.args[0]))
        elif re.search('int[^\]]*$', self.col_type):
            self.short_type = 'int'
            units = int(self.opts.units) or 1
            self.curs.execute(def_ints_sql % (units, self.args[1], units, units, self.args[1], units, self.args[0]))
        else:
            raise RuntimeError("The type of %s (%s) is not valid for partitioning on (at this time)." 
                                % (self.args[1], self.col_type))
        
        res = self.curs.fetchone()
        if not self.opts.start:
            if res[0] is None:
                self.parser.error("No data in table to use for default values, you'll need to specify explicit dates if you want to partition this table.")
            self.opts.start = res[0]
        
        if self.short_type == 'ts':
            self.opts.start = normalize_date(self.curs, self.opts.start, 'YYYYMMDD', units)
            self.opts.units = str(self.opts.scale) + ' ' + units
        elif self.short_type == 'int':
            self.opts.start = units * (self.opts.start/units)
            self.opts.units = str(self.opts.scale * units)
        
        self.opts.end = self.opts.end or res[1]
        
    def nextInterval(self, val):
        if self.short_type == 'ts':
            sql = "SELECT to_char(%s::timestamp + %s, 'YYYYMMDD')"
            self.curs.execute(sql, (val, self.opts.units))
            return self.curs.fetchone()[0]
        elif self.short_type == 'int':
            return str(int(val) + int(self.opts.units))
    
    def get_constraintdefs_str(self):
        constraints = get_constraint_defs(self.curs, self.qualified_table_name)
        constraints = '' if not constraints else ','.join(constraints)+','
        
        if self.opts.fkeys:
            fkeys = get_fkey_defs(self.curs, self.qualified_table_name)
            constraints = constraints + ('' if not fkeys else ','.join(fkeys)+',')
        return constraints
    
    def get_indexdefs_str(self):
        idxs_sql = ''
        idx_count = 0
        idx_re = re.compile(r'(create (?:unique )?index )(.*)', re.I)
        for idx in get_index_defs(self.curs, self.qualified_table_name):
            idx_count += 1
            if idx.count(self.table_name) == 1: 
                idx = re.sub(idx_re, r'\1%s_%%s_%%s_\2' % self.table_name, idx)
            else:
                i = idx.find(self.table_name) + len(self.table_name)
                idx = idx[:i]+'_%s_%s'+idx[i:]
            i = idx.rfind(self.table_name) + len(self.table_name)
            idx = idx[:i]+'_%s_%s'+idx[i:]
            idxs_sql += idx+';'
        return idxs_sql, idx_count
    
    def get_fkeydefs_str(self):
        fkeys = get_fkey_defs(self.curs, self.qualified_table_name)
        return '' if not fkeys else ','.join(fkeys)+','
        
    def create_partitions(self):
        '''
        Create the child partitions, bails out if it encounters a partition
        that already exists
        '''
        constraints_str = self.get_constraintdefs_str()
        idxs_str, idx_count = self.get_indexdefs_str()
        
        if self.opts.fkeys:
            constraints_str = constraints_str + self.get_fkeydefs_str()
        
        end_points = (self.opts.start, self.nextInterval(self.opts.start))
        while True:
            if int(end_points[0]) > int(self.opts.end):
                break
            try:
                if self.opts.ignore_errors:
                    self.curs.execute('SAVEPOINT save;')
                    
                part_table = '%s_%s_%s' % (self.qualified_table_name, end_points[0], end_points[1])
                print 'Creating ' + part_table
                self.curs.execute(create_part_sql % 
                        (part_table, constraints_str, self.ts_column, 
                         end_points[0], self.ts_column, end_points[1], self.qualified_table_name))
                
                if idxs_str:
                    self.curs.execute(idxs_str % ((end_points[0], end_points[1])*idx_count*2))
            except psycopg2.ProgrammingError, e:
                print e,
                if not self.opts.ignore_errors:
                    print 'Last query: %s' % self.curs.query
                    sys.exit(1)
                print 'Ignoring error.'
                self.curs.execute('ROLLBACK TO SAVEPOINT save;')
                
            end_points = (end_points[1], self.nextInterval(end_points[1]))
    
    def set_trigger_func(self):
        '''
        Uses the template date_part_trig.tpl.sql to build out a trigger function
        for the parent table if it's not already there
        '''

        tpl_path = os.path.dirname(os.path.realpath(__file__))
        funcs_sql = open(tpl_path+'/range_part_trig.tpl.sql').read()
        table_atts = table_attributes(self.curs, self.qualified_table_name)
        d = {'table_name': self.qualified_table_name,
             'base_table_name': self.table_name,
             'ts_column': self.ts_column,
             'table_atts': ','.join(table_atts),
             'atts_vals': " || ',' || ".join(["quote_nullable(rec.%s)" % att for att in table_atts]),
             'col_type': self.col_type
        }
        self.curs.execute(funcs_sql % d)
            
        self.curs.execute(trig_check_sql % d)
        if not self.curs.fetchone():
            self.curs.execute(create_trig_sql % d)
    
    def move_data_down(self):
        '''
        In the loop SELECT %s_ins_func(); will push any data down it can and return anything it can't.
        We then DELETE everything touched and, after the loop, re-insert data that couldn't be moved.
        '''
        d = {'table_name': self.qualified_table_name,
             'base_table_name': self.table_name,
             'ts_column': self.ts_column,
             'offset': 0,
             'limit': self.opts.migrate
            }
        
        self.curs.execute(trig_check_sql % d)
        if not self.curs.fetchone():
            print '%s is not partitioned.' % self.qualified_table_name
            sys.exit(1)
            
        keep = []
        kept = moved = 0
        while True:
            self.curs.execute(move_down_sql % d)
            for res in self.curs.fetchall():
                if res[0] is not None:
                    kept += 1
                    keep.append(res)
                else:
                    moved += 1
                    
            if self.curs.rowcount < 100:
                break
            d['offset'] += 100;
        
        all_moved = '' if keep else '(all) '
        print 'Moved %d %srows into partitions.' % (moved, all_moved)
        
        self.curs.execute('TRUNCATE %s;' % self.qualified_table_name)
        
        if keep:
            keep = (','.join([','.join(res) for res in keep])).replace('"', "'")
            self.curs.execute('INSERT INTO %s VALUES %s;' % (self.qualified_table_name, keep))
            print 'Kept %d rows in the parent table.' % kept
        
    def work(self):
        super(DatePartitioner, self).work()
        try:
            self.opts.create = True
            if self.opts.create:
                self.set_trigger_func()
        
                # build the partitions
                self.create_partitions()
                
            if self.opts.migrate:
                self.move_data_down()
            
            self.commit()
        except Exception, e:
            print 'Last query: %s' % self.curs.query
            raise
        

def main(args=None):
    if not args:
        args = sys.argv[1:]
    
    try:
        dp = DatePartitioner(args)
    except RuntimeError, e:
        print e
        sys.exit(1)
    dp.work()

if __name__ == '__main__':
    main()