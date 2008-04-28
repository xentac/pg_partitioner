#!/usr/bin/env python

import sys, os
import psycopg2
from psycopg2.extras import DictConnection
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

dates_by_count_sql = \
'''
SELECT to_char('%s'::timestamp + ('%s'::timestamp - '%s'::timestamp)*g.i/%d, 'YYYYMMDD') as ts
FROM generate_series(0, %d) g(i)
UNION
SELECT '%s' as ts
ORDER BY ts;
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
FROM pg_class c, pg_trigger t
WHERE c.oid=t.tgrelid
    AND c.relname='%s' AND t.tgname='%s_partition_trigger';
'''

create_trig_sql = \
'''
CREATE TRIGGER %(table_name)s_partition_trigger BEFORE INSERT OR UPDATE
    ON %(table_name)s FOR EACH ROW
    EXECUTE PROCEDURE %(table_name)s_ins_trig();
'''

move_down_sql = \
'''
SELECT %(table_name)s_ins_func(t.*)
FROM (SELECT *
      FROM ONLY %(table_name)s ORDER BY %(ts_column)s OFFSET %(offset)s LIMIT 100) AS t;
'''

class DatePartitioner(DBScript):
    def __init__(self, args):
        super(DatePartitioner, self).__init__('DatePartitioner', args)
        
        self.table_name = self.args[0]
        self.ts_column = self.args[1]
    
    def init_optparse(self):
        usage = "%prog [options] TABLE PARTITION_FIELD"
        parser = super(DatePartitioner, self).init_optparse(usage)
        
        g = OptionGroup(parser, "Partitioning options", 
                        "Ways to customize the number of partitions and/or the range of each.  Nothing is created if none of these is used.  This is useful for making table paritioning a two step process: 1. Create the partitions using the below options.  2. Migrate the data from the parent table into the new partitions using the above -m option.")
        g.add_option('-u', '--units', dest="units", metavar='UNIT',
                     help="A valid PG unit for the column partitioned on. Defaults to month for timestamp/date columns and 1 for integer column types")
        g.add_option('--scale', type='int', metavar='COUNT',
                     default=1,
                     help="A 'scale factor' for the units.  The resulting range of values per partition created is scale * units.  The default is 1.")
        g.add_option('-s', '--start', 
                     help="A valid date string for the start date for the partitions. If used with --units this will default to the oldest DATE_FIELD value in the table truncated to --units, else will truncate to day.")
        g.add_option('-e', '--end', 
                     help="A valid date string for the end date of the partitions.Using --unit will force this be rounded to the nearest --unit value based from --start after --end. Defaults to the current date.")
        g.add_option('-i', '--ignore_errors', action="store_true", default=False,
                     help="When creating tables, ignore any errors instead of rolling completely back. Default: False.")
        g.add_option('-m', '--migrate', action="store_true", default=False,
                     help="Migrate any data in the parent table in to available partitions. Default: False.")
        g.add_option('-f', '--fkeys', action="store_true", default=False,
                    help="Include building any fkeys present on the parent on the partitions.")
                     
        parser.add_option_group(g)
        
        return parser
    
    def validate_opts(self):
        if len(self.args) < 2:
            self.parser.error("date_partitioner.py requires both a table name and timestamp field name on that table as arguments.")
            
        if not table_exists(self.curs, self.args[0]):
            self.parser.error("%s does not exist in the given database." % self.args[0])
        
        self.col_type = get_column_type(self.curs, self.args[0], self.args[1])
        if not self.col_type:
            self.parser.error("%s does not exist on %s." % (self.args[1], self.args[0]))
        self.set_range_vars()
        print self.opts
        # self.opts.create = False
        # if self.opts.units or self.opts.count > 1 or self.opts.start or self.opts.end:
        #     self.opts.create = True
    
    def set_range_vars(self):
        if self.col_type == 'date' or self.col_type.find('time') > -1:
            self.short_type = 'ts'
            units = self.opts.units or 'month'
            self.curs.execute(def_dates_sql % (units, self.args[1], units, self.args[1], self.args[0]))
        elif self.col_type.find('int') > -1:
            self.short_type = 'int'
            units = self.opts.units or 1
            self.curs.execute(def_ints_sql % (units, self.args[1], units, units, self.args[1], units, self.args[0]))
        
        res = self.curs.fetchone()
        if not self.opts.start:
            if not res[0]:
                self.parser.error("No data in table to use for default dates, you'll need to specify explicit dates if you want to partition this table.")
            self.opts.start = res[0]
        
        if self.short_type == 'ts':
            self.opts.start = normalize_date(self.curs, self.opts.start, 'YYYYMMDD')
            self.opts.units = str(self.opts.scale) + ' ' + units
        elif self.short_type == 'int':
            self.opts.units = str(self.opts.scale * units)
        
        self.opts.end = self.opts.end or res[1]
        
    def nextInterval(self, val):
        if self.short_type == 'ts':
            sql = "SELECT to_char(%s::timestamp + %s, 'YYYYMMDD')"
            self.curs.execute(sql, (val, self.opts.units))
            return self.curs.fetchone()[0]
        elif self.short_type == 'int':
            return str(int(val) + int(self.opts.units))
        
    def create_partitions(self):
        '''
        Create the child partitions, bails out if it encounters a partition
        that already exists
        '''
        constraints = get_constraint_defs(self.curs, self.table_name)
        constraints = '' if not constraints else ','.join(constraints)+','
        
        if self.opts.fkeys:
            fkeys = get_fkey_defs(self.curs, self.table_name)
            constraints = constraints + ('' if not fkeys else ','.join(fkeys)+',')
        
        idxs_sql = ''
        idx_count = 0
        for idx in get_index_defs(self.curs, self.table_name):
            idx_count += 1
            idxs_sql += idx.replace(self.table_name, self.table_name+'_%s_%s')+'; '
        
        end_points = (self.opts.start, self.nextInterval(self.opts.start))
        while True:
            if int(end_points[0]) > int(self.opts.end):
                break
            try:
                if self.opts.ignore_errors:
                    self.curs.execute('SAVEPOINT save;')
                    
                part_table = '%s_%s_%s' % (self.table_name, end_points[0], end_points[1])
                print 'Creating ' + part_table
                self.curs.execute(create_part_sql % 
                        (part_table, constraints, self.ts_column, 
                         end_points[0], self.ts_column, end_points[1], self.table_name))
                         
                self.curs.execute(idxs_sql % ((end_points[0], end_points[1])*idx_count*2))
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
        funcs_sql = open(tpl_path+'/date_part_trig.tpl.sql').read()
        table_atts = table_attributes(self.curs, self.table_name)
        d = {'table_name': self.table_name,
             'ts_column': self.ts_column,
             'table_atts': ','.join(table_atts),
             'atts_vals': " || ',' || ".join(["quote_nullable(rec.%s)" % att for att in table_atts]),
             'col_type': self.col_type
        }
        self.curs.execute(funcs_sql % d)
            
        self.curs.execute(trig_check_sql % ((self.table_name,)*2))
        if not self.curs.fetchone():
            self.curs.execute('SELECT create_part_ins_trig(%(table_name)s)', d)
    
    def move_data_down(self):
        '''
        In the looop SELECT %s_ins_func(); will push any data down it can and return anything it can't.
        We then DELETE everything touched and, after the loop, re-insert data that couldn't be moved.
        '''
        d = {'table_name': self.table_name,
             'ts_column': self.ts_column,
             'offset': 0
            }
            
        self.curs.execute(trig_check_sql % ((self.table_name,)*2))
        if not self.curs.fetchone():
            print '%s is not partitioned.' % self.table_name
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
        
        self.curs.execute('TRUNCATE %s;' % self.table_name)
        
        if keep:
            keep = (','.join([','.join(res) for res in keep])).replace('"', "'")
            self.curs.execute('INSERT INTO %s VALUES %s;' % (self.table_name, keep))
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
    
    dp = DatePartitioner(args)
    dp.work()

if __name__ == '__main__':
    main()