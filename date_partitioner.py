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
    CHECK (%s >= '%s'::timestamp AND %s < '%s'::timestamp)
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
        usage = "%prog [options] TABLE DATE_FIELD"
        parser = super(DatePartitioner, self).init_optparse(usage)
        
        g = OptionGroup(parser, "Partitioning options", 
                        "Ways to customize the number of partitions and/or the range of each.  Nothing is created if none of these is used.  This is useful for making table paritioning a two step process: 1. Create the partitions using the below options.  2. Migrate the data from the parent table into the new partitions using the above -m option.")
        g.add_option('-u', '--units', dest="units",
                     help="A valid PG interval unit.")
        g.add_option('-c', '--count', type='int',
                     default=1,
                     help="The number of partitions to create. Or, if used with --units the number of UNITS per partition.")
        g.add_option('-s', '--start_ts', 
                     help="A valid date string for the start date for the partitions. If used with --units this will default to the oldest DATE_FIELD value in the table truncated to --units, else will truncate to day.")
        g.add_option('-e', '--end_ts', 
                     help="A valid date string for the end date of the partitions.Using --unit will force this be rounded to the nearest --unit value based from --start_ts after --end_ts. Defaults to the current date.")
        g.add_option('-i', '--ignore_errors', action="store_true", default=False,
                     help="When creating tables, ignore any errors instead of rolling completely back. Default: False.")
        g.add_option('-m', '--migrate', action="store_true", default=False,
                     help="Migrate any data in the parent table in to available partitions. Default: False.")
                     
        parser.add_option_group(g)
        
        return parser
    
    def validate_opts(self):
        if len(self.args) < 2:
            self.parser.error("date_partitioner.py requires both a table name and timestamp field name on that table as arguments.")
            
        if not table_exists(self.curs, self.args[0]):
            self.parser.error("%s does not exist in the given database." % self.args[0])
        
        if not table_has_column(self.curs, self.args[0], self.args[1]):
            self.parser.error("%s does not exist on %s or is not a date type column." % (self.args[1], self.args[0]))
        
        self.opts.create = False
        if self.opts.units or self.opts.count > 1 or self.opts.start_ts or self.opts.end_ts:
            self.opts.create = True
        
        units = self.opts.units if self.opts.units else 'month'
        self.curs.execute(def_dates_sql % (units, self.args[1], units, self.args[1], self.args[0]))
        res = self.curs.fetchone()
        if not self.opts.start_ts:
            if not res[0]:
                self.parser.error("No data in table to use for default dates, you'll need to specify explicit dates if you want to partition this table.")
            self.opts.start_ts = res[0]
        
        self.opts.start_ts = normalize_date(self.curs, self.opts.start_ts, 'YYYYMMDD')

        if self.opts.units:
             self.opts.units = str(self.opts.count) + ' ' + self.opts.units
    
        if not self.opts.end_ts:
            self.opts.end_ts = res[1]
                    
    def nextInterval(self, interval, *args):
        fmt = "to_char('%s'::timestamp + '%s', 'YYYYMMDD')"
        sql = 'SELECT '+','.join([fmt % (arg, interval) for arg in args])
        self.curs.execute(sql)
        return self.curs.fetchone()
        
    def create_partitions(self, dates):
        '''
        Create the child partitions, bails out if it encounters a partition
        that already exists
        '''
        constraints = get_constraint_defs(self.curs, self.table_name)
        constraints = '' if not constraints else ','.join(constraints)+','
        
        idxs_sql = ''
        idx_count = 0
        for idx_tup in get_index_defs(self.curs, self.table_name):
            idx_count += 1
            idxs_sql += idx_tup[0].replace(self.table_name, self.table_name+'_%s_%s')+'; '
        
        dates = (self.opts.start_ts, self.nextInterval(self.opts.units, self.opts.start_ts)[0])
        while True:
            if dates[0] > self.opts.end_ts:
                break
            try:
                if self.opts.ignore_errors:
                    self.curs.execute('SAVEPOINT save;')
                    
                part_table = '%s_%s_%s' % (self.table_name, dates[0], dates[1])
                print 'Creating ' + part_table
                self.curs.execute(create_part_sql % 
                        (part_table, constraints, self.ts_column, 
                         dates[0], self.ts_column, dates[1], self.table_name))
                         
                self.curs.execute(idxs_sql % ((dates[0], dates[1])*idx_count*2))
            except psycopg2.ProgrammingError, e:
                print e,
                if not self.opts.ignore_errors:
                    print 'Last query: %s' % self.curs.query
                    sys.exit(1)
                print 'Ignoring error.'
                self.curs.execute('ROLLBACK TO SAVEPOINT save;')
                
            dates = (dates[1], self.nextInterval(self.opts.units, dates[1])[0])
    
    def set_trigger_func(self):
        '''
        Uses the template date_part_trig.tpl.sql to build out a trigger function
        for the parent table if it's not already there
        '''

        funcs_sql = open('date_part_trig.tpl.sql').read()
        table_atts = table_attributes(self.curs, self.table_name)
        d = {'table_name': self.table_name,
             'ts_column': self.ts_column,
             'table_atts': ','.join(table_atts),
             'atts_vals': " || ',' || ".join(["quote_nullable(rec.%s)" % att for att in table_atts]),
             # 'nulls': '('+','.join(['null']*len(table_atts))+')'
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
        print 'Moved %d rows into partitions.' % moved
        
        self.curs.execute('TRUNCATE %s;' % self.table_name)
        
        if keep:
            keep = (','.join([','.join(res) for res in keep])).replace('"', "'")
            self.curs.execute('INSERT INTO %s VALUES %s;' % (self.table_name, keep))
            print 'Kept %d rows in the parent table.' % kept
        
    def work(self):
        super(DatePartitioner, self).work()
        try:
            if self.opts.create:
                self.set_trigger_func()
                # build the dates ranges lists
                args = [self.curs, self.opts.start_ts, self.opts.end_ts]
                if self.opts.units:
                    args.append(self.opts.units)
                    dates = list(get_dates_by_unit(*args))
                else:
                    args.append(self.opts.count)
                    dates = list(get_dates_by_count(*args))
        
                # build the partitions
                self.create_partitions(dates)
                
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