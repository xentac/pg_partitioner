#!/usr/bin/env python

import sys, os, re
import psycopg2
from optparse import OptionGroup
from db_script import DBScript
from util_funcs import *

def_table_schema = \
'''
SELECT n.nspname
FROM pg_namespace n, pg_class t
WHERE t.relnamespace=n.oid
    AND t.relname=%s AND pg_table_is_visible(t.oid)
'''

stages = {'create': 1,
          'migrate': 2,
          'post': 4,
          'all': 15}

class DatePartitioner(DBScript):
    def __init__(self, args):
        super(DatePartitioner, self).__init__('DatePartitioner', args)
        
    def init_optparse(self):
        usage = "%prog [options] TABLE PARTITION_FIELD"
        parser = super(DatePartitioner, self).init_optparse(usage)
        
        g = OptionGroup(parser, "Partitioning options", 
                        "Ways to customize the number of partitions and/or the range of each.  Nothing is created if none of these is used.  This is useful for making table paritioning a two step process: 1. Create the partitions using the below options.  2. Migrate the data from the parent table into the new partitions using the above -m option.")
        g.add_option('--stage', default='create',
                     help="One of: create, migrate, post, all.  create -> create partition tables, migrate -> migrate data from parent to partitions, post -> create indexes, constraints and, optionally, fkeys on partitions.")
        g.add_option('--schema', action='store_true', default=False,
                     help="Forces the partitioner schema to be loaded.  Can be run as the only non-connection option with no arguments.")
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
        g.add_option('-m', '--migrate', action="callback", default=1000, callback=self.migrate_opt_callback, dest="migrate",
                     help="Valid for the migrate stage.  Sets X where X is the # of rows to successively move from the parent to partition tables until all rows (that can be) have been moved, defaults to 1000.  Any rows for which no valid child table exists are left in the parent.")
        g.add_option('-f', '--fkeys', action="store_true", default=False,
                    help="Include building any fkeys present on the parent on the partitions.")
                     
        parser.add_option_group(g)
        
        return parser
    
    def migrate_opt_callback(self, option, opt_str, value, parser):
        assert value is None
        try:
            value = int(parser.rargs[0])
            del parser.rargs[0]
        except (ValueError, IndexError):
            value = 1000
            
        setattr(parser.values, option.dest, value)
        
    
    def validate_opts(self):
        if self.opts.schema and len(self.args) == 0:
            return
            
        if len(self.args) < 2:
            self.parser.error("date_partitioner.py requires both a table name and timestamp field name on that table as arguments.")
            
        if not table_exists(self.curs, self.args[0]):
            self.parser.error("%s does not exist in the given database." % self.args[0])
        
        if self.opts.stage not in stages.keys():
            print "Invalid stage: %s.  Valid options are: ", (self.opts.stage, ','.join(stages.values()))
            sys.exit()
        
        self.col_type = get_column_type(self.curs, self.args[0], self.args[1])
        if not self.col_type:
            self.parser.error("%s does not exist on %s." % (self.args[1], self.args[0]))
        self.set_range_vars()

    def run_stage(self, stage):
        return True if stages[self.opts.stage] & stages[stage] else False
    
    def table_is_partitioned(self):
        self.curs.execute('SELECT partitioner.get_table_partitions(%s)', (self.qualified_table_name,))
        if self.curs.rowcount:
            return True
        return False
    
    def table_has_partition_trig(self):
        trig_check_sql = \
        '''
        SELECT 1
        FROM pg_trigger t
        WHERE t.tgrelid='%s'::regclass
            AND t.tgname='%s_partition_trigger';
        '''
        
        self.curs.execute(trig_check_sql % (self.qualified_table_name, self.table_name))
        if self.curs.rowcount:
            return True
        return False
        
    
    def set_range_vars(self):
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
        
        if self.col_type == 'date' or re.search('time[^\]]*$', self.col_type):
            self.short_type = 'ts'
            units = self.opts.units or 'month'
            self.curs.execute(def_dates_sql % (units, self.args[1], units, self.args[1], self.args[0]))
        elif re.search('int[^\]]*$', self.col_type):
            self.short_type = 'int'
            try:
                units = int(self.opts.units)
            except TypeError:
                units = 1
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
        create_part_sql = \
        '''
        CREATE TABLE %s (
            %s
            CHECK (%s >= '%s' AND %s < '%s')
        ) INHERITS (%s);
        '''
        
        constraints_str = self.get_constraintdefs_str()
        
        if self.opts.fkeys:
            constraints_str = constraints_str + self.get_fkeydefs_str()
        
        built_partitions = []
        end_points = (self.opts.start, self.nextInterval(self.opts.start))
        while True:
            if int(end_points[0]) > int(self.opts.end):
                break
            try:
                if self.opts.ignore_errors:
                    self.curs.execute('SAVEPOINT create_table_save;')
                    
                part_table = '%s_%s_%s' % (self.qualified_table_name, end_points[0], end_points[1])
                print 'Creating ' + part_table
                self.curs.execute(create_part_sql % 
                        (part_table, constraints_str, self.part_column, 
                         end_points[0], self.part_column, end_points[1], self.qualified_table_name))
                
                built_partitions.append(part_table)
            except psycopg2.ProgrammingError, e:
                print e,
                if not self.opts.ignore_errors:
                    print 'Last query: %s' % self.curs.query
                    sys.exit(1)
                print 'Ignoring error.'
                self.curs.execute('ROLLBACK TO SAVEPOINT create_table_save;')
            
            end_points = (end_points[1], self.nextInterval(end_points[1]))
        
        return built_partitions
    
    def build_indexes(self):
        self.curs.execute('SELECT partitioner.get_table_partitions(%s);', (self.qualified_table_name,))
        if not self.curs.rowcount:
            print '%s has not had any partitions created for it.' % self.qualified_table_name
            sys.exit()
        partitions = [row[0] for row in self.curs.fetchall()]
        idxs_str, idx_count = self.get_indexdefs_str()
        
        if idxs_str:
            end_points_re = re.compile(r'%s_(\d*)_(\d*)' % self.table_name)
            for part in partitions:
                self.curs.execute('SAVEPOINT idx_create_save;')
                m = end_points_re.search(part)
                end_points = m.groups(1)
                try:
                    self.curs.execute(idxs_str % ((end_points[0], end_points[1])*idx_count*2))
                except psycopg2.ProgrammingError, e:
                    if e.message.strip().endswith('already exists'):
                        self.curs.execute('ROLLBACK TO SAVEPOINT idx_create_save')
    
    def set_trigger_func(self):
        '''
        Uses the template date_part_trig.tpl.sql to build out a trigger function
        for the parent table if it's not already there
        '''
        if not self.table_is_partitioned():
            print '%s has not had any partitions created for it!' % self.qualified_table_name
            sys.exit()
        
        if self.table_has_partition_trig():
            return
            
        create_trig_sql = \
        '''
        CREATE TRIGGER %(base_table_name)s_partition_trigger BEFORE INSERT OR UPDATE
            ON %(table_name)s FOR EACH ROW
            EXECUTE PROCEDURE %(table_name)s_ins_trig();
        '''

        tpl_path = os.path.dirname(os.path.realpath(__file__))
        funcs_sql = open(tpl_path+'/range_part_trig.tpl.sql').read()
        table_atts = table_attributes(self.curs, self.qualified_table_name)
        d = {'table_name': self.qualified_table_name,
             'base_table_name': self.table_name,
             'part_column': self.part_column,
             'table_atts': ','.join(table_atts),
             'atts_vals': " || ',' || ".join(["quote_nullable(rec.%s)" % att for att in table_atts]),
             'col_type': self.col_type
        }
        self.curs.execute(funcs_sql % d)
        
        self.curs.execute(create_trig_sql % d)
    
    def move_data_down(self):
        '''
        In the loop SELECT %s_ins_func(); will push any data down it can and return anything it can't.
        We then DELETE everything touched and, after the loop, re-insert data that couldn't be moved.
        '''
        move_down_sql = \
        '''
        SELECT partitioner.move_partition_data('%(table_name)s', '%(part_column)s', %(limit)s);
        '''
        
        d = {'table_name': self.qualified_table_name,
             'base_table_name': self.table_name,
             'part_column': self.part_column,
             'limit': self.opts.migrate
            }
        
        # if the partition column isn't indexed prompt before continuing...
        self.curs.execute("SELECT partitioner.column_is_indexed('%(part_column)s', '%(table_name)s')" % d)
        if not self.curs.fetchone()[0]:
            while True:
                proceed = raw_input('%(base_table_name)s.%(part_column)s is not indexed, this can seriously slow down data migration, proceed? (y/n):  ' % d)
                if proceed not in ['y', 'n', 'Y', 'N', 'yes', 'no', 'Yes', 'No']:
                    print 'Invalid input: ' + proceed
                    continue
                if proceed in ['n', 'N', 'no', 'No']:
                    sys.exit()
                break
        
        self.set_trigger_func()
            
        self.curs.execute(move_down_sql % d)
        moved = self.curs.fetchone()[0]
        print 'Moved %d rows into partitions.' % moved
        
        # self.curs.execute('TRUNCATE %s;' % self.qualified_table_name)
    
    def load_partitioner_schema(self):
        schema_check_sql = "SELECT 1 FROM pg_namespace WHERE nspname='partitioner';"
        self.curs.execute(schema_check_sql)
        if not self.curs.rowcount or self.opts.schema:
            tpl_path = os.path.dirname(os.path.realpath(__file__))
            funcs_sql = open(tpl_path+'/pg_partitioner.sql').read()
            self.curs.execute(funcs_sql)
            if self.opts.schema and len(self.args) == 0:
                self.finish()
                sys.exit()
        
    def work(self):
        super(DatePartitioner, self).work()
        
        self.load_partitioner_schema()
        
        if self.args[0].find('.') > -1:
            self.qualified_table_name = self.args[0]
            self.table_name = self.args[0].split('.')[1]
        else:
            self.curs.execute(def_table_schema, (self.args[0],))
            self.qualified_table_name = self.curs.fetchone()[0]+'.'+self.args[0]
            self.table_name = self.args[0]
        self.part_column = self.args[1]
            
        try:
            if self.run_stage('create'):
                # build the partitions
                self.create_partitions()
                
            if self.run_stage('migrate'):
                self.move_data_down()
            
            if self.run_stage('post'):
                self.build_indexes()
            
            self.finish()
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