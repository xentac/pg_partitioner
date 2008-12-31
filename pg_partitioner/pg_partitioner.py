#!/usr/bin/env python

import sys, os, re
import psycopg2
import cmd
from optparse import OptionGroup
from script import DBScript
from sql_util import *

try:
    import readline
except:
    pass

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
        super(DatePartitioner, self).__init__(args)
        
    def init_optparse(self):
        usage = "%prog [options] TABLE PARTITION_FIELD"
        parser = super(DatePartitioner, self).init_optparse(usage)
        
        g = OptionGroup(parser, "Partitioning options", 
                        "Ways to customize the number of partitions and/or the range of each.  This is useful for making table paritioning in a three step process: 1. Create the partitions using the below options.  2. Migrate the data from the parent table into the new partitions using the above -m option.  3. Create indexes and constraints on the newly created partition tables.")
        g.add_option('--stage', default='create',
                     help="One of: create, migrate, post, all.  create -> create partition tables, migrate -> migrate data from parent to partitions, post -> create indexes, constraints and, optionally, fkeys on partitions.")
        g.add_option('--schema', action='store_true', default=False,
                     help="Forces the partitioner schema to be loaded.  Can be run as the only non-connection option with no arguments.")
        g.add_option('-u', '--units', dest="units", metavar='UNIT',
                     help="A valid PG unit for the column partitioned on.  Defaults to month for timestamp/date columns and 10% of the available data range for integer column types")
        g.add_option('--scale', type='int', metavar='COUNT',
                     default=1,
                     help="A 'scale factor' for the units.  The resulting range of values per partition created is scale * units.  The default is 1.")
        g.add_option('-s', '--start', 
                     help="A valid date string for the start date for the partitions.  If used with --units this will default to the oldest DATE_FIELD value in the table truncated to --units, else will truncate to day.")
        g.add_option('-e', '--end', 
                     help="A valid date string for the end date of the partitions.  Using --unit will force this be rounded to the nearest --unit value based from --start after --end. Defaults to the current date.")
        # g.add_option('-i', '--ignore_errors', action="store_true", default=False,
        #              help="When creating tables, ignore any errors instead of rolling completely back. Default: False.")
        g.add_option('--chunk', type='int', default=1000, 
                     help="Valid for the migrate stage.  Sets X where X is the # of rows to successively move from the parent to partition tables until all rows (that can be) have been moved, defaults to 1000.  Any rows for which no valid child table exists are left in the parent.")
        g.add_option('-f', '--fkeys', action="store_true", default=False,
                    help="Include building any fkeys present on the parent on the partitions.")
                     
        parser.add_option_group(g)
        
        return parser    
    
    def validate_opts(self):
        self.load_partitioner_schema()
            
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
        return  stages[self.opts.stage] & stages[stage] and True or False
    
    def table_is_partitioned(self):
        self.curs.execute('SELECT pgpartitioner.get_partitions(%s)', (self.qualified_table_name,))
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
        SELECT to_char(date_trunc('%s', MIN(%s)), 'YYYYMMDD'), to_char(MAX(%s), 'YYYYMMDD')
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
            self.curs.execute(def_dates_sql % (units, self.args[1], self.args[1], self.args[0]))
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
        
    def build_tables(self):
        '''
        Create the child partitions, bails out if it encounters a partition
        that already exists
        '''
        create_part_sql = \
        '''
        CREATE TABLE %s (
            %s
        ) INHERITS (%s);
        INSERT INTO pgpartitioner.partitions
        (partition_oid, parent_oid, partition_type, vals)
        VALUES
        ('%s'::regclass, '%s'::regclass, 'range', ARRAY[%s])
        '''
        
        start, end = (self.opts.start, self.nextInterval(self.opts.start))
        # print start
        while True:
            if int(start) > int(self.opts.end):
                break
            
            # if str(end) < self.opts.end:
            # check_str = "CHECK (%s >= '%s' AND %s < '%s')" % (self.part_column, start, self.part_column, end)
            # else:
            #      check_str = "CHECK (%s >= '%s')" % (self.part_column, start)
                
            partition = '%s_%s' % (self.qualified_table_name, start)
            if partition in self.partitions:
                print '%s already exists....' % partition
                start, end = (end, self.nextInterval(end))
                continue
                
            try:
                self.curs.execute('SAVEPOINT create_table_save;')
                    
                check_str = "CHECK (%s >= '%s' AND %s < '%s')" % (self.part_column, start, self.part_column, end)
                vals_str = "'%s','%s'" % (start, end)
                print 'Creating %s...' % partition
                self.curs.execute(create_part_sql % 
                        (partition, check_str, self.qualified_table_name, partition, self.qualified_table_name, vals_str))
            except psycopg2.ProgrammingError, e:
                if e.message.strip().endswith('already exists'):
                    self.curs.execute('ROLLBACK TO SAVEPOINT create_table_save;')
            
            start, end = (end, self.nextInterval(end))
            self.partitions.append(partition)
            
        self.load_templated_funcs()

    def get_indexdefs_str(self):
        idxs_sql = ''
        idx_count = 0
        idx_re = re.compile(r'(create (?:unique )?index )(.*)', re.I)
        for idx in get_index_defs(self.curs, self.qualified_table_name):
            if 'unique' in idx.lower() or 'primary' in idx.lower():
                continue # we let constraint creation handle unique and primary keys
            idx_count += 1
            if idx.count(self.table_name) == 1: 
                idx = re.sub(idx_re, r'\1%s_%%s_\2' % self.table_name, idx)
            else:
                i = idx.find(self.table_name) + len(self.table_name)
                idx = idx[:i]+'_%s'+idx[i:]
            i = idx.rfind(self.table_name) + len(self.table_name)
            idx = idx[:i]+'_%s'+idx[i:]
            idxs_sql += idx+';'
        return idxs_sql, idx_count
    
    def build_indexes(self):
        if not self.partitions:
            print '%s has not had any partitions created for it!' % self.qualified_table_name
            sys.exit()
            
        idxs_str, idx_count = self.get_indexdefs_str()
        
        if idxs_str:
            partition_point_re = re.compile(r'%s_(\d*)' % self.table_name)
            for part in self.partitions:
                self.curs.execute('SAVEPOINT idx_create_save;')
                m = partition_point_re.search(part)
                partition_point = m.groups(1)[0]
                try:
                    self.curs.execute(idxs_str % ((partition_point,)*idx_count*2))
                except psycopg2.ProgrammingError, e:
                    if e.message.strip().endswith('already exists'):
                        self.curs.execute('ROLLBACK TO SAVEPOINT idx_create_save')

    def get_constraintdefs(self):
        constraints = []
        for constraint_def in get_constraint_defs(self.curs, self.qualified_table_name, self.opts.fkeys):
            constraints.append('ALTER TABLE %s_%%s ADD %s;' % (self.qualified_table_name, constraint_def))

        return constraints
    
    def build_constraints(self):
        if not self.partitions:
            print '%s has not had any partitions created for it!' % self.qualified_table_name
            sys.exit()
            
        constraints_defs = self.get_constraintdefs()
        
        if constraints_defs:
            partition_point_re = re.compile(r'%s_(\d*)' % self.table_name)
            for part in self.partitions:
                self.curs.execute('SAVEPOINT constraint_create_save;')
                m = partition_point_re.search(part)
                partition_point = m.groups(1)[0]
                for con in constraints_defs:
                    try:
                        self.curs.execute(con % (partition_point,))
                    except psycopg2.ProgrammingError, e:
                        m = e.message.strip()
                        if m.strip().endswith('already existms') or m.startswith('multiple primary keys'):
                            self.curs.execute('ROLLBACK TO SAVEPOINT constraint_create_save')
        
    def set_trigger_func(self):
        '''
        Uses the template date_part_trig.tpl.sql to build out a trigger function
        for the parent table if it's not already there
        '''
        if not self.partitions:
            print '%s has not had any partitions created for it!' % self.qualified_table_name
            sys.exit()
        
        if self.table_has_partition_trig():
            return
            
        part_trig_sql = \
        '''
        CREATE TRIGGER %(base_table_name)s_partition_trigger BEFORE INSERT OR UPDATE
            ON %(table_name)s FOR EACH ROW
            EXECUTE PROCEDURE %(table_name)s_ins_trig();
        '''
        
        d = {'table_name': self.qualified_table_name,
             'base_table_name': self.table_name,
        }
        self.curs.execute(part_trig_sql % d)
    
    def check_referencing_fkeys(self):
        refkeys_sql = '''
        SELECT DISTINCT ON (c.conname) n.nspname || '.' || t2.relname, c.conname, 
            pgpartitioner.get_attributes_str_by_attnums(t2.relname, c.conkey) as cols,
            pgpartitioner.get_attributes_str_by_attnums(t1.relname, c.confkey) as refcols
        FROM pg_class t1, pg_class t2, pg_constraint c, pg_namespace n
        WHERE c.confrelid=t1.oid AND t1.oid=%s::regclass AND c.conrelid=t2.oid
            AND t2.relnamespace=n.oid;
        '''
        fkey_trig_sql = \
        '''
        CREATE TRIGGER %(fkey_name)s_fkey_trigger BEFORE INSERT OR UPDATE
            ON %(table_name)s FOR EACH ROW
            EXECUTE PROCEDURE %(fkey_name)s_fkey_trig();
        '''
        fkey_tpl_sql = self.read_file('/fkey_trig.tpl.sql')
        self.curs.execute(refkeys_sql, (self.table_name,))
        for ret in self.curs.fetchall():
            print '\nFound fkey %s on %s(%s) referencing %s(%s)' % (ret[1], ret[0], ret[2], self.table_name, ret[3])
            while True:
                print 'Would you like to:'
                print '1. Drop it'
                print '2. Replace it with a trigger'
                print '3. Abort'
                choice = raw_input()
                if choice not in ['1', '2', '3']:
                    print 'Invalid choice'
                break
            if choice == '3':
                sys.exit()
            self.curs.execute('ALTER TABLE %s DROP CONSTRAINT %s;' % (ret[0], ret[1]))
            if choice == '2':
                d = {'table_name': ret[0],
                     'ref_table_name': self.qualified_table_name,
                     'fields': ret[2],
                     'new_vals': " || ' , ' || ".join(['NEW.'+field for field in ret[3].split(',')]),
                     'fkey_name': '%s_%s' % (ret[0].split('.')[1], ret[2].replace(',', '_'))
                    }
                    
                self.curs.execute(fkey_tpl_sql % d)
                self.curs.execute(fkey_trig_sql % d)
                
    
    def migrate_data(self):
        '''
        In the loop SELECT %s_ins_func(); will push any data down it can and return anything it can't.
        We then DELETE everything touched and, after the loop, re-insert data that couldn't be moved.
        '''
        move_down_sql = \
        '''
        SELECT pgpartitioner.partition_parent_data('%(table_name)s', '%(part_column)s', %(limit)s);
        '''
        
        d = {'table_name': self.qualified_table_name,
             'base_table_name': self.table_name,
             'part_column': self.part_column,
             'limit': self.opts.chunk
            }
        
        # if the partition column isn't indexed prompt before continuing...
        self.curs.execute("SELECT pgpartitioner.column_is_indexed('%(part_column)s', '%(table_name)s')" % d)
        if not self.curs.fetchone()[0]:
            while True:
                proceed = raw_input('\n%(base_table_name)s.%(part_column)s is not indexed, this can seriously slow down data migration, proceed? (y/n):  ' % d)
                if proceed not in ['y', 'n', 'Y', 'N', 'yes', 'no', 'Yes', 'No']:
                    print 'Invalid input: ' + proceed
                    continue
                if proceed in ['n', 'N', 'no', 'No']:
                    sys.exit()
                break
        
        self.check_referencing_fkeys()
            
        self.curs.execute(move_down_sql % d)
        moved = self.curs.fetchone()[0]
        print 'Moved %d rows into partitions.' % moved
            
    def read_file(self, tpl):
        tpl_path = os.path.dirname(os.path.realpath(__file__))
        return open(tpl_path+'/'+tpl).read()
    
    def load_partitioner_schema(self):
        schema_check_sql = "SELECT 1 FROM pg_namespace WHERE nspname='pgpartitioner';"
        self.curs.execute(schema_check_sql)
        if not self.curs.rowcount or self.opts.schema:
            print 'Loading pgparitioner schema in %s database...' % self.opts.database
            schema_sql = self.read_file('pg_partitioner.sql')

            self.curs.execute(schema_sql)
            if self.opts.schema and len(self.args) == 0:
                self.finish()
                sys.exit()
    
    def load_templated_funcs(self):
        funcs_tpl_sql = self.read_file('range_part_trig.tpl.sql')

        table_atts = table_attributes(self.curs, self.qualified_table_name)
        d = {'table_name': self.qualified_table_name,
             'base_table_name': self.table_name,
             'part_column': self.part_column,
             'table_atts': ','.join(table_atts),
             'atts_vals': " || ',' || ".join(["pgpartitioner.quote_nullable(rec.%s)" % att for att in table_atts]),
             'col_type': self.col_type
        }
        self.curs.execute(funcs_tpl_sql % d)
        
    def work(self):
        super(DatePartitioner, self).work()
        
        if self.args[0].find('.') > -1:
            self.qualified_table_name = self.args[0]
            self.table_name = self.args[0].split('.')[1]
        else:
            self.curs.execute(def_table_schema, (self.args[0],))
            self.qualified_table_name = self.curs.fetchone()[0]+'.'+self.args[0]
            self.table_name = self.args[0]
        self.part_column = self.args[1]
        
        self.curs.execute("SELECT pgpartitioner.get_partitions('%s')" % self.qualified_table_name)
        self.partitions = self.curs.rowcount and [res[0] for res in self.curs.fetchall()] or []
        try:
            if self.run_stage('create'):
                # build the partitions
                self.build_tables()
                
            if self.run_stage('migrate'):
                self.migrate_data()
            
            if self.run_stage('post'):
                self.set_trigger_func()
                self.build_indexes()
                self.build_constraints()
            
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
