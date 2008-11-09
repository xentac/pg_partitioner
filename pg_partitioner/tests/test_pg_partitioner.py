
from pydbtest import dbtestcase
import sys, os, subprocess
from copy import copy

def setUpModule():
    os.chdir(os.path.dirname(__file__))
    dbtestcase.setUpSampleDB('dateparttest')

def tearDownModule():
    dbtestcase.tearDownSampleDB()

script = "../pg_partitioner.py -d dateparttest"

class TestDatePartitioner(dbtestcase.DBTestCase):
    def setUp(self):
        parent_sql = \
        '''
        DROP TABLE IF EXISTS foo CASCADE;

        CREATE TABLE foo (
            id serial PRIMARY KEY,
            val integer,
            val_ts timestamp without time zone NOT NULL,
            
            CONSTRAINT film_id_fkey FOREIGN KEY (id) REFERENCES film (film_id)
        );

        CREATE INDEX foo_val_idx ON foo (val);
        CREATE INDEX foo_val_ts_idx ON foo (val_ts);

        INSERT INTO foo (val, val_ts) VALUES (3, '20070703'), (5, '20080101'), (6, '20071115'), (10, '20080401'), (15, '20080504'), (5, '2008-02-02'), (23, '20090101');
        '''
        default_schema_sql = 'SELECT current_schema();'
        pg_version_sql = 'SELECT version();'
        
        self.part_fmt = 'foo_%s'
        self.transactional = False
        
        self.connect('dbname=dateparttest')
        self.exec_query(parent_sql)
        
        self.exec_query(default_schema_sql)
        self.default_schema = self.cursor().fetchone()[0]
        
        self.exec_query(pg_version_sql)
        self.pg_version =  self.cursor().fetchone()[0].split()[1]
        self._commit()
    
    def tearDown(self):
        sql = "DROP TABLE IF EXISTS foo CASCADE;"
        self.exec_query(sql)
        super(TestDatePartitioner, self).tearDown()
    
    def callproc(self, cmd):
        p = subprocess.Popen(cmd.split(), stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        sts = os.waitpid(p.pid, 0)[1]
        
        return sts, p
    
    def nextInterval(self, interval, ts):
        sql = "SELECT to_char(%s::timestamp + %s, 'YYYYMMDD')"
        self.exec_query(sql, (ts, interval))
        return self.cursor().fetchone()[0]
    
    def runTableValidations(self, cmd, start_date, end_date, date_units):
        sts, p = self.callproc(cmd)
        output = p.stdout.read()
        
        date = start_date
        while date <= end_date:
            part = self.default_schema+'.'+(self.part_fmt % date)
            self.assertTableExists(part)
            self.assertNotEqual(output.find('Creating '+part), -1)
            date = self.nextInterval(date_units, date)
        
        part = self.part_fmt % self.nextInterval('-'+date_units, start_date)
        self.assertTableNotExists(part)
        self.assertEqual(output.find(part), -1)
        
        part = self.part_fmt % (self.nextInterval(date_units, date))
        self.assertTableNotExists(part)
        self.assertEqual(output.find(part), -1)
        
        # return the output incase we need to check more
        return output
    
    def testCreatesMonthRangesOnExistingData(self):
        cmd = script+" -u month foo val_ts"
        self.runTableValidations(cmd, '20070701', '20090101', '1 month')

    
    def testCreates3MonthRangesOnExistingData(self):
        cmd = script+" -u month --scale 3 --stage create foo val_ts"
        self.runTableValidations(cmd, '20070701', '20090101', '3 month')
    
    def testCreatesRangesWithSetEndTs(self):
        cmd = script+" -u month -e 20080513 foo val_ts"
        self.runTableValidations(cmd, '20070701', '20080501', '1 month')
    
    def testCreatesRangesWithSetStartTs(self):
        cmd = script+" -u month -s 20080101 foo val_ts"
        self.runTableValidations(cmd, '20080101', '20090101', '1 month')
    
    def testParentGetsInsertTrigger(self):
        cmd = script+" -u month --stage all foo val_ts"
        self.callproc(cmd)
        
        self.assertTableHasTrigger('foo', 'foo_partition_trigger', before=True, 
                                        events='insert', row=True)
    
    def testParentInsertFuncCreated(self):        
        cmd = script+" -u month --stage all foo val_ts"
        self.callproc(cmd)
        
        typname = self.pg_version.startswith('8.3') and 'trigger' or '"trigger"'
        self.assertFunctionExists('foo_ins_trig', rettype=typname)
        
    def testParitionSchemaMatchesParentNoFKeys(self):
        cmd = script+" -u month -s 20080101 -e 20080201 --stage all foo val_ts"
        self.runTableValidations(cmd, '20080101', '20080201', '1 month')
        
        for tbl in ['foo_20080101', 'foo_20080201']:
            self.assertTableExists(tbl)
            self.assertTableHasColumn(tbl, 'id', 'integer')
            self.assertTableHasColumn(tbl, 'val_ts', 'timestamp without time zone')
            self.assertTableHasColumn(tbl, 'val', 'integer')
            self.assertTableHasCheckConstraint(tbl, tbl+'_val_ts_check')
            self.assertTableHasIndex(tbl, tbl+'_val_idx', columns='val')
            self.assertTableHasIndex(tbl, tbl+'_val_ts_idx', columns='val_ts')
            self.assertTableHasPrimaryKey(tbl, 'id')
    
    def testPartitionSchemaMatchesParentWithFkeys(self):
        cmd = script+" -u month -s 20080101 -e 20080201 -f --stage all foo val_ts"
        self.runTableValidations(cmd, '20080101', '20080201', '1 month')
        
        for tbl in ['foo_20080101', 'foo_20080201']:
            self.assertTableExists(tbl)
            self.assertTableHasColumn(tbl, 'id', 'integer')
            self.assertTableHasColumn(tbl, 'val_ts', 'timestamp without time zone')
            self.assertTableHasColumn(tbl, 'val', 'integer')
            self.assertTableHasCheckConstraint(tbl, tbl+'_val_ts_check')
            self.assertTableHasIndex(tbl, tbl+'_val_idx', columns='val')
            self.assertTableHasIndex(tbl, tbl+'_val_ts_idx', columns='val_ts')
            self.assertTableHasPrimaryKey(tbl, 'id')
            self.assertTableHasFKey(tbl, 'film', tbl+'_id_fkey', 'id')
    
    def testFullRangeMigratesAllData(self):
        cmd = script+" -u month foo val_ts"
        self.callproc(cmd)
        
        cmd = script+" -u month --stage migrate foo val_ts"        
        sts, p = self.callproc(cmd)
        
        sql = "SELECT COUNT(*) FROM foo;"
        self.exec_query(sql)
        self.assertEqual(self.cursor().fetchone()[0], 7)
        
        sql = "SELECT COUNT(*) FROM ONLY foo;"
        self.exec_query(sql)
        self.assertEqual(self.cursor().fetchone()[0], 0)
        
        output = p.stdout.read()
        self.assertNotEqual(output.find('Moved 7 rows into partitions.'), -1)
    
    def testLimitedRangeKeepsDataInParent(self):
        cmd = script+" -u month -s 20080201 -e 20080501 foo val_ts"
        self.callproc(cmd)
        
        cmd = script+" -u month -s 20080201 -e 20080501 --stage migrate foo val_ts"
        sts, p = self.callproc(cmd)
        
        sql = "SELECT COUNT(*) FROM foo;"
        self.exec_query(sql)
        self.assertEqual(self.cursor().fetchone()[0], 7)
        
        sql = "SELECT COUNT(*) FROM ONLY foo;"
        self.exec_query(sql)
        self.assertEqual(self.cursor().fetchone()[0], 3)
        
        output = p.stdout.read()
        self.assertNotEqual(output.find('Moved 4 rows into partitions.'), -1)
        # self.assertNotEqual(output.find('Kept 3 rows in the parent table.'), -1)
    
    def testRunWithTestFlagDoesntCommit(self):
        cmd = script+" -u month -t foo val_ts"
        sts, p = self.callproc(cmd)
        
        output = p.stdout.read()
        date = '20070701'
        while date <= '20090101':
            part = self.default_schema+'.'+(self.part_fmt % date)
            self.assertTableNotExists(part)
            self.assertNotEqual(output.find('Creating '+part), -1) # still prints creation notices
            date = self.nextInterval('1 month', date)
        
        self.assertNotEqual(output.find('Test Run:'), -1)
        self.assertNotEqual(output.find('Rolling back test run.'), -1)