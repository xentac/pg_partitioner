
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
        
        self.part_fmt = 'foo_%s_%s'
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
        
        dates = start_date, self.nextInterval(date_units, start_date)
        while dates[0] <= end_date:
            part = self.default_schema+'.'+(self.part_fmt % dates)
            self.assertTableExists(part)
            self.assertNotEqual(output.find('Creating '+part), -1)
            dates = dates[1], self.nextInterval(date_units, dates[1])
        
        part = self.part_fmt % (self.nextInterval('-'+date_units, start_date), start_date)
        self.assertTableNotExists(part)
        self.assertEqual(output.find(part), -1)
        
        part = self.part_fmt % (dates[1], self.nextInterval(date_units, dates[1]))
        self.assertTableNotExists(part)
        self.assertEqual(output.find(part), -1)
        
        # return the output incase we need to check more
        return output
    
    def testCreatesMonthRangesOnExistingData(self):
        cmd = script+" -u month foo val_ts"
        self.runTableValidations(cmd, '20070701', '20090101', '1 month')

    
    def testCreates3MonthRangesOnExistingData(self):
        cmd = script+" -u month --scale 3 foo val_ts"
        self.runTableValidations(cmd, '20070701', '20090101', '3 month')
    
    def testCreatesRangesWithSetEndTs(self):
        cmd = script+" -u month -e 20080513 foo val_ts"
        self.runTableValidations(cmd, '20070701', '20080501', '1 month')
    
    def testCreatesRangesWithSetStartTs(self):
        cmd = script+" -u month -s 20080101 foo val_ts"
        self.runTableValidations(cmd, '20080101', '20090101', '1 month')
    
    def testParentGetsInsertTrigger(self):
        cmd = script+" -u month foo val_ts"
        self.callproc(cmd)
        
        self.assertTableHasTrigger('foo', 'foo_partition_trigger', before=True, 
                                        events='insert', row=True)
    
    def testParentInsertFuncCreated(self):
        cmd = script+" -u month foo val_ts"
        self.callproc(cmd)
        typname = 'trigger' if self.pg_version.startswith('8.3') else '"trigger"'
        self.assertFunctionExists('foo_ins_trig', rettype=typname)
        
    def testParitionSchemaMatchesParentNoFKeys(self):
        cmd = script+" -u month -s 20080101 -e 20080201 foo val_ts"
        self.runTableValidations(cmd, '20080101', '20080201', '1 month')
        
        for tbl in ['foo_20080101_20080201', 'foo_20080201_20080301']:
            self.assertTableExists(tbl)
            self.assertTableHasColumn(tbl, 'id', 'integer')
            self.assertTableHasColumn(tbl, 'val_ts', 'timestamp without time zone')
            self.assertTableHasColumn(tbl, 'val', 'integer')
            self.assertTableHasCheckConstraint(tbl, tbl+'_val_ts_check')
            self.assertTableHasIndex(tbl, tbl+'_val_idx', columns='val')
            self.assertTableHasIndex(tbl, tbl+'_val_ts_idx', columns='val_ts')
            self.assertTableHasPrimaryKey(tbl, 'id')
    
    def testPartitionSchemaMatchesParentWithFkeys(self):
        cmd = script+" -u month -s 20080101 -e 20080201 -f foo val_ts"
        self.runTableValidations(cmd, '20080101', '20080201', '1 month')
        
        for tbl in ['foo_20080101_20080201', 'foo_20080201_20080301']:
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
        cmd = script+" -u month -m foo val_ts"
        print cmd
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
        cmd = script+" -u month -s 20080101 -e 20080501 -m foo val_ts"
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
    
    def testExistingTableCreatePassesWithIgnoreFlag(self):
        cmd = script+" -u month -s 20080101 -e 20080201 foo val_ts"
        self.callproc(cmd)
        
        cmd = script+" -u month -s 20080115 -e 20080513 foo val_ts -i"
        
        output = self.runTableValidations(cmd, '20080101', '20080513', '1 month')
        self.assertEqual(output.count('relation "foo_20080101_20080201" already exists'), 1)
        self.assertEqual(output.count('relation "foo_20080201_20080301" already exists'), 1)
        self.assertEqual(output.count('Ignoring error.'), 2)
    
    def testExistingTableCreateFailsWithNoIgnoreFlag(self):
        cmd = script+" -u month -s 20080101 -e 20080201 foo val_ts"
        self.callproc(cmd)
        
        cmd = script+" -u month -s 20080115 -e 20080513 foo val_ts"
        sts, p = self.callproc(cmd)
        
        output = p.stdout.read()
        self.assertEqual(output.count('relation "foo_20080101_20080201" already exists'), 1)
        self.failUnlessRaises(self.failureException, self.runTableValidations, cmd, '20080101', '20080501', '1 month')
    
    def testRunWithTestFlagDoesntCommit(self):
        cmd = script+" -u month -t foo val_ts"
        sts, p = self.callproc(cmd)
        
        output = p.stdout.read()
        dates = '20070701', self.nextInterval('1 month', '20070701')
        while dates[0] <= '20090101':
            part = self.default_schema+'.'+(self.part_fmt % dates)
            self.assertTableNotExists(part)
            self.assertNotEqual(output.find('Creating '+part), -1) # still prints creation notices
            dates = dates[1], self.nextInterval('1 month', dates[1])
        
        self.assertNotEqual(output.find('Test Run:'), -1)
        self.assertNotEqual(output.find('Rolling back test run.'), -1)