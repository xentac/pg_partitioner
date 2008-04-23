
import dbtestcase
import subprocess

class TestDatePartitioner(dbtestcase.DBTestCase):
    def setUp(self):
        sql = \
        '''
        DROP TABLE IF EXISTS foo CASCADE;

        CREATE TABLE foo (
            id serial PRIMARY KEY,
            val integer,
            val_ts timestamp without time zone NOT NULL
        );

        CREATE INDEX foo_val_idx ON foo (val);
        CREATE INDEX foo_val_ts_idx ON foo (val_ts);

        INSERT INTO foo (val, val_ts) VALUES (3, '20070703'::timestamp), (5, '20080101'), (6, '2007-11-15'), (10, now()), (15, '20080504'), (5, '2008-02-02'), (23, '20090101');
        '''
        self.part_fmt = 'foo_%s_%s'
        self.transactional = False
        self.connect('dbname=pagila')
        self.exec_query(sql)
        self._commit()
    
    def tearDown(self):
        sql = "DROP TABLE IF EXISTS foo CASCADE;"
        self.exec_query(sql)
        super(TestDatePartitioner, self).tearDown()
    
    def nextInterval(self, interval, *args):
        fmt = "to_char('%s'::timestamp + '%s', 'YYYYMMDD')"
        sql = 'SELECT '+','.join([fmt % (arg, interval) for arg in args])
        self.exec_query(sql)
        return self.cursor().fetchone()
    
    def testCreatesMonthRangesOnExistingData(self):
        cmd = ['./date_partitioner.py', '-u', 'month', 'foo', 'val_ts']
        subprocess.call(cmd, stdout=subprocess.PIPE)
        
        dates = ('20070701', '20070801')
        while dates[0] <= '20090101':
            self.assertTableExists(self.part_fmt % dates)
            dates = (dates[1], self.nextInterval('1 month', dates[1])[0])
        self.assertTableNotExists('foo_20070601_20070701')
        self.assertTableNotExists('foo_20090201_20090301')
    
    def testCreates3MonthRangesOnExistingData(self):
        cmd = ['./date_partitioner.py', '-u', 'month', '-c', '3', 'foo', 'val_ts']
        subprocess.call(cmd, stdout=subprocess.PIPE)
        
        tbl_fmt = 'foo_%s_%s'
        dates = ('20070701', '20071001')
        while dates[0] < '20090101':
            self.assertTableExists(self.part_fmt % dates)
            dates = (dates[1], self.nextInterval('3 months', dates[1])[0])
        self.assertTableNotExists('foo_20070401_20070701')
        self.assertTableNotExists('foo_20090401_20090701')
    
    def testCreatesRangesWithSetEnds(self):
        cmd = ['./date_partitioner.py', '-u', 'month', '-s', '20080101', 
                '-e', '20080501', 'foo', 'val_ts']
        subprocess.call(cmd, stdout=subprocess.PIPE)
        
        dates = ('20080101', '20080201')
        while dates[0] <= '20080501':
            self.assertTableExists(self.part_fmt % dates)
            dates = (dates[1], self.nextInterval('1 month', dates[1])[0])
        self.assertTableNotExists('foo_20071201_20080101')
        self.assertTableNotExists('foo_20080601_20080701')
        
        
        
        