
# A much simplified version of the Skype DBScript.
# This doesn't support daemonization and doesn't u se configuration files.
# Connection paramaters can be passed on the command line  with
# the defaults coming from the environment as with psql

import sys, os
import psycopg2
from psycopg2.extras import DictConnection
from optparse import OptionParser

def default_db_user():
    return os.environ.get('PGUSER', os.environ['USER'])

def default_db_name():
    return os.environ.get('PGDATABASE', os.environ['USER'])

def default_db_port():
    return int(os.environ.get('PGPORT', 5432))

def default_db_host():
    return os.environ.get('PGHOSTADDR', os.environ.get('PGHOST', 'localhost'))

def default_db_str():
    return "dbname=%s user=%s host=%s port=%s" % (default_db_name(), default_db_user(), 
                                                    default_db_host(), default_db_port())
        
class DBScript(object):
    def __init__(self, name, args):
        self.parser = self.init_optparse()
        self.opts, self.args = self.parser.parse_args(args)
        self.con = self.get_connection()
        self.curs = self.con.cursor()
        
        self.validate_opts()
        
    def init_optparse(self, usage="%prog [options]"):
        parser = OptionParser(usage=usage)

        default_conn_str = default_db_str()
        parser.add_option('--db', default=default_conn_str, metavar='DB_CONN_STRING',
                          help="A valid psycopg2 db conneciton string.  Any options not supplied will come from: '%s'.  You *can* supply a password here, but it is recommended that you don't for security reasons (you'll be prompted instead).  You can also set the standard libpq environment variables or use a .pgpass file to obviate the need for this option entirely." % default_conn_str)
        parser.add_option('-t', '--test', action='store_true', default=False,
                          help="Test run, nothing gets commited.  Useful to check output to see if everything looks sane. Default: False")
        
        return parser
    
    def validate_opts(self):
        pass
    
    def find_db_pass(self, conn_params):
        password = ''
        passfile = os.environ.get('PGPASS', os.environ['HOME']+'/.pgpass')
        pgpass = ':'.join([conn_params['host'], str(conn_params['port']), conn_params['dbname'], conn_params['user']])
        if os.path.isfile(passfile):
            for line in open(passfile):
                if line.startswith(pgpass):
                    password = line.split(':')[-1]
                    break
        return password

    def get_connection(self):
        self.conn_params = dict([v.split('=') for v in self.opts.db.split()])
        for k in ['dbname', 'host', 'user', 'port']:
            if k not in self.conn_params:
                self.conn_params[k] = globals()['default_db_%s'%k]()
        
        # do we have a password?
        if 'password' not in self.conn_params:
            self.conn_params['password'] = self.find_db_pass(self.conn_params)
     
        con_str = "dbname=%(dbname)s user=%(user)s host=%(host)s port=%(port)s password=%(password)s"

        try:
            return DictConnection(con_str % self.conn_params)
        except psycopg2.OperationalError, e:
            if str(e).strip().endswith('no password supplied'):
                self.conn_params['password'] = getpass.getpass('password: ')
                return DictConnection(con_str % (opts.database, opts.username, opts.host, opts.port, password))
            raise e
        except psycopg2.Error, e:
            raise e
    
    def  commit(self):
        if self.opts.test:
            print 'Rolling back test run.'
            self.con.rollback()
        else:
            print 'Commtting everything.'
            self.con.commit()
    
    def work(self):
        if self.opts.test:
            print 'Test Run:'