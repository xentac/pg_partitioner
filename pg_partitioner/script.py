
# A much simplified version of the Skype DBScript.
# This doesn't support daemonization and doesn't u se configuration files.
# Connection paramaters can be passed on the command line  with
# the defaults coming from the environment as with psql

import sys, os
import getpass
import psycopg2
from psycopg2.extras import DictConnection
from optparse import OptionParser
from cmd import Cmd

def default_db_user():
    return os.environ.get('PGUSER', os.environ['USER'])

def default_db_dbname():
    return os.environ.get('PGDATABASE', os.environ['USER'])

def default_db_port():
    return int(os.environ.get('PGPORT', 5432))

def default_db_host():
    return os.environ.get('PGHOSTADDR', os.environ.get('PGHOST', ''))

def default_db_str():
    return "dbname=%s user=%s host=%s port=%s" % (default_db_dbname(), default_db_user(), 
                                                    default_db_host(), default_db_port())

class Script(object):
    def __init__(self, name, args):
        self.parser = self.init_optparse()
        self.opts, self.args = self.parser.parse_args(args)
        
    def init_optparse(self, usage="%prog [options]"):
        parser = OptionParser(usage=usage)
        return parser
    
    def validate_opts(self):
        pass
        
class DBScript(Script):
    def __init__(self, name, args):
        # self.parser = self.init_optparse()
        #         self.opts, self.args = self.parser.parse_args(args)
        super(DBScript, self).__init__('DatePartitioner', args)
        
        self.con = self.get_connection()
        self.curs = self.con.cursor()
        
        self.validate_opts()
        
    def init_optparse(self, usage):
        parser = super(DBScript, self).init_optparse(usage)

        default_conn_str = default_db_str()
        h = parser.get_option('-h')
        parser.remove_option('-h')
        h._short_opts = []
        # h.short_opts = None
        parser.add_option(h)
        parser.add_option('-d', '--database', default=default_db_dbname(), metavar='DBNAME',
                          help='specify database name to connect to (default: "%s")' % default_db_dbname())
        parser.add_option('-h', '--host', default=default_db_host(), metavar='HOSTNAME',
                          help='database server host or socket directory (default: "local socket")')
        parser.add_option('-p', '--port', default=default_db_port(), metavar='PORT',
                          help='database server port (default: "%s")' % default_db_port())
        parser.add_option('-U', '--user', default=default_db_user(), metavar='NAME',
                          help='database user name (default: "%s")' % default_db_user())
        # parser.add_option('--db', default=default_conn_str, metavar='DB_CONN_STRING',
                          # help="A valid psycopg2 db conneciton string.  Any options not supplied will come from: '%s'.  You *can* supply a password here, but it is recommended that you don't for security reasons (you'll be prompted instead).  You can also set the standard libpq environment variables or use a .pgpass file to obviate the need for this option entirely." % default_conn_str)
        parser.add_option('-t', '--test', action='store_true', default=False,
                          help="Test run, nothing gets commited.  Useful to check output to see if everything looks sane. Default: False")
        
        return parser
    
    def validate_opts(self):
        pass
    
    def find_db_pass(self, conn_params):
        password = os.environ.get('PGPASSWORD', '')
        if password:
            return password
            
        passfile = os.environ.get('PGPASS', os.environ['HOME']+'/.pgpass')
        pgpass = ':'.join([conn_params['host'], str(conn_params['port']), conn_params['dbname'], conn_params['user']])
        if os.path.isfile(passfile):
            for line in open(passfile):
                if line.startswith(pgpass):
                    password = line.split(':')[-1]
                    break
        return password

    def get_connection(self):
        conn_str = "dbname=%(dbname)s user=%(user)s port=%(port)s"
        conn_params = {'dbname' : self.opts.database,
                       'host' : self.opts.host,
                       'port' : self.opts.port,
                       'user' : self.opts.user}
        conn_params['password'] = self.find_db_pass(conn_params)
        
        if conn_params['password']:
            conn_str += ' password=%(password)s'
        if conn_params['host']:
            conn_str += ' host=%(host)s'
            
        try:
            return DictConnection(conn_str % conn_params)
        except psycopg2.OperationalError, e:
            if str(e).strip().endswith('no password supplied'):
                conn_params['password'] = getpass.getpass('password: ')
                conn_str += ' password=%(password)s'
                print conn_str % conn_params
                return DictConnection(conn_str % conn_params)
            raise e
        except psycopg2.Error, e:
            raise e
    
    def  finish(self):
        if self.opts.test:
            print 'Rolling back test run.'
            self.con.rollback()
        else:
            print 'Commtting everything.'
            self.con.commit()
    
    def work(self):
        if self.opts.test:
            print 'Test Run:'