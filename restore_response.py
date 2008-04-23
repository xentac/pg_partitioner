#!/usr/bin/env python

import sys, subprocess, os
from re import sub
from optparse import OptionValueError
from db_script import DBScript
from util_funcs import *

suffixes = ['messages_history', 'messages_history_clicks', 
          'messages_history_opens', 'messages_history_forwards',
          'messages_history_unsubs']
          
dumpfile = 'emma2.20061217.pgsql'
tmpdumpfile = 'tmpdump.pgsql'

# we change 'tbl' each call
restore_to_file_cmd = ['pg_restore', '-t', 'tbl', '-f', tmpdumpfile, dumpfile]

# these two are used together
cat_dump_cmd = ['cat', tmpdumpfile]

# cleanup the tmpdumpfile
del_tmpdumpfile_cmd = ['rm', tmpdumpfile]

archived_mailing_ids_sql = \
'''
SELECT emma_mailing_id
FROM emma_mailings
WHERE emma_account_id=%s
    AND emma_mailing_data_archived IS TRUE
ORDER BY emma_mailing_id DESC;
'''

move_data_sql = \
'''
INSERT INTO %s (%s)
SELECT %s
FROM %s
WHERE emma_mailing_id=%s
    AND row(%s) NOT IN (SELECT %s
                        FROM %s
                        WHERE emma_mailing_id=%s);
'''

set_mailing_data_archvided_false_sql = \
'''
UPDATE emma_mailings
SET emma_mailing_data_archived='f'
WHERE emma_mailing_id=%s;
'''

class RestoreResponse(DBScript):
    def __init__(self, args):
        super(RestoreResponse, self).__init__('RestoreResponse', args)
        self.account_id = int(self.args[0])
    
    def init_optparse(self):
        usage = "%%prog [options] ACCOUNT_ID"
        parser = super(RestoreResponse, self).init_optparse(usage)
        
        parser.add_option('-b', '--backup', action='store_true', default=False,
                          help="Make a backup of any existing response tables in the account. While having this is great for recovering from potential problems, be aware that dumping even a few tables from the production db can take a while.  Default: False")
        
        return parser
    
    def tbl_name_to_tmp(self, suffix):
        tbl = 'userdata_%d_%s' % (self.account_id, suffix)
        return tbl.replace('userdata', 'tmp')
    
    def replace_tbl_names(self, tbl, tmp_tbl):
       contents = open(tmpdumpfile, 'r').read()
       contents = sub(tbl, tmp_tbl, contents)
       open(tmpdumpfile, 'w').write(contents)
    
    def extract_tbl_to_file(self, tbl):
        restore_to_file_cmd[2] = tbl
        res = subprocess.call(restore_to_file_cmd)
        if res or not os.path.isfile(tmpdumpfile):
            print 'Problem restoring %s to file.' % tbl
            sys.exit(res)
    
    def push_tmp_tbl_to_db(self):
        p1 = subprocess.Popen(cat_dump_cmd, stdout=subprocess.PIPE)
        psql_cmd = ['psql', self.conn_params['dbname']]
        p2 = subprocess.Popen(psql_cmd, stdin=p1.stdout)
        res = p2.wait()
    
    def del_tmpdumfile(self):
        subprocess.call(del_tmpfumpfile_cmd)
    
    def drop_tmp_tbls(self, tbls):
        for tbl in tbls:
            if not tbl.startswith('tmp'):
                print 'Whoa!!!  Trying to drop the wrong table: %s!' % tbl
                sys.exit(1)
            print 'Dropping %s.' % tbl
            self.curs.execute('DROP TABLE %s;' % tbl)
    
    def restore_data(self, tbls):
        self.curs.execute(archived_mailing_ids_sql, (self.account_id,))
        for res in self.curs.fetchall():
            mailing_id = res[0]
            print '\nRestoring data for emma_mailing_id %s' % mailing_id
            # we need it iterate over suffixes, instead of tbls directly,to preserve order
            for suffix in suffixes:
                tbl = 'userdata_%d_%s' % (self.account_id, suffix)
                if tbl not in tbls:
                    continue
                tmp_tbl = tbls[tbl]
                atts = ','.join(table_attributes(self.curs, tbl))
                # print 'Executing: \n%s' % (move_data_sql % (tbl, atts, atts, tmp_tbl, mailing_id, atts, atts, tbl, mailing_id))
                self.curs.execute(move_data_sql % (tbl, atts, atts, tmp_tbl, mailing_id, atts, atts, tbl, mailing_id))
                print 'Restored %d rows to %s for mailing id %s.' % (self.curs.rowcount, tbl, mailing_id)
            self.curs.execute(set_mailing_data_archvided_false_sql, (mailing_id,))
    
    def backup_existing_tbls(self, tbls):
        print 'Backing up existing tables (%s) to %d.sql' % ((', '.join(tbls)), self.account_id)
        t_str = '-t ' + ' -t '.join(tbls)
        dmp_cmd = ['pg_dump']
        dmp_cmd.extend(t_str.split())
        dmp_cmd.extend(['-f', '%d_response.sql' % self.account_id, self.conn_params['dbname']])
        subprocess.call(dmp_cmd)
        
    def work(self):
        # sanity check first
        self.curs.execute(archived_mailing_ids_sql, (self.account_id,))
        if not self.curs.fetchone():
            print 'Account %d has not archived mailings.' % self.account_id
            sys.exit(0)
            
        os.chdir('/global/pgdata/pgdump')
        tbls = {}
        for suffix in suffixes:
            tbl = 'userdata_%d_%s' % (self.account_id, suffix)
            if not table_exists(self.curs, tbl):
                print '%s not found, skipping...' % tbl
                continue
            tbls[tbl] = self.tbl_name_to_tmp(suffix)
            print 'Restoring %s to %s' % (tbl, tbls[tbl])
            self.extract_tbl_to_file(tbl)
            self.replace_tbl_names(tbl, tbls[tbl])
            self.push_tmp_tbl_to_db()
        
        if self.opts.backup:
            self.backup_existing_tbls(tbls.keys())
            
        self.restore_data(tbls)
       
        self.commit()
            
        # the temp tables are created outside of this script's db transaction
        # so we need to handle them separately.
        self.drop_tmp_tbls(tbls.values())
        self.con.commit()
        
        subprocess.call(['rm', tmpdumpfile])

def main(argv=None):
    if argv is None:
        argv = sys.argv

    bc = RestoreResponse(argv[1:])
    bc.work()
if __name__ == '__main__':
    main()
