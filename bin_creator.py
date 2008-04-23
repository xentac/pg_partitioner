#!/usr/bin/env python

import sys
from db_script import DBScript
from util_funcs import *

# we don't use INCLUDING CONSTRAINTS because we use pg_get_constraint def
# to get all of the table's constraints
create_bin_sql = \
'''
CREATE TABLE %s (
    LIKE %s INCLUDING DEFAULTS
    %s
);
'''

bins_sql = \
'''
SELECT substring(relname from E'\\\d+') as max_num
FROM pg_class
WHERE relname ~ E'^%s_\\\d+$'
ORDER BY relname DESC;
'''
                     
class BinCreator(DBScript):
    def __init__(self, args):
        # DBScript.__init__(self, 'BinCreator', args)
        super(BinCreator, self).__init__('BinCreator', args)
        
        self.tpl_tbl = self.args[0]
        self.count = self.opts.count
        self.name = self.opts.name
        
    def validate_opts(self):
        if not hasattr(self.opts, 'count') and not hasattr(self.opts, 'name'):
            self.parser.error("You must include one of --count or --name")

        if len(self.args) != 1:
            self.parser.error("Wrong number of arguments (tpl table argument required)")

        if not self.args[0] or not table_exists(self.curs, self.args[0]):
            self.parser.error("Table %s does not exist." % args[0])

    def check_opt_conflict(self, option, opt_str, value, parser):
        if (option.dest == 'count' and parser.values.name) or \
                (option.dest == 'name' and parser.values.count):
            parser.error("--count and --name can not be used together.")
        setattr(parser.values, option.dest, value)

    def init_optparse(self):
        usage = "%%prog [options] TPL_TABLE"
        parser = super(BinCreator, self).init_optparse(usage)

        parser.add_option("-c", "--count", type="int", action="callback", metavar='NUM', callback=self.check_opt_conflict,
                          help="The number of new bins of the given table type to create. Can not be used with -n.")
        parser.add_option("-n", "--name", type="string", action="callback", callback=self.check_opt_conflict,
                          help="A specific name for a single new bin of the given type. Can not be used with -c.")
        return parser
    
    def build_bins(self):
        constraints = get_constraint_defs(self.curs, self.tpl_tbl)
        constraints = '' if not constraints else ','+','.join(constraints)
        
        idxs_sql = ''
        idx_count = 0
        for idx_tup in get_index_defs(self.curs, self.tpl_tbl):
            idx_count += 1
            idxs_sql += idx_tup[0].replace(idx_tup[1], idx_tup[1]+'_%s').replace(' '+self.tpl_tbl+' ', ' '+self.tpl_tbl+'_%s ')+'; '
            
        if self.name:
            try:
                print 'Creating %s... ' % self.name, 
                self.curs.execute(create_bin_sql % (self.name, self.tpl_tbl, constraints))
            except Exception, e:
                raise 
            return
        
        max_bin = self.find_max_bin_num()
        for i in xrange(max_bin + 1, max_bin + 1 + self.count):
            tbl = '%s_%d' % (self.tpl_tbl, i)
            try:
                print 'Creating %s...' % tbl
                self.curs.execute(create_bin_sql % (tbl, self.tpl_tbl, constraints))
                self.curs.execute(idxs_sql % ((tbl, i)*idx_count))
            except Exception, e:
                raise e
    
    def find_max_bin_num(self):
        try:
            self.curs.execute(bins_sql % self.tpl_tbl)
        except Exception, e:
            raise e
        res = self.curs.fetchone()
        return -1 if not res else int(res[0])
    
    def work(self):
        super(BinCreator, self).work()
        try:
            self.build_bins()
        except Exception, e:
            print 'Last query: %s' % self.curs.query
            raise 
        print 'Done.'
        self.commit()


def main(argv=None):
    if argv is None:
        argv = sys.argv

    bc = BinCreator(argv[1:])
    bc.work()
    
if __name__ == '__main__':
    main()
