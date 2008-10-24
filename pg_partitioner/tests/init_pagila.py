#!/usr/bin/env python

import os, sys
from subprocess import Popen, call, check_call, PIPE, CalledProcessError
import psycopg2

if not os.path.isfile('./pagila-0.9.0.zip'):
    print 'Missing pagila-0.9.0.zip'
    sys.exit(1)

dbname = len(sys.argv) and sys.argv[1] or 'pagila'

drop_db = 'dropdb %s' % dbname
call(drop_db, shell=True)

try:
    create_db = 'createdb %s' % dbname 
    check_call(create_db, shell=True)
except CalledProcessError, e:
    print e
    sys.exit(e.returncode)

try:
    psql = 'psql %s' % dbname
    # extract the schema into the db
    ext_schema = 'unzip -p pagila-0.9.0.zip pagila-0.9.0/pagila-schema.sql'
    p1 = Popen(ext_schema, shell=True, stdout=PIPE)
    p2 = Popen(psql, shell=True, stdin=p1.stdout)
    p2.communicate()
    
    # extract the data
    ext_data = 'unzip -p pagila-0.9.0.zip pagila-0.9.0/pagila-data.sql'
    p1 = Popen(ext_data, shell=True, stdout=PIPE)
    p2 = Popen(psql, shell=True, stdin=p1.stdout)
    p2.communicate()
except (OSError, CalledProcessError), e:
    print e
    if hasattr(e, 'child_traceback'):
        print e.child_traceback
    sys.exit(1)