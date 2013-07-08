#!/usr/bin/env python

##########
#
# Script to manage running mimeo table replication with a period set in their configuration.
# By default, refreshes are run sequentially in ascending order of their last_run value.
# Parallel refreshes are supported with -j option
#
##########

import psycopg2, sys, getopt
from multiprocessing import Process 

help_string = """
If type or batch_limit options are not given, all replication tables of all types scheduled to run will be run.\n
  --connection (-c):     Connection string for use by psycopg to connect to your database. Defaults to "host=localhost".\n
                         Highly recommended to use .pgpass file to keep credentials secure.\n
  --schema (-s):         The schema that mimeo was installed to on the destination database. Default is "mimeo".\n
  --type (-t):           Must be one of the following values: snap, inserter, updater, dml, logdel, table."
                         If you'd like to run more than one type, but not all of them, call this script separately for each type.\n
  --batch_limit (-b):    An integer representing how many replication tables you want to run for this call of the script."
                         Default is all of them that are scheduled to run.\n
  --jobs (-j):           Allows parallel running of replication jobs. Set this equal to the number of processors you want
                         to use to allow that many jobs to start simultaneously. (this uses multiprocessing library, not threading)
  --verbose (-v)         More detailed output.\n
 Example to run everything scheduled on local database:\n
         python run_refresh.py -c \"host=localhost dbname=mydb\"\n
 Example to run 10 snaps with a custom schema:\n
         python run_refresh.py -c \"host=localhost dbname=mydb\" -s replication -t snap -b 10\n
"""

try:
    opts, args = getopt.getopt(sys.argv[1:], "hvc:s:t:b:j:", ["help","verbose","connection=","schema=","type=","batch_limit=","jobs"])
except getopt.GetoptError:
    print "Invalid argument."
    print help_string
    sys.exit(2)

arg_connection = "host=localhost"
arg_schema = "mimeo"
arg_type = ""
arg_batch_limit = ""
arg_verbose = ""
arg_jobs = ""
for opt, arg in opts:
    if opt in ("-h", "--help"):
        print help_string
        sys.exit()
    elif opt in ("-c", "--connection"):
        arg_connection = arg
    elif opt in ("-s", "--schema"):
        arg_schema = arg
    elif opt in ("-t", "--type"):
        arg_type = arg
        if arg_type not in ("snap", "inserter", "updater", "dml", "logdel", "table"):
            print "--type (-t) must be one of the following: snap, inserter, updater, dml, logdel, table"
            sys.exit(2)
    elif opt in ("-b", "--batch_limit"):
        arg_batch_limit = arg
    elif opt in ("-j", "--jobs"):
        arg_jobs = int(arg)
    elif opt in ("-v", "--verbose"):
        arg_verbose = 1


def get_jobs():
    conn = psycopg2.connect(arg_connection)
    # Fetch all jobs scheduled to run
    cur = conn.cursor()
    sql = "SELECT dest_table, type FROM " + arg_schema + ".refresh_config"
    if arg_type != "":
        sql += "_" + arg_type
    sql += " WHERE period IS NOT NULL AND (CURRENT_TIMESTAMP - last_run)::interval > period ORDER BY last_run ASC"
    if arg_batch_limit != "":
        sql += " LIMIT " + arg_batch_limit
    cur.execute(sql)
    result = cur.fetchall()
    cur.close()
    return result
    conn.close()


def single_process(result):
    conn = psycopg2.connect(arg_connection)
    cur = conn.cursor()
    for i in result:
        if arg_verbose == 1:
            print "Running " + i[1] + " replication for table: " + i[0]
        sql = "SELECT " + arg_schema + ".refresh_" + i[1] + "(%s)"
        cur.execute(sql, [i[0]])
        conn.commit()
    cur.close()
    conn.close()


def refreshProc(dest_table, rtype):
    conn = psycopg2.connect(arg_connection)
    cur = conn.cursor()
    sql = "SELECT " + arg_schema + ".refresh_" + rtype + "(%s)"
    cur.execute(sql, [dest_table])
    conn.commit()
    cur.close()
    conn.close()


if __name__ == "__main__":
    result = get_jobs()
    if arg_jobs != "":
        while len(result) > 0:
            if arg_verbose == 1:
                print "Jobs left in queue: " + str(len(result))
            if len(result) < arg_jobs: # shorten the for loop if the number of tables to run is less than -j 
                arg_jobs = len(result)
            processlist = []
            for num in range(0, arg_jobs):
                i = result.pop() 
                p = Process(target=refreshProc, args=(i[0], i[1]))
                p.start()
                if arg_verbose == 1:
                    print "Running " + i[1] + " replication for table: " + i[0]
                processlist.append(p)
            for j in processlist:
                j.join()
    else:
        single_process(result)

