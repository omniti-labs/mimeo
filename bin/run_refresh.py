#!/usr/bin/env python

import argparse, psycopg2, sys
from multiprocessing import Process 

mimeo_version = "1.3.0"

parser = argparse.ArgumentParser(description="Script to manage running mimeo table replication with a period set in their configuration. By default, refreshes are run sequentially in ascending order of their last_run value. Parallel refreshes are supported with -j option. If type or batch_limit options are not given, all replication tables of all types scheduled will be run.")
parser.add_argument('-c','--connection', default="host=", help="""Connection string for use by psycopg to connect to your database. Defaults to "host=" (local socket).""")
parser.add_argument('-t','--type', choices=["snap", "inserter", "updater", "dml", "logdel", "table"], help="Must be one of the following values: snap, inserter, updater, dml, logdel, table. If you'd like to run more than one type, but not all of them, call this script separately for each type.")
parser.add_argument('-b','--batch_limit', type=int, default=-1, help="An integer representing how many replication tables you want to run for this call of the script. Default is all of them that are scheduled to run.")
parser.add_argument('-j','--jobs', type=int, default=0, help="Allows parallel running of replication jobs. Set this equal to the number of processors you want to use to allow that many jobs to start simultaneously. (this uses multiprocessing library, not threading)")
parser.add_argument('-v', '--verbose', action="store_true", help="More detailed output.")
parser.add_argument('--version', action="store_true", help="Print out the minimum version of mimeo this script is meant to work with. The version of mimeo installed may be greater than this.")
args =  parser.parse_args()

def create_conn():
    conn = psycopg2.connect(args.connection)
    conn.autocommit = True
    return conn


def close_conn(conn):
    conn.close()


def get_mimeo_schema(conn):
    cur = conn.cursor()
    sql = "SELECT nspname FROM pg_catalog.pg_namespace n, pg_catalog.pg_extension e WHERE e.extname = 'mimeo' AND e.extnamespace = n.oid"
    cur.execute(sql)
    mimeo_schema = cur.fetchone()[0]
    cur.close()
    return mimeo_schema


def get_jobs(conn, mimeo_schema):
    # Fetch all jobs scheduled to run
    cur = conn.cursor()
    sql = "SELECT dest_table, type FROM " + mimeo_schema + ".refresh_config"
    if args.type != None:
        sql += "_" + args.type
    sql += " WHERE period IS NOT NULL AND (CURRENT_TIMESTAMP - last_run)::interval > period ORDER BY last_run ASC"
    if args.batch_limit > -1:
        sql += " LIMIT " + str(args.batch_limit)
    cur.execute(sql)
    result = cur.fetchall()
    cur.close()
    return result

def print_version():
    print(mimeo_version)
    sys.exit()

def single_process(result, mimeo_schema):
    conn = create_conn() 
    cur = conn.cursor()
    for i in result:
        if args.verbose:
            print("Running " + i[1] + " replication for table: " + i[0])
        sql = "SELECT " + mimeo_schema + ".refresh_" + i[1] + "(%s)"
        cur.execute(sql, [i[0]])
    cur.close()
    close_conn(conn)


def refreshProc(dest_table, rtype, mimeo_schema):
    conn = create_conn()
    cur = conn.cursor()
    sql = "SELECT " + mimeo_schema + ".refresh_" + rtype + "(%s)"
    cur.execute(sql, [dest_table])
    cur.close()
    close_conn(conn)


if __name__ == "__main__":
    if args.version:
        print_version()

    conn = create_conn() 
    mimeo_schema = get_mimeo_schema(conn)
    result = get_jobs(conn, mimeo_schema)
    close_conn(conn)

    if args.jobs > 0:
        while len(result) > 0:
            if args.verbose:
                print("Jobs left in queue: " + str(len(result)))
            if len(result) < args.jobs: # shorten the for loop if the number of tables to run is less than -j 
                args.jobs = len(result)
            processlist = []
            for num in range(0, args.jobs):
                i = result.pop() 
                p = Process(target=refreshProc, args=(i[0], i[1], mimeo_schema))
                p.start()
                if args.verbose:
                    print("Running " + i[1] + " replication for table: " + i[0])
                processlist.append(p)
            for j in processlist:
                j.join()
    else:
        single_process(result, mimeo_schema)

