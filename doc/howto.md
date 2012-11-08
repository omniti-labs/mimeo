Mimeo - How to Setup
==================

Overview
-------
Mimeo is designed to be an easy to setup, per-table replication solution. Please don't feel overwhelmed at the length of this HowTo or all the options shown in the doc file. The extension has many options available, but I've tried to keep its basic use pretty simple, which is what is covered here. Many of the options may not be relevant for your initial needs, but they're there when/if you need them. This document will go over installing and setting up mimeo from the ground up and get each of the different types of replication working. 

This document assumes the following:

 * pg_jobmon's schema will be **jobmon**
 * mimeo's schema will be **mimeo**
 * dblink's schema will be **dblink**
 * source database will be called **sourcedb**
 * destination database will be called **destinationdb**
 * mimeo's source database role and special schema for dml replication will be **mimeo**

pg_jobmon Setup
-------------
Both pg_jobmon and mimeo require the dblink extension which is an additionally supplied module that PostgreSQL comes with. Neither extension requires dblink to be in any specific schema. Please see the core documentation for more information on obtaining and installing this extension - http://www.postgresql.org/docs/current/static/contrib.html

Please see the pg_jobmon docs and my wiki for more information on setup and use of pg_jobmon.
https://github.com/keithf4/pg_jobmon
http://keithf4.com/pg_jobmon

Mimeo Base Setup
--------------

Download mimeo from Github using your preferred method. It's recommended to clone the repository to easily get future updates.

    git clone git://github.com/keithf4/mimeo.git

To create the files that PostgreSQL requires for extension installation and place them in the correct location, use (gnu) make

    make
    make install

Install the extension on the destination database. Mimeo does not require a specific schema, but it cannot be changed after installation. Also be aware that if you're going to use DML replication, this exact same schema must be created on the source database (more on that later)
    
    destinationdb=# create schema mimeo;
    CREATE SCHEMA
    destinationdb=# create extension mimeo schema mimeo;
    CREATE EXTENSION

Any role that will be calling the functions in mimeo will need to be granted usage privileges on the mimeo schema and execution privileges on the refresh functions. That role will also, obviously, need permissions on the destination tables to select, insert, update, delete & truncate.

    destinationdb=# GRANT USAGE ON SCHEMA mimeo to some_role;
    GRANT
    destinationdb=# GRANT EXECUTE ON FUNCTION mimeo.refresh_snap(text,boolean) to some_role;
    GRANT
    destinationdb=# GRANT EXECUTE ON FUNCTION...

Source Databases
---------------

Every source database needs to have its connection information stored in mimeo's **dblink_mapping** table on the destination database. You can have as many source databases as you need, which makes creating a central replication destination for many master databases easy. All data is pulled by the destination database, never pushed by the source.

    INSERT INTO mimeo.dblink_mapping (data_source, username, pwd) 
    VALUES ('host=remote.host port=5432 dbname=sourcedb', 'mimeo', 'password');

You will also have to grant, at a minimum, SELECT privileges on all tables that will be replicated with mimeo for all replication types. For DML replication types, it is required to create a schema on the source database with the exact same name as the schema where mimeo was installed on the destination. Also grant ownership of the schema to the source database mimeo user. For each table that will be a source for DML replication, TRIGGER privileges will have to be given to the the source database mimeo user. For example, the below commands are all run on the **source** database.

    CREATE SCHEMA mimeo;
    ALTER SCHEMA mimeo OWNER TO mimeo;
    GRANT TRIGGER ON public.dml_test_source TO mimeo;
    GRANT TRIGGER ON ...

Replication Setup
---------------
Ensure all source databases have their pg_hba.conf file set accordingly to allow this remote connection.

Each of the replication types has its own maker (and destroyer) function. The dblink_id that is created when you enter data into the dblink_mapping table is used for every maker function to tell mimeo which source database to use. For snapshot and incremental, the only permissions needed are the ones listed above and to create tables on the destination database.

Snapshot replication is the easiest to setup, but should be limited to smaller tables since it truncates the destination table and repulls all the source data every time it is run. 

    destinationdb=# SELECT mimeo.snapshot_maker('public.snap_test_source', 1);
    NOTICE:  Inserting record in mimeo.refresh_config
    NOTICE:  Insert successful
    NOTICE:  attempting first snapshot
    NOTICE:  attempting second snapshot
    NOTICE:  all done
     snapshot_maker 
    ----------------

- - -
**Snapshot** is unique, however, in that it can automatically replicate column changes (add, drop, rename, & limited changing of data type). If you have any special permissions, indexes, or constraints set on the destination snap table, you'll need to use the post_script array column to store commands to be replayed if the source columns ever do change. Also be aware that for indexes and constraints, there are two underlying snap tables. When a snap table is refreshed, there is a view that swaps between which one it points to. This is done to keep locking at a minimum while a refresh is done. But it also means that you have to create indexes and constraints on both underlying snap tables. I'm working on making the copying of indexes & constraints automatic from the source database, but this is the required method to get those for now.

    UPDATE mimeo.refresh_config_snap SET post_script = '{"DROP INDEX IF EXISTS keith.manual_snap1_tblname_idx"
            , "DROP INDEX IF EXISTS keith.manual_snap2_tblname_idx"
            , "CREATE INDEX manual_snap1_tblname_idx ON keith.manual_snap_tables_snap1 (tblname)"
            , "CREATE INDEX manual_snap2_tblname_idx ON keith.manual_snap_tables_snap2 (tblname)"
            , "GRANT select ON keith.manual_snap_tables TO omniti"}', 'snap');

- - -
**Incremental** replication is useful when the source table has a timestamp column that is set at EVERY insert or update. Many other per-table replication methods rely solely on triggers on the source database (and mimeo does as well as you'll see later). But when a table has a special column like this, it's very easy to do replication without the overhead of triggers and this can greatly ease the load on the source database when a table receive a high rate of inserts (tracking web page hits for example). If the table only ever gets inserts, the inserter replication type is best to use since it has less overhead than the updater replication. Updater replication requires that the source table have either a primary or unique key. Inserter replication requires no primary/unique keys.

    destinationdb=# SELECT mimeo.inserter_maker('public.inserter_test_source', 'insert_timestamp', 1);
    NOTICE:  Snapshotting source table to pull all current source data...
    NOTICE:  table "inserter_test_source_snap1" does not exist, skipping
    CONTEXT:  SQL statement "DROP TABLE IF EXISTS mimeo_source.inserter_test_source_snap1"
    PL/pgSQL function snapshot_destroyer(text,text) line 45 at EXECUTE statement
    SQL statement "SELECT mimeo.snapshot_destroyer(p_dest_table, 'ARCHIVE')"
    PL/pgSQL function inserter_maker(text,text,integer,interval,text,text[],text,boolean) line 43 at PERFORM
    NOTICE:  table "inserter_test_source_snap2" does not exist, skipping
    CONTEXT:  SQL statement "DROP TABLE IF EXISTS mimeo_source.inserter_test_source_snap2"
    PL/pgSQL function snapshot_destroyer(text,text) line 46 at EXECUTE statement
    SQL statement "SELECT mimeo.snapshot_destroyer(p_dest_table, 'ARCHIVE')"
    PL/pgSQL function inserter_maker(text,text,integer,interval,text,text[],text,boolean) line 43 at PERFORM
    NOTICE:  Snapshot complete.
    NOTICE:  Getting the maximum destination timestamp...
    NOTICE:  Inserting data into config table
    NOTICE:  Done
     inserter_maker 
    ----------------

You can ignore most of the output of the maker functions. As long as the *NOTICE:  Done* line shows up, everything went as planned. The maker functions all internally make use of the snapshot replication method to do the initial data pull which is why you see the above references to it.

The key piece of both inserter & updater replication is the "control" column which is the second argument in the maker function call (insert_timestamp in the example). This column MUST be a timestamp column that is set at insert. And for updater replication this same column must also be updated with the current timestamp on every update.

- - -
**DML Replication** is used when neither snapshot nor incremental is convenient. This can handle replicating all Inserts, Updates and Deletes on a source table to the destination. Like most DML based replication methods, this is done using triggers on the source table to track these changes in order to replay them on the destination. As long as the permissions have been set properly as shown above, the maker function will take care of setting all this up for you.

    destinationdb=# SELECT mimeo.dml_maker('public.dml_test_source', 1, p_dest_table := 'dest_schema.dml_test_dest');
    NOTICE:  Creating objects on source database (function, trigger & queue table)...
    NOTICE:  Snapshotting source table to pull all current source data...
    NOTICE:  table "dml_test_dest_snap1" does not exist, skipping
    CONTEXT:  SQL statement "DROP TABLE IF EXISTS mimeo_dest.dml_test_dest_snap1"
    PL/pgSQL function snapshot_destroyer(text,text) line 45 at EXECUTE statement
    SQL statement "SELECT mimeo.snapshot_destroyer(p_dest_table, 'ARCHIVE')"
    PL/pgSQL function dml_maker(text,integer,text,text[],text,boolean,text[],text[]) line 156 at PERFORM
    NOTICE:  table "dml_test_dest_snap2" does not exist, skipping
    CONTEXT:  SQL statement "DROP TABLE IF EXISTS mimeo_dest.dml_test_dest_snap2"
    PL/pgSQL function snapshot_destroyer(text,text) line 46 at EXECUTE statement
    SQL statement "SELECT mimeo.snapshot_destroyer(p_dest_table, 'ARCHIVE')"
    PL/pgSQL function dml_maker(text,integer,text,text[],text,boolean,text[],text[]) line 156 at PERFORM
    NOTICE:  Inserting data into config table
    NOTICE:  Done
     dml_maker 
    -----------

In this example, I've used the option to give the destination table a different schema and name than the source. This option is available in all the replication methods. I've also shown how to use named arguments in the function call to set a specific one. This is used extensively to set different options in Mimeo, so if you're not familiar with it, please check the PostgreSQL documentation.

Mimeo has another special kind of DML replication called *logdel*. This is used to keep rows that are deleted from the source on the destination table (ie. log deleted rows). This is useful in data warehousing environments where every update of a table's rows isn't required for record keeping, but the last values of any deleted row must be kept. This replication method will add a special column on the destination table called *mimeo_source_deleted* which is a timestamp of when the row was deleted on the source. Other than this, it works exactly the same as normal DML replication

Refreshing the Destination
-----------------------

To keep the destination tables up to date, there's a set of refresh functions for each type. Each of them all take the *destination* table as the argument.

    destinationdb=# SELECT mimeo.refresh_dml('dest_schema.dml_test_dest');

There are additional options for each refresh type that can allow you to do things like completely refresh all the data on the destination or change the number of rows obtained in a single run. See the mimeo.md doc file for more details.


Scheduling
---------

PostgreSQL has no internal scheduler (something that I hope someday is fixed), so you'll have to use an external scheduler like cron to run the refresh jobs. To help make this a little easier so you don't have to schedule each and every replication job individually, a function to run every job of a certain type has been provided. You can schedule how often a job will run using the *period* column in the *mimeo.refresh_config* table with an interval value. 

    destinationdb=# update mimeo.refresh_config set period = '1 day' where dest_table = 'dest_schema.dml_test_dest';
    UPDATE 1

    destinationdb=# select * from mimeo.refresh_config where dest_table = 'dest_schema.dml_test_dest';
    -[ RECORD 1 ]------------------------------
    dest_table  | dest_schema.dml_test_dest
    type        | dml
    dblink      | 1
    last_run    | 2012-11-08 01:00:00
    filter      | 
    condition   | 
    period      | 1 day
    batch_limit | 10000

This will cause the replication job to run daily at the time listed for *last_run*. You can manually change the *last_run* value to whatever you wish if you need to reschedule the job to start running at a certain time.

You can now use the **refresh_run()** function to run your refresh jobs. The first argument, which is required, is the type of replication jobs to run (snap, inserter, updater, dml, or logdel). The second argument is optional and tells the scheduler how many of that job type to grab for that run. They are run sequentially in the order of the job's last_run value, not in parallel. If this argument is not given, the default is 4. An example crontab running all replication types is below.

    00,10,20,30,40,50 * * * *  psql -c "select mimeo.run_refresh('snap');" >/dev/null
    01,11,21,31,41,51 * * * *  psql -c "select mimeo.run_refresh('inserter', 10);" >/dev/null
    02,12,22,32,42,52 * * * *  psql -c "select mimeo.run_refresh('updater', 10);" >/dev/null
    03,13,23,33,43,53 * * * *  psql -c "select mimeo.run_refresh('dml', 10);" >/dev/null
    04,14,24,34,44,54 * * * *  psql -c "select mimeo.run_refresh('logdel', 10);" >/dev/null

This sets things up to run each job type at least every ten minutes. run_refresh() grabs all the jobs of the specified type, ordered ascending by last_run and if a job hasn't run within it's specified period, it will be added to the queue for that run up to the maximum given by the second argument (10 in the example). If there are no jobs that need to run, run_refresh just does nothing.

Conclusion
---------
Hopefully this helped you get your initial table replications set up and working. For further options, please see the mimeo.md file for all the available functions and their arguments and descriptions of all the config tables. If you need further assistance, I'm usually lurking in #postgresql on Freenode IRC (keithf4) or you can send questions or bug reports via the github page.
