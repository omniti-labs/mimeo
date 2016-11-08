Mimeo - How to Setup
==================

Overview
-------
Mimeo is designed to be an easy to setup, per-table replication solution. Please don't feel overwhelmed at the length of this HowTo or all the options shown in the doc file. The extension has many options available, but I've tried to keep its basic use pretty simple, which is what is covered here. Many of the options may not be relevant for your initial needs, but they're there when/if you need them. This document will go over installing and setting up mimeo from the ground up and getting each of the different types of replication working. 

This document assumes the following:

 * pg_jobmon's schema will be **jobmon**
 * mimeo's schema will be **mimeo**
 * dblink's schema will be **dblink**
 * source database will be called **sourcedb**
 * destination database will be called **destinationdb**
 * mimeo's source database role and special schema for dml replication will be **mimeo**

pg_jobmon Setup
-------------
Both pg_jobmon and mimeo require the dblink extension which is an additionally supplied module that PostgreSQL comes with. Neither extension requires dblink to be in any specific schema. Please see the postgresql documentation for more information on obtaining and installing this extension - http://www.postgresql.org/docs/current/static/contrib.html

Please see the pg_jobmon docs and my blog for more information on setup and use of pg_jobmon.
https://github.com/omniti-labs/pg_jobmon
http://www.keithf4.com/tag/pg_jobmon/

Mimeo Base Setup
--------------

Download mimeo from Github using your preferred method. It's recommended to clone the repository to easily get future updates.

    git clone git://github.com/omniti-labs/mimeo.git

To create the files that PostgreSQL requires for extension installation and place them in the correct location, use (gnu) make

    make
    make install

Install the extension on the destination database. Mimeo does not require a specific schema, but it cannot be changed after installation. Also be aware that if you're going to use DML replication, this exact same schema must be created on the source database (more on that later)
    
    destinationdb=# create schema mimeo;
    CREATE SCHEMA
    destinationdb=# create extension mimeo schema mimeo;
    CREATE EXTENSION

Any role that will be calling the functions in mimeo will need to be granted usage privileges on the mimeo schema and execution privileges on the refresh functions. Using the maker functions will require granting object creation permissions as well. Write permissions don't necessarily need to be granted on destination tables for roles that will call refresh functions. All refresh functions have SECURITY DEFINER set, so refreshing data can be allowed, but not editing the actual destination table directly (a handy feature for letting non-admins repull data as needed without compromising the data integrity itself).

    destinationdb=# GRANT USAGE ON SCHEMA mimeo TO some_role;
    GRANT
    destinationdb=# GRANT CREATE ON SCHEMA destination_schema TO some_maker_role;
    GRANT
    destinationdb=# GRANT EXECUTE ON FUNCTION mimeo.refresh_snap(text,boolean) TO some_role;
    GRANT
    destinationdb=# GRANT EXECUTE ON FUNCTION...
    GRANT

If you just want to grant execute to all mimeo functions, you can do a much easier command

    destinationdb=# GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA mimeo TO some_role;

Source Databases
---------------

Every source database needs to have its connection information stored in mimeo's **dblink_mapping_mimeo** table on the destination database. You can have as many source databases as you need, which makes creating a central replication destination for many master databases easy. All data is pulled by the destination database, never pushed by the source.

    INSERT INTO mimeo.dblink_mapping_mimeo (data_source, username, pwd) 
    VALUES ('host=remote.host port=5432 dbname=sourcedb', 'mimeo', 'password');

You will also have to grant, at a minimum, SELECT privileges on all tables that will be replicated with mimeo for all replication types. For DML replication types, it is required to create a schema on the source database with the exact same name as the schema where mimeo was installed on the destination. Also grant ownership of the schema to the source database mimeo role. For each table that will be a source for DML replication, TRIGGER privileges will have to be given to the the source database mimeo role. For example, the below commands are all run on the **source** database.

    CREATE SCHEMA mimeo;
    ALTER SCHEMA mimeo OWNER TO mimeo;
    GRANT SELECT ON public.dml_test_source to mimeo;
    GRANT TRIGGER ON public.dml_test_source TO mimeo;
    GRANT SELECT ON ...
    GRANT TRIGGER ON ...

Replication Setup
---------------
Ensure all source databases have their pg_hba.conf file set accordingly to allow this remote connection.

Each of the replication types has its own maker (and destroyer) function. The dblink_id that is created when you enter data into the dblink_mapping_mimeo table is used for every maker function to tell mimeo which source database to use. For snapshot and incremental, the only permissions needed are the ones listed above and to create tables on the destination database.

Snapshot replication is the easiest to setup, but should be limited to smaller or relatively static larger tables since it truncates the destination table and repulls all the source data every time it is run and the source data has changed. 

    destinationdb=# SELECT mimeo.snapshot_maker('public.snap_test_source', 1);
    NOTICE:  attempting first snapshot
    NOTICE:  attempting second snapshot
    NOTICE:  Done
     snapshot_maker 
    ----------------
     
    (1 row)

- - -
**Snapshot** is unique, however, in that it can automatically replicate column changes (add, drop, rename, & limited changing of data type). Since this process actually recreates the tables & view from scratch, if you have any special indexes or something else set on the destination snap tables or view, you'll need to use the post_script array column to store commands to be replayed if the source columns ever do change. Indexes that exist on the source and the permissions that were set on the destination will be preserved. Source constraints won't be recreated, but it's actually recommended to not have the same constraints on the destination since that can slow down the replication process. As long as the constraints exist on the source, and the only thing populating the destination tables is mimeo, you shouldn't run into data inconsistancies. But if you do need different table settings on the destination, be aware there are two tables underlying a view. When a snap table is refreshed, there is a view that swaps between which one it points to. This is done to keep locking at a minimum while a refresh is done. But it also means that you have to have commands for both underlying snap tables. 

    UPDATE mimeo.refresh_config_snap SET post_script = '{"CREATE INDEX ON public.snap_test_source_snap1 (col1)"
    , "CREATE INDEX ON public.snap_test_source_snap2 (col1)"}' 
    WHERE dest_table = 'public.snap_test_source';

- - -
**Incremental** replication is useful when the source table has a timestamp or serial/id column that is set at EVERY insert or update. Incremental also DOES NOT replicate deletes. Many other per-table replication methods rely solely on triggers on the source database (and mimeo does as well as you'll see later). But when a table has a special column like this, it's very easy to do replication without the overhead of triggers and this can greatly ease the load on the source database when a table receive a high rate of inserts (tracking web page hits for example). There's two types of incremental replication: inserter & updater. If the table only ever gets inserts (or that's all you care about replicating), the inserter replication type is best to use since it has less overhead than the updater replication. Updater will also replicate rows if that same timestamp or id column is set on every update in addition to when it's inserted. Updater replication requires that the source table have either a primary or unique key. Inserter replication requires no primary/unique keys.

    destinationdb=# SELECT mimeo.inserter_maker('public.inserter_test_source', 'time', 'insert_timestamp', 1);
    NOTICE:  Pulling all data from source...
    NOTICE:  Done
     inserter_maker 
    ----------------
     
    (1 row)

The key piece of both inserter & updater replication is the "control" column which is the second argument in the maker function call (insert_timestamp in the example). This is the column that MUST be a timestamp and is set on every insert and/or update.

- - -
**DML Replication** is used when neither snapshot nor incremental is convenient. This can handle replicating all Inserts, Updates and Deletes on a source table to the destination. Like most DML based replication methods, this is done using triggers on the source table to track these changes in order to replay them on the destination. As long as the permissions have been set properly as shown above, the maker function will take care of setting all this up for you.

    destinationdb=# SELECT mimeo.dml_maker('public.dml_test_source', 1, p_dest_table := 'dest_schema.dml_test_dest');
    NOTICE:  Creating objects on source database (function, trigger & queue table)...
    NOTICE:  Pulling data from source...
    NOTICE:  Done
     dml_maker 
    -----------
     
    (1 row)

In this example, I've used the option to give the destination table a different schema and name than the source. This option is available in all the replication methods. I've also shown how to use named arguments in the function call to set a specific one. This is used extensively to set different options in Mimeo, so if you're not familiar with it, please check the PostgreSQL documentation.

Mimeo has another special kind of DML replication called *logdel*. This is used to keep rows that are deleted from the source on the destination table (ie. log deleted rows). This is useful in data warehousing environments where every update of a table's rows isn't required for record keeping, but the last values of any deleted row must be kept. This replication method will add a special column on the destination table called *mimeo_source_deleted* which is a timestamp of when the row was deleted on the source. Other than this, it works exactly the same as normal DML replication

Refreshing the Destination
-----------------------

To keep the destination tables up to date, there's a refresh function for each type. Each of them all take the *destination* table as the argument.

    destinationdb=# SELECT mimeo.refresh_dml('dest_schema.dml_test_dest');

There are additional options for each refresh type that can allow you to do things like completely refresh all the data on the destination or change the number of rows obtained in a single run. See the mimeo.md doc file for more details.


Scheduling
---------

PostgreSQL has no internal scheduler (something that I hope someday is fixed), so you'll have to use an external scheduler like cron to run the refresh jobs. You can call each refresh job individually or, to help make this a little easier, a python script has been provided. You can schedule how often the script will run a job using the *period* column in the *mimeo.refresh_config* table with an interval value. 

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
    batch_limit | 

This will cause the replication job refresh this table at least once a day.

You can now use the **run_refresh.py** script to run your refresh jobs. You can schedule it to run as often as you like and it will only run refresh jobs that have not run since their last configured period. The --type option will only run 1 of the types given above (snap, inserter, etc). The --batch_limit option sets how many tables to replicate in a single run of the script. Running the script with no arguments will cause all scheduled jobs of all replication types to be run in order of their last_run values.

An example crontab running all replication types with some different options is below

    00,10,20,30,40,50 * * * *  run_refresh.py -c "host=localhost dbname=mydb" -t snap -b 5 >/dev/null
    01,11,21,31,41,51 * * * *  run_refresh.py -c "host=localhost dbname=mydb" -t inserter >/dev/null
    02,12,22,32,42,52 * * * *  run_refresh.py -c "host=localhost dbname=mydb" -t updater >/dev/null
    03,13,23,33,43,53 * * * *  run_refresh.py -c "host=localhost dbname=mydb" -t dml -b 10 >/dev/null
    04,14,24,34,44,54 * * * *  run_refresh.py -c "host=localhost dbname=mydb" -t logdel -b 10 >/dev/null

This sets things up to try and run a batch of each job type at least every ten minutes. It will run at most 5 snap jobs in one batch, all of the scheduled inserter & updater jobs and at most 10 dml or logdel jobs. If there are no jobs that need to run, it just does nothing. If you just want to try and run all scheduled jobs every time you can do

    * * * * *  python run_refresh.py >/dev/null

However, be aware that if there are a lot of jobs running in a single batch, it can cause the time that a job runs to drift. If you need a job to run at a specific time, it may be best to schedule it individually or spread things out like the previous example.

Conclusion
---------
Hopefully this helped you get your initial table replications set up and working. For further options, please see the mimeo.md file for all the available functions and their arguments and descriptions of all the config tables. If you need further assistance, I'm usually lurking in #postgresql on Freenode IRC (keithf4) or you can send questions or bug reports via the github page.
