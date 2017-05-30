Mimeo
=====

About
-----

Mimeo is a replication extension for copying specific tables in one of several specialized ways from any number of source databases to a destination database where mimeo is installed. 

**Snapshot replication** is for copying the entire table from the source to the destination every time it is run. This is the only one of the replication types that can automatically replicate column changes (add, drop, rename, new type). It can also detect if there has been no new DML (inserts, updates, deletes) on the source table and skip the data pull step entirely (does not work if source is a view). The "track_counts" PostgreSQL setting must be turned on for this to work (which is the default). Be aware that a column structure change will cause the tables and view to be recreated from scratch on the destination. Indexes as they exist on the source will be automatically recreated if the p_index parameter for the refresh_snap() function is set to true (default) and permissions as they exist on the destination will be preserved. But if you had any different indexes or constraints on the destination, you will have to use the *post_script* column in the config table to have them automatically recreated.

**Incremental replication** comes in two forms: Insert Only and Insert/Update Only. This can only be done on a table that has a timestamp or serial/id control column that is set during every insert and/or update. The update replication requires that the source has a primary key. Insert-only replication doesn't require a primary key, just the control column. If the source table ever has rows deleted, this WILL NOT be replicated to the destination.
For time-based incremental replication, systems that do not run in UTC time can have issues during DST changes. To account for this, these maker functions check the timezone of the server and if it is anything but UTC/GMT, it sets dst_active to true in the config table. This causes all replication to pause between 12:30am and 2:30am on the morning of any DST change day. These times can be adjusted if needed using the dst_start and dst_end columns in the refresh_config_inserter or refresh_config_updater table accordingly.
IMPORTANT: If a transaction on the source lasts longer than the interval the incremental jobs run, rows can be missed because the control field's time or id will be the time or id of the transactions' start. Be sure and set a boundary value for your tables that ensures all transactions have completed for that time or integer interval before replication tries to pull its values. The default is 10 minutes for time based and 1 for serial.
Also be aware that if you stop incremental replication permanently on a table, all of the source data may not have reached the destination due to the boundary settings and/or other methods that are used to keep incremental replication in a consistent state. Please double-check that all your source data is on the destination before destroying the source.

**DML replication** replays on the destination every insert, update and delete that happens on the source table. The special "logdel" dml replication does not remove rows that are deleted on the source. Instead it grabs the latest data that was deleted from the source, updates that on the destination and logs a timestamp of when it was deleted from the source (special destination timestamp field is called *mimeo_source_deleted* to try and keep it from conflicting with any existing column names). Be aware that for logdel, if you delete a row and then re-use that primary/unique key value again, you will lose the preserved, deleted row on the destination. Doing otherwise would either violate the key constraint or not replicate the new data.

There is also a plain table replication method that always does a truncate and refresh every time it is run, but doesn't use the view swap method that snapshot does. It just uses a normal table as the destination. It requires no primary keys, control columns or triggers on the source table. It is not recommended to use this refresh method for a regular refresh job if possible since it is much less efficient. What this is ideal for is a development database where you just want to pull data from production on an as-needed basis and be able to edit things on the destination. Since it requires no write access on the source database, you can safely connect to your production system to grab data (as long as you set permissions properly). It has options available for dealing with foreign key constraints and resetting sequences on the destination.

The **p_condition** option in the maker functions (and the **condition** column in the config tables) can be used to as a way to designate specific rows that should be replicated. This is done using the WHERE condition part of what would be a select query on the source table. You can also designate a comma separated list of tables before the WHERE keyword if you need to join against other tables on the SOURCE database. When doing this, assume that the source table is already listed as part of the FROM clause and that your table will be second in the list (which means you must begin with a comma). Please note that using the JOIN keyword to join again other tables is not guarenteed to work at this time. Some examples of how this field are used in the maker functions:

    SELECT mimeo.snapshot_maker(..., p_condition := 'WHERE col1 > 4 AND col2 <> ''test''');
    SELECT mimeo.dml_maker (..., p_condition := ', table2, table3 WHERE source_table.col1 = table2.col1 AND table1.col3 = table3.col3');

You can use views as source tables, but only in certain conditions. Snapshot, Table & Incremental replication all support views, but both DML-based replication methods do not. Also, for updater replication, you must manually provide the primary/unique key column names & types via the special parameters to its maker function. Otherwise it tries to look them up in the system catalog and fails (since views have no pk catalog entry).

Mimeo uses the **pg_jobmon** extension to provide an audit trail and monitoring capability. If you're having any problems with mimeo working, check the job logs that pg_jobmon creates. https://github.com/omniti-labs/pg_jobmon

All refresh functions use the advisory lock system to ensure that jobs do not run concurrently. If a job is found to already be running, it will cleanly exit immediately and record that another job was already running in pg_jobmon. It will also log a level 2 (WARNING) status for the job so you can monitor for a refresh job running concurrently too many times which may be an indication that replication is falling behind.

To aid in automatically running refresh jobs more easily, a python script is included (see run_refresh.py in **Scripts** section below) and should be installed to the same location as your postgresql binaries (psql, pg_dump, etc). This will automatically run any refresh jobs that have a period set in their config. Jobs can be run sequentially or in parallel. The only thing to be aware of is that there may be some time drift in when a refresh job actually runs. It will still run within the designated time period, but the exact time it runs can change depending on the batch sizes you set for how many jobs can run in a single call of the script. If you require a refresh job to run at a specific time, please call that job individually via cron (or similar tools).

The p_debug argument for any function that has it will output more verbose feedback to show what that job is doing. Most of this is also logged with pg_jobmon, but this is a quick way to see detailed info immediately.

### Adding/Removing Columns after initial setup

Adding and/or removing columns on the source database must be done carefully, and in a specific order, to avoid errors. Except for the snapshot replication method (which automatically replicates column changes), the columns should always be added on the destination table first and then the source. And the opposite should be done for removing a column you no longer want on either system (source first, destination last). The columns copied over during replication are always determined by what the source has, so if the source has columns the destination doesn't, replication will fail.
Since the source database is used as the canonical schema, this means the destination can actually have columns the source does not (and logdel replication shows this in action).
There is a function available that can help monitor for when source columns change. See check_source_columns() in Maintenance Functions.

Additional consideration must be taken with with Logdel Replication:

When adding a column, follow this order:

1. Add to destination table
2. Add to the source queue table (logdel only)
3. Add to the source table

When removing a column, follow this order:

1. Remove from source table
2. Remove from the source queue table (logdel only)
3. Remove from the destination table (if desired. It can be left for historical records with no problems.)

### Dblink Mapping Setup

The **dblink_mapping_mimeo** table contains the configuration information for the source database (where data is copied FROM). You can define as many data sources as you wish. The data source for a replicated table is declared just once in the refresh_config table mentioned below.

    insert into mimeo.dblink_mapping_mimeo (data_source, username, pwd) 
    values ('host=pghost.com port=5432 dbname=pgdb', 'refresh', 'password');

The **data_source** value is the connection format required by dblink.
**username** and **pwd** are the credentials for connecting to the source database. Password is optional if you have security set up to not require it (just leave it NULL).

For all forms of replication, the role on the source database(s) should have at minimum select access on all tables/views to be replicated. Except for dml/logdel replication, no other setup on the source is needed for mimeo to do everything it needs.  
For dml and logdel replication some additional setup is required. The source database needs a schema created with the EXACT same name as the schema where mimeo was installed on the destination. The source role should have ownership of this schema to easily allow it to do what it needs. The source role will also need TRIGGER permissions on any source tables that it will be replicating.
    
    CREATE schema <mimeo_schema>;
    ALTER SCHEMA <mimeo_schema> OWNER TO <mimeo_role>;
    GRANT TRIGGER ON <source_table> TO <mimeo_role>;


Extension Objects
-----------------

### Setup Functions

*dml_maker(p_src_table text, p_dblink_id int, p_dest_table text DEFAULT NULL, p_index boolean DEFAULT true, p_filter text[] DEFAULT NULL, p_condition text DEFAULT NULL, p_pulldata boolean DEFAULT true, p_pk_name text[] DEFAULT NULL, p_pk_type text[] DEFAULT NULL, p_jobmon boolean DEFAULT NULL, p_debug boolean DEFAULT false)*  
 * Function to automatically setup dml replication for a table. See setup instructions above for permissions that are needed on source database. By default source and destination table will have same schema and table names.  
 * Source table must have a primary key or unique index. Either the primary key or a unique index (first in alphabetical order if more than one) on the source table will be obtained automatically. Columns of primary/unique key cannot be arrays nor can they be an expression.  
 * The trigger function created on the source table has the SECURITY DEFINER flag set. This allows any writes to the source table to be able to write to the queue table as well.
 * If destination table already exists, no data will be pulled from the source. You can use the refresh_dml() 'repull' option to truncate the destination table and grab all the source data.  
 * Multiple destinations are supported for a single source table but there is a hard limit of 100 destinations. Be aware that doing so places multiple triggers on the source table which will in turn be writing to multiple queue tables to track changes. This can cause noticable performance penalties depending on the level of write traffic on the source table.
 * p_dblink_id is the data_source_id from the dblink_mapping_mimeo table for where the source table is located.  
 * p_dest_table, an optional argument,  is to set a custom destination table. Be sure to schema qualify it if needed.
 * p_index, an optional argument, sets whether to recreate all indexes that exist on the source table on the destination. Defaults to true. Note this is only applies during replication setup. Future index changes on the source will not be propagated.
 * p_filter, an optional argument, is an array list that can be used to designate only specific columns that should be used for replication. Be aware that if this option is used, indexes cannot be replicated from the source because there is currently no easy way to determine all types of indexes that may be affected by the columns that are excluded. The primary/unique key used to determine row identity will still be replicated, however, because there are checks in place to ensure those columns are not excluded.
  * Source table trigger will only fire on UPDATES of the given columns (uses UPDATE OF col1 [, col2...]).
 * p_condition, an optional argument, is used to set criteria for specific rows that should be replicated. See additional notes in **About** section above.
 * p_pulldata, an optional argument, allows you to control if data is pulled as part of the setup. Set to 'false' to configure replication with no initial data.
 * p_pk_name, an optional argument, is an array of the columns that make up the primary/unique key on the source table. This overrides the automatic retrieval from the source.
 * p_pk_type, an optional argument, is an array of the column types that make up the primary/unique key on the source table. This overrides the automatic retrieval from the source. Ensure the types are in the same order as p_pk_name.
 * p_jobmon, an optional argument, sets whether pg_jobmon logging will be used to log the running of this maker function AND whether pg_jobmon will be used to log refresh runs of the given table. Default is assumed to be true if pg_jobmon is installed and false if it is not.

*inserter_maker(p_src_table text, p_type text, p_control_field text, p_dblink_id int, p_boundary text DEFAULT NULL, p_dest_table text DEFAULT NULL, p_index boolean DEFAULT true, p_filter text[] DEFAULT NULL, p_condition text DEFAULT NULL, p_pulldata boolean DEFAULT true, p_jobmon boolean DEFAULT NULL, p_debug boolean DEFAULT false)*
 * Function to automatically setup inserter replication for a table. By default source and destination table will have same schema and table names.  
 * If destination table already exists, no data will be pulled from the source. You can use the refresh_inserter() 'repull' option to truncate the destination table and grab all the source data. Or you can set the config table's 'last_value' column for your specified table to designate when it should start. Otherwise last_value will default to the destination's max value for the control field or, if null, the time that the maker function was run.
 * p_type determines whether it is time-based or serial-based incremental replication. Valid values are: "time" or "serial".
 * p_control_field is the column which is used as the control field (a timestamp or integer column that is new for every insert).
 * p_dblink_id is the data_source_id from the dblink_mapping_mimeo table for where the source table is located.  
 * p_boundary, an optional argument, is a boundary value to prevent records being missed at the upper boundary of the batch. Argument type is text, but must be able to be converted to an interval for time-based and an integer for serial-based. This is mostly only relevant for time based replication. Set this to a value that will ensure all inserts will have finished for that time period when the replication runs. For time based the default is 10 minutes which means the destination may always be 10 minutes behind the source but that also means that all inserts on the source will have finished by the time 10 minutes has passed. For serial based replication, the default is 10. If your serial column is based on a sequence you should be able to change this to zero safely. The upper boundary will always be one less than the max at the time replication runs. If it's not based on a sequence, you'll have to set this to a value to ensure that the source is done inserting that range of numerical values by the time replication runs. For example, if you set this to 10, the destination will always be one less than "max(source_control_col) - 10" behind the source.
 * p_dest_table, an optional argument,  is to set a custom destination table. Be sure to schema qualify it if needed.
 * p_index, an optional argument, sets whether to recreate all indexes that exist on the source table on the destination. Defaults to true. Note this is only applies during replication setup. Future index changes on the source will not be propagated.
 * p_filter, an optional argument, is an array list that can be used to designate only specific columns that should be used for replication. Be aware that if this option is used, indexes cannot be replicated from the source because there is currently no easy way to determine all types of indexes that may be affected by the columns that are excluded.
 * p_condition, an optional argument, is used to set criteria for specific rows that should be replicated. See additional notes in **About** section above.
 * p_pulldata, an optional argument, allows you to control if data is pulled as part of the setup. Set to 'false' to configure replication with no initial data.
 * p_jobmon, an optional argument, sets whether pg_jobmon logging will be used to log the running of this maker function AND whether pg_jobmon will be used to log refresh runs of the given table. Default is assumed to be true if pg_jobmon is installed and false if it is not.
 
*logdel_maker(p_src_table text, p_dblink_id int, p_dest_table text DEFAULT NULL, p_index boolean DEFAULT true, p_filter text[] DEFAULT NULL, p_condition text DEFAULT NULL, p_pulldata boolean DEFAULT true, p_pk_name text[] DEFAULT NULL, p_pk_type text[] DEFAULT NULL, p_jobmon boolean DEFAULT NULL, p_debug boolean DEFAULT false)*  
 * Function to automatically setup logdel replication for a table. See setup instructions above for permissions that are needed on source database. By default source and destination table will have same schema and table names.  
 * Source table must have a primary key or unique index. Either the primary key or a unique index (first in alphabetical order if more than one) on the source table will be obtained automatically. Columns of primary/unique key cannot be arrays nor can they be an expression.  
 * The trigger function created on the source table has the SECURITY DEFINER flag set. This allows any writes to the source table to be able to write to the queue table as well.
 * If destination table already exists, no data will be pulled from the source. You can use the refresh_logdel() 'repull' option to truncate the destination table and grab all the source data.
 * Multiple destinations are supported for a single source table but there is a hard limit of 100 destinations. Be aware that doing so places multiple triggers on the source table which will in turn be writing to multiple queue tables to track changes. This can cause noticable performance penalties depending on the level of write traffic on the source table.
 * p_dblink_id is the data_source_id from the dblink_mapping_mimeo table for where the source table is located.  
 * p_dest_table, an optional argument,  is to set a custom destination table. Be sure to schema qualify it if needed.
 * p_index, an optional argument, sets whether to recreate all indexes that exist on the source table on the destination. Defaults to true. Note this is only applies during replication setup. Future index changes on the source will not be propagated.
 * p_pulldata, an optional argument, allows you to control if data is pulled as part of the setup. Set to 'false' to configure replication with no initial data.
 * p_filter, an optional argument, is an array list that can be used to designate only specific columns that should be used for replication. Be aware that if this option is used, indexes cannot be replicated from the source because there is currently no easy way to determine all types of indexes that may be affected by the columns that are excluded. The primary/unique key used to determine row identity will still be replicated, however, because there are checks in place to ensure those columns are not excluded.
  * Source table trigger will only fire on UPDATES of the given columns (uses UPDATE OF col1 [, col2...]).
 * p_condition, an optional argument, is used to set criteria for specific rows that should be replicated. See additional notes in **About** section above.
 * p_pk_name, an optional argument, is an array of the columns that make up the primary/unique key on the source table. This overrides the automatic retrieval from the source.
 * p_pk_type, an optional argument, is an array of the column types that make up the primary/unique key on the source table. This overrides the automatic retrieval from the source. Ensure the types are in the same order as p_pk_name.
 * p_jobmon, an optional argument, sets whether pg_jobmon logging will be used to log the running of this maker function AND whether pg_jobmon will be used to log refresh runs of the given table. Default is assumed to be true if pg_jobmon is installed and false if it is not.

*snapshot_maker(p_src_table text, p_dblink_id int, p_dest_table text DEFAULT NULL, p_index boolean DEFAULT true, p_filter text[] DEFAULT NULL, p_condition text DEFAULT NULL, p_pulldata boolean DEFAULT true, p_jobmon boolean DEFAULT NULL, p_debug boolean DEFAULT false)*  
 * Function to automatically setup snapshot replication for a table. By default source and destination table will have same schema and table names.  
 * Destination table CANNOT exist first due to the way the snapshot system works (view /w two tables).
 * p_dblink_id is the data_source_id from the dblink_mapping_mimeo table for where the source table is located.
 * p_dest_table, an optional argument, is to set a custom destination table. Be sure to schema qualify it if needed.
 * p_index, an optional argument, sets whether to recreate all indexes that exist on the source table on the destination. Defaults to true. Note this is only applies during replication setup. Future index changes on the source will not be propagated.
 * p_filter, an optional argument, is an array list that can be used to designate only specific columns that should be used for replication. Be aware that if this option is used, indexes cannot be replicated from the source because there is currently no easy way to determine all types of indexes that may be affected by the columns that are excluded.
 * p_condition, an optional argument, is used to set criteria for specific rows that should be replicated. See additional notes in **About** section above.
 * p_pulldata, an optional argument, allows you to control if data is pulled as part of the setup. Set to 'false' to configure replication with no initial data.
 * p_jobmon, an optional argument, sets whether pg_jobmon logging will be used to log the running of this maker function AND whether pg_jobmon will be used to log refresh runs of the given table. Default is assumed to be true if pg_jobmon is installed and false if it is not.

*table_maker(p_src_table text, p_dblink_id int, p_dest_table text DEFAULT NULL, p_index boolean DEFAULT true, p_filter text[] DEFAULT NULL, p_condition text DEFAULT NULL, p_sequences text[] DEFAULT NULL, p_pulldata boolean DEFAULT true, p_jobmon boolean DEFAULT NULL, p_debug boolean DEFAULT false)*
 * Function to automatically setup plain table replication. By default source and destination table will have same schema and table names.  
 * p_dblink_id is the data_source_id from the dblink_mapping_mimeo table for where the source table is located.
 * p_dest_table, an optional argument, is to set a custom destination table. Be sure to schema qualify it if needed.
 * p_index, an optional argument, sets whether to recreate all indexes that exist on the source table on the destination. Defaults to true. Note this is only applies during replication setup. Future index changes on the source will not be propagated.
 * p_filter, an optional argument, is a text array list that can be used to designate only specific columns that should be used for replication. Be aware that if this option is used, indexes cannot be replicated from the source because there is currently no easy way to determine all types of indexes that may be affected by the columns that are excluded. 
 * p_condition, an optional argument, is used to set criteria for specific rows that should be replicated. See additional notes in **About** section above.
 * p_sequences, an optional argument, is a text array list of schema qualified sequences used as default values in the destination table. This maker function does NOT automatically pull sequences from the source database. If you require that sequences exist on the destination, you'll have to create them and alter the table manually. This option provides an easy way to add them if your destination table exists and already has sequences. The maker function will not reset them. Run the refresh function to have them reset.
 * p_pulldata, an optional argument, allows you to control if data is pulled as part of the setup. Set to 'false' to configure replication with no initial data.
 * p_jobmon, an optional argument, sets whether pg_jobmon logging will be used to log the running of this maker function AND whether pg_jobmon will be used to log refresh runs of the given table. Default is assumed to be true if pg_jobmon is installed and false if it is not.

*updater_maker(p_src_table text, p_type text, p_control_field text, p_dblink_id int, p_boundary text DEFAULT NULL, p_dest_table text DEFAULT NULL, p_index boolean DEFAULT true, p_filter text[] DEFAULT NULL, p_condition text DEFAULT NULL, p_pulldata boolean DEFAULT true, p_pk_name text[] DEFAULT NULL, p_pk_type text[] DEFAULT NULL, p_jobmon boolean DEFAULT NULL, p_debug boolean DEFAULT false)*
 * Function to automatically setup updater replication for a table. By default source and destination table will have same schema and table names.  
 * Source table must have a primary key or unique index. Either the primary key or a unique index (first in alphabetical order if more than one) on the source table will be obtained automatically. Columns of primary/unique key cannot be arrays nor can they be an expression.  
 * If destination table already exists, no data will be pulled from the source. You can use the refresh_updater() 'repull' option to truncate the destination table and grab all the source data. Or you can set the config table's 'last_value' column for your specified table to designate when it should start. Otherwise last_value will default to the destination's max value for the control field or, if null, the time that the maker function was run.
 * p_type determines whether it is time-based or serial-based incremental replication. Valid values are: "time" or "serial".
 * p_control_field is the column which is used as the control field (a timestamp or integer column that is new for every insert AND update).
 * p_dblink_id is the data_source_id from the dblink_mapping_mimeo table for where the source table is located.  
 * p_boundary, an optional argument, is a boundary value to prevent records being missed at the upper boundary of the batch. Argument type is text, but must be able to be converted to an interval for time-based and an integer for serial-based. This is mostly only relevant for time based replication. Set this to a value that will ensure all inserts will have finished for that time period when the replication runs. For time based the default is 10 minutes which means the destination may always be 10 minutes behind the source but that also means that all inserts on the source will have finished by the time 10 minutes has passed. For serial based replication, the default is 10. If your serial column is based on a sequence you should be able to change this to zero safely. The upper boundary will always be one less than the max at the time replication runs. If it's not based on a sequence, you'll have to set this to a value to ensure that the source is done inserting that range of numerical values by the time replication runs. For example, if you set this to 10, the destination will always be one less than "max(source_control_col) - 10" behind the source.
 * p_dest_table, an optional argument,  is to set a custom destination table. Be sure to schema qualify it if needed.
 * p_index, an optional argument, sets whether to recreate all indexes that exist on the source table on the destination. Defaults to true. Note this is only applies during replication setup. Future index changes on the source will not be propagated.
 * p_filter, an optional argument, is an array list that can be used to designate only specific columns that should be used for replication. Be aware that if this option is used, indexes cannot be replicated from the source because there is currently no easy way to determine all types of indexes that may be affected by the columns that are excluded. The primary/unique key used to determine row identity will still be replicated, however, because there are checks in place to ensure those columns are not excluded.
 * p_condition, an optional argument, is used to set criteria for specific rows that should be replicated. See additional notes in **About** section above.
 * p_pulldata, an optional argument, allows you to control if data is pulled as part of the setup. Set to 'false' to configure replication with no initial data.
 * p_pk_name, an optional argument, is an array of the columns that make up the primary/unique key on the source table. This overrides the automatic retrieval from the source.
 * p_pk_type, an optional argument, is an array of the column types that make up the primary/unique key on the source table. This overrides the automatic retrieval from the source. Ensure the types are in the same order as p_pk_name.
 * p_jobmon, an optional argument, sets whether pg_jobmon logging will be used to log the running of this maker function AND whether pg_jobmon will be used to log refresh runs of the given table. Default is assumed to be true if pg_jobmon is installed and false if it is not.

### Refresh Functions

*refresh_dml(p_destination text, p_limit int default NULL, p_repull boolean DEFAULT false, p_jobmon boolean DEFAULT NULL, p_lock_wait int DEFAULT NULL, p_insert_on_fetch boolean DEFAULT NULL, p_debug boolean DEFAULT false)* 
 * Replicate tables by replaying INSERTS, UPDATES and DELETES in the order they occur on the source table. Useful for tables that are too large for snapshots.  
 * Can be setup with dml_maker(...) and removed with dml_destroyer(...) functions.  
 * p_limit, an optional argument, can be used to change the limit on how many rows are grabbed from the source with each run of the function. Defaults to all new rows if not given here or set in configuration table. Has no affect on function performance as it does with inserter/updater.
 * p_repull, an optional argument, sets a flag to repull data from the source instead of getting new data. Note that **ALL local data will be truncated** and the ENTIRE source table will be repulled.
 * p_jobmon, an optional argument, sets whether to use jobmon for the refresh run. By default uses config table value.
 * p_lock_wait, an optional argument, sets whether you want this refresh call to wait for the advisory lock on this table to be released if it is being held. See the **concurrent_lock_check()** function info in this document for more details on what are valid values for this parameter.
 * p_insert_on_fetch, an optional argument. This function batches data to process from a cursor on the source database.  Each batch is processed in full on the destination before the next is fetched to minimize disk space used, and protect against large batches using significant destination resources. If destination processing is slow (synchronous replication delays, constraint validation, etc) this extends how long share locks are held on the source database. For some workloads, this is a poor trade-off. Setting this option to false will cause all data to be fetched to a temporary table first so that locks can be released before destination processing. Note this can potentially cause very large temp tables, but can greatly lessen the transaction time on the source database. This can be set permanently in the refresh_config_dml table as well. This function argument will always override the config table value.


*refresh_inserter(p_destination text, p_limit integer DEFAULT NULL, p_repull boolean DEFAULT false, p_repull_start text DEFAULT NULL, p_repull_end text DEFAULT NULL, p_jobmon boolean DEFAULT NULL, p_lock_wait int DEFAULT NULL, p_debug boolean DEFAULT false)*  
 * Replication for tables that have INSERT ONLY data and contain a timestamp or integer column that is incremented with every INSERT.
 * Can be setup with inserter_maker(...) and removed with inserter_destroyer(...) functions.  
 * p_limit, an optional argument, can be used to change the limit on how many rows are grabbed from the source with each run of the function. Defaults to all new rows if not given here or set in configuration table. Note that this makes the refresh function slightly more expensive to run as extra checks must be run to ensure data consistency.
 * p_repull, an optional argument, sets a flag to repull data from the source instead of getting new data. If this flag is set without setting the start/end arguments as well, then **ALL local data will be truncated** and the ENTIRE source table will be repulled.
 * p_repull_start and p_repull_end, optional arguments, can set a specific time period to repull source data. This is an EXCLUSIVE time period (< start, > end). If p_repull is not set, then these arguments are ignored.
 * p_jobmon, an optional argument, sets whether to use jobmon for the refresh run. By default uses config table value.
 * p_lock_wait, an optional argument, sets whether you want this refresh call to wait for the advisory lock on this table to be released if it is being held. See the **concurrent_lock_check()** function info in this document for more details on what are valid values for this parameter.
    
*refresh_logdel(p_destination text, p_limit int DEFAULT NULL, p_repull boolean DEFAULT false, p_jobmon boolean DEFAULT NULL, p_lock_wait int DEFAULT NULL, p_insert_on_fetch boolean DEFAULT NULL, p_debug boolean DEFAULT false)*
 * Replicate tables by replaying INSERTS, UPDATES and DELETES in the order they occur on the source table, but DO NOT remove deleted tables from the destination table.
 * Can be setup with logdel_maker(...) and removed with logdel_destroyer(...) functions.  
 * p_limit, an optional argument, can be used to change the limit on how many rows are grabbed from the source with each run of the function. Defaults to all new rows if not given here or set in configuration table. Has no affect on function performance as it does with inserter/updater.
 * p_repull, an optional argument, sets a flag to repull data from the source instead of getting new data. Unlike other refresh repull options this does NOT do a TRUNCATE; it deletes all rows where mimeo_source_deleted is not null, so old deleted rows are not lost on the destination. It is highly recommended to do a manual VACUUM after this is done, possibly even a VACUUM FULL to reclaim disk space.
 * p_jobmon, an optional argument, sets whether to use jobmon for the refresh run. By default uses config table value.
 * p_lock_wait, an optional argument, sets whether you want this refresh call to wait for the advisory lock on this table to be released if it is being held. See the **concurrent_lock_check()** function info in this document for more details on what are valid values for this parameter.
 * p_insert_on_fetch, an optional argument. This function batches data to process from a cursor on the source database.  Each batch is processed in full on the destination before the next is fetched to minimize disk space used, and protect against large batches using significant destination resources. If destination processing is slow (synchronous replication delays, constraint validation, etc) this extends how long share locks are held on the source database. For some workloads, this is a poor trade-off. Setting this option to false will cause all data to be fetched to a temporary table first so that locks can be released before destination processing. Note this can potentially cause very large temp tables, but can greatly lessen the transaction time on the source database. This can be set permanently in the refresh_config_logdel table as well. This function argument will always override the config table value.

*refresh_snap(p_destination text, p_index boolean DEFAULT true, p_pulldata boolean DEFAULT true, p_jobmon boolean DEFAULT NULL, p_lock_wait int DEFAULT NULL, p_check_stats boolean DEFAULT NULL, p_debug boolean DEFAULT false)*
 * Full table replication to the destination table given by p_destination. Automatically creates destination view and tables needed if they do not already exist. * If data has not changed on the soure (insert, update or delete), no data will be repulled. pg_jobmon still records that the job ran successfully and updates last_run, so you are still able to monitor that tables using this method are refreshed on a regular basis. It just logs that no new data was pulled.
 * Can be setup with snapshot_maker(...) and removed with snapshot_destroyer(...) functions.  
 * p_index, an optional argument, sets whether to recreate all indexes if any of the columns on the source table change. Defaults to true. Note this only applies when the columns on the source change, not the indexes.
 * p_pulldata, does not generally need to be used and in most cases can just be ignored. It is primarily for internal use by the maker function to allow its p_pulldata parameter to work.
 * p_jobmon, an optional argument, sets whether to use jobmon for the refresh run. By default uses config table value.
 * p_lock_wait, an optional argument, sets whether you want this refresh call to wait for the advisory lock on this table to be released if it is being held. See the **concurrent_lock_check()** function info in this document for more details on what are valid values for this parameter.

*refresh_table(p_destination text, p_truncate_cascade boolean DEFAULT NULL, p_jobmon boolean DEFAULT NULL, p_lock_wait int DEFAULT NULL, p_debug boolean DEFAULT false)*  
 * A basic replication method that simply truncates the destination and repulls all the data.
 * Not ideal for normal replication but is useful for dev systems that need to pull from a production system and should have no write access on said system. It requires no primary keys, control columns or triggers/queues on the source.
 * Can be setup with table_maker(...) and removed with table_destroyer(...) functions.
 * If the destination table has any sequences, they can be reset by adding them to the *sequences* array column in the *refresh_config_table* table or with the *p_sequences* option in the maker function. Note this will check all tables on the destination database that have the given sequences set as a default at the time the refresh is run and reset the sequence to the highest value found.
 * p_truncate_cascade, an optional argument that will cascade the truncation of the given table to any tables that reference it with foreign keys. This argument is here to provide an override to the config table option. Both this parameter and the config table default to NOT doing a cascade, so doing this should be a conscious choice.
 * p_jobmon, an optional argument, sets whether to use jobmon for the refresh run. By default uses config table value.
 * p_lock_wait, an optional argument, sets whether you want this refresh call to wait for the advisory lock on this table to be released if it is being held. See the **concurrent_lock_check()** function info in this document for more details on what are valid values for this parameter.
 * p_check_stats, an optional argument, sets whether to check the source table statistics to see whether the table change had any changes. If true, the refresh will check the source stats and if nothing has changed, no data will be repulled. If set to false, it will NOT check statistics and will ALWAYS repull data. The default is to assume true and check the statistics. This setting can be made permanent for a given table by setting the option in the refresh_config_snap table. Setting this parameter will override the config table value.

*refresh_updater(p_destination text, p_limit integer DEFAULT NULL, p_repull boolean DEFAULT false, p_repull_start text DEFAULT NULL, p_repull_end text DEFAULT NULL, p_jobmon boolean DEFAULT NULL, p_lock_wait int DEFAULT NULL, p_debug boolean DEFAULT false)*  
 * Replication for tables that have INSERT AND/OR UPDATE ONLY data and contain a timestamp or integer column that is incremented with every INSERT AND UPDATE
 * Can be setup with updater_maker(...) and removed with updater_destroyer(...) functions.  
 * p_limit, an optional argument, can be used to change the limit on how many rows are grabbed from the source with each run of the function. Defaults to all new rows if not given here or set in configuration table. Note that this makes the refresh function slightly more expensive to run as extra checks must be run to ensure data consistency.
 * p_repull, an optional argument, sets a flag to repull data from the source instead of getting new data. If this flag is set without setting the start/end arguments as well, then **ALL local data will be truncated** and the ENTIRE source table will be repulled.
 * p_repull_start and p_repull_end, optional arguments, can set a specific time period to repull source data. This is an EXCLUSIVE time period (< start, > end). If p_repull is not set, then these arguments are ignored.
 * p_jobmon, an optional argument, sets whether to use jobmon for the refresh run. By default uses config table value.
 * p_lock_wait, an optional argument, sets whether you want this refresh call to wait for the advisory lock on this table to be released if it is being held. See the **concurrent_lock_check()** function info in this document for more details on what are valid values for this parameter.

### Cleanup Functions

*dml_destroyer(p_dest_table text, p_keep_table boolean DEFAULT true)*  
 * Function to automatically remove a dml replication table from the destination. This will also automatically remove the associated objects from the source database if the dml_maker() function was used to create it.  
 * p_keep_table is an optional, boolean parameter to say whether you want to keep or destroy your destination database table. Defaults to true to help prevent you accidentally destroying the destination when you didn't mean to. 
 * Be aware that only the owner of a table can drop triggers, so this function will fail if the source database mimeo role does not own the source table. Dropping the mimeo trigger first should allow the destroyer function to run successfully and clean the rest of the objects up. This is the way PostgreSQL permissions are currently setup and there's nothing I can do about it.

*inserter_destroyer(p_dest_table text, p_keep_table boolean DEFAULT true)*  
 * Function to automatically remove an inserter replication table from the destination.  
 * p_keep_table is an optional, boolean parameter to say whether you want to keep or destroy your destination database table. Defaults to true to help prevent you accidentally destroying the destination when you didn't mean to. 

*logdel_destroyer(p_dest_table text, p_keep_table boolean DEFAULT true)*  
 * Function to automatically remove a logdel replication table from the destination. This will also automatically remove the associated objects from the source database if the dml_maker() function was used to create it.  
 * p_keep_table is an optional, boolean parameter to say whether you want to keep or destroy your destination database table. Defaults to true to help prevent you accidentally destroying the destination when you didn't mean to. 
 * Be aware that only the owner of a table can drop triggers, so this function will fail if the source database mimeo role does not own the source table. Dropping the mimeo trigger first should allow the destroyer function to run successfully and clean the rest of the objects up. This is the way PostgreSQL permissions are currently setup and there's nothing I can do about it.

*snapshot_destroyer(p_dest_table text, p_keep_table boolean DEFAULT true)*  
 * Function to automatically remove a snapshot replication table from the destination.  
 * p_keep_table is an optional, boolean parameter to say whether you want to keep or destroy your destination database table. Defaults to true to help prevent you accidentally destroying the destination when you didn't mean to. 
  * Turns what was the view into a real table. Most recent snap is just renamed to the old view name, so all permissions, indexes, constraints, etc should be kept.  

*table_destroyer(p_dest_table text, p_keep_table boolean DEFAULT true)*  
 * Function to automatically remove a plain replication table from the destination.  
 * p_keep_table is an optional, boolean parameter to say whether you want to keep or destroy your destination database table. Defaults to true to help prevent you accidentally destroying the destination when you didn't mean to. 

*updater_destroyer(p_dest_table text, p_keep_table boolean DEFAULT true)*  
 * Function to automatically remove an updater replication table from the destination.  
 * p_keep_table is an optional, boolean parameter to say whether you want to keep or destroy your destination database table. Defaults to true to help prevent you accidentally destroying the destination when you didn't mean to. 

### Maintenance Functions

*validate_rowcount(p_destination text, p_lower_interval text DEFAULT NULL, p_upper_interval text DEFAULT NULL, p_debug boolean DEFAULT false, OUT match boolean, OUT source_count bigint, OUT dest_count bigint, OUT min_dest_value text, OUT max_dest_value text) RETURNS record*
 * This function can be used to compare the row count of the source and destination tables of a replication set.
 * Always returns the row counts of the source and destination and a boolean that says whether they match.
 * If checking an incremental replication job, will return the min & max values for the boundries of what rows were counted.
 * For snapshot, table & inserter replication, the rowcounts returned should match exactly.
 * For updater, dml & logdel replication, the row counts may not match due to the nature of the replicaton method.
 * Note that replication will be stopped on the designated table when this function is run with any replication method other than inserter/updater to try and ensure an accurate count. However, this is very difficult with any replication method other than incremental if new data is still being added on the source while validation runs.
 * p_lower_interval - used when checking incremental replication to limit the interval of data checked. This value is always given as text, but must be able to be cast to either an interval (time replication) or integer (serial replication). The value is calculated by getting the current maxiumum control value on the destination and subtracting the given interval. So if you wanted to limit the data checked to newer than the last 30 days, set this value to '30 days'.
 * p_upper_interval - Similar to p_lower_interval, but this sets the upper boundary for the interval to check. This is useful when newer, recent data is still being entered and could throw off a rowcount comparison. For example, if data within the last day is still being entered on the source, set this value to '1 day' to have it ignore the most recent day's data. Note that if a boundary value exists in the refresh_config table, that will still be used even if this value is left NULL. But if this value is set, it will always override the configured boundary value.

*check_missing_source_tables(p_data_source_id int DEFAULT NULL, p_views boolean DEFAULT false, OUT schemaname text, OUT tablename text, OUT data_source int) RETURNS SETOF record*
 * Provides monitoring capability for situations where all tables on source should be replicated.
 * Note this does not check for source views.
 * p_data_source_id - optional parameter to check one specific data source. Otherwise, all sources listed in dblink_mapping_mimeo table are checked.
 * p_views - optional parameter to include views from the source database. By default views are not included (false).
 * Returns a record value so WHERE conditions can be used to ignore tables that aren't desired.

*check_source_columns(p_data_source_id int DEFAULT NULL, OUT dest_schemaname text, OUT dest_tablename text, OUT src_schemaname text, OUT src_tablename text, OUT missing_column_name text, OUT missing_column_type text, OUT data_source int) RETURNS SETOF record*
 * Provides monitoring capability for source table columns changes not getting applied to the destination.
 * Also checks if column types have changed.
 * Accounts for when the "filter" configuration option is used to only grab specific columns.
 * Does not check if destination has columns that source does not.
 * p_data_source_id - optional parameter to check one specific data source. Otherwise, all sources listed in dblink_mapping_mimeo table are checked.
 * Returns a record value so WHERE conditions can be used to ignore tables and/or columns that don't matter for your situation.

*concurrent_lock_check(p_dest_table text, p_lock_wait int DEFAULT NULL) RETURNS boolean*
 * Mimeo uses the advisory lock system to ensure concurrent runs of a replication job on a single table do not occur. You can use this function to obtain a lock if one is available.
 * This function works like the pg_try_advisory_xact_lock() function, returning true if a lock was able to be obtained and false if it was not. So if it returns false, your app must be able to handle that failure scenario and either fail gracefully or retry. A delay between retries is highly recommendend and the length of that delay should be determined by how long a refresh run of the given table usually takes.
 * This lock must be obtained before operating on any destination table being maintained by mimeo. Failure to do so could result in a deadlock. An example of this is when a column filter is in place to not replicate all table columns, or the destination has additional columns, and you need to edit the destination table via another method and not interfere with the normal replication jobs. 
 * p_dest_table - Destination table on which to obtain a lock 
 * p_lock_wait - set a specified period of time to wait for the advisory lock before giving up. The following are valid values:
   * NULL (default value if not set): Do not wait at all for an advisory lock and immediately return FALSE if one cannot be obtained.
   * > 0: Keep retrying to obtain an advisory lock for the given table for this number of seconds before giving up and returning FALSE. If lock is obtained in this time period, will immediately return TRUE.
   * <= 0: Keep retrying indefinitely to obtain an advisory lock. Will only return when the lock is obtained and returns TRUE. Ensure your code handles this condition properly to avoid infinite waits.

*snapshot_monitor(p_rowcount bigint DEFAULT NULL::bigint, p_size bigint DEFAULT NULL::bigint, p_destination text DEFAULT NULL::text, p_debug boolean DEFAULT false) RETURNS TABLE(dest_tablename text, source_rowcount bigint, source_size bigint)*
 * Function to monitor if snapshot replication tables are possibly becoming too large to replicate in their entirety every refresh run. Given parameters below to set boundaries, tables that are returned should be reviewed for possibly changing to a more efficient replication method for their size and use-case (incremental or dml).
 * p_rowcount - parameter to set the minimum number of rows the source table has to trigger that it is too large to replicate via snapshot.
 * p_size - parameter to set the minimum size the source table should be to trigger that it is too large to replicate via snapshot.
 * p_destination - if this is set, only the given table is checked for rowcount/size.
 * Returns the destination tablename along with the rowcount and size (in bytes) obtained from the source. When both rowcount and size parameters are set at the same time, if either one matches, the snapshot destination table name will be returned. If only one is set, only that condition is considered when filtering results.


### Tables

*dblink_mapping_mimeo*  
    Stores all source database connection data

    data_source_id  - automatically assigned ID number for the source database connection
    data_source     - dblink string for source database connection
    username        - role that mimeo uses to connect to the source database
    pwd             - password for above role
    dbh_attr        - currently unused. If someone finds they need special connection attributes let me know and I'll work on incorporating this sooner.

*refresh_config*  
    Parent table for all config types. All child config tables below contain these columns. No data is actually stored in this table

    dest_table      - Tablename on destination database. If not public, should be schema qualified
    type            - Type of replication. Must of one of the following values: snap, inserter_time, inserter_serial, updater_time, updater_serial, dml, logdel
    dblink          - Foreign key on the data_source_id column from dblink_mapping_mimeo table
    last_run        - Timestamp of the last run of the job. Used by run_refresh,py to know when to do the next run of a job.
    filter          - Array containing specific column names that should be used in replication.
    condition       - Used to set criteria for specific rows that should be replicated. See additional notes in **About** section above.
    period          - Interval used for the run_refresh.py script to indicate how often this refresh job should be run at a minimum
    batch_limit     - Limit the number of rows to be processed for each run of the refresh job. If left NULL (the default), all new rows are fetched every refresh.
    jobmon          - Boolean to determine whether to use pg_jobmon extension to log replication steps. 
                      Maker functions set to true by default if it is installed. Otherwise defaults to false.

*refresh_config_snap*  
    Child of refresh_config. Contains config info for snapshot replication jobs.

    source_table    - Table name from source database. If not public, should be schema qualified.
    n_tup_ins       - Tracks the number of inserts done on the source to determine whether data needs to be repulled.
    n_tup_upd       - Tracks the number of updates done on the source to determine whether data needs to be repulled.
    n_tup_del       - Tracks the number of deletes done on the source to determine whether data needs to be repulled.
    check_stats     - Boolean value to set whether this table checks the source statistics to determine whether to repull data.
                      If true, statistics will be checked and if nothing has changed on the source, the data will NOT be repulled.
                      If false, statistics will NOT be checked and data will ALWAYS be repulled.
    post_script     - Text array of commands to run should the source columns ever change. Each value in the array is run as a single command.
                      Should contain commands for things such as recreating indexes that are different than the source, but needed on the destination.

*refresh_config_inserter*
    Child of refresh_config. Template table for inserter config tables.

    source_table    - Table name from source database. If not public, should be schema qualified
    control         - Column name that contains the timestamp that is updated on every insert

*refresh_config_inserter_time*
    Child of refresh_config_inserter. Contains config info for time-based inserter replication jobs.

    boundary        - Interval to adjust upper boundary max value of control field. Default is 10 minutes. See inserter_maker() for more info.
    last_value      - This is the max value of the control field from the last run and controls the time period of the batch of data pulled from the source table. 
    dst_active      - Boolean set to true of database is not running on a server in UTC/GMT time. See About for more info
    dst_start       - Integer representation of the time that DST starts. Ex: 00:30 would be 30
    dst_end         - Integer representation of the time that DST ends. Ex: 02:30 would be 230

*refresh_config_inserter_updater*
    Child of refresh_config_inserter. Contains config info for serial-based inserter replication jobs.

    boundary        - Integer value to adjust upper boundary max value of control field. 
                      Default is 0, but upper boundary is always 1 less than source max at time of refresh. See inserter_maker() for more info.
    last_value      - This is the max value of the control field from the last run and controls the range of the batch of data pulled from the source table. 

*refresh_config_updater*
    Child of refresh_config. Template table for updater config tables.

    source_table    - Table name from source database. If not public, should be schema qualified
    control         - Column name that contains the timestamp that is updated on every insert AND update
    pk_name         - Text array of all the column names that make up the source table primary key
    pk_type         - Text array of all the column types that make up the source table primary key

*refresh_config_updater_time*
    Child of refresh_config_updater. Contains config info for time-based updater replication jobs.

    boundary        - Interval to adjust upper boundary max value of control field. Default is 10 minutes. See updater_maker() for more info.
    last_value      - This is the max value of the control field from the last run and controls the time period of the batch of data pulled from the source table. 
    dst_active      - Boolean set to true of database is not running on a server in UTC/GMT time. See About for more info
    dst_start       - Integer representation of the time that DST starts. Ex: 00:30 would be 30
    dst_end         - Integer representation of the time that DST ends. Ex: 02:30 would be 230

*refresh_config_updater_serial*
    Child of refresh_config_updater. Contains config info for serial-based updater replication jobs.

    boundary        - Integer value to adjust upper boundary max value of control field. 
                      Default is 0, but upper boundary is always 1 less than source max at time of refresh. See updater_maker() for more info.
    last_value      - This is the max value of the control field from the last run and controls the range of the batch of data pulled from the source table. 

*refresh_config_dml*  
    Child of refresh_config. Contains config info for dml replication jobs.

    source_table    - Table name from source database. If not public, should be schema qualified
    control         - Schema qualified name of the queue table on the source database for this table
    pk_name         - Text array of all the column names that make up the source table primary key
    pk_type         - Text array of all the column types that make up the source table primary key
    insert_on_fetch - See refresh_dml() function for details on this setting. 
 
*refresh_config_logdel*  
    Child of refresh_config. Contains config info for logdel replication jobs.

    source_table    - Table name from source database. If not public, should be schema qualified
    control         - Schema qualified name of the queue table on the source database for this table
    pk_name         - Text array of all the column names that make up the source table primary key
    pk_type         - Text array of all the column types that make up the source table primary key
    insert_on_fetch - See refresh_logdel() function for details on this setting. 

*refresh_config_table*  
    Child of refresh_config. Contains config info for plain table replication jobs.

    source_table        - Table name from source database. If not public, should be schema qualified
    truncate_cascade    - Boolean that causes the truncate part of the refresh to cascade to any tables that reference it with foreign keys. 
                          Defaults to FALSE. To change this you must manually update the config table and set it to true. Be EXTREMELY careful with this option.
    sequences           - An optional text array that can contain the schema qualified names of any sequences used as default values in the destination table. 
                          These sequences will be reset every time the refresh function is run, checking all tables that use the sequence as a default value.
    
### Scripts

*run_refresh.py*
 * A python script to automatically run replication for tables that have their ''period'' set in the config table.
 * This script can be run as often as needed and refreshes will only fire if their interval period has passed.
 * By default, refreshes are run sequentially in ascending order of their last_run value. Parallel option is available.
 * --connection (-c)  Option to set the psycopg connection string to the database. Default is "host=" (local socket).
 * --type (-t)  Option to set which type of replication to run (snap, inserter, updater, dml, logdel, table). Default is all types.
 * --batch_limit (-b)  Option to set how many tables to replicate in a single run of the script. Default is all jobs scheduled to run at time script is run.
 * --jobs (-j) Allows parallel running of replication jobs. Set this equal to the number of processors you want to use to allow that many jobs to start simultaneously (uses multiprocessing library, not threading).
 * Please see the howto.md file for some examples.

### Extras
Note that items here are not kept up to date as frequently as the main extension functions. If you attempt to use these and have any issues, please report them on github and they will be fixed ASAP.

*refresh_snap_pre90.sql*
 * Alternate function for refresh_snap to provide a way to use a pre-9.0 version of PostgreSQL as the source database.
 * Useful if you're using mimeo to upgrade PostgreSQL across major versions.
 * Please read the notes in the top of this sql file for more important information.
 * Requires one of the custom array_agg functions provided if source is less than v8.4.

*snapshot_maker_pre90.sql*
 * Alternate function for snapshot_maker to provide a way to use a pre9.0 version of PostgreSQL as the source database.
 * Requires one of the custom array_agg functions provided if source is less than v8.4.
 * Requires "refresh_snap_pre90" to be installed as "refresh_snap_pre90" in mimeo extension schema.

*dml_maker_pre90.sql*
 * Alternate function for dml_maker() to provide a way to use a pre-9.0 version of PostgreSQL as the source database.
 * Also requires "refresh_snap_pre90" to be installed as "refresh_snap_pre90" in mimeo extension schema.
 * Useful if you're using mimeo to upgrade PostgreSQL across major versions.
 * Please read the notes in the top of this sql file for more important information.
 * Requires one of the custom array_agg functions provided if source is less than v8.4.

*dml_maker_81.sql*
 * Same as dml_maker_pre90, but for v8.1.
  
*refresh_dml_pre91.sql*
 * Alternate function for refresh_dml() to provide a way to use a pre-9.1 version of PostgreSQL as the source.
 * Useful if you're using mimeo to upgrade PostgreSQL across major versions. 
 * Please read the notes in the top of this sql file for more important information.
 * Requires one of the custom array_agg functions provided if source is less than v8.4.

*refresh_dml_81.sql*
 * Same as refresh_dml_pre91, but for v8.1. 

*array_agg_pre84.sql*
 * An array aggregate function to be installed on the source database if it is less than major version 8.4 but greater than major version 8.1.

*array_agg_81.sql*
 * An array aggreate function to be installed on the source database if it is version 8.1.
