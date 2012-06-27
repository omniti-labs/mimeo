mimeo
=====

Setup
-----

The **dblink_mapping** table contains the configuration information for the source database (where data is copied FROM). You can define as many data sources as you wish. The data source for a replicated table is declared just once in the refresh_config table mentioned below.

    insert into mimeo.dblink_mapping (data_source, username, pwd, dbh_attr) 
    values ('host=pghost.com port=5432 dbname=pgdb', 'refresh', 'password', null);

The data_source value is the connection format required by dblink.
**username** and **pwd** are the credentials for connecting to the source database.
The **dbh_attr** column in this table is used for ...

The role on the source database(s) should have at minimum select access on all tables/views to be replicated. For DML replication, you will also have to grant update/delete permissions on the queue table.

Some additional setup is required on the source database for DML replication...

Functions
---------

*refresh_snap(p_destination text, p_debug boolean)*  
    Full table replication to the destination table given by p_destination. Automatically creates destination view and tables needed if they do not already exist.  
    Can be setup with snapshot_maker(...) and removed with snapshot_destroyer(...) functions.  
    Passing TRUE to second argument turns on debugging output to see details in real-time.  

*refresh_inserter(p_destination text, p_debug boolean, int default 100000)*  
    Replication for tables that have INSERT ONLY data and contain a timestamp column that is incremented with every INSERT.
    Can be setup with inserter_maker(...) and removed with inserter_destroyer(...) functions. (coming soon)  
    Passing TRUE to second argument turns on debugging output to see details in real-time.  
    Third, optional argument can be used to change the limit on how many rows are grabbed from the source with each run of the function.
    
*refresh_updater(p_destination text, p_debug boolean, int default 100000)*  
    Replication for tables that have INSERT AND/OR UPDATE ONLY data and contain a timestamp column that is incrementend with every INSERT AND UPDATE
    ...SETUP...
    Passing TRUE to second argument turns on debugging output to see details in real-time.  
    Third, optional argument can be used to change the limit on how many rows are grabbed from the source with each run of the function.

*refresh_dml(p_destination text, p_debug boolean, int default 100000)*  
    Replicate tables by replaying INSERTS, UPDATES and DELETES in the order they occur on the source table. Useful for tables that are too large for snapshots.  
    ...SETUP...
    Passing TRUE to second argument turns on debugging output to see details in real-time.  
    Third, optional argument can be used to change the limit on how many rows are grabbed from the source with each run of the function.

*refresh_logdel(p_destination text, p_debug boolean, int default 100000)*  
    Replicate tables by replaying INSERTS, UPDATES and DELETES in the order they occur on the source table, but DO NOT remove deleted tables from the destination table.
    Logs a timestamp of when the row was deleted in an additional column on the destination table.
    ...SETUP...
    Passing TRUE to second argument turns on debugging output to see details in real-time.  
    Third, optional argument can be used to change the limit on how many rows are grabbed from the source with each run of the function.

*snapshot_maker(p_src_table text, p_dblink_id int)*  
    Function to automatically setup snapshot replicatation for a table. Source and destination table will have same scheme and table names.  
    Pass the data_source_id from the dblink_mapping table as the second argument for where the source table is located.

*snapshot_maker(p_src_table text, p_dest_table text, p_dblink_id int)*  
    Function to automatically setup snapshot replication for a table. Allows setting of custom destination table. Include schema name in the destination table parameter.  
    Pass the data_source_id from the dblink_mapping table as the third argument for where the source table is located.

*snapshot_destroyer(p_dest_table text, p_archive_option text)*  
    Function to automatically remove a snapshot replication table from the destination.  
    Pass 'ARCHIVE' as the second argument to keep a permanent copy of the snapshot table on the destination. Turns what was the view into a real table. 
    Pass any other value to completely remove everything.
    
    
