mimeo
=====

Setup
-----

The **dblink_mapping** table contains the configuration information for the source database (where data is copied FROM). You can define as many data sources as you wish. The data source for a replicated table is declared just once in the refresh_config table mentioned below.
    insert into mimeo.dblink_mapping (data_source_id, data_source, username, pwd, dbh_attr) 
    values (nextval('mimeo.dblink_mapping_data_source_id_seq'), 'host=pghost.com port=5432 dbname=pgdb', 'refresh', 'password', null);
There is a sequence (seen in example) that should be used for the data_source_id.
The data_source value is the connection format required by dblink.
username and pwd is the credentials for connecting to the source database.
The **dbh_attr** column in this table is used for ...

The role on the source database(s) should have at minimum select access on all tables/views to be replicated. For DML replication, you will also have to grant update/delete permissions on the queue table.

Some additional setup is required on the source database for DML replication...

Functions
---------

*refresh_snap(p_destination text, p_debug boolean)*  
    Full table replication to the destination table given by p_destination. Automatically creates destination view and tables needed if they do not already exist.  
    Configuration for replication is contained in refresh_config table.  
    Passing TRUE to second argument turns on debugging output to see details in real-time.  

*refresh_incremental(p_destination text, p_debug boolean)*  
    


*refresh_dml(p_destination text, p_debug boolean)*  
    
    
