/*
 * Checks monitoring functions to ensure they catch new source tables and source column changes on non-snap tables
 * Further checks that snapshot replication copies column changes
 */
\set ON_ERROR_ROLLBACK 1
\set ON_ERROR_STOP true

-- This should be the last batch of tests since I don't feel like resetting the batch limits for any to come after them

SELECT set_config('search_path','mimeo, dblink, public',false);

SELECT plan(6);

SELECT dblink_connect('mimeo_test', 'host=localhost port=5432 dbname=mimeo_source user=mimeo_owner password=mimeo_owner');
SELECT is(dblink_get_connections() @> '{mimeo_test}', 't', 'Remote database connection established');
SELECT dblink_connect('mimeo_owner', 'host=localhost port=5432 dbname=mimeo_source user=mimeo_owner password=mimeo_owner');
SELECT is(dblink_get_connections() @> '{mimeo_owner}', 't', 'Remote database connection established');

SELECT dblink_exec('mimeo_test', 'CREATE TABLE mimeo_source.brand_new_table(id serial, stuff text)');

SELECT results_eq('SELECT schemaname, tablename FROM check_missing_source_tables()'
    , $$VALUES ('mimeo_source', 'brand_new_table') $$
    , 'Ensure check_missing_source_tables() returns table on source that doesn''t exist on destination');

SELECT is_empty('SELECT * FROM check_source_columns()', 'Check that check_source_columns returns nothing before changes on source');

SELECT dblink_exec('mimeo_test', 'DROP VIEW mimeo_source.snap_test_source_view');
SELECT dblink_exec('mimeo_test', 'DROP VIEW mimeo_source.inserter_test_source_view');
SELECT dblink_exec('mimeo_test', 'DROP VIEW mimeo_source.updater_test_source_view');
SELECT dblink_exec('mimeo_test', 'ALTER TABLE mimeo_source.snap_test_source DROP COLUMN col3');
SELECT dblink_exec('mimeo_test', 'ALTER TABLE mimeo_source.snap_test_source ADD COLUMN col4 varchar(42)');
SELECT dblink_exec('mimeo_test', 'ALTER TABLE mimeo_source.inserter_test_source ADD COLUMN col4 text');
SELECT dblink_exec('mimeo_test', 'ALTER TABLE mimeo_source.updater_test_source ALTER COLUMN col4 TYPE text');
SELECT dblink_exec('mimeo_test', 'ALTER TABLE mimeo_source.dml_test_source ADD COLUMN col4 inet');
SELECT dblink_exec('mimeo_test', 'ALTER TABLE mimeo_source.logdel_test_source ADD COLUMN col4 point');
SELECT dblink_exec('mimeo_test', 'ALTER TABLE mimeo_source.logdel_test_source ALTER col1 TYPE text');
SELECT dblink_exec('mimeo_owner', 'CREATE VIEW mimeo_source.snap_test_source_view AS SELECT * FROM mimeo_source.snap_test_source');
SELECT dblink_exec('mimeo_owner', 'CREATE VIEW mimeo_source.inserter_test_source_view AS SELECT * FROM mimeo_source.inserter_test_source');
SELECT dblink_exec('mimeo_owner', 'CREATE VIEW mimeo_source.updater_test_source_view AS SELECT * FROM mimeo_source.updater_test_source');
-- Ensure views have permissions needed for mimeo_test role to call refreshes
SELECT dblink_exec('mimeo_owner', 'GRANT SELECT, TRIGGER ON ALL TABLES IN SCHEMA mimeo_source TO mimeo_test');

SELECT results_eq('SELECT dest_schemaname, dest_tablename, src_schemaname, src_tablename, missing_column_name, missing_column_type FROM check_source_columns() ORDER BY 1,2,3,4,5,6'
    , $$VALUES ('mimeo_dest','dml_test_dest_multi','mimeo_source','dml_test_source','col4','inet')
        , ('mimeo_dest','inserter_test_dest','mimeo_source','inserter_test_source','col4','text')
        , ('mimeo_dest','inserter_test_dest_condition','mimeo_source','inserter_test_source','col4','text')
        , ('mimeo_dest','inserter_test_dest_nodata','mimeo_source','inserter_test_source','col4','text')
        , ('mimeo_dest','inserter_test_dest_serial','mimeo_source','inserter_test_source','col4','text')
        , ('mimeo_dest','inserter_test_dest_serial_view','mimeo_source','inserter_test_source_view','col4','text')
        , ('mimeo_dest','logdel_test_dest_multi','mimeo_source','logdel_test_source','col1','text')
        , ('mimeo_dest','logdel_test_dest_multi','mimeo_source','logdel_test_source','col4','point')
        , ('mimeo_dest','snap_test_dest','mimeo_source','snap_test_source','col4','character varying(42)')
        , ('mimeo_dest','snap_test_dest_condition','mimeo_source','snap_test_source','col4','character varying(42)')
        , ('mimeo_dest','snap_test_dest_nodata','mimeo_source','snap_test_source','col4','character varying(42)')
        , ('mimeo_dest','table_test_dest','mimeo_source','snap_test_source','col4','character varying(42)')
        , ('mimeo_dest','table_test_dest_condition','mimeo_source','snap_test_source','col4','character varying(42)')
        , ('mimeo_dest','table_test_dest_nodata','mimeo_source','snap_test_source','col4','character varying(42)')
        , ('mimeo_dest','table_test_dest_view','mimeo_source','snap_test_source_view','col4','character varying(42)')
        , ('mimeo_dest','updater_test_dest','mimeo_source','updater_test_source','col4','text')
        , ('mimeo_dest','updater_test_dest_condition','mimeo_source','updater_test_source','col4','text')
        , ('mimeo_dest','updater_test_dest_nodata','mimeo_source','updater_test_source','col4','text')
        , ('mimeo_dest','updater_test_dest_serial','mimeo_source','updater_test_source','col4','text')
        , ('mimeo_dest','updater_test_dest_serial_view','mimeo_source','updater_test_source_view','col4','text')
        , ('mimeo_source','dml_test_source','mimeo_source','dml_test_source','col4','inet')
        , ('mimeo_source','inserter_test_source','mimeo_source','inserter_test_source','col4','text')
        , ('mimeo_source','inserter_test_source_view','mimeo_source','inserter_test_source_view','col4','text')
        , ('mimeo_source','logdel_test_source','mimeo_source','logdel_test_source','col1','text')
        , ('mimeo_source','logdel_test_source','mimeo_source','logdel_test_source','col4','point')
        , ('mimeo_source','snap_test_source','mimeo_source','snap_test_source','col4','character varying(42)')
        , ('mimeo_source','snap_test_source_view','mimeo_source','snap_test_source_view','col4','character varying(42)')
        , ('mimeo_source','updater_test_source','mimeo_source','updater_test_source','col4','text')
        , ('mimeo_source','updater_test_source_view','mimeo_source','updater_test_source_view','col4','text')$$
    , ' Ensure check_source_columns() returns correct column diff from source');

SELECT diag('Running snap refresh...');

SELECT refresh_snap('mimeo_source.snap_test_source');
SELECT refresh_snap('mimeo_dest.snap_test_dest');
SELECT refresh_snap('mimeo_dest.snap_test_dest_nodata');
SELECT refresh_snap('mimeo_dest.snap_test_dest_filter');
SELECT refresh_snap('mimeo_dest.snap_test_dest_condition');
SELECT refresh_snap('mimeo_source.snap_test_source_empty');
SELECT refresh_snap('mimeo_dest.snap_test_dest_change_col');
SELECT refresh_snap('mimeo_source.snap_test_source_view');

SELECT results_eq('SELECT dest_schemaname, dest_tablename, src_schemaname, src_tablename, missing_column_name, missing_column_type FROM check_source_columns() ORDER BY 1,2,3,4,5,6'
    , $$VALUES ('mimeo_dest','dml_test_dest_multi','mimeo_source','dml_test_source','col4','inet')
        , ('mimeo_dest','inserter_test_dest','mimeo_source','inserter_test_source','col4','text')
        , ('mimeo_dest','inserter_test_dest_condition','mimeo_source','inserter_test_source','col4','text')
        , ('mimeo_dest','inserter_test_dest_nodata','mimeo_source','inserter_test_source','col4','text')
        , ('mimeo_dest','inserter_test_dest_serial','mimeo_source','inserter_test_source','col4','text')
        , ('mimeo_dest','inserter_test_dest_serial_view','mimeo_source','inserter_test_source_view','col4','text')
        , ('mimeo_dest','logdel_test_dest_multi','mimeo_source','logdel_test_source','col1','text')
        , ('mimeo_dest','logdel_test_dest_multi','mimeo_source','logdel_test_source','col4','point')
        , ('mimeo_dest','table_test_dest','mimeo_source','snap_test_source','col4','character varying(42)')
        , ('mimeo_dest','table_test_dest_condition','mimeo_source','snap_test_source','col4','character varying(42)')
        , ('mimeo_dest','table_test_dest_nodata','mimeo_source','snap_test_source','col4','character varying(42)')
        , ('mimeo_dest','table_test_dest_view','mimeo_source','snap_test_source_view','col4','character varying(42)')
        , ('mimeo_dest','updater_test_dest','mimeo_source','updater_test_source','col4','text')
        , ('mimeo_dest','updater_test_dest_condition','mimeo_source','updater_test_source','col4','text')
        , ('mimeo_dest','updater_test_dest_nodata','mimeo_source','updater_test_source','col4','text')
        , ('mimeo_dest','updater_test_dest_serial','mimeo_source','updater_test_source','col4','text')
        , ('mimeo_dest','updater_test_dest_serial_view','mimeo_source','updater_test_source_view','col4','text')
        , ('mimeo_source','dml_test_source','mimeo_source','dml_test_source','col4','inet')
        , ('mimeo_source','inserter_test_source','mimeo_source','inserter_test_source','col4','text')
        , ('mimeo_source','inserter_test_source_view','mimeo_source','inserter_test_source_view','col4','text')
        , ('mimeo_source','logdel_test_source','mimeo_source','logdel_test_source','col1','text')
        , ('mimeo_source','logdel_test_source','mimeo_source','logdel_test_source','col4','point')
        , ('mimeo_source','updater_test_source','mimeo_source','updater_test_source','col4','text')
        , ('mimeo_source','updater_test_source_view','mimeo_source','updater_test_source_view','col4','text')$$
    , ' Ensure snapshots replicated column changes by checking that check_source_columns() no longer lists them');


SELECT dblink_disconnect('mimeo_test');
SELECT dblink_disconnect('mimeo_owner');

SELECT * FROM finish();

