\set ON_ERROR_ROLLBACK 1
\set ON_ERROR_STOP true

SELECT set_config('search_path','mimeo, dblink, public',false);

SELECT plan(10);

SELECT throws_ok('SELECT refresh_snap(''fake_snap_test'')', 'No configuration found for Refresh Snap: fake_snap_test', 'Testing refresh_snap exception block');
SELECT throws_ok('SELECT refresh_table(''fake_table_test'')', 'No configuration found for Refresh Table: fake_table_test', 'Testing refresh_table exception block');
SELECT throws_ok('SELECT refresh_inserter(''fake_inserter_test'')', 'No configuration found for refresh_inserter on table fake_inserter_test', 'Testing refresh_inserter exception block');
SELECT throws_ok('SELECT refresh_updater(''fake_updater_test'')', 'No configuration found for refresh_updater on table fake_updater_test', 'Testing refresh_updater exception block');
SELECT throws_ok('SELECT refresh_dml(''fake_dml_test'')', 'No configuration found for Refresh DML: fake_dml_test', 'Testing refresh_dml exception block');
SELECT throws_ok('SELECT refresh_logdel(''fake_logdel_test'')', 'No configuration found for Refresh Log Del: fake_logdel_test', 'Testing refresh_logdel exception block');

SELECT throws_ok('SELECT mimeo.inserter_maker(''mimeo_source.inserter_test_source'',''serial'',''col1'',data_source_id,''5'',''mimeo_dest.inserter_test_dest'') FROM mimeo.dblink_mapping_mimeo WHERE username = ''mimeo_test''', 'Inserter replication already defined for mimeo_dest.inserter_test_dest', 'Testing unique inserter trigger');

SELECT throws_ok('SELECT inserter_maker(''mimeo_source.inserter_test_source'', ''time'', ''col3'', data_source_id, ''0'', ''mimeo_dest.inserter_test_dest_serial'') FROM dblink_mapping_mimeo WHERE username = ''mimeo_test''', 'Inserter replication already defined for mimeo_dest.inserter_test_dest_serial', 'Testing unique inserter trigger');

SELECT throws_ok('SELECT updater_maker(''mimeo_source.updater_test_source'', ''serial'', ''col1'', data_source_id, ''5'', ''mimeo_dest.updater_test_dest'') FROM dblink_mapping_mimeo WHERE username = ''mimeo_test''', 'Updater replication already defined for mimeo_dest.updater_test_dest', 'Testing unique updater trigger');

SELECT throws_ok('SELECT updater_maker(''mimeo_source.updater_test_source'', ''time'', ''col3'', data_source_id, ''0'', ''mimeo_dest.updater_test_dest_serial'') FROM dblink_mapping_mimeo WHERE username = ''mimeo_test''', 'Updater replication already defined for mimeo_dest.updater_test_dest_serial', 'Testing unique updater trigger');


SELECT * FROM finish();
