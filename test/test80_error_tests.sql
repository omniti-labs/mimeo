\set ON_ERROR_ROLLBACK 1
\set ON_ERROR_STOP true

SELECT set_config('search_path','mimeo, dblink, public',false);

SELECT plan(10);

SELECT throws_ok('SELECT refresh_snap(''fake_snap_test'')', 'Destination table given in argument (fake_snap_test) is not managed by mimeo', 'Testing refresh_snap exception block');
SELECT throws_ok('SELECT refresh_table(''fake_table_test'')', 'Destination table given in argument (fake_table_test) is not managed by mimeo', 'Testing refresh_table exception block');
SELECT throws_ok('SELECT refresh_inserter(''fake_inserter_test'')', 'Destination table given in argument (fake_inserter_test) is not managed by mimeo', 'Testing refresh_inserter exception block');
SELECT throws_ok('SELECT refresh_updater(''fake_updater_test'')', 'Destination table given in argument (fake_updater_test) is not managed by mimeo', 'Testing refresh_updater exception block');
SELECT throws_ok('SELECT refresh_dml(''fake_dml_test'')', 'Destination table given in argument (fake_dml_test) is not managed by mimeo', 'Testing refresh_dml exception block');
SELECT throws_ok('SELECT refresh_logdel(''fake_logdel_test'')', 'Destination table given in argument (fake_logdel_test) is not managed by mimeo', 'Testing refresh_logdel exception block');

SELECT throws_ok('SELECT mimeo.inserter_maker(''mimeo_source.inserter_test_source'',''serial'',''col1'',data_source_id,''5'',''mimeo_dest.inserter_test_dest'') FROM mimeo.dblink_mapping_mimeo WHERE username = ''mimeo_test''', 'Inserter replication already defined for mimeo_dest.inserter_test_dest', 'Testing unique inserter trigger');

SELECT throws_ok('SELECT inserter_maker(''mimeo_source.inserter_test_source'', ''time'', ''col3'', data_source_id, ''0'', ''mimeo_dest.inserter_test_dest_serial'') FROM dblink_mapping_mimeo WHERE username = ''mimeo_test''', 'Inserter replication already defined for mimeo_dest.inserter_test_dest_serial', 'Testing unique inserter trigger');

SELECT throws_ok('SELECT updater_maker(''mimeo_source.updater_test_source'', ''serial'', ''col1'', data_source_id, ''5'', ''mimeo_dest.updater_test_dest'') FROM dblink_mapping_mimeo WHERE username = ''mimeo_test''', 'Updater replication already defined for mimeo_dest.updater_test_dest', 'Testing unique updater trigger');

SELECT throws_ok('SELECT updater_maker(''mimeo_source.updater_test_source'', ''time'', ''col3'', data_source_id, ''0'', ''mimeo_dest.updater_test_dest_serial'') FROM dblink_mapping_mimeo WHERE username = ''mimeo_test''', 'Updater replication already defined for mimeo_dest.updater_test_dest_serial', 'Testing unique updater trigger');


SELECT * FROM finish();
