\set ON_ERROR_ROLLBACK 1
\set ON_ERROR_STOP true

SELECT set_config('search_path','mimeo, dblink, tap',false);

SELECT plan(6);

SELECT throws_ok('SELECT refresh_snap(''fake_snap_test'')', 'No configuration found for Refresh Snap: fake_snap_test', 'Testing refresh_snap exception block');
SELECT throws_ok('SELECT refresh_table(''fake_table_test'')', 'No configuration found for Refresh Table: fake_table_test', 'Testing refresh_table exception block');
SELECT throws_ok('SELECT refresh_inserter(''fake_inserter_test'')', 'No configuration found for Refresh Inserter: fake_inserter_test', 'Testing refresh_inserter exception block');
SELECT throws_ok('SELECT refresh_updater(''fake_updater_test'')', 'No configuration found for Refresh Updater: fake_updater_test', 'Testing refresh_updater exception block');
SELECT throws_ok('SELECT refresh_dml(''fake_dml_test'')', 'No configuration found for Refresh DML: fake_dml_test', 'Testing refresh_dml exception block');
SELECT throws_ok('SELECT refresh_logdel(''fake_logdel_test'')', 'No configuration found for Refresh Log Del: fake_logdel_test', 'Testing refresh_logdel exception block');

SELECT * FROM finish();
