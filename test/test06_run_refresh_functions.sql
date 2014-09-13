\set ON_ERROR_ROLLBACK 1
\set ON_ERROR_STOP true

SELECT set_config('search_path','mimeo, dblink, public',false);

SELECT plan(1);

SELECT diag('Running refresh for: mimeo_source.snap_test_source');
SELECT refresh_snap('mimeo_source.snap_test_source');
SELECT diag('Running refresh for: mimeo_dest.snap_test_dest');
SELECT refresh_snap('mimeo_dest.snap_test_dest');
SELECT diag('Running refresh for: mimeo_dest.snap_test_dest_nodata');
SELECT refresh_snap('mimeo_dest.snap_test_dest_nodata', p_jobmon := false);
SELECT diag('Running refresh for: mimeo_dest.snap_test_dest_filter');
SELECT refresh_snap('mimeo_dest.snap_test_dest_filter');
SELECT diag('Running refresh for: mimeo_dest.snap_test_dest_condition');
SELECT refresh_snap('mimeo_dest.snap_test_dest_condition');
SELECT diag('Running refresh for: mimeo_source.snap_test_source_empty');
SELECT refresh_snap('mimeo_source.snap_test_source_empty');
SELECT diag('Running refresh for: mimeo_dest.snap_test_dest_change_col');
SELECT refresh_snap('mimeo_dest.snap_test_dest_change_col');
SELECT diag('Running refresh for: mimeo_source.Snap-test-Source');
SELECT refresh_snap('mimeo_source.Snap-test-Source');
SELECT diag('Running refresh for: mimeo_source.snap_test_source_view');
SELECT refresh_snap('mimeo_source.snap_test_source_view');


SELECT diag('Running refresh for: mimeo_dest.table_test_dest');
SELECT refresh_table('mimeo_dest.table_test_dest');
SELECT diag('Running refresh for: mimeo_dest.table_test_dest_nodata');
SELECT refresh_table('mimeo_dest.table_test_dest_nodata', p_jobmon := false);
SELECT diag('Running refresh for: mimeo_dest.table_test_dest_filter');
SELECT refresh_table('mimeo_dest.table_test_dest_filter');
SELECT diag('Running refresh for: mimeo_dest.table_test_dest_condition');
SELECT refresh_table('mimeo_dest.table_test_dest_condition');
SELECT diag('Running refresh for: mimeo_dest.table_test_dest_empty');
SELECT refresh_table('mimeo_dest.table_test_dest_empty');
SELECT diag('Running refresh for: mimeo_dest.Table-test-Source');
SELECT refresh_table('mimeo_dest.Table-test-Source');
SELECT diag('Running refresh for: mimeo_dest.table_test_dest_view');
SELECT refresh_table('mimeo_dest.table_test_dest_view');


--time
SELECT diag('Running refresh for: mimeo_source.inserter_test_source');
SELECT refresh_inserter('mimeo_source.inserter_test_source');
SELECT diag('Running refresh for: mimeo_dest.inserter_test_dest');
SELECT refresh_inserter('mimeo_dest.inserter_test_dest');
SELECT diag('Running refresh for: mimeo_dest.inserter_test_dest_nodata');
SELECT refresh_inserter('mimeo_dest.inserter_test_dest_nodata', p_jobmon := false);
SELECT diag('Running refresh for: mimeo_dest.inserter_test_dest_filter');
SELECT refresh_inserter('mimeo_dest.inserter_test_dest_filter');
SELECT diag('Running refresh for: mimeo_dest.inserter_test_dest_condition');
SELECT refresh_inserter('mimeo_dest.inserter_test_dest_condition');
SELECT diag('Running refresh for: mimeo_source.inserter_test_source_empty');
SELECT refresh_inserter('mimeo_source.inserter_test_source_empty');
SELECT diag('Running refresh for: mimeo_source.Inserter-Test-Source');
SELECT refresh_inserter('mimeo_source.Inserter-Test-Source');
SELECT diag('Running refresh for: mimeo_source.inserter_test_source_view');
SELECT refresh_inserter('mimeo_source.inserter_test_source_view');
--serial
SELECT diag('Running refresh for: mimeo_dest.inserter_test_dest_serial');
SELECT refresh_inserter('mimeo_dest.inserter_test_dest_serial');
SELECT diag('Running refresh for: mimeo_dest.Inserter-Test-Source_Serial');
SELECT refresh_inserter('mimeo_dest.Inserter-Test-Source_Serial');
SELECT diag('Running refresh for: mimeo_dest.inserter_test_dest_serial_view');
SELECT refresh_inserter('mimeo_dest.inserter_test_dest_serial_view');

--time
SELECT diag('Running refresh for: mimeo_source.updater_test_source');
SELECT refresh_updater('mimeo_source.updater_test_source');
SELECT diag('Running refresh for: mimeo_dest.updater_test_dest');
SELECT refresh_updater('mimeo_dest.updater_test_dest');
SELECT diag('Running refresh for: mimeo_dest.updater_test_dest_nodata');
SELECT refresh_updater('mimeo_dest.updater_test_dest_nodata', p_jobmon := false);
SELECT diag('Running refresh for: mimeo_dest.updater_test_dest_filter');
SELECT refresh_updater('mimeo_dest.updater_test_dest_filter');
SELECT diag('Running refresh for: mimeo_dest.updater_test_dest_condition');
SELECT refresh_updater('mimeo_dest.updater_test_dest_condition');
SELECT diag('Running refresh for: mimeo_source.updater_test_source_empty');
SELECT refresh_updater('mimeo_source.updater_test_source_empty');
SELECT diag('Running refresh for: mimeo_source.Updater-Test-Source');
SELECT refresh_updater('mimeo_source.Updater-Test-Source');
SELECT diag('Running refresh for: mimeo_source.updater_test_source_view');
SELECT refresh_updater('mimeo_source.updater_test_source_view');
--serial
SELECT diag('Running refresh for: mimeo_dest.updater_test_dest_serial');
SELECT refresh_updater('mimeo_dest.updater_test_dest_serial');
SELECT diag('Running refresh for: mimeo_dest.Updater-Test-Source_Serial');
SELECT refresh_updater('mimeo_dest.Updater-Test-Source_Serial');
SELECT diag('Running refresh for: mimeo_dest.updater_test_dest_serial_view');
SELECT refresh_updater('mimeo_dest.updater_test_dest_serial_view');


SELECT diag('Running refresh for: mimeo_source.dml_test_source');
SELECT refresh_dml('mimeo_source.dml_test_source');
SELECT diag('Running refresh for: mimeo_dest.dml_test_dest');
SELECT refresh_dml('mimeo_dest.dml_test_dest');
SELECT diag('Running refresh for: mimeo_dest.dml_test_dest_multi');
SELECT refresh_dml('mimeo_dest.dml_test_dest_multi');
SELECT diag('Running refresh for: mimeo_dest.dml_test_dest_nodata');
SELECT refresh_dml('mimeo_dest.dml_test_dest_nodata', p_jobmon := false);
SELECT diag('Running refresh for: mimeo_dest.dml_test_dest_filter');
SELECT refresh_dml('mimeo_dest.dml_test_dest_filter');
SELECT diag('Running refresh for: mimeo_dest.dml_test_dest_condition');
SELECT refresh_dml('mimeo_dest.dml_test_dest_condition');
SELECT diag('Running refresh for: mimeo_source.dml_test_source_empty');
SELECT refresh_dml('mimeo_source.dml_test_source_empty');
SELECT diag('Running refresh for: mimeo_source.Dml-Test-Source');
SELECT refresh_dml('mimeo_source.Dml-Test-Source');


SELECT diag('Running refresh for: mimeo_source.logdel_test_source');
SELECT refresh_logdel('mimeo_source.logdel_test_source');
SELECT diag('Running refresh for: mimeo_dest.logdel_test_dest');
SELECT refresh_logdel('mimeo_dest.logdel_test_dest');
SELECT diag('Running refresh for: mimeo_dest.logdel_test_dest_multi');
SELECT refresh_logdel('mimeo_dest.logdel_test_dest_multi');
SELECT diag('Running refresh for: mimeo_dest.logdel_test_dest_nodata');
SELECT refresh_logdel('mimeo_dest.logdel_test_dest_nodata', p_jobmon := false);
SELECT diag('Running refresh for: mimeo_dest.logdel_test_dest_filter');
SELECT refresh_logdel('mimeo_dest.logdel_test_dest_filter');
SELECT diag('Running refresh for: mimeo_dest.logdel_test_dest_condition');
SELECT refresh_logdel('mimeo_dest.logdel_test_dest_condition');
SELECT diag('Running refresh for: mimeo_source.logdel_test_source_empty');
SELECT refresh_logdel('mimeo_source.logdel_test_source_empty');
SELECT diag('Running refresh for: mimeo_source.LogDel-Test-Source');
SELECT refresh_logdel('mimeo_source.LogDel-Test-Source');

SELECT pass('Completed refresh function runs');

SELECT * FROM finish();
