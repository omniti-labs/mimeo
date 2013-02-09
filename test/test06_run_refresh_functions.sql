SELECT set_config('search_path','mimeo, dblink, tap',false);

SELECT plan(1);

SELECT refresh_snap('mimeo_source.snap_test_source');
SELECT refresh_snap('mimeo_dest.snap_test_dest');
SELECT refresh_snap('mimeo_dest.snap_test_dest_nodata');
SELECT refresh_snap('mimeo_dest.snap_test_dest_filter');
SELECT refresh_snap('mimeo_dest.snap_test_dest_condition');
SELECT refresh_snap('mimeo_source.snap_test_source_empty');

SELECT refresh_inserter('mimeo_source.inserter_test_source');
SELECT refresh_inserter('mimeo_dest.inserter_test_dest');
SELECT refresh_inserter('mimeo_dest.inserter_test_dest_nodata');
SELECT refresh_inserter('mimeo_dest.inserter_test_dest_filter');
SELECT refresh_inserter('mimeo_dest.inserter_test_dest_condition');
SELECT refresh_inserter('mimeo_source.inserter_test_source_empty');

SELECT refresh_updater('mimeo_source.updater_test_source');
SELECT refresh_updater('mimeo_dest.updater_test_dest');
SELECT refresh_updater('mimeo_dest.updater_test_dest_nodata');
SELECT refresh_updater('mimeo_dest.updater_test_dest_filter');
SELECT refresh_updater('mimeo_dest.updater_test_dest_condition');
SELECT refresh_updater('mimeo_source.updater_test_source_empty');

SELECT refresh_dml('mimeo_source.dml_test_source');
SELECT refresh_dml('mimeo_dest.dml_test_dest');
SELECT refresh_dml('mimeo_dest.dml_test_dest_nodata');
SELECT refresh_dml('mimeo_dest.dml_test_dest_filter');
SELECT refresh_dml('mimeo_dest.dml_test_dest_condition');
SELECT refresh_dml('mimeo_source.dml_test_source_empty');

SELECT refresh_logdel('mimeo_source.logdel_test_source');
SELECT refresh_logdel('mimeo_dest.logdel_test_dest');
SELECT refresh_logdel('mimeo_dest.logdel_test_dest_nodata');
SELECT refresh_logdel('mimeo_dest.logdel_test_dest_filter');
SELECT refresh_logdel('mimeo_dest.logdel_test_dest_condition');
SELECT refresh_logdel('mimeo_source.logdel_test_source_empty');

SELECT pass('Completed refresh function runs');

SELECT * FROM finish();
