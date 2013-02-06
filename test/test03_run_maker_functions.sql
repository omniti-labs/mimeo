SELECT set_config('search_path','mimeo, dblink, tap',false);

SELECT plan(1);

SELECT snapshot_maker('mimeo_source.snap_test_source',data_source_id) FROM dblink_mapping WHERE username = 'mimeo_test';
SELECT snapshot_maker('mimeo_source.snap_test_source', data_source_id, 'mimeo_dest.snap_test_dest') FROM dblink_mapping WHERE username = 'mimeo_test';
SELECT snapshot_maker('mimeo_source.snap_test_source', data_source_id, 'mimeo_dest.snap_test_dest_nodata', p_pulldata := false) FROM dblink_mapping WHERE username = 'mimeo_test';
SELECT snapshot_maker('mimeo_source.snap_test_source', data_source_id, 'mimeo_dest.snap_test_dest_filter', p_filter := '{"col1","col2"}') FROM dblink_mapping WHERE username = 'mimeo_test';
SELECT snapshot_maker('mimeo_source.snap_test_source', data_source_id, 'mimeo_dest.snap_test_dest_condition', p_condition := 'WHERE col1 > 9000') FROM dblink_mapping WHERE username = 'mimeo_test';

SELECT inserter_maker('mimeo_source.inserter_test_source', 'col3', data_source_id, '00:00:05'::interval) FROM dblink_mapping WHERE username = 'mimeo_test';
SELECT inserter_maker('mimeo_source.inserter_test_source', 'col3', data_source_id,'00:00:05'::interval, 'mimeo_dest.inserter_test_dest') FROM dblink_mapping WHERE username = 'mimeo_test';
SELECT inserter_maker('mimeo_source.inserter_test_source', 'col3', data_source_id,'00:00:05'::interval, 'mimeo_dest.inserter_test_dest_nodata', p_pulldata := false) FROM dblink_mapping WHERE username = 'mimeo_test';
SELECT inserter_maker('mimeo_source.inserter_test_source', 'col3', data_source_id,'00:00:05'::interval, 'mimeo_dest.inserter_test_dest_filter', p_filter := '{"col1","col3"}') FROM dblink_mapping WHERE username = 'mimeo_test';
SELECT inserter_maker('mimeo_source.inserter_test_source', 'col3', data_source_id,'00:00:05'::interval, 'mimeo_dest.inserter_test_dest_condition', p_condition := 'WHERE col1 > 9000') FROM dblink_mapping WHERE username = 'mimeo_test';

SELECT updater_maker('mimeo_source.updater_test_source', 'col3', data_source_id, '00:00:05'::interval) FROM dblink_mapping WHERE username = 'mimeo_test';
SELECT updater_maker('mimeo_source.updater_test_source', 'col3', data_source_id, '00:00:05'::interval, 'mimeo_dest.updater_test_dest') FROM dblink_mapping WHERE username = 'mimeo_test';
SELECT updater_maker('mimeo_source.updater_test_source', 'col3', data_source_id, '00:00:05'::interval, 'mimeo_dest.updater_test_dest_nodata', p_pulldata := false) FROM dblink_mapping WHERE username = 'mimeo_test';
SELECT updater_maker('mimeo_source.updater_test_source', 'col3', data_source_id, '00:00:05'::interval, 'mimeo_dest.updater_test_dest_filter', p_filter := '{"col1","col3"}') FROM dblink_mapping WHERE username = 'mimeo_test';
SELECT updater_maker('mimeo_source.updater_test_source', 'col3', data_source_id, '00:00:05'::interval, 'mimeo_dest.updater_test_dest_condition', p_condition := 'WHERE col1 > 9000') FROM dblink_mapping WHERE username = 'mimeo_test';

SELECT dml_maker('mimeo_source.dml_test_source', data_source_id) FROM dblink_mapping WHERE username = 'mimeo_test';
SELECT dml_maker('mimeo_source.dml_test_source2', data_source_id, 'mimeo_dest.dml_test_dest') FROM dblink_mapping WHERE username = 'mimeo_test';
SELECT dml_maker('mimeo_source.dml_test_source_nodata', data_source_id, 'mimeo_dest.dml_test_dest_nodata', p_pulldata := false) FROM dblink_mapping WHERE username = 'mimeo_test';
SELECT dml_maker('mimeo_source.dml_test_source_filter', data_source_id, 'mimeo_dest.dml_test_dest_filter', p_filter := '{"col1","col2"}') FROM dblink_mapping WHERE username = 'mimeo_test';
SELECT dml_maker('mimeo_source.dml_test_source_condition', data_source_id, 'mimeo_dest.dml_test_dest_condition', p_condition := 'WHERE col1 > 9000') FROM dblink_mapping WHERE username = 'mimeo_test';


SELECT logdel_maker('mimeo_source.logdel_test_source', data_source_id) FROM dblink_mapping WHERE username = 'mimeo_test';
SELECT logdel_maker('mimeo_source.logdel_test_source2', data_source_id, 'mimeo_dest.logdel_test_dest') FROM dblink_mapping WHERE username = 'mimeo_test';
SELECT logdel_maker('mimeo_source.logdel_test_source_nodata', data_source_id, 'mimeo_dest.logdel_test_dest_nodata', p_pulldata := false) FROM dblink_mapping WHERE username = 'mimeo_test';
SELECT logdel_maker('mimeo_source.logdel_test_source_filter', data_source_id, 'mimeo_dest.logdel_test_dest_filter', p_filter := '{"col1","col2"}') FROM dblink_mapping WHERE username = 'mimeo_test';
SELECT logdel_maker('mimeo_source.logdel_test_source_condition', data_source_id, 'mimeo_dest.logdel_test_dest_condition', p_condition := 'WHERE col1 > 9000') FROM dblink_mapping WHERE username = 'mimeo_test';

SELECT pass('Maker functions finished. Sleeping for 40 seconds to ensure gap for incremental tests...');
SELECT pg_sleep(40);

SELECT * FROM finish();
