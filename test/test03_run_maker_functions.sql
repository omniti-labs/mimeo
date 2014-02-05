\set ON_ERROR_ROLLBACK 1
\set ON_ERROR_STOP true

SELECT set_config('search_path','mimeo, dblink, public',false);

SELECT plan(1);

SELECT snapshot_maker('mimeo_source.snap_test_source',data_source_id) FROM dblink_mapping_mimeo WHERE username = 'mimeo_test';
SELECT snapshot_maker('mimeo_source.snap_test_source', data_source_id, 'mimeo_dest.snap_test_dest') FROM dblink_mapping_mimeo WHERE username = 'mimeo_test';
SELECT snapshot_maker('mimeo_source.snap_test_source', data_source_id, 'mimeo_dest.snap_test_dest_nodata', p_pulldata := false) FROM dblink_mapping_mimeo WHERE username = 'mimeo_test';
SELECT snapshot_maker('mimeo_source.snap_test_source', data_source_id, 'mimeo_dest.snap_test_dest_filter', p_filter := '{"col1","col2"}') FROM dblink_mapping_mimeo WHERE username = 'mimeo_test';
SELECT snapshot_maker('mimeo_source.snap_test_source', data_source_id, 'mimeo_dest.snap_test_dest_condition', p_condition := 'WHERE col1 > 9000') FROM dblink_mapping_mimeo WHERE username = 'mimeo_test';
SELECT snapshot_maker('mimeo_source.snap_test_source_empty', data_source_id) FROM dblink_mapping_mimeo WHERE username = 'mimeo_test';
SELECT snapshot_maker('mimeo_source.snap_test_source_change_col', data_source_id, 'mimeo_dest.snap_test_dest_change_col') FROM dblink_mapping_mimeo WHERE username = 'mimeo_test';
ALTER TABLE mimeo_dest.snap_test_dest_change_col OWNER TO mimeo_test;
GRANT SELECT ON TABLE mimeo_dest.snap_test_dest_change_col TO mimeo_dumb_role;
GRANT SELECT ON TABLE mimeo_dest.snap_test_dest_change_col_snap1 TO mimeo_dumb_role;
GRANT SELECT ON TABLE mimeo_dest.snap_test_dest_change_col_snap2 TO mimeo_dumb_role;

-- Just reuse snap table source since it's doing mostly the same thing
SELECT table_maker('mimeo_source.snap_test_source', data_source_id, 'mimeo_dest.table_test_dest', p_sequences := '{"mimeo_dest.col1_seq"}') FROM dblink_mapping_mimeo WHERE username = 'mimeo_test';
SELECT table_maker('mimeo_source.snap_test_source', data_source_id, 'mimeo_dest.table_test_dest_nodata', p_pulldata := false) FROM dblink_mapping_mimeo WHERE username = 'mimeo_test';
SELECT table_maker('mimeo_source.snap_test_source', data_source_id, 'mimeo_dest.table_test_dest_filter', p_filter := '{"col1","col2"}') FROM dblink_mapping_mimeo WHERE username = 'mimeo_test';
SELECT table_maker('mimeo_source.snap_test_source', data_source_id, 'mimeo_dest.table_test_dest_condition', p_condition := 'WHERE col1 > 9000') FROM dblink_mapping_mimeo WHERE username = 'mimeo_test';
SELECT table_maker('mimeo_source.snap_test_source_empty', data_source_id, 'mimeo_dest.table_test_dest_empty') FROM dblink_mapping_mimeo WHERE username = 'mimeo_test';
CREATE SEQUENCE mimeo_dest.col1_seq;    -- Add a sequence to test that sequence reset is working
ALTER TABLE mimeo_dest.table_test_dest ALTER col1 SET DEFAULT nextval('mimeo_dest.col1_seq');

SELECT inserter_maker('mimeo_source.inserter_test_source', 'col3', data_source_id, '00:00:05'::interval) FROM dblink_mapping_mimeo WHERE username = 'mimeo_test';
SELECT inserter_maker('mimeo_source.inserter_test_source', 'col3', data_source_id,'00:00:05'::interval, 'mimeo_dest.inserter_test_dest') FROM dblink_mapping_mimeo WHERE username = 'mimeo_test';
SELECT inserter_maker('mimeo_source.inserter_test_source', 'col3', data_source_id,'00:00:05'::interval, 'mimeo_dest.inserter_test_dest_nodata', p_pulldata := false) FROM dblink_mapping_mimeo WHERE username = 'mimeo_test';
SELECT inserter_maker('mimeo_source.inserter_test_source', 'col3', data_source_id,'00:00:05'::interval, 'mimeo_dest.inserter_test_dest_filter', p_filter := '{"col1","col3"}') FROM dblink_mapping_mimeo WHERE username = 'mimeo_test';
SELECT inserter_maker('mimeo_source.inserter_test_source', 'col3', data_source_id,'00:00:05'::interval, 'mimeo_dest.inserter_test_dest_condition', p_condition := 'WHERE col1 > 9000') FROM dblink_mapping_mimeo WHERE username = 'mimeo_test';
SELECT inserter_maker('mimeo_source.inserter_test_source_empty', 'col3', data_source_id) FROM dblink_mapping_mimeo WHERE username = 'mimeo_test';

SELECT updater_maker('mimeo_source.updater_test_source', 'col3', data_source_id, '00:00:05'::interval) FROM dblink_mapping_mimeo WHERE username = 'mimeo_test';
SELECT updater_maker('mimeo_source.updater_test_source', 'col3', data_source_id, '00:00:05'::interval, 'mimeo_dest.updater_test_dest') FROM dblink_mapping_mimeo WHERE username = 'mimeo_test';
SELECT updater_maker('mimeo_source.updater_test_source', 'col3', data_source_id, '00:00:05'::interval, 'mimeo_dest.updater_test_dest_nodata', p_pulldata := false) FROM dblink_mapping_mimeo WHERE username = 'mimeo_test';
SELECT updater_maker('mimeo_source.updater_test_source', 'col3', data_source_id, '00:00:05'::interval, 'mimeo_dest.updater_test_dest_filter', p_filter := '{"col1","col3"}') FROM dblink_mapping_mimeo WHERE username = 'mimeo_test';
SELECT updater_maker('mimeo_source.updater_test_source', 'col3', data_source_id, '00:00:05'::interval, 'mimeo_dest.updater_test_dest_condition', p_condition := 'WHERE col1 > 9000') FROM dblink_mapping_mimeo WHERE username = 'mimeo_test';
SELECT updater_maker('mimeo_source.updater_test_source_empty', 'col3', data_source_id) FROM dblink_mapping_mimeo WHERE username = 'mimeo_test';

SELECT dml_maker('mimeo_source.dml_test_source', data_source_id) FROM dblink_mapping_mimeo WHERE username = 'mimeo_test';
SELECT dml_maker('mimeo_source.dml_test_source2', data_source_id, 'mimeo_dest.dml_test_dest') FROM dblink_mapping_mimeo WHERE username = 'mimeo_test';
SELECT dml_maker('mimeo_source.dml_test_source', data_source_id, 'mimeo_dest.dml_test_dest_multi') FROM dblink_mapping_mimeo WHERE username = 'mimeo_test';
SELECT dml_maker('mimeo_source.dml_test_source_nodata', data_source_id, 'mimeo_dest.dml_test_dest_nodata', p_pulldata := false) FROM dblink_mapping_mimeo WHERE username = 'mimeo_test';
SELECT dml_maker('mimeo_source.dml_test_source_filter', data_source_id, 'mimeo_dest.dml_test_dest_filter', p_filter := '{"col1","col2"}') FROM dblink_mapping_mimeo WHERE username = 'mimeo_test';
SELECT dml_maker('mimeo_source.dml_test_source_condition', data_source_id, 'mimeo_dest.dml_test_dest_condition', p_condition := 'WHERE col1 > 9000') FROM dblink_mapping_mimeo WHERE username = 'mimeo_test';
SELECT dml_maker('mimeo_source.dml_test_source_empty', data_source_id) FROM dblink_mapping_mimeo WHERE username = 'mimeo_test';


SELECT logdel_maker('mimeo_source.logdel_test_source', data_source_id) FROM dblink_mapping_mimeo WHERE username = 'mimeo_test';
SELECT logdel_maker('mimeo_source.logdel_test_source2', data_source_id, 'mimeo_dest.logdel_test_dest') FROM dblink_mapping_mimeo WHERE username = 'mimeo_test';
SELECT logdel_maker('mimeo_source.logdel_test_source', data_source_id, 'mimeo_dest.logdel_test_dest_multi') FROM dblink_mapping_mimeo WHERE username = 'mimeo_test';
SELECT logdel_maker('mimeo_source.logdel_test_source_nodata', data_source_id, 'mimeo_dest.logdel_test_dest_nodata', p_pulldata := false) FROM dblink_mapping_mimeo WHERE username = 'mimeo_test';
SELECT logdel_maker('mimeo_source.logdel_test_source_filter', data_source_id, 'mimeo_dest.logdel_test_dest_filter', p_filter := '{"col1","col2"}') FROM dblink_mapping_mimeo WHERE username = 'mimeo_test';
SELECT logdel_maker('mimeo_source.logdel_test_source_condition', data_source_id, 'mimeo_dest.logdel_test_dest_condition', p_condition := 'WHERE col1 > 9000') FROM dblink_mapping_mimeo WHERE username = 'mimeo_test';
SELECT logdel_maker('mimeo_source.logdel_test_source_empty', data_source_id) FROM dblink_mapping_mimeo WHERE username = 'mimeo_test';

SELECT pass('Maker functions finished. Sleeping for 40 seconds to ensure gap for incremental tests...');
SELECT pg_sleep(40);

SELECT * FROM finish();
