\set ON_ERROR_ROLLBACK 1
\set ON_ERROR_STOP true

SELECT set_config('search_path','mimeo, dblink, public',false);

SELECT plan(1);

SELECT diag('Running maker for destination: mimeo_source.snap_test_source');
SELECT snapshot_maker('mimeo_source.snap_test_source',data_source_id) FROM dblink_mapping_mimeo WHERE username = 'mimeo_test';
SELECT diag('Running maker for destination: mimeo_dest.snap_test_dest');
SELECT snapshot_maker('mimeo_source.snap_test_source', data_source_id, 'mimeo_dest.snap_test_dest') FROM dblink_mapping_mimeo WHERE username = 'mimeo_test';
SELECT diag('Running maker for destination: mimeo_dest.snap_test_dest_nodata');
SELECT snapshot_maker('mimeo_source.snap_test_source', data_source_id, 'mimeo_dest.snap_test_dest_nodata', p_pulldata := false) FROM dblink_mapping_mimeo WHERE username = 'mimeo_test';
SELECT diag('Running maker for destination: mimeo_dest.snap_test_dest_filter');
SELECT snapshot_maker('mimeo_source.snap_test_source', data_source_id, 'mimeo_dest.snap_test_dest_filter', p_filter := '{"col1","col2"}') FROM dblink_mapping_mimeo WHERE username = 'mimeo_test';
SELECT diag('Running maker for destination: mimeo_dest.snap_test_dest_condition');
SELECT snapshot_maker('mimeo_source.snap_test_source', data_source_id, 'mimeo_dest.snap_test_dest_condition', p_condition := 'WHERE col1 > 9000') FROM dblink_mapping_mimeo WHERE username = 'mimeo_test';
SELECT diag('Running maker for destination: mimeo_source.snap_test_source_empty');
SELECT snapshot_maker('mimeo_source.snap_test_source_empty', data_source_id) FROM dblink_mapping_mimeo WHERE username = 'mimeo_test';
SELECT diag('Running maker for destination: mimeo_source.snap_test_source_change_col');
SELECT snapshot_maker('mimeo_source.snap_test_source_change_col', data_source_id, 'mimeo_dest.snap_test_dest_change_col') FROM dblink_mapping_mimeo WHERE username = 'mimeo_test';
SELECT diag('Running maker for destination: mimeo_source.snap_test_source_view');
SELECT snapshot_maker('mimeo_source.snap_test_source_view', data_source_id) FROM dblink_mapping_mimeo WHERE username = 'mimeo_test';
-- Change owner to view and underlying tables since view select permissions are based on the view owner
ALTER TABLE mimeo_dest.snap_test_dest_change_col OWNER TO mimeo_test;
ALTER TABLE mimeo_dest.snap_test_dest_change_col_snap1 OWNER TO mimeo_test;
ALTER TABLE mimeo_dest.snap_test_dest_change_col_snap2 OWNER TO mimeo_test;
GRANT SELECT ON TABLE mimeo_dest.snap_test_dest_change_col TO mimeo_dumb_role;
GRANT SELECT ON TABLE mimeo_dest.snap_test_dest_change_col_snap1 TO mimeo_dumb_role;
GRANT SELECT ON TABLE mimeo_dest.snap_test_dest_change_col_snap2 TO mimeo_dumb_role;
SELECT diag('Running maker for destination: mimeo_source.Snap-test-Source');
SELECT snapshot_maker('mimeo_source.Snap-test-Source', data_source_id) FROM dblink_mapping_mimeo WHERE username = 'mimeo_test';
-- Change owner to view and underlying tables since view select permissions are based on the view owner
ALTER TABLE mimeo_source."Snap-test-Source" OWNER TO mimeo_test;
ALTER TABLE mimeo_source."Snap-test-Source_snap1" OWNER TO mimeo_test;
ALTER TABLE mimeo_source."Snap-test-Source_snap2" OWNER TO mimeo_test;
GRANT SELECT ON TABLE mimeo_source."Snap-test-Source" TO mimeo_dumb_role;
GRANT SELECT ON TABLE mimeo_source."Snap-test-Source_snap1" TO mimeo_dumb_role;
GRANT SELECT ON TABLE mimeo_source."Snap-test-Source_snap2" TO mimeo_dumb_role;

-- Table maker
-- Just reuse snap table source since it's doing mostly the same thing
SELECT diag('Running maker for destination: mimeo_dest.table_test_dest');
SELECT table_maker('mimeo_source.snap_test_source', data_source_id, 'mimeo_dest.table_test_dest', p_sequences := '{"mimeo_dest.col1_seq"}') FROM dblink_mapping_mimeo WHERE username = 'mimeo_test';
SELECT diag('Running maker for destination: mimeo_dest.table_test_dest_nodata');
SELECT table_maker('mimeo_source.snap_test_source', data_source_id, 'mimeo_dest.table_test_dest_nodata', p_pulldata := false) FROM dblink_mapping_mimeo WHERE username = 'mimeo_test';
SELECT diag('Running maker for destination: mimeo_dest.table_test_dest_filter');
SELECT table_maker('mimeo_source.snap_test_source', data_source_id, 'mimeo_dest.table_test_dest_filter', p_filter := '{"col1","col2"}') FROM dblink_mapping_mimeo WHERE username = 'mimeo_test';
SELECT diag('Running maker for destination: mimeo_dest.table_test_dest_condition');
SELECT table_maker('mimeo_source.snap_test_source', data_source_id, 'mimeo_dest.table_test_dest_condition', p_condition := 'WHERE col1 > 9000') FROM dblink_mapping_mimeo WHERE username = 'mimeo_test';
SELECT diag('Running maker for destination: mimeo_dest.table_test_dest_empty');
SELECT table_maker('mimeo_source.snap_test_source_empty', data_source_id, 'mimeo_dest.table_test_dest_empty') FROM dblink_mapping_mimeo WHERE username = 'mimeo_test';
SELECT diag('Running maker for destination: mimeo_dest.Table-test-Source');
SELECT table_maker('mimeo_source.Snap-test-Source', data_source_id, 'mimeo_dest.Table-test-Source', p_sequences := '{"mimeo_dest.PRIMARY-seq"}') FROM dblink_mapping_mimeo WHERE username = 'mimeo_test';
SELECT diag('Running maker for destination: mimeo_dest.table_test_dest_view');
SELECT table_maker('mimeo_source.snap_test_source_view', data_source_id, 'mimeo_dest.table_test_dest_view') FROM dblink_mapping_mimeo WHERE username = 'mimeo_test';
CREATE SEQUENCE mimeo_dest.col1_seq;    -- Add a sequence to test that sequence reset is working
CREATE SEQUENCE mimeo_dest."PRIMARY-seq";
ALTER TABLE mimeo_dest.table_test_dest ALTER col1 SET DEFAULT nextval('mimeo_dest.col1_seq');
ALTER TABLE mimeo_dest."Table-test-Source" ALTER "primary" SET DEFAULT nextval('mimeo_dest."PRIMARY-seq"');

-- Inserter time
SELECT diag('Running maker for destination: mimeo_source.inserter_test_source');
SELECT inserter_maker('mimeo_source.inserter_test_source', 'time', 'col3', data_source_id, '00:00:05') FROM dblink_mapping_mimeo WHERE username = 'mimeo_test';
SELECT diag('Running maker for destination: mimeo_dest.inserter_test_dest');
SELECT inserter_maker('mimeo_source.inserter_test_source', 'time', 'col3', data_source_id,'00:00:05', 'mimeo_dest.inserter_test_dest') FROM dblink_mapping_mimeo WHERE username = 'mimeo_test';
SELECT diag('Running maker for destination: mimeo_dest.inserter_test_dest_nodata');
SELECT inserter_maker('mimeo_source.inserter_test_source', 'time', 'col3', data_source_id,'00:00:05', 'mimeo_dest.inserter_test_dest_nodata', p_pulldata := false) FROM dblink_mapping_mimeo WHERE username = 'mimeo_test';
SELECT diag('Running maker for destination: mimeo_dest.inserter_test_dest_filter');
SELECT inserter_maker('mimeo_source.inserter_test_source', 'time', 'col3', data_source_id,'00:00:05', 'mimeo_dest.inserter_test_dest_filter', p_filter := '{"col1","col3"}') FROM dblink_mapping_mimeo WHERE username = 'mimeo_test';
SELECT diag('Running maker for destination: mimeo_dest.inserter_test_dest_condition');
SELECT inserter_maker('mimeo_source.inserter_test_source', 'time', 'col3', data_source_id,'00:00:05', 'mimeo_dest.inserter_test_dest_condition', p_condition := 'WHERE col1 > 9000') FROM dblink_mapping_mimeo WHERE username = 'mimeo_test';
SELECT diag('Running maker for destination: mimeo_source.inserter_test_source_empty');
SELECT inserter_maker('mimeo_source.inserter_test_source_empty', 'time', 'col3', data_source_id, '00:00:05') FROM dblink_mapping_mimeo WHERE username = 'mimeo_test';
SELECT diag('Running maker for destination: mimeo_source.Inserter-Test-Source');
SELECT inserter_maker('mimeo_source.Inserter-Test-Source', 'time', 'Col-3', data_source_id, '00:00:05') FROM dblink_mapping_mimeo WHERE username = 'mimeo_test';
SELECT diag('Running maker for destination: mimeo_source.inserter_test_source_view');
SELECT inserter_maker('mimeo_source.inserter_test_source_view', 'time', 'col3', data_source_id, '00:00:05') FROM dblink_mapping_mimeo WHERE username = 'mimeo_test';

--Inserter serial
SELECT diag('Running maker for destination: mimeo_dest.inserter_test_dest_serial');
SELECT inserter_maker('mimeo_source.inserter_test_source', 'serial', 'col1', data_source_id, '0', 'mimeo_dest.inserter_test_dest_serial') FROM dblink_mapping_mimeo WHERE username = 'mimeo_test';
SELECT diag('Running maker for destination: mimeo_dest.Inserter-Test-Source_Serial');
SELECT inserter_maker('mimeo_source.Inserter-Test-Source', 'serial', 'col1', data_source_id, '0', 'mimeo_dest.Inserter-Test-Source_Serial') FROM dblink_mapping_mimeo WHERE username = 'mimeo_test';
SELECT diag('Running maker for destination: mimeo_dest.inserter_test_dest_serial_view');
SELECT inserter_maker('mimeo_source.inserter_test_source_view', 'serial', 'col1', data_source_id, '0', 'mimeo_dest.inserter_test_dest_serial_view') FROM dblink_mapping_mimeo WHERE username = 'mimeo_test';

-- Updater time
SELECT diag('Running maker for destination: mimeo_source.updater_test_source');
SELECT updater_maker('mimeo_source.updater_test_source', 'time', 'col3', data_source_id, '00:00:05') FROM dblink_mapping_mimeo WHERE username = 'mimeo_test';
SELECT diag('Running maker for destination: mimeo_dest.updater_test_dest');
SELECT updater_maker('mimeo_source.updater_test_source', 'time', 'col3', data_source_id, '00:00:05', 'mimeo_dest.updater_test_dest') FROM dblink_mapping_mimeo WHERE username = 'mimeo_test';
SELECT diag('Running maker for destination: mimeo_dest.updater_test_dest_nodata');
SELECT updater_maker('mimeo_source.updater_test_source', 'time', 'col3', data_source_id, '00:00:05', 'mimeo_dest.updater_test_dest_nodata', p_pulldata := false) FROM dblink_mapping_mimeo WHERE username = 'mimeo_test';
SELECT diag('Running maker for destination: mimeo_dest.updater_test_dest_filter');
SELECT updater_maker('mimeo_source.updater_test_source', 'time', 'col3', data_source_id, '00:00:05', 'mimeo_dest.updater_test_dest_filter', p_filter := '{"col1","col3"}') FROM dblink_mapping_mimeo WHERE username = 'mimeo_test';
SELECT diag('Running maker for destination: mimeo_dest.updater_test_dest_condition');
SELECT updater_maker('mimeo_source.updater_test_source', 'time', 'col3', data_source_id, '00:00:05', 'mimeo_dest.updater_test_dest_condition', p_condition := 'WHERE col1 > 9000') FROM dblink_mapping_mimeo WHERE username = 'mimeo_test';
SELECT diag('Running maker for destination: mimeo_source.updater_test_source_empty');
SELECT updater_maker('mimeo_source.updater_test_source_empty', 'time', 'col3', data_source_id, '00:00:05') FROM dblink_mapping_mimeo WHERE username = 'mimeo_test';
SELECT diag('Running maker for destination: mimeo_source.Updater-Test-Source');
SELECT updater_maker('mimeo_source.Updater-Test-Source', 'time', 'Col3', data_source_id, '00:00:05') FROM dblink_mapping_mimeo WHERE username = 'mimeo_test';
SELECT diag('Running maker for destination: mimeo_source.updater_test_source_view');
SELECT updater_maker('mimeo_source.updater_test_source_view', 'time', 'col3', data_source_id, '00:00:05', p_pk_name := '{"col1"}', p_pk_type := '{"int"}') FROM dblink_mapping_mimeo WHERE username = 'mimeo_test';

-- Updater serial
SELECT diag('Running maker for destination: mimeo_dest.updater_test_dest_serial');
SELECT updater_maker('mimeo_source.updater_test_source', 'serial', 'col4', data_source_id, '0', 'mimeo_dest.updater_test_dest_serial') FROM dblink_mapping_mimeo WHERE username = 'mimeo_test';
SELECT diag('Running maker for destination: mimeo_dest.Updater-Test-Source_Serial');
SELECT updater_maker('mimeo_source.Updater-Test-Source', 'serial', 'COL-1', data_source_id, '0', 'mimeo_dest.Updater-Test-Source_Serial') FROM dblink_mapping_mimeo WHERE username = 'mimeo_test';
SELECT diag('Running maker for destination: mimeo_dest.updater_test_dest_serial_view');
SELECT updater_maker('mimeo_source.updater_test_source_view', 'serial', 'col4', data_source_id, '0', 'mimeo_dest.updater_test_dest_serial_view', p_pk_name := '{"col1"}', p_pk_type := '{"int"}') FROM dblink_mapping_mimeo WHERE username = 'mimeo_test';

-- DML
SELECT diag('Running maker for destination: mimeo_source.dml_test_source');
SELECT dml_maker('mimeo_source.dml_test_source', data_source_id) FROM dblink_mapping_mimeo WHERE username = 'mimeo_test';
SELECT diag('Running maker for destination: mimeo_dest.dml_test_dest');
SELECT dml_maker('mimeo_source.dml_test_source2', data_source_id, 'mimeo_dest.dml_test_dest') FROM dblink_mapping_mimeo WHERE username = 'mimeo_test';
-- Testing insert_on_fetch = false
UPDATE refresh_config_dml SET insert_on_fetch = false WHERE dest_table = 'mimeo_dest.dml_test_dest';
SELECT diag('Running maker for destination: mimeo_dest.dml_test_dest_multi');
SELECT dml_maker('mimeo_source.dml_test_source', data_source_id, 'mimeo_dest.dml_test_dest_multi') FROM dblink_mapping_mimeo WHERE username = 'mimeo_test';
SELECT diag('Running maker for destination: mimeo_dest.dml_test_dest_nodata');
SELECT dml_maker('mimeo_source.dml_test_source_nodata', data_source_id, 'mimeo_dest.dml_test_dest_nodata', p_pulldata := false) FROM dblink_mapping_mimeo WHERE username = 'mimeo_test';
SELECT diag('Running maker for destination: mimeo_dest.dml_test_dest_filter');
SELECT dml_maker('mimeo_source.dml_test_source_filter', data_source_id, 'mimeo_dest.dml_test_dest_filter', p_filter := '{"col1","col2"}') FROM dblink_mapping_mimeo WHERE username = 'mimeo_test';
SELECT diag('Running maker for destination: mimeo_dest.dml_test_dest_condition');
SELECT dml_maker('mimeo_source.dml_test_source_condition', data_source_id, 'mimeo_dest.dml_test_dest_condition', p_condition := 'WHERE col1 > 9000') FROM dblink_mapping_mimeo WHERE username = 'mimeo_test';
SELECT diag('Running maker for destination: mimeo_source.dml_test_source_empty');
SELECT dml_maker('mimeo_source.dml_test_source_empty', data_source_id) FROM dblink_mapping_mimeo WHERE username = 'mimeo_test';
SELECT diag('Running maker for destination: mimeo_source.Dml-Test-Source');
SELECT dml_maker('mimeo_source.Dml-Test-Source', data_source_id) FROM dblink_mapping_mimeo WHERE username = 'mimeo_test';

-- Logdel
SELECT diag('Running maker for destination: mimeo_source.logdel_test_source');
SELECT logdel_maker('mimeo_source.logdel_test_source', data_source_id) FROM dblink_mapping_mimeo WHERE username = 'mimeo_test';
SELECT diag('Running maker for destination: mimeo_dest.logdel_test_dest');
SELECT logdel_maker('mimeo_source.logdel_test_source2', data_source_id, 'mimeo_dest.logdel_test_dest') FROM dblink_mapping_mimeo WHERE username = 'mimeo_test';
-- Testing insert_on_fetch = false
UPDATE refresh_config_logdel SET insert_on_fetch = false WHERE dest_table = 'mimeo_dest.logdel_test_dest';

SELECT diag('Running maker for destination: mimeo_dest.logdel_test_dest_multi');
SELECT logdel_maker('mimeo_source.logdel_test_source', data_source_id, 'mimeo_dest.logdel_test_dest_multi') FROM dblink_mapping_mimeo WHERE username = 'mimeo_test';
SELECT diag('Running maker for destination: mimeo_dest.logdel_test_dest_nodata');
SELECT logdel_maker('mimeo_source.logdel_test_source_nodata', data_source_id, 'mimeo_dest.logdel_test_dest_nodata', p_pulldata := false) FROM dblink_mapping_mimeo WHERE username = 'mimeo_test';
SELECT diag('Running maker for destination: mimeo_dest.logdel_test_dest_filter');
SELECT logdel_maker('mimeo_source.logdel_test_source_filter', data_source_id, 'mimeo_dest.logdel_test_dest_filter', p_filter := '{"col1","col2"}') FROM dblink_mapping_mimeo WHERE username = 'mimeo_test';
SELECT diag('Running maker for destination: mimeo_dest.logdel_test_dest_condition');
SELECT logdel_maker('mimeo_source.logdel_test_source_condition', data_source_id, 'mimeo_dest.logdel_test_dest_condition', p_condition := 'WHERE col1 > 9000') FROM dblink_mapping_mimeo WHERE username = 'mimeo_test';
SELECT diag('Running maker for destination: mimeo_source.logdel_test_source_empty');
SELECT logdel_maker('mimeo_source.logdel_test_source_empty', data_source_id) FROM dblink_mapping_mimeo WHERE username = 'mimeo_test';
SELECT diag('Running maker for destination: mimeo_source.LogDel-Test-Source');
SELECT logdel_maker('mimeo_source.LogDel-Test-Source', data_source_id) FROM dblink_mapping_mimeo WHERE username = 'mimeo_test';

SELECT pass('Maker functions finished. Sleeping for 40 seconds to ensure gap for incremental tests...');
SELECT pg_sleep(40);

SELECT * FROM finish();
