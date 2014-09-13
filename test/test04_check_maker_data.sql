\set ON_ERROR_ROLLBACK 1
\set ON_ERROR_STOP true

SELECT set_config('search_path','mimeo, dblink, public',false);

SELECT plan(173);

SELECT dblink_connect('mimeo_test', 'host=localhost port=5432 dbname=mimeo_source user=mimeo_owner password=mimeo_owner');
SELECT is(dblink_get_connections() @> '{mimeo_test}', 't', 'Remote database connection established');

-- ########## SNAP TESTS ##########
SELECT has_view('mimeo_source', 'snap_test_source', 'Check that view exists: mimeo_source.snap_test_source');
SELECT results_eq('SELECT col1, col2, col3 FROM mimeo_source.snap_test_source ORDER BY col1 ASC',
    'SELECT * FROM dblink(''mimeo_test'', ''SELECT col1, col2, col3 FROM mimeo_source.snap_test_source ORDER BY col1 ASC'') t (col1 int, col2 text, col3 timestamptz)',
    'Check data for: mimeo_source.snap_test_source');
SELECT has_index('mimeo_source','snap_test_source_snap1','snap1_snap_test_source_snap1_col2_idx','col2','Check index for: mimeo_source.snap_test_source_snap1');
SELECT has_index('mimeo_source','snap_test_source_snap2','snap2_snap_test_source_snap2_col2_idx','col2','Check index for: mimeo_source.snap_test_source_snap2');

SELECT has_view('mimeo_dest', 'snap_test_dest', 'Check that view exists: mimeo_dest.snap_test_dest');
SELECT results_eq('SELECT col1, col2, col3 FROM mimeo_dest.snap_test_dest ORDER BY col1 ASC',
    'SELECT * FROM dblink(''mimeo_test'', ''SELECT col1, col2, col3 FROM mimeo_source.snap_test_source ORDER BY col1 ASC'') t (col1 int, col2 text, col3 timestamptz)',
    'Check data for: mimeo_dest.snap_test_dest');
SELECT has_index('mimeo_dest','snap_test_dest_snap1','snap1_snap_test_dest_snap1_col2_idx','col2','Check index for: mimeo_dest.snap_test_dest_snap1');
SELECT has_index('mimeo_dest','snap_test_dest_snap2','snap2_snap_test_dest_snap2_col2_idx','col2','Check index for: mimeo_dest.snap_test_dest_snap2');

SELECT has_view('mimeo_dest', 'snap_test_dest_nodata', 'Check that view exists: mimeo_dest.snap_test_dest_nodata');
SELECT is_empty('SELECT col1, col2, col3 FROM mimeo_dest.snap_test_dest_nodata ORDER BY col1 ASC', 'Check data for: mimeo_dest.snap_test_dest_nodata');
SELECT has_index('mimeo_dest','snap_test_dest_nodata_snap1','snap1_snap_test_dest_nodata_snap1_col2_idx','col2','Check index for: mimeo_dest.snap_test_dest_nodata_snap1');
SELECT has_index('mimeo_dest','snap_test_dest_nodata_snap2','snap2_snap_test_dest_nodata_snap2_col2_idx','col2','Check index for: mimeo_dest.snap_test_dest_nodata_snap2');

SELECT has_view('mimeo_dest', 'snap_test_dest_filter', 'Check that view exists: mimeo_dest.snap_test_dest_filter');
SELECT results_eq('SELECT col1, col2 FROM mimeo_dest.snap_test_dest_filter ORDER BY col1 ASC',
    'SELECT * FROM dblink(''mimeo_test'', ''SELECT col1, col2 FROM mimeo_source.snap_test_source ORDER BY col1 ASC'') t (col1 int, col2 text)',
    'Check data for: mimeo_dest.snap_test_dest_filter');
-- Column filter means regular indexes cannot be made
SELECT hasnt_index('mimeo_dest','snap_test_dest_filter_snap1','snap1_snap_test_dest_filter_snap1_col2_idx','Check index for: mimeo_dest.snap_test_dest_filter_snap1');
SELECT hasnt_index('mimeo_dest','snap_test_dest_filter_snap2','snap2_snap_test_dest_filter_snap2_col2_idx','Check index for: mimeo_dest.snap_test_dest_filter_snap2');
SELECT hasnt_column('mimeo_dest', 'snap_test_dest_filter_snap1', 'col3', 'Check that snap_test_dest_filter_snap1 DOESN''T have col3');
SELECT hasnt_column('mimeo_dest', 'snap_test_dest_filter_snap2', 'col3', 'Check that snap_test_dest_filter_snap2 DOESN''T have col3');

SELECT has_view('mimeo_dest', 'snap_test_dest_condition', 'Check that view exists: mimeo_dest.snap_test_dest_condition');
SELECT results_eq('SELECT col1, col2, col3 FROM mimeo_dest.snap_test_dest_condition ORDER BY col1 ASC',
    'SELECT * FROM dblink(''mimeo_test'', ''SELECT col1, col2, col3 FROM mimeo_source.snap_test_source WHERE col1 > 9000 ORDER BY col1 ASC'') t (col1 int, col2 text, col3 timestamptz)',
    'Check data for: mimeo_dest.snap_test_dest_condition');
SELECT has_index('mimeo_dest','snap_test_dest_condition_snap1','snap1_snap_test_dest_condition_snap1_col2_idx','col2','Check index for: mimeo_dest.snap_test_dest_condition_snap1');
SELECT has_index('mimeo_dest','snap_test_dest_condition_snap2','snap2_snap_test_dest_condition_snap2_col2_idx','col2','Check index for: mimeo_dest.snap_test_dest_condition_snap2');

SELECT has_view('mimeo_source', 'snap_test_source_empty', 'Check that view exists: mimeo_source.snap_test_source_empty');
SELECT is_empty('SELECT col1, col2, col3 FROM mimeo_source.snap_test_source_empty ORDER BY col1 ASC', 'Check data for: mimeo_source.snap_test_source_empty');
SELECT index_is_unique('mimeo_source','snap_test_source_empty_snap1', 'snap1_snap_test_source_empty_snap1_col1_key', 'Check unique index for: mimeo_source.snap_test_source_empty_snap1');
SELECT index_is_unique('mimeo_source','snap_test_source_empty_snap2', 'snap2_snap_test_source_empty_snap2_col1_key', 'Check unique index for: mimeo_source.snap_test_source_empty_snap2');

SELECT has_view('mimeo_dest', 'snap_test_dest_change_col', 'Check that view exists: mimeo_dest.snap_test_dest_change_col');
SELECT results_eq('SELECT col1, col2, col3 FROM mimeo_dest.snap_test_dest_change_col ORDER BY col1 ASC',
    'SELECT * FROM dblink(''mimeo_test'', ''SELECT col1, col2, col3 FROM mimeo_source.snap_test_source_change_col ORDER BY col1 ASC'') t (col1 int, col2 text, col3 timestamptz)',
    'Check data for: mimeo_dest.snap_test_dest');
SELECT col_is_pk('mimeo_dest','snap_test_dest_change_col_snap1', ARRAY['col1'],'Check primary key for: mimeo_dest.snap_test_dest_change_col_snap1');
SELECT col_is_pk('mimeo_dest','snap_test_dest_change_col_snap2', ARRAY['col1'],'Check primary key for: mimeo_dest.snap_test_dest_change_col_snap2');
SELECT has_index('mimeo_dest', 'snap_test_dest_change_col_snap1', 'snap1_mimeo_check_exp_index_time', '((col3 > ''2013-04-01 00:00:00-04''::timestamp with time zone))', 'Check time expression index on mimeo_dest.snap_test_dest_change_col_snap1'); 
SELECT has_index('mimeo_dest', 'snap_test_dest_change_col_snap2', 'snap2_mimeo_check_exp_index_time', '((col3 > ''2013-04-01 00:00:00-04''::timestamp with time zone))', 'Check time expression index on mimeo_dest.snap_test_dest_change_col_snap2'); 
SELECT has_index('mimeo_dest', 'snap_test_dest_change_col_snap1', 'snap1_mimeo_check_exp_index_lower', 'lower(col2)', 'Check lower() expression index on mimeo_dest.snap_test_dest_change_col_snap1'); 
SELECT has_index('mimeo_dest', 'snap_test_dest_change_col_snap2', 'snap2_mimeo_check_exp_index_lower', 'lower(col2)', 'Check lower() expression index on mimeo_dest.snap_test_dest_change_col_snap2'); 
SELECT table_privs_are('mimeo_dest', 'snap_test_dest_change_col', 'mimeo_dumb_role', ARRAY['SELECT'], 'Checking mimeo_dumb_role privileges for mimeo_dest.snap_test_dest_change_col');
SELECT table_privs_are('mimeo_dest', 'snap_test_dest_change_col_snap1', 'mimeo_dumb_role', ARRAY['SELECT'], 'Checking mimeo_dumb_role privileges for mimeo_dest.snap_test_dest_change_col_snap1');
SELECT table_privs_are('mimeo_dest', 'snap_test_dest_change_col_snap2', 'mimeo_dumb_role', ARRAY['SELECT'], 'Checking mimeo_dumb_role privileges for mimeo_dest.snap_test_dest_change_col_snap2');
SELECT view_owner_is ('mimeo_dest', 'snap_test_dest_change_col', 'mimeo_test', 'Check ownership for view mimeo_dest.snap_test_dest_change_col');
SELECT table_owner_is ('mimeo_dest', 'snap_test_dest_change_col_snap1', 'mimeo_test', 'Check ownership for view mimeo_dest.snap_test_dest_change_col_snap1');
SELECT table_owner_is ('mimeo_dest', 'snap_test_dest_change_col_snap2', 'mimeo_test', 'Check ownership for view mimeo_dest.snap_test_dest_change_col_snap2');

SELECT has_view('mimeo_source', 'Snap-test-Source', 'Check that view exists: mimeo_source.Snap-test-Source');
SELECT results_eq('SELECT "primary", col2, "COL-3" FROM mimeo_source."Snap-test-Source" ORDER BY "primary" ASC',
    'SELECT * FROM dblink(''mimeo_test'', ''SELECT "primary", col2, "COL-3" FROM mimeo_source."Snap-test-Source" ORDER BY "primary" ASC'') t ("primary" int, col2 text, "Col-3" timestamptz)',
    'Check data for: mimeo_source.Snap-test-Source');
SELECT has_index('mimeo_source','Snap-test-Source_snap1', 'snap1_Snap-test-Source_col2_idx', 'col2','Check index for: mimeo_dest.Snap-test-Source_snap1');
SELECT has_index('mimeo_source','Snap-test-Source_snap2', 'snap2_Snap-test-Source_col2_idx', 'col2','Check index for: mimeo_dest.Snap-test-Source_snap1');
SELECT table_privs_are('mimeo_source', 'Snap-test-Source', 'mimeo_dumb_role', ARRAY['SELECT'], 'Checking mimeo_dumb_role privileges for mimeo_source.Snap-test-Source');
SELECT table_privs_are('mimeo_source', 'Snap-test-Source_snap1', 'mimeo_dumb_role', ARRAY['SELECT'], 'Checking mimeo_dumb_role privileges for mimeo_source.Snap-test-Source_snap1');
SELECT table_privs_are('mimeo_source', 'Snap-test-Source_snap2', 'mimeo_dumb_role', ARRAY['SELECT'], 'Checking mimeo_dumb_role privileges for mimeo_source.Snap-test-Source_snap2');
SELECT view_owner_is ('mimeo_source', 'Snap-test-Source', 'mimeo_test', 'Check ownership for view mimeo_source.Snap-test-Source');
SELECT table_owner_is ('mimeo_source', 'Snap-test-Source_snap1', 'mimeo_test', 'Check ownership for view mimeo_source.Snap-test-Source_snap1');
SELECT table_owner_is ('mimeo_source', 'Snap-test-Source_snap2', 'mimeo_test', 'Check ownership for view mimeo_source.Snap-test-Source_snap2');

SELECT has_view('mimeo_source', 'snap_test_source_view', 'Check that view exists: mimeo_source.snap_test_source_view');
SELECT results_eq('SELECT col1, col2, col3 FROM mimeo_source.snap_test_source_view ORDER BY col1 ASC',
    'SELECT * FROM dblink(''mimeo_test'', ''SELECT col1, col2, col3 FROM mimeo_source.snap_test_source ORDER BY col1 ASC'') t (col1 int, col2 text, col3 timestamptz)',
    'Check data for: mimeo_source.snap_test_source_view');

-- ########## PLAIN TABLE TESTS ##########
SELECT results_eq('SELECT col1, col2, col3 FROM mimeo_dest.table_test_dest ORDER BY col1 ASC',
    'SELECT * FROM dblink(''mimeo_test'', ''SELECT col1, col2, col3 FROM mimeo_source.snap_test_source ORDER BY col1 ASC'') t (col1 int, col2 text, col3 timestamptz)',
    'Check data for: mimeo_dest.table_test_dest');
SELECT has_index('mimeo_dest','table_test_dest','table_test_dest_col2_idx','col2','Check index for: mimeo_dest.table_test_dest');
SELECT has_sequence('mimeo_dest', 'col1_seq', 'Checking that sequence exists: mimeo_dest.col1_seq');

SELECT is_empty('SELECT col1, col2, col3 FROM mimeo_dest.table_test_dest_nodata ORDER BY col1 ASC', 'Check data for: mimeo_dest.table_test_dest_nodata');
SELECT has_index('mimeo_dest','table_test_dest_nodata','table_test_dest_nodata_col2_idx','col2','Check index for: mimeo_dest.table_test_dest_nodata');

SELECT results_eq('SELECT col1, col2 FROM mimeo_dest.table_test_dest_filter ORDER BY col1 ASC',
    'SELECT * FROM dblink(''mimeo_test'', ''SELECT col1, col2 FROM mimeo_source.snap_test_source ORDER BY col1 ASC'') t (col1 int, col2 text)',
    'Check data for: mimeo_dest.table_test_dest_filter');
-- Column filter means regular indexes cannot be made
SELECT hasnt_index('mimeo_dest','table_test_dest_filter','table_test_dest_filter_snap1_col2_idx','Check index for: mimeo_dest.table_test_dest_filter');
SELECT hasnt_column('mimeo_dest', 'table_test_dest_filter', 'col3', 'Check that table_test_dest_filter_snap1 DOESN''T have col3');

SELECT results_eq('SELECT col1, col2, col3 FROM mimeo_dest.table_test_dest_condition ORDER BY col1 ASC',
    'SELECT * FROM dblink(''mimeo_test'', ''SELECT col1, col2, col3 FROM mimeo_source.snap_test_source WHERE col1 > 9000 ORDER BY col1 ASC'') t (col1 int, col2 text, col3 timestamptz)',
    'Check data for: mimeo_dest.table_test_dest_condition');
SELECT has_index('mimeo_dest','table_test_dest_condition','table_test_dest_condition_col2_idx','col2','Check index for: mimeo_dest.table_test_dest_condition');

SELECT is_empty('SELECT col1, col2, col3 FROM mimeo_dest.table_test_dest_empty ORDER BY col1 ASC', 'Check data for: mimeo_dest.table_test_dest_empty');

SELECT results_eq('SELECT "primary", col2, "COL-3" FROM mimeo_dest."Table-test-Source" ORDER BY "primary" ASC',
    'SELECT * FROM dblink(''mimeo_test'', ''SELECT "primary", col2, "COL-3" FROM mimeo_source."Snap-test-Source" ORDER BY "primary" ASC'') t ("primary" int, col2 text, col3 timestamptz)',
    'Check data for: mimeo_dest.Table-test-Source');
SELECT has_index('mimeo_dest','Table-test-Source','Snap-test-Source_col2_idx','col2','Check index for: mimeo_dest.Table-test-Source');
SELECT has_sequence('mimeo_dest', 'PRIMARY-seq', 'Checking that sequence exists: mimeo_dest.PRIMARY-seq');

SELECT results_eq('SELECT col1, col2, col3 FROM mimeo_dest.table_test_dest_view ORDER BY col1 ASC',
    'SELECT * FROM dblink(''mimeo_test'', ''SELECT col1, col2, col3 FROM mimeo_source.snap_test_source ORDER BY col1 ASC'') t (col1 int, col2 text, col3 timestamptz)',
    'Check data for: mimeo_dest.table_test_dest_view');

-- ########## INSERTER TESTS ##########
-- Time
SELECT results_eq('SELECT col1, col2, col3 FROM mimeo_source.inserter_test_source ORDER BY col1 ASC',
    'SELECT * FROM dblink(''mimeo_test'', ''SELECT col1, col2, col3 FROM mimeo_source.inserter_test_source ORDER BY col1 ASC'') t (col1 int, col2 text, col3 timestamptz)',
    'Check data for: mimeo_source.inserter_test_source');
SELECT col_is_pk('mimeo_source','inserter_test_source', 'col1', 'Check primary key for: mimeo_source.inserter_test_source');
SELECT has_index('mimeo_source','inserter_test_source','inserter_test_source_col2_idx','col2','Check index for: mimeo_source.inserter_test_source');

SELECT results_eq('SELECT col1, col2, col3 FROM mimeo_dest.inserter_test_dest ORDER BY col1 ASC',
    'SELECT * FROM dblink(''mimeo_test'', ''SELECT col1, col2, col3 FROM mimeo_source.inserter_test_source ORDER BY col1 ASC'') t (col1 int, col2 text, col3 timestamptz)',
    'Check data for: mimeo_dest.inserter_test_dest');
SELECT col_is_pk('mimeo_dest','inserter_test_dest', 'col1', 'Check primary key for: mimeo_dest.inserter_test_dest');
SELECT has_index('mimeo_dest','inserter_test_dest','inserter_test_dest_col2_idx','col2','Check index for: mimeo_dest.inserter_test_dest');

SELECT is_empty('SELECT col1, col2, col3 FROM mimeo_dest.inserter_test_dest_nodata ORDER BY col1 ASC', 'Check data for: mimeo_dest.inserter_test_dest_nodata');
SELECT col_is_pk('mimeo_dest','inserter_test_dest_nodata', 'col1', 'Check primary key for: mimeo_dest.inserter_test_dest_nodata');
SELECT has_index('mimeo_dest','inserter_test_dest_nodata','inserter_test_dest_nodata_col2_idx','col2','Check index for: mimeo_dest.inserter_test_dest_nodata');

SELECT results_eq('SELECT col1, col3 FROM mimeo_dest.inserter_test_dest_filter ORDER BY col1 ASC',
    'SELECT * FROM dblink(''mimeo_test'', ''SELECT col1, col3 FROM mimeo_source.inserter_test_source ORDER BY col1 ASC'') t (col1 int, col3 timestamptz)',
    'Check data for: mimeo_dest.inserter_test_dest_filter');
SELECT col_is_pk('mimeo_dest','inserter_test_dest_filter', 'col1', 'Check primary key for: mimeo_dest.inserter_test_dest_filter');
SELECT hasnt_column('mimeo_dest', 'inserter_test_dest_filter', 'col2', 'Check that mimeo_dest.inserter_test_dest_filter DOESN''T have col2');

SELECT results_eq('SELECT col1, col2, col3 FROM mimeo_dest.inserter_test_dest_condition ORDER BY col1 ASC',
    'SELECT * FROM dblink(''mimeo_test'', ''SELECT col1, col2, col3 FROM mimeo_source.inserter_test_source WHERE col1 > 9000 ORDER BY col1 ASC'') t (col1 int, col2 text, col3 timestamptz)',
    'Check data for: mimeo_dest.inserter_test_dest_condition');
SELECT col_is_pk('mimeo_dest','inserter_test_dest_condition', 'col1', 'Check primary key for: mimeo_dest.inserter_test_dest_condition');
SELECT has_index('mimeo_dest','inserter_test_dest_condition','inserter_test_dest_condition_col2_idx','col2','Check index for: mimeo_dest.inserter_test_dest_condition');

SELECT is_empty('SELECT col1, col2, col3 FROM mimeo_source.inserter_test_source_empty ORDER BY col1 ASC', 'Check data for: mimeo_source.inserter_test_dest_empty');

SELECT results_eq('SELECT col1, "group", "Col-3" FROM mimeo_source."Inserter-Test-Source" ORDER BY col1 ASC',
    'SELECT * FROM dblink(''mimeo_test'', ''SELECT col1, "group", "Col-3" FROM mimeo_source."Inserter-Test-Source" ORDER BY col1 ASC'') t (col1 int, "group" text, "Col-3" timestamptz)',
    'Check data for: mimeo_source."Inserter-Test-Source"');
SELECT col_is_pk('mimeo_source','Inserter-Test-Source', 'col1', 'Check primary key for: mimeo_source.Inserter-Test-Source');
SELECT has_index('mimeo_source','Inserter-Test-Source','Inserter-Test-Source-group-Idx','"group"','Check index for: mimeo_source.Inserter-Test-Source');

SELECT results_eq('SELECT col1, col2, col3 FROM mimeo_source.inserter_test_source_view ORDER BY col1 ASC',
    'SELECT * FROM dblink(''mimeo_test'', ''SELECT col1, col2, col3 FROM mimeo_source.inserter_test_source ORDER BY col1 ASC'') t (col1 int, col2 text, col3 timestamptz)',
    'Check data for: mimeo_source.inserter_test_source_view');

-- Serial
SELECT results_eq('SELECT col1, col2, col3 FROM mimeo_dest.inserter_test_dest_serial ORDER BY col1 ASC',
    'SELECT * FROM dblink(''mimeo_test'', ''SELECT col1, col2, col3 FROM mimeo_source.inserter_test_source WHERE col1 < (SELECT max(col1) FROM mimeo_source.inserter_test_source) ORDER BY col1 ASC'') t (col1 int, col2 text, col3 timestamptz)',
    'Check data for: mimeo_dest.inserter_test_dest_serial');
SELECT col_is_pk('mimeo_dest','inserter_test_dest_serial', 'col1', 'Check primary key for: mimeo_dest.inserter_test_dest_serial');
SELECT has_index('mimeo_dest','inserter_test_dest_serial','inserter_test_dest_serial_col2_idx','col2','Check index for: mimeo_dest.inserter_test_dest_serial');

SELECT results_eq('SELECT col1, "group", "Col-3" FROM mimeo_dest."Inserter-Test-Source_Serial" ORDER BY col1 ASC',
    'SELECT * FROM dblink(''mimeo_test'', ''SELECT col1, "group", "Col-3" FROM mimeo_source."Inserter-Test-Source" WHERE col1 < (SELECT max(col1) FROM mimeo_source."Inserter-Test-Source") ORDER BY col1 ASC'') t (col1 int, "group" text, "Col-3" timestamptz)',
    'Check data for: mimeo_dest."Inserter-Test-Source_Serial"');
SELECT col_is_pk('mimeo_dest','Inserter-Test-Source_Serial', 'col1', 'Check primary key for: mimeo_source.Inserter-Test-Source');
SELECT has_index('mimeo_dest','Inserter-Test-Source_Serial','Inserter-Test-Source-group-Idx','"group"','Check index for: mimeo_source.Inserter-Test-Source_Serial');

SELECT results_eq('SELECT col1, col2, col3 FROM mimeo_dest.inserter_test_dest_serial_view ORDER BY col1 ASC',
    'SELECT * FROM dblink(''mimeo_test'', ''SELECT col1, col2, col3 FROM mimeo_source.inserter_test_source WHERE col1 < (SELECT max(col1) FROM mimeo_source.inserter_test_source) ORDER BY col1 ASC'') t (col1 int, col2 text, col3 timestamptz)',
    'Check data for: mimeo_dest.inserter_test_dest_serial_view');

-- ########## UPDATER TESTS ##########
-- Time
SELECT results_eq('SELECT col1, col2, col3 FROM mimeo_source.updater_test_source ORDER BY col1 ASC',
    'SELECT * FROM dblink(''mimeo_test'', ''SELECT col1, col2, col3 FROM mimeo_source.updater_test_source ORDER BY col1 ASC'') t (col1 int, col2 text, col3 timestamptz)',
    'Check data for: mimeo_source.updater_test_source');
SELECT col_is_pk('mimeo_source','updater_test_source', 'col1', 'Check primary key for: mimeo_source.updater_test_source');
SELECT has_index('mimeo_source','updater_test_source','updater_test_source_col2_idx','col2','Check index for: mimeo_source.updater_test_source');

SELECT results_eq('SELECT col1, col2, col3 FROM mimeo_dest.updater_test_dest ORDER BY col1 ASC',
    'SELECT * FROM dblink(''mimeo_test'', ''SELECT col1, col2, col3 FROM mimeo_source.updater_test_source ORDER BY col1 ASC'') t (col1 int, col2 text, col3 timestamptz)',
    'Check data for: mimeo_dest.updater_test_dest');
SELECT col_is_pk('mimeo_dest','updater_test_dest', 'col1', 'Check primary key for: mimeo_dest.updater_test_dest');
SELECT has_index('mimeo_dest','updater_test_dest','updater_test_dest_col2_idx','col2','Check index for: mimeo_dest.updater_test_dest');

SELECT is_empty('SELECT col1, col2, col3 FROM mimeo_dest.updater_test_dest_nodata ORDER BY col1 ASC', 'Check data for: mimeo_dest.updater_test_dest_nodata');
SELECT col_is_pk('mimeo_dest','updater_test_dest_nodata', 'col1', 'Check primary key for: mimeo_dest.updater_test_dest_nodata');
SELECT has_index('mimeo_dest','updater_test_dest_nodata','updater_test_dest_nodata_col2_idx','col2','Check index for: mimeo_dest.updater_test_dest_nodata');

SELECT results_eq('SELECT col1, col3 FROM mimeo_dest.updater_test_dest_filter ORDER BY col1 ASC',
    'SELECT * FROM dblink(''mimeo_test'', ''SELECT col1, col3 FROM mimeo_source.updater_test_source ORDER BY col1 ASC'') t (col1 int, col3 timestamptz)',
    'Check data for: mimeo_dest.updater_test_dest_filter');
SELECT col_is_pk('mimeo_dest','updater_test_dest_filter', 'col1', 'Check primary key for: mimeo_dest.updater_test_dest_filter');
SELECT hasnt_column('mimeo_dest', 'updater_test_dest_filter', 'col2', 'Check that mimeo_dest.updater_test_dest_filter DOESN''T have col2');

SELECT results_eq('SELECT col1, col2, col3 FROM mimeo_dest.updater_test_dest_condition ORDER BY col1 ASC',
    'SELECT * FROM dblink(''mimeo_test'', ''SELECT col1, col2, col3 FROM mimeo_source.updater_test_source WHERE col1 > 9000 ORDER BY col1 ASC'') t (col1 int, col2 text, col3 timestamptz)',
    'Check data for: mimeo_dest.updater_test_dest_condition');
SELECT col_is_pk('mimeo_dest','updater_test_dest_condition', 'col1', 'Check primary key for: mimeo_dest.updater_test_dest_condition');
SELECT has_index('mimeo_dest','updater_test_dest_condition','updater_test_dest_condition_col2_idx','col2','Check index for: mimeo_dest.updater_test_dest_condition');

SELECT is_empty('SELECT col1, col2, col3 FROM mimeo_source.updater_test_source_empty ORDER BY col1 ASC', 'Check data for: mimeo_source.updater_test_source_empty');

SELECT results_eq('SELECT "COL-1", "group", "Col3" FROM mimeo_source."Updater-Test-Source" ORDER BY "COL-1" ASC',
    'SELECT * FROM dblink(''mimeo_test'', ''SELECT "COL-1", "group", "Col3" FROM mimeo_source."Updater-Test-Source" ORDER BY "COL-1" ASC'') t ("COL-1" int, "group" text, "Col3" timestamptz)',
    'Check data for: mimeo_source."Updater-Test-Source"');
SELECT col_is_pk('mimeo_source','Updater-Test-Source', 'COL-1', 'Check primary key for: mimeo_source."Updater-Test-Source"');
SELECT has_index('mimeo_source','Updater-Test-Source','Updater-Test-Source-group-Idx','"group"','Check index for: mimeo_source."Updater-Test-Source"');

SELECT results_eq('SELECT col1, col2, col3 FROM mimeo_source.updater_test_source_view ORDER BY col1 ASC',
    'SELECT * FROM dblink(''mimeo_test'', ''SELECT col1, col2, col3 FROM mimeo_source.updater_test_source ORDER BY col1 ASC'') t (col1 int, col2 text, col3 timestamptz)',
    'Check data for: mimeo_source.updater_test_source_view');

-- Serial
SELECT results_eq('SELECT col1, col2, col3, col4 FROM mimeo_dest.updater_test_dest_serial ORDER BY col4 ASC',
    'SELECT * FROM dblink(''mimeo_test'', ''SELECT col1, col2, col3, col4 FROM mimeo_source.updater_test_source WHERE col4 < (SELECT max(col4) FROM mimeo_source.updater_test_source ) ORDER BY col4 ASC'') t (col1 int, col2 text, col3 timestamptz, col4 int)',
    'Check data for: mimeo_dest.updater_test_dest_serial');
SELECT col_is_pk('mimeo_dest','updater_test_dest', 'col1', 'Check primary key for: mimeo_dest.updater_test_dest_serial');
SELECT has_index('mimeo_dest','updater_test_dest','updater_test_dest_col2_idx','col2','Check index for: mimeo_dest.updater_test_dest_serial (co2)');
SELECT has_index('mimeo_dest','updater_test_dest','updater_test_dest_col4_idx','col4','Check index for: mimeo_dest.updater_test_dest_serial (col4)');

SELECT results_eq('SELECT "COL-1", "group", "Col3" FROM mimeo_dest."Updater-Test-Source_Serial" ORDER BY "COL-1" ASC',
    'SELECT * FROM dblink(''mimeo_test'', ''SELECT "COL-1", "group", "Col3" FROM mimeo_source."Updater-Test-Source" WHERE "COL-1" < (SELECT max("COL-1") FROM mimeo_source."Updater-Test-Source") ORDER BY "COL-1" ASC'') t ("COL-1" int, "group" text, "Col3" timestamptz)',
    'Check data for: mimeo_source."Updater-Test-Source_Serial"');
SELECT col_is_pk('mimeo_dest','Updater-Test-Source_Serial', 'COL-1', 'Check primary key for: mimeo_source."Updater-Test-Source_Serial"');
SELECT has_index('mimeo_dest','Updater-Test-Source_Serial','Updater-Test-Source-group-Idx','"group"','Check index for: mimeo_source."Updater-Test-Source_Serial"');

SELECT results_eq('SELECT col1, col2, col3, col4 FROM mimeo_dest.updater_test_dest_serial_view ORDER BY col4 ASC',
    'SELECT * FROM dblink(''mimeo_test'', ''SELECT col1, col2, col3, col4 FROM mimeo_source.updater_test_source WHERE col4 < (SELECT max(col4) FROM mimeo_source.updater_test_source ) ORDER BY col4 ASC'') t (col1 int, col2 text, col3 timestamptz, col4 int)',
    'Check data for: mimeo_dest.updater_test_dest_serial_view');

-- ########## DML TESTS ##########
SELECT results_eq('SELECT col1, col2, col3 FROM mimeo_source.dml_test_source ORDER BY col1 ASC',
    'SELECT * FROM dblink(''mimeo_test'', ''SELECT col1, col2, col3 FROM mimeo_source.dml_test_source ORDER BY col1 ASC'') t (col1 int, col2 text, col3 timestamptz)',
    'Check data for: mimeo_source.dml_test_source');
SELECT col_is_pk('mimeo_source','dml_test_source', 'col1', 'Check primary key for: mimeo_source.dml_test_source');
SELECT has_index('mimeo_source','dml_test_source','dml_test_source_col2_idx','col2','Check index for: mimeo_source.dml_test_source');

SELECT results_eq('SELECT col1, col2, col3 FROM mimeo_dest.dml_test_dest ORDER BY col1 ASC',
    'SELECT * FROM dblink(''mimeo_test'', ''SELECT col1, col2, col3 FROM mimeo_source.dml_test_source2 ORDER BY col1 ASC'') t (col1 int, col2 varchar(255), col3 timestamptz)',
    'Check data for: mimeo_dest.dml_test_dest');
SELECT col_is_pk('mimeo_dest','dml_test_dest',ARRAY['col2','col1'],'Check primary key for: mimeo_dest.dml_test_dest');

-- Multi-destination test
SELECT results_eq('SELECT col1, col2, col3 FROM mimeo_dest.dml_test_dest_multi ORDER BY col1 ASC',
    'SELECT * FROM dblink(''mimeo_test'', ''SELECT col1, col2, col3 FROM mimeo_source.dml_test_source ORDER BY col1 ASC'') t (col1 int, col2 text, col3 timestamptz)',
    'Check data for: mimeo_dest.dml_test_dest_multi');
SELECT col_is_pk('mimeo_dest','dml_test_dest_multi','col1','Check primary key for: mimeo_dest.dml_test_dest_multi');
SELECT has_index('mimeo_dest','dml_test_dest_multi','dml_test_dest_multi_col2_idx','col2','Check index for: mimeo_source.dml_test_dest_multi');

SELECT is_empty('SELECT col1, col2, col3 FROM mimeo_dest.dml_test_dest_nodata ORDER BY col1 ASC',
    'Check data for: mimeo_dest.dml_test_dest_nodata');
SELECT index_is_unique('mimeo_dest','dml_test_dest_nodata', 'dml_test_dest_nodata_col1_key', 'Check unique index for: mimeo_dest.dml_test_dest_nodata');

SELECT results_eq('SELECT col1, col2 FROM mimeo_dest.dml_test_dest_filter ORDER BY col1 ASC',
    'SELECT * FROM dblink(''mimeo_test'', ''SELECT col1, col2 FROM mimeo_source.dml_test_source_filter ORDER BY col1 ASC'') t (col1 int, col2 text)',
    'Check data for: mimeo_dest.dml_test_dest_filter');
SELECT index_is_unique('mimeo_dest','dml_test_dest_filter', 'dml_test_dest_filter_col1_key', 'Check unique index for: mimeo_dest.dml_test_dest_filter');
SELECT hasnt_column('mimeo_dest', 'dml_test_dest_filter', 'col3', 'Check that dml_test_dest_filter DOESN''T have col3');

SELECT results_eq('SELECT col1, col2, col3 FROM mimeo_dest.dml_test_dest_condition ORDER BY col1 ASC',
    'SELECT * FROM dblink(''mimeo_test'', ''SELECT col1, col2, col3 FROM mimeo_source.dml_test_source_condition WHERE col1 > 9000 ORDER BY col1 ASC'') t (col1 int, col2 text, col3 timestamptz)',
    'Check data for: mimeo_dest.dml_test_dest_condition');
SELECT col_is_pk('mimeo_dest','dml_test_dest_condition','col1','Check primary key for: mimeo_dest.dml_test_dest_condition');
SELECT index_is_unique('mimeo_dest','dml_test_dest_condition', 'dml_test_dest_condition_col2_key', 'Check unique index for: mimeo_dest.dml_test_dest_condition');

SELECT is_empty('SELECT col1, col2, col3 FROM mimeo_source.dml_test_source_empty ORDER BY col1 ASC', 'Check data for: mimeo_source.dml_test_dest_empty');

SELECT results_eq('SELECT "COL1", "group", "Col-3" FROM mimeo_source."Dml-Test-Source" ORDER BY "COL1" ASC',
    'SELECT * FROM dblink(''mimeo_test'', ''SELECT "COL1", "group", "Col-3" FROM mimeo_source."Dml-Test-Source" ORDER BY "COL1" ASC'') t ("COL1" int, "group" text, "Col-3" timestamptz)',
    'Check data for: mimeo_source.Dml-Test-Source');
SELECT col_is_pk('mimeo_source','Dml-Test-Source', 'COL1', 'Check primary key for: mimeo_source.Dml-Test-Source');
SELECT has_index('mimeo_source','Dml-Test-Source','Dml-Test-Source-group-Idx','"group"','Check index for: mimeo_source.Dml-Test-Source');


-- ########## LOGDEL TESTS ##########
SELECT results_eq('SELECT col1, col2, col3 FROM mimeo_source.logdel_test_source ORDER BY col1 ASC',
    'SELECT * FROM dblink(''mimeo_test'', ''SELECT col1, col2, col3 FROM mimeo_source.logdel_test_source ORDER BY col1 ASC'') t (col1 int, col2 text, col3 timestamptz)',
    'Check data for: mimeo_source.logdel_test_source');
SELECT col_is_pk('mimeo_source', 'logdel_test_source', 'col1', 'Check primary key for: mimeo_source.logdel_test_source');
SELECT has_index('mimeo_source', 'logdel_test_source','logdel_test_source_col2_idx','col2','Check index for: mimeo_source.logdel_test_source');
SELECT has_index('mimeo_source', 'logdel_test_source', 'logdel_test_source_mimeo_source_deleted','mimeo_source_deleted', 'Check for special column index in: mimeo_source.logdel_test_source');

SELECT results_eq('SELECT col1, col2, col3 FROM mimeo_dest.logdel_test_dest ORDER BY col1 ASC',
    'SELECT * FROM dblink(''mimeo_test'', ''SELECT col1, col2, col3 FROM mimeo_source.logdel_test_source2 ORDER BY col1 ASC'') t (col1 int, col2 text, col3 timestamptz)',
    'Check data for: mimeo_dest.logdel_test_dest');
SELECT col_is_pk('mimeo_dest','logdel_test_dest',ARRAY['col2','col1'],'Check primary key for: mimeo_dest.logdel_test_dest');
SELECT has_index('mimeo_dest', 'logdel_test_dest', 'logdel_test_dest_mimeo_source_deleted','mimeo_source_deleted', 'Check for special column index in: mimeo_dest.logdel_test_dest');

-- Multi-destination check
SELECT results_eq('SELECT col1, col2, col3 FROM mimeo_dest.logdel_test_dest_multi ORDER BY col1 ASC',
    'SELECT * FROM dblink(''mimeo_test'', ''SELECT col1, col2, col3 FROM mimeo_source.logdel_test_source ORDER BY col1 ASC'') t (col1 int, col2 text, col3 timestamptz)',
    'Check data for: mimeo_dest.logdel_test_dest_multi');
SELECT col_is_pk('mimeo_dest', 'logdel_test_dest_multi','col1','Check primary key for: mimeo_dest.logdel_test_dest_multi');
SELECT has_index('mimeo_dest', 'logdel_test_dest_multi','logdel_test_dest_multi_col2_idx','col2','Check index for: mimeo_source.logdel_test_dest_multi');
SELECT has_index('mimeo_dest', 'logdel_test_dest_multi', 'logdel_test_dest_multi_mimeo_source_deleted','mimeo_source_deleted', 'Check for special column index in: mimeo_source.logdel_test_source_multi');

SELECT is_empty('SELECT col1, col2, col3 FROM mimeo_dest.logdel_test_dest_nodata ORDER BY col1 ASC',
    'Check data for: mimeo_dest.logdel_test_dest_nodata');
SELECT index_is_unique('mimeo_dest','logdel_test_dest_nodata', 'logdel_test_dest_nodata_col1_key', 'Check unique index for col1: mimeo_dest.logdel_test_dest_nodata');
SELECT index_is_unique('mimeo_dest','logdel_test_dest_nodata', 'logdel_test_dest_nodata_col2_key', 'Check unique index for col2: mimeo_dest.logdel_test_dest_nodata');
SELECT has_index('mimeo_dest', 'logdel_test_dest_nodata', 'logdel_test_dest_nodata_mimeo_source_deleted','mimeo_source_deleted', 'Check for special column index in: mimeo_dest.logdel_test_dest_nodata');

SELECT results_eq('SELECT col1, col2 FROM mimeo_dest.logdel_test_dest_filter ORDER BY col1 ASC',
    'SELECT * FROM dblink(''mimeo_test'', ''SELECT col1, col2 FROM mimeo_source.logdel_test_source_filter ORDER BY col1 ASC'') t (col1 int, col2 text)',
    'Check data for: mimeo_dest.logdel_test_dest_filter');
SELECT index_is_unique('mimeo_dest','logdel_test_dest_filter', 'logdel_test_dest_filter_col1_key', 'Check unique index for col1: mimeo_dest.logdel_test_dest_filter');
-- Column filter means regular indexes cannot be made
SELECT hasnt_index('mimeo_dest','logdel_test_dest_filter', 'logdel_test_dest_filter_col2_key', 'Check index for col2: mimeo_dest.logdel_test_dest_filter');
SELECT hasnt_column('mimeo_dest', 'logdel_test_dest_filter', 'col3', 'Check that logdel_test_dest_filter DOESN''T have col3');
SELECT has_index('mimeo_dest', 'logdel_test_dest_filter', 'logdel_test_dest_filter_mimeo_source_deleted','mimeo_source_deleted', 'Check for special column index in: mimeo_dest.logdel_test_dest_filter');

SELECT results_eq('SELECT col1, col2 FROM mimeo_dest.logdel_test_dest_condition ORDER BY col1 ASC',
    'SELECT * FROM dblink(''mimeo_test'', ''SELECT col1, col2 FROM mimeo_source.logdel_test_source_condition WHERE col1 > 9000 ORDER BY col1 ASC'') t (col1 int, col2 text)',
    'Check data for: mimeo_dest.logdel_test_dest_condition');
SELECT col_is_pk('mimeo_dest','logdel_test_dest_condition', 'col1', 'Check primary key for: mimeo_dest.logdel_test_dest_condition');
SELECT index_is_unique('mimeo_dest','logdel_test_dest_condition', 'logdel_test_dest_condition_col2_key', 'Check unique index for col2: mimeo_dest.logdel_test_dest_condition');
SELECT has_index('mimeo_dest', 'logdel_test_dest_condition', 'logdel_test_dest_condition_mimeo_source_deleted','mimeo_source_deleted', 'Check for special column index in: mimeo_dest.logdel_test_dest_condition');

SELECT is_empty('SELECT col1, col2, col3 FROM mimeo_source.logdel_test_source_empty ORDER BY col1 ASC', 'Check data for: mimeo_source.logdel_test_source_empty');
SELECT has_index('mimeo_source', 'logdel_test_source_empty', 'logdel_test_source_empty_mimeo_source_deleted','mimeo_source_deleted', 'Check for special column index in: mimeo_source.logdel_test_source_empty');

SELECT results_eq('SELECT "COL1", "group", "Col-3" FROM mimeo_source."LogDel-Test-Source" ORDER BY "COL1" ASC',
    'SELECT * FROM dblink(''mimeo_test'', ''SELECT "COL1", "group", "Col-3" FROM mimeo_source."LogDel-Test-Source" ORDER BY "COL1" ASC'') t ("COL1" int, "group" text, "Col-3" timestamptz)',
    'Check data for: mimeo_source.LogDel-Test-Source');
SELECT col_is_pk('mimeo_source', 'LogDel-Test-Source', 'COL1', 'Check primary key for: mimeo_source.LogDel-Test-Source');
SELECT has_index('mimeo_source', 'LogDel-Test-Source','LogDel-Test-Source-group-Idx','"group"','Check index for: mimeo_source.LogDel-Test-Source');
SELECT has_index('mimeo_source', 'LogDel-Test-Source', 'LogDel-Test-Source_mimeo_source_deleted','mimeo_source_deleted', 'Check for special column index in: mimeo_source.LogDel-Test-Source');


SELECT dblink_disconnect('mimeo_test');
--SELECT is('SELECT dblink_get_connections() @> ''{mimeo_test}''','', 'Close remote database connection');

SELECT * FROM finish();
