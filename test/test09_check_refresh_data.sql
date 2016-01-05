\set ON_ERROR_ROLLBACK 1
\set ON_ERROR_STOP true

-- Check refresh functions again with batches larger than a single cursor fetch and also make sure they work after a data repull

SELECT set_config('search_path','mimeo, dblink, public',false);

SELECT plan(81);

SELECT diag('Running refresh for: mimeo_source.snap_test_source');
SELECT refresh_snap('mimeo_source.snap_test_source');
SELECT diag('Running refresh for: mimeo_dest.snap_test_dest');
SELECT refresh_snap('mimeo_dest.snap_test_dest');
SELECT diag('Running refresh for: mimeo_dest.snap_test_dest_nodata');
SELECT refresh_snap('mimeo_dest.snap_test_dest_nodata');
SELECT diag('Running refresh for: mimeo_dest.snap_test_dest_filter');
SELECT refresh_snap('mimeo_dest.snap_test_dest_filter');
SELECT diag('Running refresh for: mimeo_dest.snap_test_dest_condition');
SELECT refresh_snap('mimeo_dest.snap_test_dest_condition');
-- Call twice to change both snap tables
SELECT diag('Running refresh for: mimeo_dest.snap_test_dest_change_col (1st time)');
SELECT refresh_snap('mimeo_dest.snap_test_dest_change_col');
SELECT diag('Running refresh for: mimeo_dest.snap_test_dest_change_col (2nd time)');
SELECT refresh_snap('mimeo_dest.snap_test_dest_change_col');
-- Call twice to change both snap tables
SELECT diag('Running refresh for: mimeo_source.Snap-test-Source (1st time)');
SELECT refresh_snap('mimeo_source.Snap-test-Source');
SELECT diag('Running refresh for: mimeo_source.Snap-test-Source (2nd time)');
SELECT refresh_snap('mimeo_source.Snap-test-Source');


SELECT diag('Running refresh for: mimeo_dest.table_test_dest');
SELECT refresh_table('mimeo_dest.table_test_dest');
SELECT diag('Running refresh for: mimeo_dest.table_test_dest_nodata');
SELECT refresh_table('mimeo_dest.table_test_dest_nodata');
SELECT diag('Running refresh for: mimeo_dest.table_test_dest_filter');
SELECT refresh_table('mimeo_dest.table_test_dest_filter');
SELECT diag('Running refresh for: mimeo_dest.table_test_dest_condition');
SELECT refresh_table('mimeo_dest.table_test_dest_condition');
SELECT diag('Running refresh for: mimeo_dest.table_test_dest_empty');
SELECT refresh_table('mimeo_dest.table_test_dest_empty');
SELECT diag('Running refresh for: mimeo_dest.Table-test-Source');
SELECT refresh_table('mimeo_dest.Table-test-Source');

--time
SELECT diag('Running refresh for: mimeo_source.inserter_test_source');
SELECT refresh_inserter('mimeo_source.inserter_test_source');
SELECT diag('Running refresh for: mimeo_dest.inserter_test_dest');
SELECT refresh_inserter('mimeo_dest.inserter_test_dest');
SELECT diag('Running refresh for: mimeo_dest.inserter_test_dest_nodata');
SELECT refresh_inserter('mimeo_dest.inserter_test_dest_nodata');
SELECT diag('Running refresh for: mimeo_dest.inserter_test_dest_filter');
SELECT refresh_inserter('mimeo_dest.inserter_test_dest_filter');
SELECT diag('Running refresh for: mimeo_dest.inserter_test_dest_condition');
SELECT refresh_inserter('mimeo_dest.inserter_test_dest_condition');
SELECT diag('Running refresh for: mimeo_source.Inserter-Test-Source');
SELECT refresh_inserter('mimeo_source.Inserter-Test-Source');
--serial
SELECT diag('Running refresh for: mimeo_dest.inserter_test_dest_serial');
SELECT refresh_inserter('mimeo_dest.inserter_test_dest_serial');
SELECT diag('Running refresh for: mimeo_dest.Inserter-Test-Source_Serial');
SELECT refresh_inserter('mimeo_dest.Inserter-Test-Source_Serial');

--time
SELECT diag('Running refresh for: mimeo_source.updater_test_source');
SELECT refresh_updater('mimeo_source.updater_test_source');
SELECT diag('Running refresh for: mimeo_dest.updater_test_dest');
SELECT refresh_updater('mimeo_dest.updater_test_dest');
SELECT diag('Running refresh for: mimeo_dest.updater_test_dest_nodata');
SELECT refresh_updater('mimeo_dest.updater_test_dest_nodata');
SELECT diag('Running refresh for: mimeo_dest.updater_test_dest_filter');
SELECT refresh_updater('mimeo_dest.updater_test_dest_filter');
SELECT diag('Running refresh for: mimeo_dest.updater_test_dest_condition');
SELECT refresh_updater('mimeo_dest.updater_test_dest_condition');
SELECT diag('Running refresh for: mimeo_source.Updater-Test-Source');
SELECT refresh_updater('mimeo_source.Updater-Test-Source');
--serial
SELECT diag('Running refresh for: mimeo_dest.updater_test_dest_serial');
SELECT refresh_updater('mimeo_dest.updater_test_dest_serial');
SELECT diag('Running refresh for: mimeo_dest.Updater-Test-Source_Serial');
SELECT refresh_updater('mimeo_dest.Updater-Test-Source_Serial');

SELECT diag('Running refresh for: mimeo_source.dml_test_source');
SELECT refresh_dml('mimeo_source.dml_test_source');
SELECT diag('Running refresh for: mimeo_dest.dml_test_dest');
SELECT refresh_dml('mimeo_dest.dml_test_dest');
SELECT diag('Running refresh for: mimeo_dest.dml_test_dest_multi');
SELECT refresh_dml('mimeo_dest.dml_test_dest_multi');
SELECT diag('Running refresh for: mimeo_dest.dml_test_dest_nodata');
SELECT refresh_dml('mimeo_dest.dml_test_dest_nodata');
SELECT diag('Running refresh for: mimeo_dest.dml_test_dest_filter');
SELECT refresh_dml('mimeo_dest.dml_test_dest_filter');
SELECT diag('Running refresh for: mimeo_dest.dml_test_dest_condition');
SELECT refresh_dml('mimeo_dest.dml_test_dest_condition');
SELECT diag('Running refresh for: mimeo_source.Dml-Test-Source');
SELECT refresh_dml('mimeo_source.Dml-Test-Source');

SELECT diag('Running refresh for: mimeo_source.logdel_test_source');
SELECT refresh_logdel('mimeo_source.logdel_test_source');
SELECT diag('Running refresh for: mimeo_dest.logdel_test_dest');
SELECT refresh_logdel('mimeo_dest.logdel_test_dest');
SELECT diag('Running refresh for: mimeo_dest.logdel_test_dest_multi');
SELECT refresh_logdel('mimeo_dest.logdel_test_dest_multi');
SELECT diag('Running refresh for: mimeo_dest.logdel_test_dest_nodata');
SELECT refresh_logdel('mimeo_dest.logdel_test_dest_nodata');
SELECT diag('Running refresh for: mimeo_dest.logdel_test_dest_filter');
SELECT refresh_logdel('mimeo_dest.logdel_test_dest_filter');
SELECT diag('Running refresh for: mimeo_dest.logdel_test_dest_condition');
SELECT refresh_logdel('mimeo_dest.logdel_test_dest_condition');
SELECT diag('Running refresh for: mimeo_source.LogDel-Test-Source');
SELECT refresh_logdel('mimeo_source.LogDel-Test-Source');

SELECT dblink_connect('mimeo_test', 'host=localhost port=5432 dbname=mimeo_source user=mimeo_owner password=mimeo_owner');
SELECT is(dblink_get_connections() @> '{mimeo_test}', 't', 'Remote database connection established'); 

-- ########## SNAP TESTS ##########
SELECT results_eq('SELECT col1, col2, col3 FROM mimeo_source.snap_test_source ORDER BY col1 ASC',
    'SELECT * FROM dblink(''mimeo_test'', ''SELECT col1, col2, col3 FROM mimeo_source.snap_test_source ORDER BY col1 ASC'') t (col1 int, col2 text, col3 timestamptz)',
    'Check data for: mimeo_source.snap_test_source');

SELECT results_eq('SELECT col1, col2, col3 FROM mimeo_dest.snap_test_dest ORDER BY col1 ASC',
    'SELECT * FROM dblink(''mimeo_test'', ''SELECT col1, col2, col3 FROM mimeo_source.snap_test_source ORDER BY col1 ASC'') t (col1 int, col2 text, col3 timestamptz)',
    'Check data for: mimeo_dest.snap_test_dest');

SELECT results_eq('SELECT col1, col2, col3 FROM mimeo_dest.snap_test_dest_nodata ORDER BY col1 ASC',
    'SELECT * FROM dblink(''mimeo_test'', ''SELECT col1, col2, col3 FROM mimeo_source.snap_test_source ORDER BY col1 ASC'') t (col1 int, col2 text, col3 timestamptz)',
    'Check data for: mimeo_dest.snap_test_dest');

SELECT results_eq('SELECT col1, col2 FROM mimeo_dest.snap_test_dest_filter ORDER BY col1 ASC',
    'SELECT * FROM dblink(''mimeo_test'', ''SELECT col1, col2 FROM mimeo_source.snap_test_source ORDER BY col1 ASC'') t (col1 int, col2 text)',
    'Check data for: mimeo_dest.snap_test_dest_filter');
SELECT hasnt_column('mimeo_dest', 'snap_test_dest_filter_snap1', 'col3', 'Check that snap_test_dest_filter_snap1 DOESN''T have col3');
SELECT hasnt_column('mimeo_dest', 'snap_test_dest_filter_snap2', 'col3', 'Check that snap_test_dest_filter_snap2 DOESN''T have col3');

SELECT results_eq('SELECT col1, col2, col3 FROM mimeo_dest.snap_test_dest_condition ORDER BY col1 ASC',
    'SELECT * FROM dblink(''mimeo_test'', ''SELECT col1, col2, col3 FROM mimeo_source.snap_test_source WHERE col1 > 9000 ORDER BY col1 ASC'') t (col1 int, col2 text, col3 timestamptz)',
    'Check data for: mimeo_dest.snap_test_dest_condition');

SELECT results_eq('SELECT col1, col3, col4 FROM mimeo_dest.snap_test_dest_change_col ORDER BY col1 ASC',
    'SELECT * FROM dblink(''mimeo_test'', ''SELECT col1, col3, col4 FROM mimeo_source.snap_test_source_change_col ORDER BY col1 ASC'') t (col1 int, col3 timestamptz, col4 bigint)',
    'Check data for: mimeo_dest.snap_test_dest_change_col');
SELECT has_view('mimeo_dest', 'snap_test_dest_change_col', 'Check that view exists: mimeo_dest.snap_test_dest_change_col');
SELECT columns_are('mimeo_dest', 'snap_test_dest_change_col_snap1', ARRAY['col1', 'col3', 'col4'], 'Check that column change propagated for mimeo_dest.snap_test_dest_change_col_snap1');
SELECT columns_are('mimeo_dest', 'snap_test_dest_change_col_snap2', ARRAY['col1', 'col3', 'col4'], 'Check that column change propagated for mimeo_dest.snap_test_dest_change_col_snap2');
SELECT col_is_pk('mimeo_dest','snap_test_dest_change_col_snap1', ARRAY['col1'],'Check primary key for: mimeo_dest.snap_test_dest_change_col_snap1');
SELECT col_is_pk('mimeo_dest','snap_test_dest_change_col_snap2', ARRAY['col1'],'Check primary key for: mimeo_dest.snap_test_dest_change_col_snap2');
SELECT has_index('mimeo_dest', 'snap_test_dest_change_col_snap1', 'snap1_mimeo_check_exp_index_time', '((col3 > ''2013-04-01 00:00:00-04''::timestamp with time zone))', 'Check time expression index on mimeo_dest.snap_test_dest_change_col_snap1. NOTE: This test may fail if your timezone is not -04.'); 
SELECT has_index('mimeo_dest', 'snap_test_dest_change_col_snap2', 'snap2_mimeo_check_exp_index_time', '((col3 > ''2013-04-01 00:00:00-04''::timestamp with time zone))', 'Check time expression index on mimeo_dest.snap_test_dest_change_col_snap2. NOTE: This test may fail if your timezone is not -04.'); 
SELECT table_privs_are('mimeo_dest', 'snap_test_dest_change_col', 'mimeo_dumb_role', ARRAY['SELECT'], 'Checking mimeo_dumb_role privileges for mimeo_dest.snap_test_dest_change_col');
SELECT table_privs_are('mimeo_dest', 'snap_test_dest_change_col_snap1', 'mimeo_dumb_role', ARRAY['SELECT'], 'Checking mimeo_dumb_role privileges for mimeo_dest.snap_test_dest_change_col_snap1');
SELECT table_privs_are('mimeo_dest', 'snap_test_dest_change_col_snap2', 'mimeo_dumb_role', ARRAY['SELECT'], 'Checking mimeo_dumb_role privileges for mimeo_dest.snap_test_dest_change_col_snap2');
SELECT view_owner_is ('mimeo_dest', 'snap_test_dest_change_col', 'mimeo_test', 'Check ownership for view mimeo_dest.snap_test_dest_change_col');

SELECT results_eq('SELECT "primary", col2, "COL-3", "Col4" FROM mimeo_source."Snap-test-Source" ORDER BY "primary" ASC',
    'SELECT * FROM dblink(''mimeo_test'', ''SELECT "primary", col2, "COL-3", "Col4" FROM mimeo_source."Snap-test-Source" ORDER BY "primary" ASC'') t ("primary" int, col2 text, "Col-3" timestamptz, "Col4" inet)',
    'Check data for: mimeo_source.Snap-test-Source');
SELECT columns_are('mimeo_source', 'Snap-test-Source_snap1', ARRAY['primary', 'col2', 'COL-3', 'Col4'], 'Check that column change propagated for mimeo_dest.Snap-test-Source_snap1');
SELECT columns_are('mimeo_source', 'Snap-test-Source_snap2', ARRAY['primary', 'col2', 'COL-3', 'Col4'], 'Check that column change propagated for mimeo_dest.Snap-test-Source_snap2');
SELECT has_index('mimeo_source','Snap-test-Source_snap1', 'snap1_Snap-test-Source_col2_idx', 'col2','Check index for: mimeo_dest.Snap-test-Source_snap1');
SELECT has_index('mimeo_source','Snap-test-Source_snap2', 'snap2_Snap-test-Source_col2_idx', 'col2','Check index for: mimeo_dest.Snap-test-Source_snap1');
SELECT table_privs_are('mimeo_source', 'Snap-test-Source', 'mimeo_dumb_role', ARRAY['SELECT'], 'Checking mimeo_dumb_role privileges for mimeo_source.Snap-test-Source');
SELECT table_privs_are('mimeo_source', 'Snap-test-Source_snap1', 'mimeo_dumb_role', ARRAY['SELECT'], 'Checking mimeo_dumb_role privileges for mimeo_source.Snap-test-Source_snap1');
SELECT table_privs_are('mimeo_source', 'Snap-test-Source_snap2', 'mimeo_dumb_role', ARRAY['SELECT'], 'Checking mimeo_dumb_role privileges for mimeo_source.Snap-test-Source_snap2');
SELECT view_owner_is ('mimeo_source', 'Snap-test-Source', 'mimeo_test', 'Check ownership for view mimeo_source.Snap-test-Source');
SELECT table_owner_is ('mimeo_source', 'Snap-test-Source_snap1', 'mimeo_test', 'Check ownership for view mimeo_source.Snap-test-Source_snap1');
SELECT table_owner_is ('mimeo_source', 'Snap-test-Source_snap2', 'mimeo_test', 'Check ownership for view mimeo_source.Snap-test-Source_snap2');


SELECT results_eq('SELECT match FROM validate_rowcount(''mimeo_dest.snap_test_dest'')', ARRAY[true], 'Check validate_rowcount match');

-- ########## PLAIN TABLE TESTS ##########
SELECT results_eq('SELECT col1, col2, col3 FROM mimeo_dest.table_test_dest ORDER BY col1 ASC',
    'SELECT * FROM dblink(''mimeo_test'', ''SELECT col1, col2, col3 FROM mimeo_source.snap_test_source ORDER BY col1 ASC'') t (col1 int, col2 text, col3 timestamptz)',
    'Check data for: mimeo_dest.table_test_dest');
SELECT results_eq('SELECT last_value::text FROM mimeo_dest.col1_seq', ARRAY['100000'], 'Check that destination sequence was reset for mimeo_dest.table_test_dest');

SELECT results_eq('SELECT col1, col2, col3 FROM mimeo_dest.table_test_dest_nodata ORDER BY col1 ASC',
    'SELECT * FROM dblink(''mimeo_test'', ''SELECT col1, col2, col3 FROM mimeo_source.snap_test_source ORDER BY col1 ASC'') t (col1 int, col2 text, col3 timestamptz)',
    'Check data for: mimeo_dest.table_test_dest');

SELECT results_eq('SELECT col1, col2 FROM mimeo_dest.table_test_dest_filter ORDER BY col1 ASC',
    'SELECT * FROM dblink(''mimeo_test'', ''SELECT col1, col2 FROM mimeo_source.snap_test_source ORDER BY col1 ASC'') t (col1 int, col2 text)',
    'Check data for: mimeo_dest.table_test_dest_filter');
SELECT hasnt_column('mimeo_dest', 'table_test_dest_filter', 'col3', 'Check that table_test_dest_filter_snap1 DOESN''T have col3');

SELECT results_eq('SELECT col1, col2, col3 FROM mimeo_dest.table_test_dest_condition ORDER BY col1 ASC',
    'SELECT * FROM dblink(''mimeo_test'', ''SELECT col1, col2, col3 FROM mimeo_source.snap_test_source WHERE col1 > 9000 ORDER BY col1 ASC'') t (col1 int, col2 text, col3 timestamptz)',
    'Check data for: mimeo_dest.table_test_dest_condition');

SELECT is_empty('SELECT col1, col2, col3 FROM mimeo_dest.table_test_dest_empty ORDER BY col1 ASC', 'Check data for: mimeo_dest.table_test_dest_empty');

SELECT results_eq('SELECT "primary", col2, "COL-3" FROM mimeo_dest."Table-test-Source" ORDER BY "primary" ASC',
    'SELECT * FROM dblink(''mimeo_test'', ''SELECT "primary", col2, "COL-3" FROM mimeo_source."Snap-test-Source" ORDER BY "primary" ASC'') t ("primary" int, col2 text, col3 timestamptz)',
    'Check data for: mimeo_dest.Table-test-Source');
SELECT results_eq('SELECT last_value::text FROM mimeo_dest."PRIMARY-seq"', ARRAY['100000'], 'Check that destination sequence was reset for mimeo_dest.Table-test-Source');

SELECT results_eq('SELECT match FROM validate_rowcount(''mimeo_dest.table_test_dest'')', ARRAY[true], 'Check validate_rowcount match');

-- ########## INSERTER TESTS ##########
-- Time
SELECT results_eq('SELECT col1, col2, col3 FROM mimeo_source.inserter_test_source ORDER BY col1 ASC',
    'SELECT * FROM dblink(''mimeo_test'', ''SELECT col1, col2, col3 FROM mimeo_source.inserter_test_source ORDER BY col1 ASC'') t (col1 int, col2 text, col3 timestamptz)',
    'Check data for: mimeo_source.inserter_test_source');

SELECT results_eq('SELECT col1, col2, col3 FROM mimeo_dest.inserter_test_dest ORDER BY col1 ASC',
    'SELECT * FROM dblink(''mimeo_test'', ''SELECT col1, col2, col3 FROM mimeo_source.inserter_test_source ORDER BY col1 ASC'') t (col1 int, col2 text, col3 timestamptz)',
    'Check data for: mimeo_dest.inserter_test_dest');

SELECT results_eq('SELECT col1, col2, col3 FROM mimeo_dest.inserter_test_dest_nodata ORDER BY col1 ASC',
    'SELECT * FROM dblink(''mimeo_test'', ''SELECT col1, col2, col3 FROM mimeo_source.inserter_test_source WHERE col1 >= 10001 ORDER BY col1 ASC'') t (col1 int, col2 text, col3 timestamptz)',
    'Check data for: mimeo_dest.inserter_test_dest_nodata');

SELECT results_eq('SELECT col1, col3 FROM mimeo_dest.inserter_test_dest_filter ORDER BY col1 ASC',
    'SELECT * FROM dblink(''mimeo_test'', ''SELECT col1, col3 FROM mimeo_source.inserter_test_source ORDER BY col1 ASC'') t (col1 int, col3 timestamptz)',
    'Check data for: mimeo_dest.inserter_test_dest_filter');

SELECT results_eq('SELECT col1, col2, col3 FROM mimeo_dest.inserter_test_dest_condition ORDER BY col1 ASC',
    'SELECT * FROM dblink(''mimeo_test'', ''SELECT col1, col2, col3 FROM mimeo_source.inserter_test_source WHERE col1 > 9000 ORDER BY col1 ASC'') t (col1 int, col2 text, col3 timestamptz)',
    'Check data for: mimeo_dest.inserter_test_dest_condition');

SELECT results_eq('SELECT col1, "group", "Col-3" FROM mimeo_source."Inserter-Test-Source" ORDER BY col1 ASC',
    'SELECT * FROM dblink(''mimeo_test'', ''SELECT col1, "group", "Col-3" FROM mimeo_source."Inserter-Test-Source" ORDER BY col1 ASC'') t (col1 int, "group" text, "Col-3" timestamptz)',
    'Check data for: mimeo_source."Inserter-Test-Source"');

SELECT results_eq('SELECT match FROM validate_rowcount(''mimeo_dest.inserter_test_dest'')', ARRAY[true], 'Check validate_rowcount match');

-- Serial
SELECT results_eq('SELECT col1, col2, col3 FROM mimeo_dest.inserter_test_dest_serial ORDER BY col1 ASC',
    'SELECT * FROM dblink(''mimeo_test'', ''SELECT col1, col2, col3 FROM mimeo_source.inserter_test_source WHERE col1 < (SELECT max(col1) FROM mimeo_source.inserter_test_source) ORDER BY col1 ASC'') t (col1 int, col2 text, col3 timestamptz)',
    'Check data for: mimeo_dest.inserter_test_dest_serial');

SELECT results_eq('SELECT col1, "group", "Col-3" FROM mimeo_dest."Inserter-Test-Source_Serial" ORDER BY col1 ASC',
    'SELECT * FROM dblink(''mimeo_test'', ''SELECT col1, "group", "Col-3" FROM mimeo_source."Inserter-Test-Source" WHERE col1 < (SELECT max(col1) FROM mimeo_source."Inserter-Test-Source") ORDER BY col1 ASC'') t (col1 int, "group" text, "Col-3" timestamptz)',
    'Check data for: mimeo_dest."Inserter-Test-Source_Serial"');

-- ########## UPDATER TESTS ##########
-- Time
SELECT results_eq('SELECT col1, col2, col3 FROM mimeo_source.updater_test_source ORDER BY col1 ASC',
    'SELECT * FROM dblink(''mimeo_test'', ''SELECT col1, col2, col3 FROM mimeo_source.updater_test_source ORDER BY col1 ASC'') t (col1 int, col2 text, col3 timestamptz)',
    'Check data for: mimeo_source.updater_test_source');

SELECT results_eq('SELECT col1, col2, col3 FROM mimeo_dest.updater_test_dest ORDER BY col1 ASC',
    'SELECT * FROM dblink(''mimeo_test'', ''SELECT col1, col2, col3 FROM mimeo_source.updater_test_source ORDER BY col1 ASC'') t (col1 int, col2 text, col3 timestamptz)',
    'Check data for: mimeo_dest.updater_test_dest');

SELECT results_eq('SELECT col1, col2, col3 FROM mimeo_dest.updater_test_dest_nodata ORDER BY col1 ASC',
    'SELECT * FROM dblink(''mimeo_test'', ''SELECT col1, col2, col3 FROM mimeo_source.updater_test_source WHERE col1 >= 9500 ORDER BY col1 ASC'') t (col1 int, col2 text, col3 timestamptz)',
    'Check data for: mimeo_dest.updater_test_dest_nodata');

SELECT results_eq('SELECT col1, col3 FROM mimeo_dest.updater_test_dest_filter ORDER BY col1 ASC',
    'SELECT * FROM dblink(''mimeo_test'', ''SELECT col1, col3 FROM mimeo_source.updater_test_source ORDER BY col1 ASC'') t (col1 int, col3 timestamptz)',
    'Check data for: mimeo_dest.updater_test_dest_filter');

SELECT results_eq('SELECT col1, col2, col3 FROM mimeo_dest.updater_test_dest_condition ORDER BY col1 ASC',
    'SELECT * FROM dblink(''mimeo_test'', ''SELECT col1, col2, col3 FROM mimeo_source.updater_test_source WHERE col1 > 9000 ORDER BY col1 ASC'') t (col1 int, col2 text, col3 timestamptz)',
    'Check data for: mimeo_dest.updater_test_dest_condition');

SELECT results_eq('SELECT "COL-1", "group", "Col3" FROM mimeo_source."Updater-Test-Source" ORDER BY "COL-1" ASC',
    'SELECT * FROM dblink(''mimeo_test'', ''SELECT "COL-1", "group", "Col3" FROM mimeo_source."Updater-Test-Source" ORDER BY "COL-1" ASC'') t ("COL-1" int, "group" text, "Col3" timestamptz)',
    'Check data for: mimeo_source."Updater-Test-Source"');

SELECT results_eq('SELECT match FROM validate_rowcount(''mimeo_dest.updater_test_dest'')', ARRAY[true], 'Check validate_rowcount match');

-- Serial
SELECT results_eq('SELECT col1, col2, col3, col4 FROM mimeo_dest.updater_test_dest_serial ORDER BY col4 ASC',
    'SELECT * FROM dblink(''mimeo_test'', ''SELECT col1, col2, col3, col4 FROM mimeo_source.updater_test_source WHERE col4 < (SELECT max(col4) FROM mimeo_source.updater_test_source) ORDER BY col4 ASC'') t (col1 int, col2 text, col3 timestamptz, col4 int)',
    'Check data for: mimeo_dest.updater_test_dest_serial');

SELECT results_eq('SELECT "COL-1", "group", "Col3" FROM mimeo_dest."Updater-Test-Source_Serial" ORDER BY "COL-1" ASC',
    'SELECT * FROM dblink(''mimeo_test'', ''SELECT "COL-1", "group", "Col3" FROM mimeo_source."Updater-Test-Source" WHERE "COL-1" < (SELECT max("COL-1") FROM mimeo_source."Updater-Test-Source") ORDER BY "COL-1" ASC'') t ("COL-1" int, "group" text, "Col3" timestamptz)',
    'Check data for: mimeo_source."Updater-Test-Source_Serial"');

-- ########## DML TESTS ##########
SELECT results_eq('SELECT col1, col2, col3 FROM mimeo_source.dml_test_source ORDER BY col1 ASC',
    'SELECT * FROM dblink(''mimeo_test'', ''SELECT col1, col2, col3 FROM mimeo_source.dml_test_source ORDER BY col1 ASC'') t (col1 int, col2 text, col3 timestamptz)',
    'Check data for: mimeo_source.dml_test_source');

SELECT results_eq('SELECT col1, col2, col3 FROM mimeo_dest.dml_test_dest ORDER BY col1, col2 ASC',
    'SELECT * FROM dblink(''mimeo_test'', ''SELECT col1, col2, col3 FROM mimeo_source.dml_test_source2 ORDER BY col1, col2 ASC'') t (col1 int, col2 varchar(255), col3 timestamptz)',
    'Check data for: mimeo_dest.dml_test_dest');
SELECT is_empty('SELECT * FROM mimeo_dest.dml_test_dest WHERE col1 between 45000 and 46000', 'Check that deleted rows are gone from mimeo_dest.dml_test_dest');

SELECT results_eq('SELECT col1, col2, col3 FROM mimeo_dest.dml_test_dest_multi ORDER BY col1 ASC',
    'SELECT * FROM dblink(''mimeo_test'', ''SELECT col1, col2, col3 FROM mimeo_source.dml_test_source ORDER BY col1 ASC'') t (col1 int, col2 text, col3 timestamptz)',
    'Check data for: mimeo_dest.dml_test_dest_multi');

SELECT results_eq('SELECT col1, col2, col3 FROM mimeo_dest.dml_test_dest_nodata ORDER BY col1 ASC',
    'SELECT * FROM dblink(''mimeo_test'', ''SELECT col1, col2, col3 FROM mimeo_source.dml_test_source_nodata WHERE col1 >= 10001 ORDER BY col1 ASC'') t (col1 int, col2 text, col3 timestamptz)',
    'Check data for: mimeo_dest.dml_test_dest_nodata');

SELECT results_eq('SELECT col1, col2 FROM mimeo_dest.dml_test_dest_filter ORDER BY col1 ASC',
    'SELECT * FROM dblink(''mimeo_test'', ''SELECT col1, col2 FROM mimeo_source.dml_test_source_filter ORDER BY col1 ASC'') t (col1 int, col2 text)',
    'Check data for: mimeo_dest.dml_test_dest_filter');

SELECT results_eq('SELECT col1, col2, col3 FROM mimeo_dest.dml_test_dest_condition ORDER BY col1 ASC',
    'SELECT * FROM dblink(''mimeo_test'', ''SELECT col1, col2, col3 FROM mimeo_source.dml_test_source_condition WHERE col1 > 9000 ORDER BY col1 ASC'') t (col1 int, col2 text, col3 timestamptz)',
    'Check data for: mimeo_dest.dml_test_dest_condition');
SELECT is_empty('SELECT * FROM mimeo_dest.dml_test_dest_condition WHERE col1 <= 10000', 'Check that deleted row is gone from mimeo_dest.dml_test_dest_condition');

SELECT results_eq('SELECT "COL1", "group", "Col-3" FROM mimeo_source."Dml-Test-Source" ORDER BY "COL1" ASC',
    'SELECT * FROM dblink(''mimeo_test'', ''SELECT "COL1", "group", "Col-3" FROM mimeo_source."Dml-Test-Source" ORDER BY "COL1" ASC'') t ("COL1" int, "group" text, "Col-3" timestamptz)',
    'Check data for: mimeo_source.Dml-Test-Source');

SELECT results_eq('SELECT match FROM validate_rowcount(''mimeo_dest.dml_test_dest'')', ARRAY[true], 'Check validate_rowcount match');

-- ########## LOGDEL TESTS ##########
SELECT results_eq('SELECT col1, col2, col3 FROM mimeo_source.logdel_test_source ORDER BY col1 ASC',
    'SELECT * FROM dblink(''mimeo_test'', ''SELECT col1, col2, col3 FROM mimeo_source.logdel_test_source ORDER BY col1 ASC'') t (col1 int, col2 text, col3 timestamptz)',
    'Check data for: mimeo_source.logdel_test_source');

SELECT results_eq('SELECT col1, col2, col3 FROM mimeo_dest.logdel_test_dest WHERE (col1 < 12500 or col1 > 12520) AND (col1 < 45500 or col1 > 45520) ORDER BY col1, col2 ASC',
    'SELECT * FROM dblink(''mimeo_test'', ''SELECT col1, col2, col3 FROM mimeo_source.logdel_test_source2 ORDER BY col1, col2 ASC'') t (col1 int, col2 text, col3 timestamptz)',
    'Check data for: mimeo_dest.logdel_test_dest');
-- Ensure originally deleted rows are still there
SELECT results_eq('SELECT col2 FROM mimeo_dest.logdel_test_dest WHERE (col1 between 12500 and 12520) AND mimeo_source_deleted IS NOT NULL order by col2',
    ARRAY['test12500','test12501','test12502','test12503','test12504','test12505','test12506','test12507','test12508','test12509','test12510',
        'test12511','test12512','test12513','test12514','test12515','test12516','test12517','test12518','test12519','test12520'],
    'Check that deleted rows are logged in mimeo_dest.logdel_test_dest');
SELECT results_eq('SELECT col2 FROM mimeo_dest.logdel_test_dest WHERE (col1 between 45500 and 45520) AND mimeo_source_deleted IS NOT NULL order by col2',
    ARRAY['test45500','test45501','test45502','test45503','test45504','test45505','test45506','test45507','test45508','test45509','test45510',
        'test45511','test45512','test45513','test45514','test45515','test45516','test45517','test45518','test45519','test45520'],
    'Check that deleted rows are logged in mimeo_dest.logdel_test_dest');

SELECT results_eq('SELECT col1, col2, col3 FROM mimeo_dest.logdel_test_dest_multi ORDER BY col1 ASC',
    'SELECT * FROM dblink(''mimeo_test'', ''SELECT col1, col2, col3 FROM mimeo_source.logdel_test_source ORDER BY col1 ASC'') t (col1 int, col2 text, col3 timestamptz)',
    'Check data for: mimeo_dest.logdel_test_dest_multi');

SELECT results_eq('SELECT col1, col2, col3 FROM mimeo_dest.logdel_test_dest_nodata ORDER BY col1 ASC',
    'SELECT * FROM dblink(''mimeo_test'', ''SELECT col1, col2, col3 FROM mimeo_source.logdel_test_source_nodata WHERE col1 >= 10001 ORDER BY col1 ASC'') t (col1 int, col2 text, col3 timestamptz)',
    'Check data for: mimeo_dest.logdel_test_dest_nodata');

SELECT results_eq('SELECT col1, col2 FROM mimeo_dest.logdel_test_dest_filter ORDER BY col1 ASC',
    'SELECT * FROM dblink(''mimeo_test'', ''SELECT col1, col2 FROM mimeo_source.logdel_test_source_filter ORDER BY col1 ASC'') t (col1 int, col2 text)',
    'Check data for: mimeo_dest.logdel_test_dest_filter');

SELECT results_eq('SELECT col1, col2 FROM mimeo_dest.logdel_test_dest_condition WHERE col1 <> 11 ORDER BY col1, col2 ASC',
    'SELECT * FROM dblink(''mimeo_test'', ''SELECT col1, col2 FROM mimeo_source.logdel_test_source_condition WHERE col1 > 9000 ORDER BY col1, col2 ASC'') t (col1 int, col2 text)',
    'Check data for: mimeo_dest.logdel_test_dest_condition');

SELECT results_eq('SELECT "COL1", "group", "Col-3" FROM mimeo_source."LogDel-Test-Source" ORDER BY "COL1" ASC',
    'SELECT * FROM dblink(''mimeo_test'', ''SELECT "COL1", "group", "Col-3" FROM mimeo_source."LogDel-Test-Source" ORDER BY "COL1" ASC'') t ("COL1" int, "group" text, "Col-3" timestamptz)',
    'Check data for: mimeo_source.LogDel-Test-Source');

SELECT results_eq('SELECT match FROM validate_rowcount(''mimeo_source.logdel_test_source'')', ARRAY[true], 'Check validate_rowcount match');
SELECT results_eq('SELECT match FROM validate_rowcount(''mimeo_dest.logdel_test_dest'')', ARRAY[false], 'Check validate_rowcount match');

SELECT dblink_disconnect('mimeo_test');
--SELECT is('SELECT dblink_get_connections() @> ''{mimeo_test}''','', 'Close remote database connection');

SELECT pg_sleep(5);

SELECT * FROM finish();
