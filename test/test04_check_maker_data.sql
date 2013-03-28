SELECT set_config('search_path','mimeo, dblink, tap',false);

SELECT plan(117);

SELECT dblink_connect('mimeo_test', 'host=localhost port=5432 dbname=mimeo_source user=mimeo_test password=mimeo_test');
SELECT is(dblink_get_connections() @> '{mimeo_test}', 't', 'Remote database connection established');

-- ########## SNAP TESTS ##########
SELECT has_view('mimeo_source', 'snap_test_source', 'Check that view exists: mimeo_source.snap_test_source');
SELECT results_eq('SELECT col1, col2, col3 FROM mimeo_source.snap_test_source ORDER BY col1 ASC',
    'SELECT * FROM dblink(''mimeo_test'', ''SELECT col1, col2, col3 FROM mimeo_source.snap_test_source ORDER BY col1 ASC'') t (col1 int, col2 text, col3 timestamptz)',
    'Check data for: mimeo_source.snap_test_source');
SELECT has_index('mimeo_source','snap_test_source_snap1','snap1_snap_test_source_col2_idx','col2','Check index for: mimeo_source.snap_test_source_snap1');
SELECT has_index('mimeo_source','snap_test_source_snap2','snap2_snap_test_source_col2_idx','col2','Check index for: mimeo_source.snap_test_source_snap2');

SELECT has_view('mimeo_dest', 'snap_test_dest', 'Check that view exists: mimeo_dest.snap_test_dest');
SELECT results_eq('SELECT col1, col2, col3 FROM mimeo_dest.snap_test_dest ORDER BY col1 ASC',
    'SELECT * FROM dblink(''mimeo_test'', ''SELECT col1, col2, col3 FROM mimeo_source.snap_test_source ORDER BY col1 ASC'') t (col1 int, col2 text, col3 timestamptz)',
    'Check data for: mimeo_dest.snap_test_dest');
SELECT has_index('mimeo_dest','snap_test_dest_snap1','snap1_snap_test_dest_col2_idx','col2','Check index for: mimeo_dest.snap_test_dest_snap1');
SELECT has_index('mimeo_dest','snap_test_dest_snap2','snap2_snap_test_dest_col2_idx','col2','Check index for: mimeo_dest.snap_test_dest_snap2');

SELECT has_view('mimeo_dest', 'snap_test_dest_nodata', 'Check that view exists: mimeo_dest.snap_test_dest_nodata');
SELECT is_empty('SELECT col1, col2, col3 FROM mimeo_dest.snap_test_dest_nodata ORDER BY col1 ASC', 'Check data for: mimeo_dest.snap_test_dest_nodata');
SELECT has_index('mimeo_dest','snap_test_dest_nodata_snap1','snap1_snap_test_dest_nodata_col2_idx','col2','Check index for: mimeo_dest.snap_test_dest_nodata_snap1');
SELECT has_index('mimeo_dest','snap_test_dest_nodata_snap2','snap2_snap_test_dest_nodata_col2_idx','col2','Check index for: mimeo_dest.snap_test_dest_nodata_snap2');

SELECT has_view('mimeo_dest', 'snap_test_dest_filter', 'Check that view exists: mimeo_dest.snap_test_dest_filter');
SELECT results_eq('SELECT col1, col2 FROM mimeo_dest.snap_test_dest_filter ORDER BY col1 ASC',
    'SELECT * FROM dblink(''mimeo_test'', ''SELECT col1, col2 FROM mimeo_source.snap_test_source ORDER BY col1 ASC'') t (col1 int, col2 text)',
    'Check data for: mimeo_dest.snap_test_dest_filter');
-- Column filter means regular indexes cannot be made
SELECT hasnt_index('mimeo_dest','snap_test_dest_filter_snap1','snap1_snap_test_dest_filter_col2_idx','Check index for: mimeo_dest.snap_test_dest_filter_snap1');
SELECT hasnt_index('mimeo_dest','snap_test_dest_filter_snap2','snap2_snap_test_dest_filter_col2_idx','Check index for: mimeo_dest.snap_test_dest_filter_snap2');
SELECT hasnt_column('mimeo_dest', 'snap_test_dest_filter_snap1', 'col3', 'Check that snap_test_dest_filter_snap1 DOESN''T have col3');
SELECT hasnt_column('mimeo_dest', 'snap_test_dest_filter_snap2', 'col3', 'Check that snap_test_dest_filter_snap2 DOESN''T have col3');

SELECT has_view('mimeo_dest', 'snap_test_dest_condition', 'Check that view exists: mimeo_dest.snap_test_dest_condition');
SELECT results_eq('SELECT col1, col2, col3 FROM mimeo_dest.snap_test_dest_condition ORDER BY col1 ASC',
    'SELECT * FROM dblink(''mimeo_test'', ''SELECT col1, col2, col3 FROM mimeo_source.snap_test_source WHERE col1 > 9000 ORDER BY col1 ASC'') t (col1 int, col2 text, col3 timestamptz)',
    'Check data for: mimeo_dest.snap_test_dest_condition');
SELECT has_index('mimeo_dest','snap_test_dest_condition_snap1','snap1_snap_test_dest_condition_col2_idx','col2','Check index for: mimeo_dest.snap_test_dest_condition_snap1');
SELECT has_index('mimeo_dest','snap_test_dest_condition_snap2','snap2_snap_test_dest_condition_col2_idx','col2','Check index for: mimeo_dest.snap_test_dest_condition_snap2');

SELECT has_view('mimeo_source', 'snap_test_source_empty', 'Check that view exists: mimeo_source.snap_test_source_empty');
SELECT is_empty('SELECT col1, col2, col3 FROM mimeo_source.snap_test_source_empty ORDER BY col1 ASC', 'Check data for: mimeo_source.snap_test_source_empty');
SELECT index_is_unique('mimeo_source','snap_test_source_empty_snap1', 'snap1_snap_test_source_empty_col1_key', 'Check unique index for: mimeo_source.snap_test_source_empty_snap1');
SELECT index_is_unique('mimeo_source','snap_test_source_empty_snap2', 'snap2_snap_test_source_empty_col1_key', 'Check unique index for: mimeo_source.snap_test_source_empty_snap2');

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


-- ########## PLAIN TABLE TESTS ##########
SELECT results_eq('SELECT col1, col2, col3 FROM mimeo_dest.table_test_dest ORDER BY col1 ASC',
    'SELECT * FROM dblink(''mimeo_test'', ''SELECT col1, col2, col3 FROM mimeo_source.snap_test_source ORDER BY col1 ASC'') t (col1 int, col2 text, col3 timestamptz)',
    'Check data for: mimeo_dest.table_test_dest');
SELECT has_index('mimeo_dest','table_test_dest','table_test_dest_col2_idx','col2','Check index for: mimeo_dest.table_test_dest');

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

-- ########## INSERTER TESTS ##########
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

-- ########## UPDATER TESTS ##########
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

-- ########## DML TESTS ##########
SELECT results_eq('SELECT col1, col2, col3 FROM mimeo_source.dml_test_source ORDER BY col1 ASC',
    'SELECT * FROM dblink(''mimeo_test'', ''SELECT col1, col2, col3 FROM mimeo_source.dml_test_source ORDER BY col1 ASC'') t (col1 int, col2 text, col3 timestamptz)',
    'Check data for: mimeo_source.dml_test_source');
SELECT col_is_pk('mimeo_source','dml_test_source', 'col1', 'Check primary key for: mimeo_source.dml_test_source');
SELECT has_index('mimeo_source','dml_test_source','dml_test_source_col2_idx','col2','Check index for: mimeo_source.dml_test_source');

SELECT results_eq('SELECT col1, col2, col3 FROM mimeo_dest.dml_test_dest ORDER BY col1 ASC',
    'SELECT * FROM dblink(''mimeo_test'', ''SELECT col1, col2, col3 FROM mimeo_source.dml_test_source2 ORDER BY col1 ASC'') t (col1 int, col2 text, col3 timestamptz)',
    'Check data for: mimeo_dest.dml_test_dest');
SELECT col_is_pk('mimeo_dest','dml_test_dest',ARRAY['col2','col1'],'Check primary key for: mimeo_dest.dml_test_dest');

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

SELECT dblink_disconnect('mimeo_test');
--SELECT is('SELECT dblink_get_connections() @> ''{mimeo_test}''','', 'Close remote database connection');

SELECT * FROM finish();
