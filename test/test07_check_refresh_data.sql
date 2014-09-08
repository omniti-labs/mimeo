\set ON_ERROR_ROLLBACK 1
\set ON_ERROR_STOP true

SELECT set_config('search_path','mimeo, dblink, public',false);

SELECT plan(57);

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

SELECT is_empty('SELECT col1, col2, col3 FROM mimeo_source.snap_test_source_empty ORDER BY col1 ASC', 'Check data for: mimeo_source.snap_test_source_empty');

SELECT results_eq('SELECT col1, col2, col3 FROM mimeo_dest.snap_test_dest_change_col ORDER BY col1 ASC',
    'SELECT * FROM dblink(''mimeo_test'', ''SELECT col1, col2, col3 FROM mimeo_source.snap_test_source_change_col ORDER BY col1 ASC'') t (col1 int, col2 text, col3 timestamptz)',
    'Check data for: mimeo_dest.snap_test_dest_change_col');

SELECT results_eq('SELECT "primary", col2, "COL-3" FROM mimeo_source."Snap-test-Source" ORDER BY "primary" ASC',
    'SELECT * FROM dblink(''mimeo_test'', ''SELECT "primary", col2, "COL-3" FROM mimeo_source."Snap-test-Source" ORDER BY "primary" ASC'') t ("primary" int, col2 text, "Col-3" timestamptz)',
    'Check data for: mimeo_source.Snap-test-Source');

-- ########## PLAIN TABLE TESTS ##########
SELECT results_eq('SELECT col1, col2, col3 FROM mimeo_dest.table_test_dest ORDER BY col1 ASC',
    'SELECT * FROM dblink(''mimeo_test'', ''SELECT col1, col2, col3 FROM mimeo_source.snap_test_source ORDER BY col1 ASC'') t (col1 int, col2 text, col3 timestamptz)',
    'Check data for: mimeo_dest.table_test_dest');
-- Make sure sequences are getting reset if configured to do so on destination
SELECT results_eq('SELECT last_value::text FROM mimeo_dest.col1_seq', ARRAY['20000'], 'Check that destination sequence was reset for mimeo_dest.table_test_dest');

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
-- Make sure sequences are getting reset if configured to do so on destination
SELECT results_eq('SELECT last_value::text FROM mimeo_dest."PRIMARY-seq"', ARRAY['20000'], 'Check that destination sequence was reset for mimeo_dest.Table-test-Source');

-- ########## INSERTER TESTS ##########
-- Time
SELECT results_eq('SELECT col1, col2, col3 FROM mimeo_source.inserter_test_source ORDER BY col1 ASC',
    'SELECT * FROM dblink(''mimeo_test'', ''SELECT col1, col2, col3 FROM mimeo_source.inserter_test_source ORDER BY col1 ASC'') t (col1 int, col2 text, col3 timestamptz)',
    'Check data for: mimeo_source.inserter_test_source');

SELECT results_eq('SELECT col1, col2, col3 FROM mimeo_dest.inserter_test_dest ORDER BY col1 ASC',
    'SELECT * FROM dblink(''mimeo_test'', ''SELECT col1, col2, col3 FROM mimeo_source.inserter_test_source ORDER BY col1 ASC'') t (col1 int, col2 text, col3 timestamptz)',
    'Check data for: mimeo_dest.inserter_test_dest');

SELECT results_eq('SELECT col1, col2, col3 FROM mimeo_dest.inserter_test_dest_nodata ORDER BY col1 ASC',
    'SELECT * FROM dblink(''mimeo_test'', ''SELECT col1, col2, col3 FROM mimeo_source.inserter_test_source WHERE col1 > 10000 ORDER BY col1 ASC'') t (col1 int, col2 text, col3 timestamptz)',
    'Check data for: mimeo_dest.inserter_test_dest');

SELECT results_eq('SELECT col1, col3 FROM mimeo_dest.inserter_test_dest_filter ORDER BY col1 ASC',
    'SELECT * FROM dblink(''mimeo_test'', ''SELECT col1, col3 FROM mimeo_source.inserter_test_source ORDER BY col1 ASC'') t (col1 int, col3 timestamptz)',
    'Check data for: mimeo_dest.inserter_test_dest_filter');

SELECT results_eq('SELECT col1, col2, col3 FROM mimeo_dest.inserter_test_dest_condition ORDER BY col1 ASC',
    'SELECT * FROM dblink(''mimeo_test'', ''SELECT col1, col2, col3 FROM mimeo_source.inserter_test_source WHERE col1 > 9000 ORDER BY col1 ASC'') t (col1 int, col2 text, col3 timestamptz)',
    'Check data for: mimeo_dest.inserter_test_dest_condition');

SELECT is_empty('SELECT col1, col2, col3 FROM mimeo_source.inserter_test_source_empty ORDER BY col1 ASC', 'Check data for: mimeo_source.inserter_test_dest_empty');

SELECT results_eq('SELECT col1, "group", "Col-3" FROM mimeo_source."Inserter-Test-Source" ORDER BY col1 ASC',
    'SELECT * FROM dblink(''mimeo_test'', ''SELECT col1, "group", "Col-3" FROM mimeo_source."Inserter-Test-Source" ORDER BY col1 ASC'') t (col1 int, "group" text, "Col-3" timestamptz)',
    'Check data for: mimeo_source."Inserter-Test-Source"');

-- Serial
SELECT results_eq('SELECT col1, col2, col3 FROM mimeo_dest.inserter_test_dest_serial ORDER BY col1 ASC',
    'SELECT * FROM dblink(''mimeo_test'', ''SELECT col1, col2, col3 FROM mimeo_source.inserter_test_source WHERE col1 < (SELECT max(col1) FROM mimeo_source.inserter_test_source) ORDER BY col1 ASC'') t (col1 int, col2 text, col3 timestamptz)',
    'Check data for: mimeo_dest.inserter_test_dest_serial');

SELECT results_eq('SELECT col1, "group", "Col-3" FROM mimeo_dest."Inserter-Test-Source_Serial" ORDER BY col1 ASC',
    'SELECT * FROM dblink(''mimeo_test'', ''SELECT col1, "group", "Col-3" FROM mimeo_source."Inserter-Test-Source" WHERE col1 < (SELECT max(col1) FROM mimeo_source."Inserter-Test-Source") ORDER BY col1 ASC'') t (col1 int, "group" text, "Col-3" timestamptz)',
    'Check data for: mimeo_dest."Inserter-Test-Source_Serial"');

-- ########## UPDATER TESTS ##########
--Time
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

SELECT is_empty('SELECT col1, col2, col3 FROM mimeo_source.updater_test_source_empty ORDER BY col1 ASC', 'Check data for: mimeo_source.updater_test_source_empty');

SELECT results_eq('SELECT "COL-1", "group", "Col3" FROM mimeo_source."Updater-Test-Source" ORDER BY "COL-1" ASC',
    'SELECT * FROM dblink(''mimeo_test'', ''SELECT "COL-1", "group", "Col3" FROM mimeo_source."Updater-Test-Source" ORDER BY "COL-1" ASC'') t ("COL-1" int, "group" text, "Col3" timestamptz)',
    'Check data for: mimeo_source."Updater-Test-Source"');

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
SELECT is_empty('SELECT * FROM mimeo_dest.dml_test_dest WHERE col1 between 9500 and 10500', 'Check that deleted rows are gone from mimeo_dest.dml_test_dest');

SELECT results_eq('SELECT col1, col2, col3 FROM mimeo_dest.dml_test_dest_multi ORDER BY col1 ASC',
    'SELECT * FROM dblink(''mimeo_test'', ''SELECT col1, col2, col3 FROM mimeo_source.dml_test_source ORDER BY col1 ASC'') t (col1 int, col2 text, col3 timestamptz)',
    'Check data for: mimeo_dest.dml_test_dest_multi');

SELECT results_eq('SELECT col1, col2, col3 FROM mimeo_dest.dml_test_dest_nodata ORDER BY col1 ASC',
    'SELECT * FROM dblink(''mimeo_test'', ''SELECT col1, col2, col3 FROM mimeo_source.dml_test_source_nodata WHERE col1 > 10000 ORDER BY col1 ASC'') t (col1 int, col2 text, col3 timestamptz)',
    'Check data for: mimeo_dest.dml_test_dest_nodata');

SELECT results_eq('SELECT col1, col2 FROM mimeo_dest.dml_test_dest_filter ORDER BY col1 ASC',
    'SELECT * FROM dblink(''mimeo_test'', ''SELECT col1, col2 FROM mimeo_source.dml_test_source_filter ORDER BY col1 ASC'') t (col1 int, col2 text)',
    'Check data for: mimeo_dest.dml_test_dest_filter');

SELECT results_eq('SELECT col1, col2, col3 FROM mimeo_dest.dml_test_dest_condition ORDER BY col1 ASC',
    'SELECT * FROM dblink(''mimeo_test'', ''SELECT col1, col2, col3 FROM mimeo_source.dml_test_source_condition WHERE col1 > 9000 ORDER BY col1 ASC'') t (col1 int, col2 text, col3 timestamptz)',
    'Check data for: mimeo_dest.dml_test_dest_condition');
SELECT is_empty('SELECT * FROM mimeo_dest.dml_test_dest_condition WHERE col1 <= 10000', 'Check that deleted row is gone from mimeo_dest.dml_test_dest_condition');

SELECT is_empty('SELECT col1, col2, col3 FROM mimeo_source.dml_test_source_empty ORDER BY col1 ASC', 'Check data for: mimeo_source.dml_test_dest_empty');

SELECT results_eq('SELECT "COL1", "group", "Col-3" FROM mimeo_source."Dml-Test-Source" ORDER BY "COL1" ASC',
    'SELECT * FROM dblink(''mimeo_test'', ''SELECT "COL1", "group", "Col-3" FROM mimeo_source."Dml-Test-Source" ORDER BY "COL1" ASC'') t ("COL1" int, "group" text, "Col-3" timestamptz)',
    'Check data for: mimeo_source.Dml-Test-Source');

-- ########## LOGDEL TESTS ##########
SELECT results_eq('SELECT col1, col2, col3 FROM mimeo_source.logdel_test_source ORDER BY col1 ASC',
    'SELECT * FROM dblink(''mimeo_test'', ''SELECT col1, col2, col3 FROM mimeo_source.logdel_test_source ORDER BY col1 ASC'') t (col1 int, col2 text, col3 timestamptz)',
    'Check data for: mimeo_source.logdel_test_source');

SELECT results_eq('SELECT col1, col2, col3 FROM mimeo_dest.logdel_test_dest WHERE col1 < 12500 or col1 > 12520 ORDER BY col1, col2 ASC',
    'SELECT * FROM dblink(''mimeo_test'', ''SELECT col1, col2, col3 FROM mimeo_source.logdel_test_source2 ORDER BY col1, col2 ASC'') t (col1 int, col2 text, col3 timestamptz)',
    'Check data for: mimeo_dest.logdel_test_dest');
SELECT results_eq('SELECT col2 FROM mimeo_dest.logdel_test_dest WHERE (col1 between 12500 and 12520) AND mimeo_source_deleted IS NOT NULL order by col2',
    ARRAY['test12500','test12501','test12502','test12503','test12504','test12505','test12506','test12507','test12508','test12509','test12510',
        'test12511','test12512','test12513','test12514','test12515','test12516','test12517','test12518','test12519','test12520'],
    'Check that deleted rows are logged in mimeo_dest.logdel_test_dest');

SELECT results_eq('SELECT col1, col2, col3 FROM mimeo_dest.logdel_test_dest_multi ORDER BY col1 ASC',
    'SELECT * FROM dblink(''mimeo_test'', ''SELECT col1, col2, col3 FROM mimeo_source.logdel_test_source ORDER BY col1 ASC'') t (col1 int, col2 text, col3 timestamptz)',
    'Check data for: mimeo_dest.logdel_test_dest_multi');

SELECT results_eq('SELECT col1, col2, col3 FROM mimeo_dest.logdel_test_dest_nodata ORDER BY col1 ASC',
    'SELECT * FROM dblink(''mimeo_test'', ''SELECT col1, col2, col3 FROM mimeo_source.logdel_test_source_nodata WHERE col1 > 10000 ORDER BY col1 ASC'') t (col1 int, col2 text, col3 timestamptz)',
    'Check data for: mimeo_dest.logdel_test_dest_nodata');

SELECT results_eq('SELECT col1, col2 FROM mimeo_dest.logdel_test_dest_filter ORDER BY col1 ASC',
    'SELECT * FROM dblink(''mimeo_test'', ''SELECT col1, col2 FROM mimeo_source.logdel_test_source_filter ORDER BY col1 ASC'') t (col1 int, col2 text)',
    'Check data for: mimeo_dest.logdel_test_dest_filter');

SELECT results_eq('SELECT col1, col2 FROM mimeo_dest.logdel_test_dest_condition WHERE col1 <> 11 ORDER BY col1, col2 ASC',
    'SELECT * FROM dblink(''mimeo_test'', ''SELECT col1, col2 FROM mimeo_source.logdel_test_source_condition WHERE col1 > 9000 ORDER BY col1, col2 ASC'') t (col1 int, col2 text)',
    'Check data for: mimeo_dest.logdel_test_dest_condition');

SELECT is_empty('SELECT col1, col2, col3 FROM mimeo_source.logdel_test_source_empty ORDER BY col1 ASC', 'Check data for: mimeo_source.logdel_test_source_empty');

SELECT results_eq('SELECT "COL1", "group", "Col-3" FROM mimeo_source."LogDel-Test-Source" ORDER BY "COL1" ASC',
    'SELECT * FROM dblink(''mimeo_test'', ''SELECT "COL1", "group", "Col-3" FROM mimeo_source."LogDel-Test-Source" ORDER BY "COL1" ASC'') t ("COL1" int, "group" text, "Col-3" timestamptz)',
    'Check data for: mimeo_source.LogDel-Test-Source');

SELECT dblink_disconnect('mimeo_test');
--SELECT is('SELECT dblink_get_connections() @> ''{mimeo_test}''','', 'Close remote database connection');

SELECT * FROM finish();
