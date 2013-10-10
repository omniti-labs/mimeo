\set ON_ERROR_ROLLBACK 1
\set ON_ERROR_STOP true

SELECT set_config('search_path','mimeo, dblink, public',false);

SELECT plan(38);

SELECT dblink_connect('mimeo_test', 'host=localhost port=5432 dbname=mimeo_source user=mimeo_test password=mimeo_test');
SELECT is(dblink_get_connections() @> '{mimeo_test}', 't', 'Remote database connection established');

SELECT diag('Updating source rows for repull test...');
SELECT dblink_exec('mimeo_test', 'UPDATE mimeo_source.inserter_test_source SET col2 = ''repull''||col2::text');
SELECT dblink_exec('mimeo_test', 'UPDATE mimeo_source.updater_test_source SET col2 = ''repull''||col2::text');
SELECT dblink_exec('mimeo_test', 'UPDATE mimeo_source.dml_test_source SET col2 = ''repull''||col2::text');
SELECT dblink_exec('mimeo_test', 'UPDATE mimeo_source.dml_test_source2 SET col2 = ''repull''||col2::text');
SELECT dblink_exec('mimeo_test', 'UPDATE mimeo_source.dml_test_source_nodata SET col2 = ''repull''||col2::text');
SELECT dblink_exec('mimeo_test', 'UPDATE mimeo_source.dml_test_source_filter SET col2 = ''repull''||col2::text');
SELECT dblink_exec('mimeo_test', 'UPDATE mimeo_source.dml_test_source_condition SET col2 = ''repull''||col2::text');
SELECT dblink_exec('mimeo_test', 'UPDATE mimeo_source.logdel_test_source SET col2 = ''repull''||col2::text');
SELECT dblink_exec('mimeo_test', 'UPDATE mimeo_source.logdel_test_source2 SET col2 = ''repull''||col2::text');
SELECT dblink_exec('mimeo_test', 'UPDATE mimeo_source.logdel_test_source_nodata SET col2 = ''repull''||col2::text');
SELECT dblink_exec('mimeo_test', 'UPDATE mimeo_source.logdel_test_source_filter SET col2 = ''repull''||col2::text');
SELECT dblink_exec('mimeo_test', 'UPDATE mimeo_source.logdel_test_source_condition SET col2 = ''repull''||col2::text');

SELECT diag('Running refresh functions to repull all rows. This will take a bit...');
SELECT refresh_inserter('mimeo_source.inserter_test_source', p_repull := true);
SELECT refresh_inserter('mimeo_dest.inserter_test_dest', p_repull := true);
SELECT refresh_inserter('mimeo_dest.inserter_test_dest_nodata', p_repull := true);
SELECT refresh_inserter('mimeo_dest.inserter_test_dest_filter', p_repull := true);
SELECT refresh_inserter('mimeo_dest.inserter_test_dest_condition', p_repull := true);

SELECT refresh_updater('mimeo_source.updater_test_source', p_repull := true);
SELECT refresh_updater('mimeo_dest.updater_test_dest', p_repull := true);
SELECT refresh_updater('mimeo_dest.updater_test_dest_nodata', p_repull := true);
SELECT refresh_updater('mimeo_dest.updater_test_dest_filter');
SELECT refresh_updater('mimeo_dest.updater_test_dest_condition', p_repull := true);

SELECT refresh_dml('mimeo_source.dml_test_source', p_repull := true);
SELECT refresh_dml('mimeo_dest.dml_test_dest', p_repull := true);
SELECT refresh_dml('mimeo_dest.dml_test_dest_nodata', p_repull := true);
SELECT refresh_dml('mimeo_dest.dml_test_dest_filter', p_repull := true);
SELECT refresh_dml('mimeo_dest.dml_test_dest_condition', p_repull := true);

SELECT refresh_logdel('mimeo_source.logdel_test_source', p_repull := true);
SELECT refresh_logdel('mimeo_dest.logdel_test_dest', p_repull := true);
SELECT refresh_logdel('mimeo_dest.logdel_test_dest_nodata', p_repull := true);
SELECT refresh_logdel('mimeo_dest.logdel_test_dest_filter', p_repull := true);
SELECT refresh_logdel('mimeo_dest.logdel_test_dest_condition', p_repull := true);

-- ########## INSERTER TESTS ##########
SELECT results_eq('SELECT col1, col2, col3 FROM mimeo_source.inserter_test_source ORDER BY col1 ASC',
    'SELECT * FROM dblink(''mimeo_test'', ''SELECT col1, col2, col3 FROM mimeo_source.inserter_test_source ORDER BY col1 ASC'') t (col1 int, col2 text, col3 timestamptz)',
    'Check data for: mimeo_source.inserter_test_source');

SELECT results_eq('SELECT col1, col2, col3 FROM mimeo_dest.inserter_test_dest ORDER BY col1 ASC',
    'SELECT * FROM dblink(''mimeo_test'', ''SELECT col1, col2, col3 FROM mimeo_source.inserter_test_source ORDER BY col1 ASC'') t (col1 int, col2 text, col3 timestamptz)',
    'Check data for: mimeo_dest.inserter_test_dest');

SELECT results_eq('SELECT col1, col2, col3 FROM mimeo_dest.inserter_test_dest_nodata ORDER BY col1 ASC',
    'SELECT * FROM dblink(''mimeo_test'', ''SELECT col1, col2, col3 FROM mimeo_source.inserter_test_source ORDER BY col1 ASC'') t (col1 int, col2 text, col3 timestamptz)',
    'Check data for: mimeo_dest.inserter_test_dest');

SELECT results_eq('SELECT col1, col3 FROM mimeo_dest.inserter_test_dest_filter ORDER BY col1 ASC',
    'SELECT * FROM dblink(''mimeo_test'', ''SELECT col1, col3 FROM mimeo_source.inserter_test_source ORDER BY col1 ASC'') t (col1 int, col3 timestamptz)',
    'Check data for: mimeo_dest.inserter_test_dest_filter');

SELECT results_eq('SELECT col1, col2, col3 FROM mimeo_dest.inserter_test_dest_condition ORDER BY col1 ASC',
    'SELECT * FROM dblink(''mimeo_test'', ''SELECT col1, col2, col3 FROM mimeo_source.inserter_test_source WHERE col1 > 9000 ORDER BY col1 ASC'') t (col1 int, col2 text, col3 timestamptz)',
    'Check data for: mimeo_dest.inserter_test_dest_condition');

-- ########## UPDATER TESTS ##########
SELECT results_eq('SELECT col1, col2, col3 FROM mimeo_source.updater_test_source ORDER BY col1 ASC',
    'SELECT * FROM dblink(''mimeo_test'', ''SELECT col1, col2, col3 FROM mimeo_source.updater_test_source ORDER BY col1 ASC'') t (col1 int, col2 text, col3 timestamptz)',
    'Check data for: mimeo_source.updater_test_source');

SELECT results_eq('SELECT col1, col2, col3 FROM mimeo_dest.updater_test_dest ORDER BY col1 ASC',
    'SELECT * FROM dblink(''mimeo_test'', ''SELECT col1, col2, col3 FROM mimeo_source.updater_test_source ORDER BY col1 ASC'') t (col1 int, col2 text, col3 timestamptz)',
    'Check data for: mimeo_dest.updater_test_dest');

SELECT results_eq('SELECT col1, col2, col3 FROM mimeo_dest.updater_test_dest_nodata ORDER BY col1 ASC',
    'SELECT * FROM dblink(''mimeo_test'', ''SELECT col1, col2, col3 FROM mimeo_source.updater_test_source ORDER BY col1 ASC'') t (col1 int, col2 text, col3 timestamptz)',
    'Check data for: mimeo_dest.updater_test_dest_nodata');

SELECT results_eq('SELECT col1, col3 FROM mimeo_dest.updater_test_dest_filter ORDER BY col1 ASC',
    'SELECT * FROM dblink(''mimeo_test'', ''SELECT col1, col3 FROM mimeo_source.updater_test_source ORDER BY col1 ASC'') t (col1 int, col3 timestamptz)',
    'Check data for: mimeo_dest.updater_test_dest_filter');

SELECT results_eq('SELECT col1, col2, col3 FROM mimeo_dest.updater_test_dest_condition ORDER BY col1 ASC',
    'SELECT * FROM dblink(''mimeo_test'', ''SELECT col1, col2, col3 FROM mimeo_source.updater_test_source WHERE col1 > 9000 ORDER BY col1 ASC'') t (col1 int, col2 text, col3 timestamptz)',
    'Check data for: mimeo_dest.updater_test_dest_condition');

-- ########## DML TESTS ##########
SELECT results_eq('SELECT col1, col2, col3 FROM mimeo_source.dml_test_source ORDER BY col1 ASC',
    'SELECT * FROM dblink(''mimeo_test'', ''SELECT col1, col2, col3 FROM mimeo_source.dml_test_source ORDER BY col1 ASC'') t (col1 int, col2 text, col3 timestamptz)',
    'Check data for: mimeo_source.dml_test_source');

SELECT results_eq('SELECT col1, col2, col3 FROM mimeo_dest.dml_test_dest ORDER BY col1 ASC',
    'SELECT * FROM dblink(''mimeo_test'', ''SELECT col1, col2, col3 FROM mimeo_source.dml_test_source2 ORDER BY col1 ASC'') t (col1 int, col2 text, col3 timestamptz)',
    'Check data for: mimeo_dest.dml_test_dest');
SELECT is_empty('SELECT * FROM mimeo_dest.dml_test_dest WHERE col1 between 9500 and 10500', 'Check that deleted row is gone from mimeo_dest.dml_test_dest');

SELECT results_eq('SELECT col1, col2, col3 FROM mimeo_dest.dml_test_dest_nodata ORDER BY col1 ASC',
    'SELECT * FROM dblink(''mimeo_test'', ''SELECT col1, col2, col3 FROM mimeo_source.dml_test_source_nodata ORDER BY col1 ASC'') t (col1 int, col2 text, col3 timestamptz)',
    'Check data for: mimeo_dest.dml_test_dest_nodata');

SELECT results_eq('SELECT col1, col2 FROM mimeo_dest.dml_test_dest_filter ORDER BY col1 ASC',
    'SELECT * FROM dblink(''mimeo_test'', ''SELECT col1, col2 FROM mimeo_source.dml_test_source_filter ORDER BY col1 ASC'') t (col1 int, col2 text)',
    'Check data for: mimeo_dest.dml_test_dest_filter');

SELECT results_eq('SELECT col1, col2, col3 FROM mimeo_dest.dml_test_dest_condition ORDER BY col1 ASC',
    'SELECT * FROM dblink(''mimeo_test'', ''SELECT col1, col2, col3 FROM mimeo_source.dml_test_source_condition WHERE col1 > 9000 ORDER BY col1 ASC'') t (col1 int, col2 text, col3 timestamptz)',
    'Check data for: mimeo_dest.dml_test_dest_condition');
SELECT is_empty('SELECT * FROM mimeo_dest.dml_test_dest_condition WHERE col1 <= 10000', 'Check that deleted row is gone from mimeo_dest.dml_test_dest_condition');

-- ########## LOGDEL TESTS ##########
SELECT results_eq('SELECT col1, col2, col3 FROM mimeo_source.logdel_test_source ORDER BY col1 ASC',
    'SELECT * FROM dblink(''mimeo_test'', ''SELECT col1, col2, col3 FROM mimeo_source.logdel_test_source ORDER BY col1 ASC'') t (col1 int, col2 text, col3 timestamptz)',
    'Check data for: mimeo_source.logdel_test_source');

SELECT results_eq('SELECT col1, col2, col3 FROM mimeo_dest.logdel_test_dest WHERE (col1 < 12500 OR col1 > 12520) AND (col1 < 45500 OR col1 > 45520) ORDER BY col1, col2 ASC',
    'SELECT * FROM dblink(''mimeo_test'', ''SELECT col1, col2, col3 FROM mimeo_source.logdel_test_source2 ORDER BY col1, col2 ASC'') t (col1 int, col2 text, col3 timestamptz)',
    'Check data for: mimeo_dest.logdel_test_dest');
SELECT results_eq('SELECT col2 FROM mimeo_dest.logdel_test_dest WHERE (col1 between 12500 and 12520) AND mimeo_source_deleted IS NOT NULL order by col2',
    ARRAY['test12500','test12501','test12502','test12503','test12504','test12505','test12506','test12507','test12508','test12509','test12510',
        'test12511','test12512','test12513','test12514','test12515','test12516','test12517','test12518','test12519','test12520'],
    'Check that deleted rows are logged in mimeo_dest.logdel_test_dest');
SELECT results_eq('SELECT col2 FROM mimeo_dest.logdel_test_dest WHERE (col1 between 45500 and 45520) AND mimeo_source_deleted IS NOT NULL order by col2',
    ARRAY['test45500','test45501','test45502','test45503','test45504','test45505','test45506','test45507','test45508','test45509','test45510',
        'test45511','test45512','test45513','test45514','test45515','test45516','test45517','test45518','test45519','test45520'],
    'Check that deleted rows are logged in mimeo_dest.logdel_test_dest');

SELECT results_eq('SELECT col1, col2, col3 FROM mimeo_dest.logdel_test_dest_nodata ORDER BY col1 ASC',
    'SELECT * FROM dblink(''mimeo_test'', ''SELECT col1, col2, col3 FROM mimeo_source.logdel_test_source_nodata ORDER BY col1 ASC'') t (col1 int, col2 text, col3 timestamptz)',
    'Check data for: mimeo_dest.logdel_test_dest_nodata');

SELECT results_eq('SELECT col1, col2 FROM mimeo_dest.logdel_test_dest_filter ORDER BY col1 ASC',
    'SELECT * FROM dblink(''mimeo_test'', ''SELECT col1, col2 FROM mimeo_source.logdel_test_source_filter ORDER BY col1 ASC'') t (col1 int, col2 text)',
    'Check data for: mimeo_dest.logdel_test_dest_filter');

SELECT results_eq('SELECT col1, col2 FROM mimeo_dest.logdel_test_dest_condition WHERE col1 <> 11 ORDER BY col1, col2 ASC',
    'SELECT * FROM dblink(''mimeo_test'', ''SELECT col1, col2 FROM mimeo_source.logdel_test_source_condition WHERE col1 > 9000 ORDER BY col1, col2 ASC'') t (col1 int, col2 text)',
    'Check data for: mimeo_dest.logdel_test_dest_condition');

SELECT is_empty('SELECT col1, col2, col3 FROM mimeo_source.logdel_test_source_empty ORDER BY col1 ASC', 'Check data for: mimeo_source.logdel_test_source_empty');

-- Test specific time period repull
SELECT diag('Inserting values to test for specific time period repull...');

SELECT dblink_exec('mimeo_test', 'INSERT INTO mimeo_source.inserter_test_source VALUES (generate_series(100001,100010), ''test''||generate_series(100001,100010)::text, ''2013-01-01 02:00:00'')');
SELECT dblink_exec('mimeo_test', 'INSERT INTO mimeo_source.inserter_test_source VALUES (generate_series(100011,100020), ''test''||generate_series(100011,100020)::text, ''2013-01-01 02:05:00'')');
SELECT dblink_exec('mimeo_test', 'INSERT INTO mimeo_source.inserter_test_source VALUES (generate_series(100021,100030), ''test''||generate_series(100021,100030)::text, ''2013-01-01 02:10:00'')');

SELECT dblink_exec('mimeo_test', 'INSERT INTO mimeo_source.updater_test_source VALUES (generate_series(100001,100010), ''test''||generate_series(100001,100010)::text, ''2013-01-01 02:00:00'')');
SELECT dblink_exec('mimeo_test', 'INSERT INTO mimeo_source.updater_test_source VALUES (generate_series(100011,100020), ''test''||generate_series(100011,100020)::text, ''2013-01-01 02:05:00'')');
SELECT dblink_exec('mimeo_test', 'INSERT INTO mimeo_source.updater_test_source VALUES (generate_series(100021,100030), ''test''||generate_series(100021,100030)::text, ''2013-01-01 02:10:00'')');

SELECT diag ('Repulling specific time periods...');
SELECT refresh_inserter('mimeo_source.inserter_test_source', p_repull := true, p_repull_start := '2013-01-01 01:00:00', p_repull_end := '2013-01-01 03:00:00');
SELECT refresh_updater('mimeo_source.updater_test_source', p_repull := true, p_repull_start := '2013-01-01 01:00:00', p_repull_end := '2013-01-01 03:00:00');

SELECT results_eq('SELECT col1, col2, col3 FROM mimeo_source.inserter_test_source ORDER BY col1 ASC',
    'SELECT * FROM dblink(''mimeo_test'', ''SELECT col1, col2, col3 FROM mimeo_source.inserter_test_source ORDER BY col1 ASC'') t (col1 int, col2 text, col3 timestamptz)',
    'Check data for: mimeo_source.inserter_test_source');

SELECT results_eq('SELECT col1, col2, col3 FROM mimeo_source.updater_test_source ORDER BY col1 ASC',
    'SELECT * FROM dblink(''mimeo_test'', ''SELECT col1, col2, col3 FROM mimeo_source.updater_test_source ORDER BY col1 ASC'') t (col1 int, col2 text, col3 timestamptz)',
    'Check data for: mimeo_source.updater_test_source');

-- Double-check that the last_value in the config matches the real control column value for incremental replication
SELECT results_eq('SELECT max(col3) FROM mimeo_source.inserter_test_source',
    'SELECT last_value FROM mimeo.refresh_config_inserter WHERE dest_table = ''mimeo_source.inserter_test_source''',
    'Check last_value for mimeo_source.inserter_test_source');
SELECT results_eq('SELECT max(col3) FROM mimeo_dest.inserter_test_dest',
    'SELECT last_value FROM mimeo.refresh_config_inserter WHERE dest_table = ''mimeo_dest.inserter_test_dest''',
    'Check last_value for mimeo_dest.inserter_test_dest');
SELECT results_eq('SELECT max(col3) FROM mimeo_dest.inserter_test_dest_nodata',
    'SELECT last_value FROM mimeo.refresh_config_inserter WHERE dest_table = ''mimeo_dest.inserter_test_dest_nodata''',
    'Check last_value for mimeo_dest.inserter_test_dest_nodata');
SELECT results_eq('SELECT max(col3) FROM mimeo_dest.inserter_test_dest_filter',
    'SELECT last_value FROM mimeo.refresh_config_inserter WHERE dest_table = ''mimeo_dest.inserter_test_dest_filter''',
    'Check last_value for mimeo_dest.inserter_test_dest_filter');
SELECT results_eq('SELECT max(col3) FROM mimeo_dest.inserter_test_dest_condition',
    'SELECT last_value FROM mimeo.refresh_config_inserter WHERE dest_table = ''mimeo_dest.inserter_test_dest_condition''',
    'Check last_value for mimeo_dest.inserter_test_dest_condition');

SELECT results_eq('SELECT max(col3) FROM mimeo_source.updater_test_source',
    'SELECT last_value FROM mimeo.refresh_config_updater WHERE dest_table = ''mimeo_source.updater_test_source''',
    'Check last_value for mimeo_source.updater_test_source');
SELECT results_eq('SELECT max(col3) FROM mimeo_dest.updater_test_dest',
    'SELECT last_value FROM mimeo.refresh_config_updater WHERE dest_table = ''mimeo_dest.updater_test_dest''',
    'Check last_value for mimeo_dest.updater_test_dest');
SELECT results_eq('SELECT max(col3) FROM mimeo_dest.updater_test_dest_nodata',
    'SELECT last_value FROM mimeo.refresh_config_updater WHERE dest_table = ''mimeo_dest.updater_test_dest_nodata''',
    'Check last_value for mimeo_dest.updater_test_dest_nodata');
SELECT results_eq('SELECT max(col3) FROM mimeo_dest.updater_test_dest_filter',
    'SELECT last_value FROM mimeo.refresh_config_updater WHERE dest_table = ''mimeo_dest.updater_test_dest_filter''',
    'Check last_value for mimeo_dest.updater_test_dest_filter');
SELECT results_eq('SELECT max(col3) FROM mimeo_dest.updater_test_dest_condition',
    'SELECT last_value FROM mimeo.refresh_config_updater WHERE dest_table = ''mimeo_dest.updater_test_dest_condition''',
    'Check last_value for mimeo_dest.updater_test_dest_condition');


SELECT dblink_disconnect('mimeo_test');
--SELECT is_empty('SELECT dblink_get_connections() @> ''{mimeo_test}''', 'Close remote database connection');

SELECT * FROM finish();
