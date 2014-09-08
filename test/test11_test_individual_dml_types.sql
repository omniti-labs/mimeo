\set ON_ERROR_ROLLBACK 1
\set ON_ERROR_STOP true

-- Test when there are only inserts, updates or deletes in a single refresh batch in dml/logdel. Catches bug found in 0.9.2

SELECT set_config('search_path','mimeo, dblink, public',false);

SELECT plan(9);

SELECT dblink_connect('mimeo_test', 'host=localhost port=5432 dbname=mimeo_source user=mimeo_owner password=mimeo_owner');
SELECT is(dblink_get_connections() @> '{mimeo_test}', 't', 'Remote database connection established');

-- Test INSERT only
SELECT diag('Inserting rows: mimeo_source.dml_test_source');
SELECT dblink_exec('mimeo_test', 'INSERT INTO mimeo_source.dml_test_source VALUES (generate_series(100031,100040), ''test''||generate_series(100031,100040)::text)');
SELECT diag('Inserting rows: mimeo_source.logdel_test_source');
SELECT dblink_exec('mimeo_test', 'INSERT INTO mimeo_source.logdel_test_source VALUES (generate_series(100031,100040), ''test''||generate_series(100031,100040)::text)');
SELECT diag('Running refresh: mimeo_source.dml_test_source');
SELECT refresh_dml('mimeo_source.dml_test_source');
SELECT diag('Running refresh: mimeo_source.logdel_test_source');
SELECT refresh_logdel('mimeo_source.logdel_test_source');
SELECT results_eq('SELECT col1, col2, col3 FROM mimeo_source.dml_test_source ORDER BY col1 ASC',
    'SELECT * FROM dblink(''mimeo_test'', ''SELECT col1, col2, col3 FROM mimeo_source.dml_test_source ORDER BY col1 ASC'') t (col1 int, col2 text, col3 timestamptz)',
    'Check data for: mimeo_source.dml_test_source');
SELECT results_eq('SELECT col1, col2, col3 FROM mimeo_source.logdel_test_source ORDER BY col1 ASC',
    'SELECT * FROM dblink(''mimeo_test'', ''SELECT col1, col2, col3 FROM mimeo_source.logdel_test_source ORDER BY col1 ASC'') t (col1 int, col2 text, col3 timestamptz)',
    'Check data for: mimeo_source.logdel_test_source');

-- Test UPDATE only
SELECT diag('Updating rows: mimeo_source.dml_test_source');
SELECT dblink_exec('mimeo_test', 'UPDATE mimeo_source.dml_test_source SET col2 = ''update_test'' WHERE col1 between 100031 and 100040');
SELECT diag('Updating rows: mimeo_source.logdel_test_source');
SELECT dblink_exec('mimeo_test', 'UPDATE mimeo_source.logdel_test_source SET col2 = ''update_test''||col1 WHERE col1 between 100031 and 100040');
SELECT diag('Running refresh: mimeo_source.dml_test_source');
SELECT refresh_dml('mimeo_source.dml_test_source');
SELECT diag('Running refresh: mimeo_source.logdel_test_source');
SELECT refresh_logdel('mimeo_source.logdel_test_source');
SELECT results_eq('SELECT col1, col2, col3 FROM mimeo_source.dml_test_source ORDER BY col1 ASC',
    'SELECT * FROM dblink(''mimeo_test'', ''SELECT col1, col2, col3 FROM mimeo_source.dml_test_source ORDER BY col1 ASC'') t (col1 int, col2 text, col3 timestamptz)',
    'Check data for: mimeo_source.dml_test_source');
SELECT results_eq('SELECT col1, col2, col3 FROM mimeo_source.logdel_test_source ORDER BY col1 ASC',
    'SELECT * FROM dblink(''mimeo_test'', ''SELECT col1, col2, col3 FROM mimeo_source.logdel_test_source ORDER BY col1 ASC'') t (col1 int, col2 text, col3 timestamptz)',
    'Check data for: mimeo_source.logdel_test_source');

-- Test DELETE only
SELECT diag('Deleting rows: mimeo_source.dml_test_source');
SELECT dblink_exec('mimeo_test', 'DELETE FROM mimeo_source.dml_test_source WHERE col1 between 100031 and 100040');
SELECT diag('Deleting rows: mimeo_source.logdel_test_source');
SELECT dblink_exec('mimeo_test', 'DELETE FROM mimeo_source.logdel_test_source WHERE col1 between 100031 and 100040');
SELECT diag('Running refresh: mimeo_source.dml_test_source');
SELECT refresh_dml('mimeo_source.dml_test_source');
SELECT diag('Running refresh: mimeo_source.logdel_test_source');
SELECT refresh_logdel('mimeo_source.logdel_test_source');
SELECT results_eq('SELECT col1, col2, col3 FROM mimeo_source.dml_test_source ORDER BY col1 ASC',
    'SELECT * FROM dblink(''mimeo_test'', ''SELECT col1, col2, col3 FROM mimeo_source.dml_test_source ORDER BY col1 ASC'') t (col1 int, col2 text, col3 timestamptz)',
    'Check data for: mimeo_source.dml_test_source');
SELECT results_eq('SELECT col1, col2, col3 FROM mimeo_dest.logdel_test_dest WHERE (col1 < 12500 OR col1 > 12520) AND (col1 < 45500 OR col1 > 45520) AND (col1 < 100031 or col1 > 100040) ORDER BY col1, col2 ASC',
    'SELECT * FROM dblink(''mimeo_test'', ''SELECT col1, col2, col3 FROM mimeo_source.logdel_test_source2 ORDER BY col1, col2 ASC'') t (col1 int, col2 text, col3 timestamptz)',
    'Check data for: mimeo_dest.logdel_test_dest');
SELECT results_eq('SELECT col1, col2, col3 FROM mimeo_source.logdel_test_source WHERE col1 < 100031 or col1 > 100040 ORDER BY col1 ASC',
    'SELECT * FROM dblink(''mimeo_test'', ''SELECT col1, col2, col3 FROM mimeo_source.logdel_test_source ORDER BY col1 ASC'') t (col1 int, col2 text, col3 timestamptz)',
    'Check data for: mimeo_source.logdel_test_source');
SELECT results_eq('SELECT col2 FROM mimeo_source.logdel_test_source WHERE (col1 between 100031 and 100040) AND mimeo_source_deleted IS NOT NULL order by col2',
    ARRAY['update_test100031','update_test100032','update_test100033','update_test100034','update_test100035','update_test100036','update_test100037','update_test100038','update_test100039','update_test100040'],
    'Check that deleted rows are logged in mimeo_dest.logdel_test_dest');


SELECT dblink_disconnect('mimeo_test');
--SELECT is_empty('SELECT dblink_get_connections() @> ''{mimeo_test}''', 'Close remote database connection');

SELECT * FROM finish();
