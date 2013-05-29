\set ON_ERROR_ROLLBACK 1
\set ON_ERROR_STOP true

SELECT set_config('search_path','mimeo, dblink, tap',false);

SELECT plan(1);

SELECT dblink_connect('mimeo_test', 'host=localhost port=5432 dbname=mimeo_source user=mimeo_test password=mimeo_test');
SELECT is(dblink_get_connections() @> '{mimeo_test}', 't', 'Remote database connection established');

-- Insert new data
SELECT dblink_exec('mimeo_test', 'INSERT INTO mimeo_source.snap_test_source VALUES (generate_series(10001,20000), ''test''||generate_series(10001,20000)::text)');
SELECT dblink_exec('mimeo_test', 'INSERT INTO mimeo_source.snap_test_source_change_col VALUES (generate_series(10001,20000), ''test''||generate_series(10001,20000)::text)');
SELECT dblink_exec('mimeo_test', 'INSERT INTO mimeo_source.inserter_test_source VALUES (generate_series(10001,20000), ''test''||generate_series(10001,20000)::text)');
SELECT dblink_exec('mimeo_test', 'INSERT INTO mimeo_source.updater_test_source VALUES (generate_series(10001,20000), ''test''||generate_series(10001,20000)::text)');
SELECT dblink_exec('mimeo_test', 'INSERT INTO mimeo_source.dml_test_source VALUES (generate_series(10001,20000), ''test''||generate_series(10001,20000)::text)');
SELECT dblink_exec('mimeo_test', 'INSERT INTO mimeo_source.dml_test_source2 VALUES (generate_series(10001,20000), ''test''||generate_series(10001,20000)::text)');
SELECT dblink_exec('mimeo_test', 'INSERT INTO mimeo_source.dml_test_source_nodata VALUES (generate_series(10001,20000), ''test''||generate_series(10001,20000)::text)');
SELECT dblink_exec('mimeo_test', 'INSERT INTO mimeo_source.dml_test_source_filter VALUES (generate_series(10001,20000), ''test''||generate_series(10001,20000)::text)');
SELECT dblink_exec('mimeo_test', 'INSERT INTO mimeo_source.dml_test_source_condition VALUES (generate_series(10001,20000), ''test''||generate_series(10001,20000)::text)');
SELECT dblink_exec('mimeo_test', 'INSERT INTO mimeo_source.logdel_test_source VALUES (generate_series(10001,20000), ''test''||generate_series(10001,20000)::text)');
SELECT dblink_exec('mimeo_test', 'INSERT INTO mimeo_source.logdel_test_source2 VALUES (generate_series(10001,20000), ''test''||generate_series(10001,20000)::text)');
SELECT dblink_exec('mimeo_test', 'INSERT INTO mimeo_source.logdel_test_source_nodata VALUES (generate_series(10001,20000), ''test''||generate_series(10001,20000)::text)');
SELECT dblink_exec('mimeo_test', 'INSERT INTO mimeo_source.logdel_test_source_filter VALUES (generate_series(10001,20000), ''test''||generate_series(10001,20000)::text)');
SELECT dblink_exec('mimeo_test', 'INSERT INTO mimeo_source.logdel_test_source_condition VALUES (generate_series(10001,20000), ''test''||generate_series(10001,20000)::text)');

-- Data for testing updater
SELECT dblink_exec('mimeo_test', 'UPDATE mimeo_source.updater_test_source SET col2 = ''changed'', col3 = clock_timestamp() WHERE col1 between 9500 and 11500');

-- Data for testing dml
SELECT dblink_exec('mimeo_test', 'UPDATE mimeo_source.dml_test_source2 SET col2 = ''changed'' WHERE col1 = 4 AND col2 = ''test4''');
SELECT dblink_exec('mimeo_test', 'UPDATE mimeo_source.dml_test_source2 SET col2 = ''changed'' WHERE col1 between 8000 and 9000');
SELECT dblink_exec('mimeo_test', 'DELETE FROM mimeo_source.dml_test_source2 WHERE col1 between 9500 and 10500');
SELECT dblink_exec('mimeo_test', 'UPDATE mimeo_source.dml_test_source_condition SET col2 = ''changed''||col1 WHERE col1 > 15000');
SELECT dblink_exec('mimeo_test', 'DELETE FROM mimeo_source.dml_test_source_condition WHERE col1 <= 10000');

-- Data for testing logdel
SELECT dblink_exec('mimeo_test', 'UPDATE mimeo_source.logdel_test_source2 SET col2 = ''changed'' WHERE col1 = 4 AND col2 = ''test4''');
SELECT dblink_exec('mimeo_test', 'UPDATE mimeo_source.logdel_test_source2 SET col2 = ''changed'' WHERE col1 between 9600 and 10200');
SELECT dblink_exec('mimeo_test', 'DELETE FROM mimeo_source.logdel_test_source2 WHERE col1 between 12500 and 12520');
SELECT dblink_exec('mimeo_test', 'UPDATE mimeo_source.logdel_test_source_condition SET col2 = ''changed''||col1 WHERE col1 > 18000');


SELECT dblink_disconnect('mimeo_test');
--SELECT is_empty('SELECT dblink_get_connections() @> ''{mimeo_test}''', 'Close remote database connection');

SELECT diag('Completed 2nd batch of data inserts/updates/deletes for remote tables. Sleeping for 10 seconds to ensure gap for incremental tests...');
SELECT pg_sleep(10);

SELECT * FROM finish();
