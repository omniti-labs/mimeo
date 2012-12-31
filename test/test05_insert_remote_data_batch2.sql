SELECT set_config('search_path','mimeo, dblink, tap',false);

SELECT plan(2);

SELECT dblink_connect('mimeo_test', 'host=localhost port=5432 dbname=mimeo_source user=mimeo_test password=mimeo_test');
SELECT is(dblink_get_connections() @> '{mimeo_test}', 't', 'Remote database connection established');

-- Insert new data
SELECT dblink_exec('mimeo_test', 'INSERT INTO mimeo_source.snap_test_source VALUES (generate_series(11,20), ''test''||generate_series(11,20)::text)');
SELECT dblink_exec('mimeo_test', 'INSERT INTO mimeo_source.inserter_test_source VALUES (generate_series(11,20), ''test''||generate_series(11,20)::text)');
SELECT dblink_exec('mimeo_test', 'INSERT INTO mimeo_source.updater_test_source VALUES (generate_series(11,20), ''test''||generate_series(11,20)::text)');
SELECT dblink_exec('mimeo_test', 'INSERT INTO mimeo_source.dml_test_source VALUES (generate_series(11,20), ''test''||generate_series(11,20)::text)');
SELECT dblink_exec('mimeo_test', 'INSERT INTO mimeo_source.dml_test_source2 VALUES (generate_series(11,20), ''test''||generate_series(11,20)::text)');
SELECT dblink_exec('mimeo_test', 'INSERT INTO mimeo_source.dml_test_source_nodata VALUES (generate_series(11,20), ''test''||generate_series(11,20)::text)');
SELECT dblink_exec('mimeo_test', 'INSERT INTO mimeo_source.dml_test_source_filter VALUES (generate_series(11,20), ''test''||generate_series(11,20)::text)');
SELECT dblink_exec('mimeo_test', 'INSERT INTO mimeo_source.dml_test_source_condition VALUES (generate_series(11,20), ''test''||generate_series(11,20)::text)');
SELECT dblink_exec('mimeo_test', 'INSERT INTO mimeo_source.logdel_test_source VALUES (generate_series(11,20), ''test''||generate_series(11,20)::text)');
SELECT dblink_exec('mimeo_test', 'INSERT INTO mimeo_source.logdel_test_source2 VALUES (generate_series(11,20), ''test''||generate_series(11,20)::text)');
SELECT dblink_exec('mimeo_test', 'INSERT INTO mimeo_source.logdel_test_source_nodata VALUES (generate_series(11,20), ''test''||generate_series(11,20)::text)');
SELECT dblink_exec('mimeo_test', 'INSERT INTO mimeo_source.logdel_test_source_filter VALUES (generate_series(11,20), ''test''||generate_series(11,20)::text)');
SELECT dblink_exec('mimeo_test', 'INSERT INTO mimeo_source.logdel_test_source_condition VALUES (generate_series(11,20), ''test''||generate_series(11,20)::text)');

-- Data for testing updater
SELECT dblink_exec('mimeo_test', 'UPDATE mimeo_source.updater_test_source SET col2 = ''changed'', col3 = clock_timestamp() WHERE col1 = 13');

-- Data for testing dml
SELECT dblink_exec('mimeo_test', 'UPDATE mimeo_source.dml_test_source2 SET col2 = ''changed'' WHERE col1 = 4 AND col2 = ''test4''');
SELECT dblink_exec('mimeo_test', 'DELETE FROM mimeo_source.dml_test_source2 WHERE col1 = 8');
SELECT dblink_exec('mimeo_test', 'UPDATE mimeo_source.dml_test_source_condition SET col2 = ''changed'' WHERE col1 = 9');
SELECT dblink_exec('mimeo_test', 'DELETE FROM mimeo_source.dml_test_source_condition where col1 = 11');

-- Data for testing logdel
SELECT dblink_exec('mimeo_test', 'UPDATE mimeo_source.logdel_test_source2 SET col2 = ''changed'' WHERE col1 = 4 AND col2 = ''test4''');
SELECT dblink_exec('mimeo_test', 'DELETE FROM mimeo_source.logdel_test_source2 WHERE col1 = 8');
SELECT dblink_exec('mimeo_test', 'UPDATE mimeo_source.logdel_test_source_condition SET col2 = ''changed'' WHERE col1 = 9');
SELECT dblink_exec('mimeo_test', 'DELETE FROM mimeo_source.logdel_test_source_condition where col1 = 11');


SELECT dblink_disconnect('mimeo_test');
--SELECT is_empty('SELECT dblink_get_connections() @> ''{mimeo_test}''', 'Close remote database connection');

SELECT pass('Completed 2nd batch of data inserts/updates/deletes for remote tables. Sleeping for 10 seconds to ensure gap for incremental tests...');
SELECT pg_sleep(10);

SELECT * FROM finish();
