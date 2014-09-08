\set ON_ERROR_ROLLBACK 1
\set ON_ERROR_STOP true

SELECT set_config('search_path','mimeo, dblink, public',false);

SELECT plan(2);

-- Test that non-dblink, non-owner roles can still insert to source tables
SELECT dblink_connect('mimeo_test', 'host=localhost port=5432 dbname=mimeo_source user=mimeo_dumb_role password=mimeo_test');
SELECT is(dblink_get_connections() @> '{mimeo_test}', 't', 'Remote database connection established');

-- Insert new data
SELECT diag('Inserting more data for: mimeo_source.snap_test_source');
SELECT dblink_exec('mimeo_test', 'INSERT INTO mimeo_source.snap_test_source VALUES (generate_series(10001,20000), ''test''||generate_series(10001,20000)::text)');
SELECT diag('Inserting more data for: mimeo_source.snap_test_source_change_col');
SELECT dblink_exec('mimeo_test', 'INSERT INTO mimeo_source.snap_test_source_change_col VALUES (generate_series(10001,20000), ''test''||generate_series(10001,20000)::text)');
SELECT diag('Inserting more data for: mimeo_source.Snap-test-Source');
SELECT dblink_exec('mimeo_test', 'INSERT INTO mimeo_source."Snap-test-Source" VALUES (generate_series(10001,20000), ''test''||generate_series(10001,20000)::text)');

SELECT diag('Inserting more data for: mimeo_source.inserter_test_source');
SELECT dblink_exec('mimeo_test', 'INSERT INTO mimeo_source.inserter_test_source VALUES (generate_series(10001,20000), ''test''||generate_series(10001,20000)::text)');
SELECT diag('Inserting more data for: mimeo_source."Inserter-Test-Source"');
SELECT dblink_exec('mimeo_test', 'INSERT INTO mimeo_source."Inserter-Test-Source" VALUES (generate_series(10001,20000), ''test''||generate_series(10001,20000)::text)');

SELECT diag('Inserting more data for: mimeo_source.updater_test_source');
SELECT dblink_exec('mimeo_test', 'INSERT INTO mimeo_source.updater_test_source VALUES (generate_series(10001,20000), ''test''||generate_series(10001,20000)::text)');
SELECT diag('Inserting more data for: mimeo_source."Updater-Test-Source"');
SELECT dblink_exec('mimeo_test', 'INSERT INTO mimeo_source."Updater-Test-Source" VALUES (generate_series(10001,20000), ''test''||generate_series(10001,20000)::text)');

SELECT diag('Inserting more data for: mimeo_source.dml_test_source');
SELECT dblink_exec('mimeo_test', 'INSERT INTO mimeo_source.dml_test_source VALUES (generate_series(10001,20000), ''test''||generate_series(10001,20000)::text)');
SELECT diag('Inserting more data for: mimeo_source.dml_test_source2');
SELECT dblink_exec('mimeo_test', 'INSERT INTO mimeo_source.dml_test_source2 VALUES (generate_series(10001,20000), ''test''||generate_series(10001,20000)::text)');
SELECT diag('Inserting more data for: mimeo_source.dml_test_source_nodata');
SELECT dblink_exec('mimeo_test', 'INSERT INTO mimeo_source.dml_test_source_nodata VALUES (generate_series(10001,20000), ''test''||generate_series(10001,20000)::text)');
SELECT diag('Inserting more data for: mimeo_source.dml_test_source_filter');
SELECT dblink_exec('mimeo_test', 'INSERT INTO mimeo_source.dml_test_source_filter VALUES (generate_series(10001,20000), ''test''||generate_series(10001,20000)::text)');
SELECT diag('Inserting more data for: mimeo_source.dml_test_source_condition');
SELECT dblink_exec('mimeo_test', 'INSERT INTO mimeo_source.dml_test_source_condition VALUES (generate_series(10001,20000), ''test''||generate_series(10001,20000)::text)');
SELECT diag('Inserting more data for: mimeo_source."Dml-Test-Source"');
SELECT dblink_exec('mimeo_test', 'INSERT INTO mimeo_source."Dml-Test-Source" VALUES (generate_series(10001,20000), ''test''||generate_series(10001,20000)::text)');

SELECT diag('Inserting more data for: mimeo_source.logdel_test_source');
SELECT dblink_exec('mimeo_test', 'INSERT INTO mimeo_source.logdel_test_source VALUES (generate_series(10001,20000), ''test''||generate_series(10001,20000)::text)');
SELECT diag('Inserting more data for: mimeo_source.logdel_test_source2');
SELECT dblink_exec('mimeo_test', 'INSERT INTO mimeo_source.logdel_test_source2 VALUES (generate_series(10001,20000), ''test''||generate_series(10001,20000)::text)');
SELECT diag('Inserting more data for: mimeo_source.logdel_test_source_nodata');
SELECT dblink_exec('mimeo_test', 'INSERT INTO mimeo_source.logdel_test_source_nodata VALUES (generate_series(10001,20000), ''test''||generate_series(10001,20000)::text)');
SELECT diag('Inserting more data for: mimeo_source.logdel_test_source_filter');
SELECT dblink_exec('mimeo_test', 'INSERT INTO mimeo_source.logdel_test_source_filter VALUES (generate_series(10001,20000), ''test''||generate_series(10001,20000)::text)');
SELECT diag('Inserting more data for: mimeo_source.logdel_test_source_condition');
SELECT dblink_exec('mimeo_test', 'INSERT INTO mimeo_source.logdel_test_source_condition VALUES (generate_series(10001,20000), ''test''||generate_series(10001,20000)::text)');
SELECT diag('Inserting more data for: mimeo_source."LogDel-Test-Source"');
SELECT dblink_exec('mimeo_test', 'INSERT INTO mimeo_source."LogDel-Test-Source" VALUES (generate_series(10001,20000), ''test''||generate_series(10001,20000)::text)');

-- Data for testing updater
SELECT diag('Updating data for: mimeo_source.updater_test_source');
SELECT dblink_exec('mimeo_test', 'UPDATE mimeo_source.updater_test_source SET col2 = ''changed'', col3 = clock_timestamp(), col4 = nextval(''mimeo_source.updater_test_source_col4_seq'') WHERE col1 between 9500 and 11500');

-- Data for testing dml
SELECT diag('Updating data for: mimeo_source.dml_test_source2');
SELECT dblink_exec('mimeo_test', 'UPDATE mimeo_source.dml_test_source2 SET col2 = ''changed'' WHERE col1 = 4 AND col2 = ''test4''');
SELECT diag('Updating data for: mimeo_source.dml_test_source2');
SELECT dblink_exec('mimeo_test', 'UPDATE mimeo_source.dml_test_source2 SET col2 = ''changed'' WHERE col1 between 8000 and 9000');
SELECT diag('Deleting data for: mimeo_source.dml_test_source2');
SELECT dblink_exec('mimeo_test', 'DELETE FROM mimeo_source.dml_test_source2 WHERE col1 between 9500 and 10500');
SELECT diag('Updating data for: mimeo_source.dml_test_source_condition');
SELECT dblink_exec('mimeo_test', 'UPDATE mimeo_source.dml_test_source_condition SET col2 = ''changed''||col1 WHERE col1 > 15000');
SELECT diag('Deleting data for: mimeo_source.dml_test_source_condition');
SELECT dblink_exec('mimeo_test', 'DELETE FROM mimeo_source.dml_test_source_condition WHERE col1 <= 10000');

-- Data for testing logdel
SELECT diag('Updating data for: mimeo_source.logdel_test_source2');
SELECT dblink_exec('mimeo_test', 'UPDATE mimeo_source.logdel_test_source2 SET col2 = ''changed'' WHERE col1 = 4 AND col2 = ''test4''');
SELECT diag('Updating data for: mimeo_source.logdel_test_source2');
SELECT dblink_exec('mimeo_test', 'UPDATE mimeo_source.logdel_test_source2 SET col2 = ''changed'' WHERE col1 between 9600 and 10200');
SELECT diag('Deleting data for: mimeo_source.logdel_test_source2');
SELECT dblink_exec('mimeo_test', 'DELETE FROM mimeo_source.logdel_test_source2 WHERE col1 between 12500 and 12520');
SELECT diag('Updating data for: mimeo_source.logdel_test_source_condition');
SELECT dblink_exec('mimeo_test', 'UPDATE mimeo_source.logdel_test_source_condition SET col2 = ''changed''||col1 WHERE col1 > 18000');


SELECT dblink_disconnect('mimeo_test');
--SELECT is_empty('SELECT dblink_get_connections() @> ''{mimeo_test}''', 'Close remote database connection');

SELECT pass('Completed 2nd batch of data inserts/updates/deletes for remote tables. Sleeping for 10 seconds to ensure gap for incremental tests...');
SELECT pg_sleep(10);

SELECT * FROM finish();
