\set ON_ERROR_ROLLBACK 1
\set ON_ERROR_STOP true

SELECT set_config('search_path','mimeo, dblink, public',false);

SELECT plan(2);

SELECT dblink_connect('mimeo_test', 'host=localhost port=5432 dbname=mimeo_source user=mimeo_owner password=mimeo_owner');
SELECT is(dblink_get_connections() @> '{mimeo_test}', 't', 'Remote database connection established');

-- Change column on snap table to ensure the change propagates and permissions are kept
SELECT diag('Changing columns for snap test');
SELECT dblink_exec('mimeo_test', 'ALTER TABLE mimeo_source.snap_test_source_change_col DROP COLUMN col2');
SELECT dblink_exec('mimeo_test', 'ALTER TABLE mimeo_source.snap_test_source_change_col ADD COLUMN col4 bigint');
SELECT dblink_exec('mimeo_test', 'ALTER TABLE mimeo_source."Snap-test-Source" ADD COLUMN "Col4" inet');
-- Add filter for Table refresh to avoid new column issues
UPDATE refresh_config_table SET filter = '{"primary", "col2", "COL-3"}' WHERE dest_table = 'mimeo_dest.Table-test-Source';
SELECT dblink_exec('mimeo_test', 'INSERT INTO mimeo_source.snap_test_source_change_col (col1, col4) VALUES (generate_series(20001,100000), generate_series(20001,100000))');

-- Insert new data
SELECT diag('Inserting more data for: mimeo_source.snap_test_source');
SELECT dblink_exec('mimeo_test', 'INSERT INTO mimeo_source.snap_test_source VALUES (generate_series(20001,100000), ''test''||generate_series(20001,100000)::text)');
SELECT diag('Inserting more data for: mimeo_source.Snap-test-Source');
SELECT dblink_exec('mimeo_test', 'INSERT INTO mimeo_source."Snap-test-Source" VALUES (generate_series(20001,100000), ''test''||generate_series(20001,100000)::text)');

SELECT diag('Inserting more data for: mimeo_source.inserter_test_source');
SELECT dblink_exec('mimeo_test', 'INSERT INTO mimeo_source.inserter_test_source VALUES (generate_series(20001,100000), ''test''||generate_series(20001,100000)::text)');
SELECT diag('Inserting more data for: mimeo_source."Inserter-Test-Source"');
SELECT dblink_exec('mimeo_test', 'INSERT INTO mimeo_source."Inserter-Test-Source" VALUES (generate_series(20001,100000), ''test''||generate_series(20001,100000)::text)');

SELECT diag('Inserting more data for: mimeo_source.updater_test_source');
SELECT dblink_exec('mimeo_test', 'INSERT INTO mimeo_source.updater_test_source VALUES (generate_series(20001,100000), ''test''||generate_series(20001,100000)::text)');
SELECT diag('Inserting more data for: mimeo_source."Updater-Test-Source"');
SELECT dblink_exec('mimeo_test', 'INSERT INTO mimeo_source."Updater-Test-Source" VALUES (generate_series(20001,100000), ''test''||generate_series(20001,100000)::text)');

SELECT diag('Inserting more data for: mimeo_source.dml_test_source');
SELECT dblink_exec('mimeo_test', 'INSERT INTO mimeo_source.dml_test_source VALUES (generate_series(20001,100000), ''test''||generate_series(20001,100000)::text)');
SELECT diag('Inserting more data for: mimeo_source.dml_test_source2');
SELECT dblink_exec('mimeo_test', 'INSERT INTO mimeo_source.dml_test_source2 VALUES (generate_series(20001,100000), ''test''||generate_series(20001,100000)::text)');
SELECT diag('Inserting more data for: mimeo_source.dml_test_source_nodata');
SELECT dblink_exec('mimeo_test', 'INSERT INTO mimeo_source.dml_test_source_nodata VALUES (generate_series(20001,100000), ''test''||generate_series(20001,100000)::text)');
SELECT diag('Inserting more data for: mimeo_source.dml_test_source_filter');
SELECT dblink_exec('mimeo_test', 'INSERT INTO mimeo_source.dml_test_source_filter VALUES (generate_series(20001,100000), ''test''||generate_series(20001,100000)::text)');
SELECT diag('Inserting more data for: mimeo_source.dml_test_source_condition');
SELECT dblink_exec('mimeo_test', 'INSERT INTO mimeo_source.dml_test_source_condition VALUES (generate_series(20001,100000), ''test''||generate_series(20001,100000)::text)');
SELECT diag('Inserting more data for: mimeo_source."Dml-Test-Source"');
SELECT dblink_exec('mimeo_test', 'INSERT INTO mimeo_source."Dml-Test-Source" VALUES (generate_series(20001,100000), ''test''||generate_series(20001,100000)::text)');

SELECT diag('Inserting more data for: mimeo_source.logdel_test_source');
SELECT dblink_exec('mimeo_test', 'INSERT INTO mimeo_source.logdel_test_source VALUES (generate_series(20001,100000), ''test''||generate_series(20001,100000)::text)');
SELECT diag('Inserting more data for: mimeo_source.logdel_test_source2');
SELECT dblink_exec('mimeo_test', 'INSERT INTO mimeo_source.logdel_test_source2 VALUES (generate_series(20001,100000), ''test''||generate_series(20001,100000)::text)');
SELECT diag('Inserting more data for: mimeo_source.logdel_test_source_nodata');
SELECT dblink_exec('mimeo_test', 'INSERT INTO mimeo_source.logdel_test_source_nodata VALUES (generate_series(20001,100000), ''test''||generate_series(20001,100000)::text)');
SELECT diag('Inserting more data for: mimeo_source.logdel_test_source_filter');
SELECT dblink_exec('mimeo_test', 'INSERT INTO mimeo_source.logdel_test_source_filter VALUES (generate_series(20001,100000), ''test''||generate_series(20001,100000)::text)');
SELECT diag('Inserting more data for: mimeo_source.logdel_test_source_condition');
SELECT dblink_exec('mimeo_test', 'INSERT INTO mimeo_source.logdel_test_source_condition VALUES (generate_series(20001,100000), ''test''||generate_series(20001,100000)::text)');
SELECT diag('Inserting more data for: mimeo_source."LogDel-Test-Source"');
SELECT dblink_exec('mimeo_test', 'INSERT INTO mimeo_source."LogDel-Test-Source" VALUES (generate_series(20001,100000), ''test''||generate_series(20001,100000)::text)');

-- Data for testing updater
SELECT diag('Updating data for: mimeo_source.updater_test_source');
SELECT dblink_exec('mimeo_test', 'UPDATE mimeo_source.updater_test_source SET col2 = ''changed'', col3 = clock_timestamp(), col4 = nextval(''mimeo_source.updater_test_source_col4_seq'') WHERE col1 between 25000 and 30000');

-- Data for testing dml
SELECT diag('Updating data for: mimeo_source.dml_test_source2');
SELECT dblink_exec('mimeo_test', 'UPDATE mimeo_source.dml_test_source2 SET col2 = ''changed'' WHERE col1 between 35000 and 41000');
SELECT diag('Deleting data for: mimeo_source.dml_test_source2');
SELECT dblink_exec('mimeo_test', 'DELETE FROM mimeo_source.dml_test_source2 WHERE col1 between 45000 and 46000');
SELECT diag('Updating data for: mimeo_source.dml_test_source_condition');
SELECT dblink_exec('mimeo_test', 'UPDATE mimeo_source.dml_test_source_condition SET col2 = ''changed''||col1 WHERE col1 > 95000');
SELECT diag('Deleting data for: mimeo_source.dml_test_source_condition');
SELECT dblink_exec('mimeo_test', 'DELETE FROM mimeo_source.dml_test_source_condition WHERE col1 <= 30000');

-- Data for testing logdel
SELECT diag('Updating data for: mimeo_source.logdel_test_source2');
SELECT dblink_exec('mimeo_test', 'UPDATE mimeo_source.logdel_test_source2 SET col2 = ''changed'' WHERE col1 between 36000 and 42000');
SELECT diag('Deleting data for: mimeo_source.logdel_test_source2');
SELECT dblink_exec('mimeo_test', 'DELETE FROM mimeo_source.logdel_test_source2 WHERE col1 between 45500 and 45520');
SELECT diag('Updating data for: mimeo_source.logdel_test_source_condition');
SELECT dblink_exec('mimeo_test', 'UPDATE mimeo_source.logdel_test_source_condition SET col2 = ''changed''||col1 WHERE col1 > 18000');


SELECT dblink_disconnect('mimeo_test');
--SELECT is_empty('SELECT dblink_get_connections() @> ''{mimeo_test}''', 'Close remote database connection');

SELECT pass('Completed 3rd batch of data inserts/updates/deletes for remote tables. Sleeping for 10 seconds to ensure gap for incremental tests...');
SELECT pg_sleep(10);

SELECT * FROM finish();
