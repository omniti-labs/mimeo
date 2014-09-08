\set ON_ERROR_ROLLBACK 1
\set ON_ERROR_STOP true

-- This should be the last batch of tests since I don't feel like resetting the batch limits for any to come after them

SELECT set_config('search_path','mimeo, dblink, public',false);

SELECT plan(37);

SELECT dblink_connect('mimeo_test', 'host=localhost port=5432 dbname=mimeo_source user=mimeo_owner password=mimeo_owner');
SELECT is(dblink_get_connections() @> '{mimeo_test}', 't', 'Remote database connection established');

UPDATE refresh_config_inserter SET batch_limit = 500 WHERE dest_table = 'mimeo_source.inserter_test_source';
UPDATE refresh_config_inserter SET batch_limit = 500 WHERE dest_table = 'mimeo_source.Inserter-Test-Source';
UPDATE refresh_config_inserter SET batch_limit = 500 WHERE dest_table = 'mimeo_dest.inserter_test_dest_serial';
UPDATE refresh_config_inserter SET batch_limit = 500 WHERE dest_table = 'mimeo_dest.Inserter-Test-Source_Serial';
UPDATE refresh_config_updater SET batch_limit = 500 WHERE dest_table = 'mimeo_source.updater_test_source';
UPDATE refresh_config_updater SET batch_limit = 500 WHERE dest_table = 'mimeo_source.Updater-Test-Source';
UPDATE refresh_config_updater SET batch_limit = 500 WHERE dest_table = 'mimeo_dest.updater_test_dest_serial';
UPDATE refresh_config_updater SET batch_limit = 500 WHERE dest_table = 'mimeo_dest.Updater-Test-Source_Serial';
UPDATE refresh_config_dml SET batch_limit = 500 WHERE dest_table = 'mimeo_source.dml_test_source';
UPDATE refresh_config_dml SET batch_limit = 500 WHERE dest_table = 'mimeo_source.Dml-Test-Source';
UPDATE refresh_config_logdel SET batch_limit = 500 WHERE dest_table = 'mimeo_source.logdel_test_source';
UPDATE refresh_config_logdel SET batch_limit = 500 WHERE dest_table = 'mimeo_source.LogDel-Test-Source';

SELECT results_eq ('SELECT batch_limit FROM refresh_config_inserter WHERE dest_table = ''mimeo_source.inserter_test_source''', ARRAY[500], 
    'Check that batch_limit got set for time based inserter');
SELECT results_eq ('SELECT batch_limit FROM refresh_config_inserter WHERE dest_table = ''mimeo_source.Inserter-Test-Source''', ARRAY[500], 
    'Check that batch_limit got set for time based inserter');
SELECT results_eq ('SELECT batch_limit FROM refresh_config_inserter WHERE dest_table = ''mimeo_dest.inserter_test_dest_serial''', ARRAY[500], 
    'Check that batch_limit got set for serial based inserter');
SELECT results_eq ('SELECT batch_limit FROM refresh_config_inserter WHERE dest_table = ''mimeo_dest.Inserter-Test-Source_Serial''', ARRAY[500], 
    'Check that batch_limit got set for serial based inserter');
SELECT results_eq ('SELECT batch_limit FROM refresh_config_updater WHERE dest_table = ''mimeo_source.updater_test_source''', ARRAY[500], 
    'Check that batch_limit got set for updater');
SELECT results_eq ('SELECT batch_limit FROM refresh_config_updater WHERE dest_table = ''mimeo_source.Updater-Test-Source''', ARRAY[500], 
    'Check that batch_limit got set for updater');
SELECT results_eq ('SELECT batch_limit FROM refresh_config_updater WHERE dest_table = ''mimeo_dest.updater_test_dest_serial''', ARRAY[500], 
    'Check that batch_limit got set for serial based updater');
SELECT results_eq ('SELECT batch_limit FROM refresh_config_updater WHERE dest_table = ''mimeo_dest.Updater-Test-Source_Serial''', ARRAY[500], 
    'Check that batch_limit got set for serial based updater');
SELECT results_eq ('SELECT batch_limit FROM refresh_config_dml WHERE dest_table = ''mimeo_source.dml_test_source''', ARRAY[500], 
    'Check that batch_limit got set for dml');
SELECT results_eq ('SELECT batch_limit FROM refresh_config_dml WHERE dest_table = ''mimeo_source.Dml-Test-Source''', ARRAY[500], 
    'Check that batch_limit got set for dml');
SELECT results_eq ('SELECT batch_limit FROM refresh_config_logdel WHERE dest_table = ''mimeo_source.logdel_test_source''', ARRAY[500], 
    'Check that batch_limit got set for logdel');
SELECT results_eq ('SELECT batch_limit FROM refresh_config_logdel WHERE dest_table = ''mimeo_source.LogDel-Test-Source''', ARRAY[500], 
    'Check that batch_limit got set for logdel');

SELECT diag('Inserting more data for: mimeo_source.inserter_test_source');
SELECT dblink_exec('mimeo_test', 'INSERT INTO mimeo_source.inserter_test_source (col1, col2, col3) VALUES (generate_series(100031,110000), ''test''||generate_series(100031,110000)::text, now())');
SELECT diag('Inserting more data for: mimeo_source.Inserter-Test-Source');
SELECT dblink_exec('mimeo_test', 'INSERT INTO mimeo_source."Inserter-Test-Source" (col1, "group", "Col-3") VALUES (generate_series(100031,110000), ''test''||generate_series(100031,110000)::text, now())');
SELECT diag('Inserting more data for: mimeo_source.updater_test_source');
SELECT dblink_exec('mimeo_test', 'INSERT INTO mimeo_source.updater_test_source (col1, col2, col3) VALUES (generate_series(100031,110000), ''test''||generate_series(100031,110000)::text, now())');
SELECT diag('Inserting more data for: mimeo_source.Updater-Test-Source');
SELECT dblink_exec('mimeo_test', 'INSERT INTO mimeo_source."Updater-Test-Source" ("COL-1", "group", "Col3") VALUES (generate_series(100031,110000), ''test''||generate_series(100031,110000)::text, now())');
SELECT diag('Inserting more data for: mimeo_source.dml_test_source');
SELECT dblink_exec('mimeo_test', 'INSERT INTO mimeo_source.dml_test_source (col1, col2, col3) VALUES (generate_series(100001,110000), ''test''||generate_series(100001,110000)::text, now())');
SELECT diag('Inserting more data for: mimeo_source.Dml-Test-Source');
SELECT dblink_exec('mimeo_test', 'INSERT INTO mimeo_source."Dml-Test-Source" ("COL1", "group", "Col-3") VALUES (generate_series(100001,110000), ''test''||generate_series(100001,110000)::text, now())');
SELECT diag('Inserting more data for: mimeo_source.logdel_test_source');
SELECT dblink_exec('mimeo_test', 'INSERT INTO mimeo_source.logdel_test_source (col1, col2, col3) VALUES (generate_series(100001,110000), ''test''||generate_series(100001,110000)::text, now())');
SELECT diag('Inserting more data for: mimeo_source."LogDel-Test-Source"');
SELECT dblink_exec('mimeo_test', 'INSERT INTO mimeo_source."LogDel-Test-Source" ("COL1", "group", "Col-3") VALUES (generate_series(100001,110000), ''test''||generate_series(100001,110000)::text, now())');

SELECT diag('Sleeping for 10 seconds to ensure gap for incremental tests...');
SELECT pg_sleep(10);

SELECT diag('Running batch limit refresh for: mimeo_source.inserter_test_source');
SELECT refresh_inserter('mimeo_source.inserter_test_source');
SELECT diag('Running batch limit refresh for: mimeo_source.Inserter-Test-Source');
SELECT refresh_inserter('mimeo_source.Inserter-Test-Source');
SELECT diag('Running batch limit refresh for: mimeo_dest.inserter_test_dest_serial');
SELECT refresh_inserter('mimeo_dest.inserter_test_dest_serial');
SELECT diag('Running batch limit refresh for: mimeo_dest.Inserter-Test-Source_Serial');
SELECT refresh_inserter('mimeo_dest.Inserter-Test-Source_Serial');
SELECT diag('Running batch limit refresh for: mimeo_source.updater_test_source');
SELECT refresh_updater('mimeo_source.updater_test_source');
SELECT diag('Running batch limit refresh for: mimeo_source.Updater-Test-Source');
SELECT refresh_updater('mimeo_source.Updater-Test-Source');
SELECT diag('Running batch limit refresh for: mimeo_dest.updater_test_dest_serial');
SELECT refresh_updater('mimeo_dest.updater_test_dest_serial');
SELECT diag('Running batch limit refresh for: mimeo_dest.Updater-Test-Source_Serial');
SELECT refresh_updater('mimeo_dest.Updater-Test-Source_Serial');
SELECT diag('Running batch limit refresh for: mimeo_source.dml_test_source');
SELECT refresh_dml('mimeo_source.dml_test_source');
SELECT diag('Running batch limit refresh for: mimeo_source.Dml-Test-Source');
SELECT refresh_dml('mimeo_source.Dml-Test-Source');
SELECT diag('Running batch limit refresh for: mimeo_source.logdel_test_source');
SELECT refresh_logdel('mimeo_source.logdel_test_source');
SELECT diag('Running batch limit refresh for: mimeo_source.LogDel-Test-Source');
SELECT refresh_logdel('mimeo_source.LogDel-Test-Source');
-- #### Time based INSERTER & UPDATER should have gotten no rows due to all new column values having the same timestamp
-- Make sure to exclude the repull test rows from the destination query since they have an odd timestamp value
SELECT results_eq('SELECT col1, col2, col3 FROM mimeo_source.inserter_test_source WHERE col1 <= 100000 ORDER BY col1 ASC',
    'SELECT * FROM dblink(''mimeo_test'', ''SELECT col1, col2, col3 FROM mimeo_source.inserter_test_source WHERE col1 <= 100000 ORDER BY col1 ASC'') t (col1 int, col2 text, col3 timestamptz)',
    'Check data for: mimeo_source.inserter_test_source');

SELECT results_eq('SELECT col1, "group", "Col-3" FROM mimeo_source."Inserter-Test-Source" WHERE col1 <= 100000 ORDER BY col1 ASC',
    'SELECT * FROM dblink(''mimeo_test'', ''SELECT col1, "group", "Col-3" FROM mimeo_source."Inserter-Test-Source" WHERE col1 <= 100000 ORDER BY col1 ASC'') t (col1 int, "group" text, "Col-3" timestamptz)',
    'Check data for: mimeo_source."Inserter-Test-Source"');

SELECT results_eq('SELECT col1, col2, col3 FROM mimeo_source.updater_test_source WHERE col1 <= 100000 ORDER BY col1 ASC',
    'SELECT * FROM dblink(''mimeo_test'', ''SELECT col1, col2, col3 FROM mimeo_source.updater_test_source WHERE col1 <= 100000 ORDER BY col1 ASC'') t (col1 int, col2 text, col3 timestamptz)',
    'Check data for: mimeo_source.updater_test_source');

SELECT results_eq('SELECT "COL-1", "group", "Col3" FROM mimeo_source."Updater-Test-Source" WHERE "COL-1" <= 100000 ORDER BY "COL-1" ASC',
    'SELECT * FROM dblink(''mimeo_test'', ''SELECT "COL-1", "group", "Col3" FROM mimeo_source."Updater-Test-Source" WHERE "COL-1" <= 100000 ORDER BY "COL-1" ASC'') t ("COL-1" int, "group" text, "Col3" timestamptz)',
    'Check data for: mimeo_source."Updater-Test-Source"');

-- #### Serial based INSERTER & UPDATE and DML & LOGDEL should have worked fine and only gotten 500 rows. Should be warning in jobmon log, but don't need to test for that here
-- Batch is from 99999 to 100499 and one value is removed from high boundary
SELECT results_eq('SELECT col1, col2, col3 FROM mimeo_dest.inserter_test_dest_serial ORDER BY col1 ASC',
    'SELECT * FROM dblink(''mimeo_test'', ''SELECT col1, col2, col3 FROM mimeo_source.inserter_test_source WHERE col1 < 100499 ORDER BY col1 ASC'') t (col1 int, col2 text, col3 timestamptz)',
    'Check data for: mimeo_dest.inserter_test_dest_serial');

-- Batch is from 99999 to 100499 and one value is removed from high boundary
SELECT results_eq('SELECT col1, "group", "Col-3" FROM mimeo_dest."Inserter-Test-Source_Serial" ORDER BY col1 ASC',
    'SELECT * FROM dblink(''mimeo_test'', ''SELECT col1, "group", "Col-3" FROM mimeo_source."Inserter-Test-Source" WHERE col1 < 100499 ORDER BY col1 ASC'') t (col1 int, "group" text, "Col-3" timestamptz)',
    'Check data for: mimeo_dest."Inserter-Test-Source_Serial"');

-- Batch is from 99999 to 100499 and one value is removed from high boundary
SELECT results_eq('SELECT col1, col2, col3, col4 FROM mimeo_dest.updater_test_dest_serial ORDER BY col1 ASC',
    'SELECT * FROM dblink(''mimeo_test'', ''SELECT col1, col2, col3, col4 FROM mimeo_source.updater_test_source WHERE col1 < 100499 ORDER BY col1 ASC'') t (col1 int, col2 text, col3 timestamptz, col4 int)',
    'Check data for: mimeo_dest.updater_test_dest_serial');

-- Batch is from 99999 to 100499 and one value is removed from high boundary
SELECT results_eq('SELECT "COL-1", "group", "Col3" FROM mimeo_dest."Updater-Test-Source_Serial" ORDER BY "COL-1" ASC',
    'SELECT * FROM dblink(''mimeo_test'', ''SELECT "COL-1", "group", "Col3" FROM mimeo_source."Updater-Test-Source" WHERE "COL-1" < 100499 ORDER BY "COL-1" ASC'') t ("COL-1" int, "group" text, "Col3" timestamptz)',
    'Check data for: mimeo_source."Updater-Test-Source_Serial"');

SELECT results_eq('SELECT col1, col2, col3 FROM mimeo_source.dml_test_source ORDER BY col1 ASC',
    'SELECT * FROM dblink(''mimeo_test'', ''SELECT col1, col2, col3 FROM mimeo_source.dml_test_source WHERE col1 <= 100500 ORDER BY col1 ASC'') t (col1 int, col2 text, col3 timestamptz)',
    'Check data for: mimeo_source.dml_test_source');

SELECT results_eq('SELECT "COL1", "group", "Col-3" FROM mimeo_source."Dml-Test-Source" ORDER BY "COL1" ASC',
    'SELECT * FROM dblink(''mimeo_test'', ''SELECT "COL1", "group", "Col-3" FROM mimeo_source."Dml-Test-Source" WHERE "COL1" <= 100500 ORDER BY "COL1" ASC'') t ("COL1" int, "group" text, "Col-3" timestamptz)',
    'Check data for: mimeo_source.Dml-Test-Source');

SELECT results_eq('SELECT col1, col2, col3 FROM mimeo_source.logdel_test_source ORDER BY col1 ASC',
    'SELECT * FROM dblink(''mimeo_test'', ''SELECT col1, col2, col3 FROM mimeo_source.logdel_test_source WHERE col1 <= 100500 ORDER BY col1 ASC'') t (col1 int, col2 text, col3 timestamptz)',
    'Check data for: mimeo_source.logdel_test_source');

SELECT results_eq('SELECT "COL1", "group", "Col-3" FROM mimeo_source."LogDel-Test-Source" ORDER BY "COL1" ASC',
    'SELECT * FROM dblink(''mimeo_test'', ''SELECT "COL1", "group", "Col-3" FROM mimeo_source."LogDel-Test-Source" WHERE "COL1" <= 100500 ORDER BY "COL1" ASC'') t ("COL1" int, "group" text, "Col-3" timestamptz)',
    'Check data for: mimeo_source.LogDel-Test-Source');

SELECT diag('Running full data refresh for: mimeo_source.inserter_test_source');
SELECT refresh_inserter('mimeo_source.inserter_test_source', p_limit := 20000);
SELECT diag('Running full data refresh for: mimeo_source.Inserter-Test-Source');
SELECT refresh_inserter('mimeo_source.Inserter-Test-Source', p_limit := 20000);
SELECT diag('Running full data refresh for: mimeo_dest.inserter_test_dest_serial');
SELECT refresh_inserter('mimeo_dest.inserter_test_dest_serial', p_limit := 20000);
SELECT diag('Running full data refresh for: mimeo_dest.Inserter-Test-Source_Serial');
SELECT refresh_inserter('mimeo_dest.Inserter-Test-Source_Serial', p_limit := 20000);
SELECT diag('Running full data refresh for: mimeo_source.updater_test_source');
SELECT refresh_updater('mimeo_source.updater_test_source', p_limit := 20000);
SELECT diag('Running full data refresh for: mimeo_source.Updater-Test-Source');
SELECT refresh_updater('mimeo_source.Updater-Test-Source', p_limit := 20000);
SELECT diag('Running full data refresh for: mimeo_dest.updater_test_dest_serial');
SELECT refresh_updater('mimeo_dest.updater_test_dest_serial', p_limit := 20000);
SELECT diag('Running full data refresh for: mimeo_dest.Updater-Test-Source_Serial');
SELECT refresh_updater('mimeo_dest.Updater-Test-Source_Serial', p_limit := 20000);
SELECT diag('Running full data refresh for: mimeo_source.dml_test_source');
SELECT refresh_dml('mimeo_source.dml_test_source', p_limit := 20000);
SELECT diag('Running full data refresh for: mimeo_source.Dml-Test-Source');
SELECT refresh_dml('mimeo_source.Dml-Test-Source', p_limit := 20000);
SELECT diag('Running full data refresh for: mimeo_source.logdel_test_source');
SELECT refresh_logdel('mimeo_source.logdel_test_source', p_limit := 20000);
SELECT diag('Running full data refresh for: mimeo_source.LogDel-Test-Source');
SELECT refresh_logdel('mimeo_source.LogDel-Test-Source', p_limit := 20000);

-- #### Should now have all rows
SELECT results_eq('SELECT col1, col2, col3 FROM mimeo_source.inserter_test_source ORDER BY col1 ASC',
    'SELECT * FROM dblink(''mimeo_test'', ''SELECT col1, col2, col3 FROM mimeo_source.inserter_test_source ORDER BY col1 ASC'') t (col1 int, col2 text, col3 timestamptz)',
    'Check data for: mimeo_source.inserter_test_source');

SELECT results_eq('SELECT col1, "group", "Col-3" FROM mimeo_source."Inserter-Test-Source" ORDER BY col1 ASC',
    'SELECT * FROM dblink(''mimeo_test'', ''SELECT col1, "group", "Col-3" FROM mimeo_source."Inserter-Test-Source" ORDER BY col1 ASC'') t (col1 int, "group" text, "Col-3" timestamptz)',
    'Check data for: mimeo_source."Inserter-Test-Source"');

SELECT results_eq('SELECT col1, col2, col3 FROM mimeo_dest.inserter_test_dest_serial ORDER BY col1 ASC',
    'SELECT * FROM dblink(''mimeo_test'', ''SELECT col1, col2, col3 FROM mimeo_source.inserter_test_source WHERE col1 < (SELECT max(col1) FROM mimeo_source.inserter_test_source) ORDER BY col1 ASC'') t (col1 int, col2 text, col3 timestamptz)',
    'Check data for: mimeo_dest.inserter_test_dest_serial');

SELECT results_eq('SELECT col1, "group", "Col-3" FROM mimeo_dest."Inserter-Test-Source_Serial" ORDER BY col1 ASC',
    'SELECT * FROM dblink(''mimeo_test'', ''SELECT col1, "group", "Col-3" FROM mimeo_source."Inserter-Test-Source" WHERE col1 < (SELECT max(col1) FROM mimeo_source."Inserter-Test-Source") ORDER BY col1 ASC'') t (col1 int, "group" text, "Col-3" timestamptz)',
    'Check data for: mimeo_dest."Inserter-Test-Source_Serial"');

SELECT results_eq('SELECT col1, col2, col3 FROM mimeo_source.updater_test_source ORDER BY col1 ASC',
    'SELECT * FROM dblink(''mimeo_test'', ''SELECT col1, col2, col3 FROM mimeo_source.updater_test_source ORDER BY col1 ASC'') t (col1 int, col2 text, col3 timestamptz)',
    'Check data for: mimeo_source.updater_test_source');

SELECT results_eq('SELECT "COL-1", "group", "Col3" FROM mimeo_source."Updater-Test-Source" ORDER BY "COL-1" ASC',
    'SELECT * FROM dblink(''mimeo_test'', ''SELECT "COL-1", "group", "Col3" FROM mimeo_source."Updater-Test-Source" ORDER BY "COL-1" ASC'') t ("COL-1" int, "group" text, "Col3" timestamptz)',
    'Check data for: mimeo_source."Updater-Test-Source"');

SELECT results_eq('SELECT col1, col2, col3, col4 FROM mimeo_dest.updater_test_dest_serial ORDER BY col4 ASC',
    'SELECT * FROM dblink(''mimeo_test'', ''SELECT col1, col2, col3, col4 FROM mimeo_source.updater_test_source WHERE col4 < (SELECT max(col4) FROM mimeo_source.updater_test_source) ORDER BY col4 ASC'') t (col1 int, col2 text, col3 timestamptz, col4 int)',
    'Check data for: mimeo_dest.updater_test_dest_serial');

SELECT results_eq('SELECT "COL-1", "group", "Col3" FROM mimeo_dest."Updater-Test-Source_Serial" ORDER BY "COL-1" ASC',
    'SELECT * FROM dblink(''mimeo_test'', ''SELECT "COL-1", "group", "Col3" FROM mimeo_source."Updater-Test-Source" WHERE "COL-1" < (SELECT max("COL-1") FROM mimeo_source."Updater-Test-Source") ORDER BY "COL-1" ASC'') t ("COL-1" int, "group" text, "Col3" timestamptz)',
    'Check data for: mimeo_source."Updater-Test-Source_Serial"');

SELECT results_eq('SELECT col1, col2, col3 FROM mimeo_source.dml_test_source ORDER BY col1 ASC',
    'SELECT * FROM dblink(''mimeo_test'', ''SELECT col1, col2, col3 FROM mimeo_source.dml_test_source ORDER BY col1 ASC'') t (col1 int, col2 text, col3 timestamptz)',
    'Check data for: mimeo_source.dml_test_source');

SELECT results_eq('SELECT "COL1", "group", "Col-3" FROM mimeo_source."Dml-Test-Source" ORDER BY "COL1" ASC',
    'SELECT * FROM dblink(''mimeo_test'', ''SELECT "COL1", "group", "Col-3" FROM mimeo_source."Dml-Test-Source" ORDER BY "COL1" ASC'') t ("COL1" int, "group" text, "Col-3" timestamptz)',
    'Check data for: mimeo_source.Dml-Test-Source');

SELECT results_eq('SELECT col1, col2, col3 FROM mimeo_source.logdel_test_source ORDER BY col1 ASC',
    'SELECT * FROM dblink(''mimeo_test'', ''SELECT col1, col2, col3 FROM mimeo_source.logdel_test_source ORDER BY col1 ASC'') t (col1 int, col2 text, col3 timestamptz)',
    'Check data for: mimeo_source.logdel_test_source');

SELECT results_eq('SELECT "COL1", "group", "Col-3" FROM mimeo_source."LogDel-Test-Source" ORDER BY "COL1" ASC',
    'SELECT * FROM dblink(''mimeo_test'', ''SELECT "COL1", "group", "Col-3" FROM mimeo_source."LogDel-Test-Source" ORDER BY "COL1" ASC'') t ("COL1" int, "group" text, "Col-3" timestamptz)',
    'Check data for: mimeo_source.LogDel-Test-Source');

SELECT dblink_disconnect('mimeo_test');
--SELECT is_empty('SELECT dblink_get_connections() @> ''{mimeo_test}''', 'Close remote database connection');

SELECT * FROM finish();
