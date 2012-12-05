SELECT set_config('search_path','mimeo, dblink, tap',false);

SELECT plan(2);

-- Setup remote tables for replication testing
SELECT dblink_connect('mimeo_test', 'host=localhost port=5432 dbname=mimeo_source user=mimeo_test password=mimeo_test');
SELECT is(dblink_get_connections() @> '{mimeo_test}', 't', 'Remote database connection established');

SELECT dblink_exec('mimeo_test', 'CREATE SCHEMA mimeo_source');
SELECT dblink_exec('mimeo_test', 'CREATE SCHEMA mimeo');

SELECT dblink_exec('mimeo_test', 'CREATE TABLE mimeo_source.snap_test_source (
    col1 int,
    col2 text,
    col3 timestamptz DEFAULT clock_timestamp())');
SELECT dblink_exec('mimeo_test', 'INSERT INTO mimeo_source.snap_test_source VALUES (generate_series(1,10), ''test''||generate_series(1,10)::text)');
SELECT dblink_exec('mimeo_test', 'CREATE INDEX ON mimeo_source.snap_test_source (col2)');

SELECT dblink_exec('mimeo_test', 'CREATE TABLE mimeo_source.inserter_test_source (
    col1 int PRIMARY KEY,
    col2 text,
    col3 timestamptz DEFAULT clock_timestamp())');
SELECT dblink_exec('mimeo_test', 'INSERT INTO mimeo_source.inserter_test_source VALUES (generate_series(1,10), ''test''||generate_series(1,10)::text)');
SELECT dblink_exec('mimeo_test', 'CREATE INDEX ON mimeo_source.inserter_test_source (col2)');

SELECT dblink_exec('mimeo_test', 'CREATE TABLE mimeo_source.updater_test_source (
    col1 int PRIMARY KEY,
    col2 text,
    col3 timestamptz DEFAULT clock_timestamp())');
SELECT dblink_exec('mimeo_test', 'INSERT INTO mimeo_source.updater_test_source VALUES (generate_series(1,10), ''test''||generate_series(1,10)::text)');
SELECT dblink_exec('mimeo_test', 'CREATE INDEX ON mimeo_source.updater_test_source (col2)');

-- Must do separate tables due to queue table needing to be distinct
SELECT dblink_exec('mimeo_test', 'CREATE TABLE mimeo_source.dml_test_source (
    col1 int PRIMARY KEY,
    col2 text,
    col3 timestamptz DEFAULT clock_timestamp())');
SELECT dblink_exec('mimeo_test', 'CREATE INDEX ON mimeo_source.dml_test_source (col2)');
SELECT dblink_exec('mimeo_test', 'INSERT INTO mimeo_source.dml_test_source VALUES (generate_series(1,10), ''test''||generate_series(1,10)::text)');
SELECT dblink_exec('mimeo_test', 'CREATE TABLE mimeo_source.dml_test_source2 (
    col1 int,
    col2 text,
    col3 timestamptz DEFAULT clock_timestamp(),
    PRIMARY KEY (col2, col1) )');
SELECT dblink_exec('mimeo_test', 'INSERT INTO mimeo_source.dml_test_source2 VALUES (generate_series(1,10), ''test''||generate_series(1,10)::text)');
SELECT dblink_exec('mimeo_test', 'CREATE TABLE mimeo_source.dml_test_source_nodata (
    col1 int UNIQUE NOT NULL,
    col2 text,
    col3 timestamptz DEFAULT clock_timestamp() )');
SELECT dblink_exec('mimeo_test', 'INSERT INTO mimeo_source.dml_test_source_nodata VALUES (generate_series(1,10), ''test''||generate_series(1,10)::text)');
SELECT dblink_exec('mimeo_test', 'CREATE TABLE mimeo_source.dml_test_source_filter (
    col1 int UNIQUE NOT NULL,
    col2 text,
    col3 timestamptz DEFAULT clock_timestamp() )');
SELECT dblink_exec('mimeo_test', 'INSERT INTO mimeo_source.dml_test_source_filter VALUES (generate_series(1,10), ''test''||generate_series(1,10)::text)');
SELECT dblink_exec('mimeo_test', 'CREATE TABLE mimeo_source.dml_test_source_condition (
    col1 int PRIMARY KEY,
    col2 text UNIQUE NOT NULL,
    col3 timestamptz DEFAULT clock_timestamp() )');
SELECT dblink_exec('mimeo_test', 'INSERT INTO mimeo_source.dml_test_source_condition VALUES (generate_series(1,10), ''test''||generate_series(1,10)::text)');

-- Must do separate tables due to queue table needing to be distinct
SELECT dblink_exec('mimeo_test', 'CREATE TABLE mimeo_source.logdel_test_source (
    col1 int PRIMARY KEY,
    col2 text,
    col3 timestamptz DEFAULT clock_timestamp())');
SELECT dblink_exec('mimeo_test', 'INSERT INTO mimeo_source.logdel_test_source VALUES (generate_series(1,10), ''test''||generate_series(1,10)::text)');
SELECT dblink_exec('mimeo_test', 'CREATE INDEX ON mimeo_source.logdel_test_source (col2)');
SELECT dblink_exec('mimeo_test', 'CREATE TABLE mimeo_source.logdel_test_source2 (
    col1 int,
    col2 text,
    col3 timestamptz DEFAULT clock_timestamp(),
    PRIMARY KEY (col2, col1) )');
SELECT dblink_exec('mimeo_test', 'INSERT INTO mimeo_source.logdel_test_source2 VALUES (generate_series(1,10), ''test''||generate_series(1,10)::text)');
SELECT dblink_exec('mimeo_test', 'CREATE TABLE mimeo_source.logdel_test_source_nodata (
    col1 int UNIQUE NOT NULL,
    col2 text UNIQUE NOT NULL,
    col3 timestamptz DEFAULT clock_timestamp() )');
SELECT dblink_exec('mimeo_test', 'INSERT INTO mimeo_source.logdel_test_source_nodata VALUES (generate_series(1,10), ''test''||generate_series(1,10)::text)');
SELECT dblink_exec('mimeo_test', 'CREATE TABLE mimeo_source.logdel_test_source_filter (
    col1 int UNIQUE NOT NULL,
    col2 text UNIQUE NOT NULL,
    col3 timestamptz DEFAULT clock_timestamp() )');
SELECT dblink_exec('mimeo_test', 'INSERT INTO mimeo_source.logdel_test_source_filter VALUES (generate_series(1,10), ''test''||generate_series(1,10)::text)');
SELECT dblink_exec('mimeo_test', 'CREATE TABLE mimeo_source.logdel_test_source_condition (
    col1 int PRIMARY KEY,
    col2 text UNIQUE NOT NULL,
    col3 timestamptz DEFAULT clock_timestamp() )');
SELECT dblink_exec('mimeo_test', 'INSERT INTO mimeo_source.logdel_test_source_condition VALUES (generate_series(1,10), ''test''||generate_series(1,10)::text)');

SELECT dblink_disconnect('mimeo_test');
--SELECT is('SELECT dblink_get_connections() @> ''{mimeo_test}''','{}', 'Close remote database connection');

SELECT pass('Completed remote table setup');

SELECT * FROM finish();
