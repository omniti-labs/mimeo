\set ON_ERROR_ROLLBACK 1
\set ON_ERROR_STOP true

SELECT set_config('search_path','mimeo, dblink, public',false);

SELECT plan(1);

-- Setup remote tables for replication testing
SELECT dblink_connect('mimeo_test', 'host=localhost port=5432 dbname=mimeo_source user=mimeo_test password=mimeo_test');
SELECT is(dblink_get_connections() @> '{mimeo_test}', 't', 'Remote database connection established');

SELECT dblink_exec('mimeo_test', 'CREATE SCHEMA mimeo_source');
SELECT dblink_exec('mimeo_test', 'GRANT USAGE ON SCHEMA mimeo_source TO mimeo_dumb_role');
-- Test special strings in role names
SELECT dblink_exec('mimeo_test', 'GRANT USAGE ON SCHEMA mimeo_source TO "mimeo-dumber-role"');
SELECT dblink_exec('mimeo_test', 'CREATE SCHEMA mimeo');

SELECT dblink_exec('mimeo_test', 'CREATE TABLE mimeo_source.snap_test_source (
    col1 int,
    col2 text,
    col3 timestamptz DEFAULT clock_timestamp())');
SELECT dblink_exec('mimeo_test', 'INSERT INTO mimeo_source.snap_test_source VALUES (generate_series(1,10000), ''test''||generate_series(1,10000)::text)');
SELECT dblink_exec('mimeo_test', 'CREATE INDEX ON mimeo_source.snap_test_source (col2)');
SELECT dblink_exec('mimeo_test', 'CREATE TABLE mimeo_source.snap_test_source_empty (
    col1 int UNIQUE NOT NULL,
    col2 varchar(255),
    col3 timestamptz DEFAULT clock_timestamp())');
SELECT dblink_exec('mimeo_test', 'CREATE TABLE mimeo_source.snap_test_source_change_col (
    col1 int primary key,
    col2 text,
    col3 timestamptz DEFAULT clock_timestamp())');
SELECT dblink_exec('mimeo_test', 'INSERT INTO mimeo_source.snap_test_source_change_col VALUES (generate_series(1,10000), ''test''||generate_series(1,10000)::text)');
SELECT dblink_exec('mimeo_test', 'CREATE INDEX mimeo_check_exp_index_time ON mimeo_source.snap_test_source_change_col ((col3 > ''2013-04-01 00:00:00''))');
SELECT dblink_exec('mimeo_test', 'CREATE INDEX mimeo_check_exp_index_lower ON mimeo_source.snap_test_source_change_col (lower(col2))');

SELECT dblink_exec('mimeo_test', 'CREATE TABLE mimeo_source.inserter_test_source (
    col1 int PRIMARY KEY,
    col2 text,
    col3 timestamptz DEFAULT clock_timestamp())');
SELECT dblink_exec('mimeo_test', 'INSERT INTO mimeo_source.inserter_test_source VALUES (generate_series(1,10000), ''test''||generate_series(1,10000)::text)');
SELECT dblink_exec('mimeo_test', 'CREATE INDEX ON mimeo_source.inserter_test_source (col2)');
SELECT dblink_exec('mimeo_test', 'CREATE TABLE mimeo_source.inserter_test_source_empty (
    col1 int PRIMARY KEY,
    col2 text,
    col3 timestamptz DEFAULT clock_timestamp())');

SELECT dblink_exec('mimeo_test', 'CREATE TABLE mimeo_source.updater_test_source (
    col1 int PRIMARY KEY,
    col2 text,
    col3 timestamptz DEFAULT clock_timestamp())');
SELECT dblink_exec('mimeo_test', 'INSERT INTO mimeo_source.updater_test_source VALUES (generate_series(1,10000), ''test''||generate_series(1,10000)::text)');
SELECT dblink_exec('mimeo_test', 'CREATE INDEX ON mimeo_source.updater_test_source (col2)');
SELECT dblink_exec('mimeo_test', 'CREATE TABLE mimeo_source.updater_test_source_empty (
    col1 int PRIMARY KEY,
    col2 text,
    col3 timestamptz DEFAULT clock_timestamp())');

-- Must do separate tables due to queue table needing to be distinct
SELECT dblink_exec('mimeo_test', 'CREATE TABLE mimeo_source.dml_test_source (
    col1 int PRIMARY KEY,
    col2 text,
    col3 timestamptz DEFAULT clock_timestamp())');
SELECT dblink_exec('mimeo_test', 'CREATE INDEX ON mimeo_source.dml_test_source (col2)');
SELECT dblink_exec('mimeo_test', 'INSERT INTO mimeo_source.dml_test_source VALUES (generate_series(1,10000), ''test''||generate_series(1,10000)::text)');
SELECT dblink_exec('mimeo_test', 'GRANT SELECT, INSERT, UPDATE, DELETE ON mimeo_source.dml_test_source TO mimeo_dumb_role');
SELECT dblink_exec('mimeo_test', 'CREATE TABLE mimeo_source.dml_test_source2 (
    col1 int,
    col2 varchar(255),
    col3 timestamptz DEFAULT clock_timestamp(),
    PRIMARY KEY (col2, col1) )');
SELECT dblink_exec('mimeo_test', 'INSERT INTO mimeo_source.dml_test_source2 VALUES (generate_series(1,10000), ''test''||generate_series(1,10000)::text)');
-- Add another row with only one column of the composite key different to test for edge case
SELECT dblink_exec('mimeo_test', 'INSERT INTO mimeo_source.dml_test_source2 VALUES (4, ''test44'')');
SELECT dblink_exec('mimeo_test', 'INSERT INTO mimeo_source.dml_test_source2 VALUES (44, ''test444'')');
SELECT dblink_exec('mimeo_test', 'INSERT INTO mimeo_source.dml_test_source2 VALUES (444, ''test4444'')');
SELECT dblink_exec('mimeo_test', 'GRANT SELECT, INSERT, UPDATE, DELETE ON mimeo_source.dml_test_source2 TO mimeo_dumb_role');
-- Test special strings in role names
SELECT dblink_exec('mimeo_test', 'GRANT SELECT, INSERT, UPDATE, DELETE ON mimeo_source.dml_test_source2 TO "mimeo-dumber-role"');
SELECT dblink_exec('mimeo_test', 'CREATE TABLE mimeo_source.dml_test_source_nodata (
    col1 int UNIQUE NOT NULL,
    col2 text,
    col3 timestamptz DEFAULT clock_timestamp() )');
SELECT dblink_exec('mimeo_test', 'INSERT INTO mimeo_source.dml_test_source_nodata VALUES (generate_series(1,10000), ''test''||generate_series(1,10000)::text)');
SELECT dblink_exec('mimeo_test', 'CREATE TABLE mimeo_source.dml_test_source_filter (
    col1 int UNIQUE NOT NULL,
    col2 text,
    col3 timestamptz DEFAULT clock_timestamp() )');
SELECT dblink_exec('mimeo_test', 'INSERT INTO mimeo_source.dml_test_source_filter VALUES (generate_series(1,10000), ''test''||generate_series(1,10000)::text)');
SELECT dblink_exec('mimeo_test', 'CREATE TABLE mimeo_source.dml_test_source_condition (
    col1 int PRIMARY KEY,
    col2 text UNIQUE NOT NULL,
    col3 timestamptz DEFAULT clock_timestamp() )');
SELECT dblink_exec('mimeo_test', 'INSERT INTO mimeo_source.dml_test_source_condition VALUES (generate_series(1,10000), ''test''||generate_series(1,10000)::text)');
SELECT dblink_exec('mimeo_test', 'CREATE TABLE mimeo_source.dml_test_source_empty (
    col1 int PRIMARY KEY,
    col2 text UNIQUE NOT NULL,
    col3 timestamptz DEFAULT clock_timestamp() )');

-- Must do separate tables due to queue table needing to be distinct
SELECT dblink_exec('mimeo_test', 'CREATE TABLE mimeo_source.logdel_test_source (
    col1 int PRIMARY KEY,
    col2 text,
    col3 timestamptz DEFAULT clock_timestamp())');
SELECT dblink_exec('mimeo_test', 'INSERT INTO mimeo_source.logdel_test_source VALUES (generate_series(1,10000), ''test''||generate_series(1,10000)::text)');
SELECT dblink_exec('mimeo_test', 'CREATE INDEX ON mimeo_source.logdel_test_source (col2)');
SELECT dblink_exec('mimeo_test', 'GRANT SELECT, INSERT, UPDATE, DELETE ON mimeo_source.logdel_test_source TO mimeo_dumb_role');
SELECT dblink_exec('mimeo_test', 'CREATE TABLE mimeo_source.logdel_test_source2 (
    col1 int,
    col2 text,
    col3 timestamptz DEFAULT clock_timestamp(),
    PRIMARY KEY (col2, col1) )');
SELECT dblink_exec('mimeo_test', 'INSERT INTO mimeo_source.logdel_test_source2 VALUES (generate_series(1,10000), ''test''||generate_series(1,10000)::text)');
-- Add another row with only one column of the composite key different to test for edge case
SELECT dblink_exec('mimeo_test', 'INSERT INTO mimeo_source.logdel_test_source2 VALUES (4, ''test44'')');
SELECT dblink_exec('mimeo_test', 'INSERT INTO mimeo_source.logdel_test_source2 VALUES (44, ''test4444'')');
SELECT dblink_exec('mimeo_test', 'INSERT INTO mimeo_source.logdel_test_source2 VALUES (444, ''test4444'')');
SELECT dblink_exec('mimeo_test', 'GRANT SELECT, INSERT, UPDATE, DELETE ON mimeo_source.logdel_test_source2 TO mimeo_dumb_role');
-- Test special strings in role names
SELECT dblink_exec('mimeo_test', 'GRANT SELECT, INSERT, UPDATE, DELETE ON mimeo_source.logdel_test_source2 TO "mimeo-dumber-role"');
SELECT dblink_exec('mimeo_test', 'CREATE TABLE mimeo_source.logdel_test_source_nodata (
    col1 int UNIQUE NOT NULL,
    col2 text UNIQUE NOT NULL,
    col3 timestamptz DEFAULT clock_timestamp() )');
SELECT dblink_exec('mimeo_test', 'INSERT INTO mimeo_source.logdel_test_source_nodata VALUES (generate_series(1,10000), ''test''||generate_series(1,10000)::text)');
SELECT dblink_exec('mimeo_test', 'CREATE TABLE mimeo_source.logdel_test_source_filter (
    col1 int UNIQUE NOT NULL,
    col2 text UNIQUE NOT NULL,
    col3 timestamptz DEFAULT clock_timestamp() )');
SELECT dblink_exec('mimeo_test', 'INSERT INTO mimeo_source.logdel_test_source_filter VALUES (generate_series(1,10000), ''test''||generate_series(1,10000)::text)');
SELECT dblink_exec('mimeo_test', 'CREATE TABLE mimeo_source.logdel_test_source_condition (
    col1 int PRIMARY KEY,
    col2 text UNIQUE NOT NULL,
    col3 timestamptz DEFAULT clock_timestamp() )');
SELECT dblink_exec('mimeo_test', 'INSERT INTO mimeo_source.logdel_test_source_condition VALUES (generate_series(1,10000), ''test''||generate_series(1,10000)::text)');
SELECT dblink_exec('mimeo_test', 'CREATE TABLE mimeo_source.logdel_test_source_empty (
    col1 int PRIMARY KEY,
    col2 text UNIQUE NOT NULL,
    col3 timestamptz DEFAULT clock_timestamp() )');

SELECT dblink_disconnect('mimeo_test');
--SELECT is('SELECT dblink_get_connections() @> ''{mimeo_test}''','{}', 'Close remote database connection');

SELECT diag('Completed remote table setup');

SELECT * FROM finish();
