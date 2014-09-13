\set ON_ERROR_ROLLBACK 1
\set ON_ERROR_STOP true

SELECT set_config('search_path','mimeo, dblink, public',false);

SELECT plan(2);

-- Setup remote tables for replication testing
-- Create source objects with a different role than will be setting up replication to be able to ensure object owners are granted necessary permissions
SELECT dblink_connect('mimeo_owner', 'host=localhost port=5432 dbname=mimeo_source user=mimeo_owner password=mimeo_owner');
SELECT is(dblink_get_connections() @> '{mimeo_owner}', 't', 'Remote database connection established');

SELECT diag('Creating source schema & roles');
SELECT dblink_exec('mimeo_owner', 'CREATE SCHEMA mimeo_source');
SELECT dblink_exec('mimeo_owner', 'GRANT USAGE ON SCHEMA mimeo_source TO mimeo_test');
SELECT dblink_exec('mimeo_owner', 'GRANT USAGE ON SCHEMA mimeo_source TO mimeo_dumb_role');
-- Test special strings in role names
SELECT dblink_exec('mimeo_owner', 'GRANT USAGE ON SCHEMA mimeo_source TO "mimeo-dumber-role"');
SELECT dblink_exec('mimeo_owner', 'CREATE SCHEMA mimeo');
SELECT dblink_exec('mimeo_owner', 'ALTER SCHEMA mimeo OWNER TO mimeo_test');

-- Snapshots
SELECT diag('Creating source table: mimeo_source.snap_test_source');
SELECT dblink_exec('mimeo_owner', 'CREATE TABLE mimeo_source.snap_test_source (
    col1 int,
    col2 text,
    col3 timestamptz DEFAULT clock_timestamp())');
SELECT dblink_exec('mimeo_owner', 'INSERT INTO mimeo_source.snap_test_source VALUES (generate_series(1,10000), ''test''||generate_series(1,10000)::text)');
SELECT dblink_exec('mimeo_owner', 'CREATE INDEX ON mimeo_source.snap_test_source (col2)');
SELECT diag('Creating source table: mimeo_source.snap_test_source_empty');
SELECT dblink_exec('mimeo_owner', 'CREATE TABLE mimeo_source.snap_test_source_empty (
    col1 int UNIQUE NOT NULL,
    col2 varchar(255),
    col3 timestamptz DEFAULT clock_timestamp())');
SELECT diag('Creating source table: mimeo_source.snap_test_source_change_col');
SELECT dblink_exec('mimeo_owner', 'CREATE TABLE mimeo_source.snap_test_source_change_col (
    col1 int primary key,
    col2 text,
    col3 timestamptz DEFAULT clock_timestamp())');
SELECT dblink_exec('mimeo_owner', 'INSERT INTO mimeo_source.snap_test_source_change_col VALUES (generate_series(1,10000), ''test''||generate_series(1,10000)::text)');
SELECT dblink_exec('mimeo_owner', 'CREATE INDEX mimeo_check_exp_index_time ON mimeo_source.snap_test_source_change_col ((col3 > ''2013-04-01 00:00:00''))');
SELECT dblink_exec('mimeo_owner', 'CREATE INDEX mimeo_check_exp_index_lower ON mimeo_source.snap_test_source_change_col (lower(col2))');
-- Test for special charaters, mixed case & reserved words
SELECT diag('Creating source table: mimeo_source."Snap-test-Source"');
SELECT dblink_exec('mimeo_owner', 'CREATE TABLE mimeo_source."Snap-test-Source" (
    "primary" int,
    col2 text,
    "COL-3" timestamptz DEFAULT clock_timestamp())');
SELECT dblink_exec('mimeo_owner', 'INSERT INTO mimeo_source."Snap-test-Source" VALUES (generate_series(1,10000), ''test''||generate_series(1,10000)::text)');
SELECT dblink_exec('mimeo_owner', 'CREATE INDEX ON mimeo_source."Snap-test-Source" (col2)');
-- View
SELECT diag('Creating source view: mimeo_source.snap_test_source_view');
SELECT dblink_exec('mimeo_owner', 'CREATE VIEW mimeo_source.snap_test_source_view AS SELECT * FROM mimeo_source.snap_test_source');

-- Inserter
SELECT diag('Creating source table: mimeo_source.inserter_test_source');
SELECT dblink_exec('mimeo_owner', 'CREATE TABLE mimeo_source.inserter_test_source (
    col1 int PRIMARY KEY,
    col2 text,
    col3 timestamptz DEFAULT clock_timestamp())');
SELECT dblink_exec('mimeo_owner', 'INSERT INTO mimeo_source.inserter_test_source VALUES (generate_series(1,10000), ''test''||generate_series(1,10000)::text)');
SELECT dblink_exec('mimeo_owner', 'CREATE INDEX ON mimeo_source.inserter_test_source (col2)');
SELECT diag('Creating source table: mimeo_source.inserter_test_source_empty');
SELECT dblink_exec('mimeo_owner', 'CREATE TABLE mimeo_source.inserter_test_source_empty (
    col1 int PRIMARY KEY,
    col2 text,
    col3 timestamptz DEFAULT clock_timestamp())');
-- Test for special charaters, mixed case & reserved words
SELECT diag('Creating source table: mimeo_source."Inserter-Test-Source"');
SELECT dblink_exec('mimeo_owner', 'CREATE TABLE mimeo_source."Inserter-Test-Source" (
        col1 int PRIMARY KEY,
        "group" text,
        "Col-3" timestamptz DEFAULT clock_timestamp() )');
SELECT dblink_exec('mimeo_owner', 'CREATE INDEX "Inserter-Test-Source-group-Idx" ON mimeo_source."Inserter-Test-Source" ("group")');
SELECT dblink_exec('mimeo_owner', 'INSERT INTO mimeo_source."Inserter-Test-Source" VALUES (generate_series(1,10000), ''test''||generate_series(1,10000)::text)');
-- View
SELECT diag('Creating source view: mimeo_source.inserter_test_source_view');
SELECT dblink_exec('mimeo_owner', 'CREATE VIEW mimeo_source.inserter_test_source_view AS SELECT * FROM mimeo_source.inserter_test_source');


-- Updater
SELECT diag('Creating source table: mimeo_source.updater_test_source');
SELECT dblink_exec('mimeo_owner', 'CREATE TABLE mimeo_source.updater_test_source (
    col1 int PRIMARY KEY,
    col2 text,
    col3 timestamptz DEFAULT clock_timestamp(),
    col4 serial)');
SELECT dblink_exec('mimeo_owner', 'INSERT INTO mimeo_source.updater_test_source VALUES (generate_series(1,10000), ''test''||generate_series(1,10000)::text)');
SELECT dblink_exec('mimeo_owner', 'CREATE INDEX ON mimeo_source.updater_test_source (col2)');
SELECT dblink_exec('mimeo_owner', 'CREATE INDEX ON mimeo_source.updater_test_source (col4)');
SELECT diag('Creating source table: mimeo_source.updater_test_source_empty');
SELECT dblink_exec('mimeo_owner', 'CREATE TABLE mimeo_source.updater_test_source_empty (
    col1 int PRIMARY KEY,
    col2 text,
    col3 timestamptz DEFAULT clock_timestamp())');
-- Test for special charaters, mixed case & reserved words
SELECT diag('Creating source table: mimeo_source."Updater-Test-Source"');
SELECT dblink_exec('mimeo_owner', 'CREATE TABLE mimeo_source."Updater-Test-Source" (
        "COL-1" int PRIMARY KEY,
        "group" text,
        "Col3" timestamptz DEFAULT clock_timestamp() )');
SELECT dblink_exec('mimeo_owner', 'CREATE INDEX "Updater-Test-Source-group-Idx" ON mimeo_source."Updater-Test-Source" ("group")');
SELECT dblink_exec('mimeo_owner', 'INSERT INTO mimeo_source."Updater-Test-Source" VALUES (generate_series(1,10000), ''test''||generate_series(1,10000)::text)');
-- View
SELECT diag('Creating source view: mimeo_source.updater_test_source_view');
SELECT dblink_exec('mimeo_owner', 'CREATE VIEW mimeo_source.updater_test_source_view AS SELECT * FROM mimeo_source.updater_test_source');

-- DML
-- Must do separate tables due to queue table needing to be distinct
SELECT diag('Creating source table: mimeo_source.dml_test_source');
SELECT dblink_exec('mimeo_owner', 'CREATE TABLE mimeo_source.dml_test_source (
    col1 int PRIMARY KEY,
    col2 text,
    col3 timestamptz DEFAULT clock_timestamp())');
SELECT dblink_exec('mimeo_owner', 'CREATE INDEX ON mimeo_source.dml_test_source (col2)');
SELECT dblink_exec('mimeo_owner', 'INSERT INTO mimeo_source.dml_test_source VALUES (generate_series(1,10000), ''test''||generate_series(1,10000)::text)');
SELECT diag('Creating source table: mimeo_source.dml_test_source2');
SELECT dblink_exec('mimeo_owner', 'CREATE TABLE mimeo_source.dml_test_source2 (
    col1 int,
    col2 varchar(255),
    col3 timestamptz DEFAULT clock_timestamp(),
    PRIMARY KEY (col2, col1) )');
SELECT dblink_exec('mimeo_owner', 'INSERT INTO mimeo_source.dml_test_source2 VALUES (generate_series(1,10000), ''test''||generate_series(1,10000)::text)');
-- Add another row with only one column of the composite key different to test for edge case
SELECT dblink_exec('mimeo_owner', 'INSERT INTO mimeo_source.dml_test_source2 VALUES (4, ''test44'')');
SELECT dblink_exec('mimeo_owner', 'INSERT INTO mimeo_source.dml_test_source2 VALUES (44, ''test444'')');
SELECT dblink_exec('mimeo_owner', 'INSERT INTO mimeo_source.dml_test_source2 VALUES (444, ''test4444'')');
-- Test special strings in role names
SELECT dblink_exec('mimeo_owner', 'GRANT SELECT, INSERT, UPDATE, DELETE ON mimeo_source.dml_test_source2 TO "mimeo-dumber-role"');
SELECT diag('Creating source table: mimeo_source.dml_test_source_nodata');
SELECT dblink_exec('mimeo_owner', 'CREATE TABLE mimeo_source.dml_test_source_nodata (
    col1 int UNIQUE NOT NULL,
    col2 text,
    col3 timestamptz DEFAULT clock_timestamp() )');
SELECT dblink_exec('mimeo_owner', 'INSERT INTO mimeo_source.dml_test_source_nodata VALUES (generate_series(1,10000), ''test''||generate_series(1,10000)::text)');
SELECT diag('Creating source table: mimeo_source.dml_test_source_filter');
SELECT dblink_exec('mimeo_owner', 'CREATE TABLE mimeo_source.dml_test_source_filter (
    col1 int UNIQUE NOT NULL,
    col2 text,
    col3 timestamptz DEFAULT clock_timestamp() )');
SELECT dblink_exec('mimeo_owner', 'INSERT INTO mimeo_source.dml_test_source_filter VALUES (generate_series(1,10000), ''test''||generate_series(1,10000)::text)');
SELECT diag('Creating source table: mimeo_source.dml_test_source_condition');
SELECT dblink_exec('mimeo_owner', 'CREATE TABLE mimeo_source.dml_test_source_condition (
    col1 int PRIMARY KEY,
    col2 text UNIQUE NOT NULL,
    col3 timestamptz DEFAULT clock_timestamp() )');
SELECT dblink_exec('mimeo_owner', 'INSERT INTO mimeo_source.dml_test_source_condition VALUES (generate_series(1,10000), ''test''||generate_series(1,10000)::text)');
SELECT diag('Creating source table: mimeo_source.dml_test_source_empty');
SELECT dblink_exec('mimeo_owner', 'CREATE TABLE mimeo_source.dml_test_source_empty (
    col1 int PRIMARY KEY,
    col2 text UNIQUE NOT NULL,
    col3 timestamptz DEFAULT clock_timestamp() )');
-- Test for special charaters, mixed case & reserved words
SELECT diag('Creating source table: mimeo_source.Dml-Test-Source');
SELECT dblink_exec('mimeo_owner', 'CREATE TABLE mimeo_source."Dml-Test-Source" (
        "COL1" int PRIMARY KEY,
        "group" text,
        "Col-3" timestamptz DEFAULT clock_timestamp() )');
SELECT dblink_exec('mimeo_owner', 'CREATE INDEX "Dml-Test-Source-group-Idx" ON mimeo_source."Dml-Test-Source" ("group")');
SELECT dblink_exec('mimeo_owner', 'INSERT INTO mimeo_source."Dml-Test-Source" VALUES (generate_series(1,10000), ''test''||generate_series(1,10000)::text)');


-- Logdel
-- Must do separate tables due to queue table needing to be distinct
SELECT diag('Creating source table: mimeo_source.logdel_test_source');
SELECT dblink_exec('mimeo_owner', 'CREATE TABLE mimeo_source.logdel_test_source (
    col1 int PRIMARY KEY,
    col2 text,
    col3 timestamptz DEFAULT clock_timestamp())');
SELECT dblink_exec('mimeo_owner', 'INSERT INTO mimeo_source.logdel_test_source VALUES (generate_series(1,10000), ''test''||generate_series(1,10000)::text)');
SELECT dblink_exec('mimeo_owner', 'CREATE INDEX ON mimeo_source.logdel_test_source (col2)');
SELECT diag('Creating source table: mimeo_source.logdel_test_source2');
SELECT dblink_exec('mimeo_owner', 'CREATE TABLE mimeo_source.logdel_test_source2 (
    col1 int,
    col2 text,
    col3 timestamptz DEFAULT clock_timestamp(),
    PRIMARY KEY (col2, col1) )');
SELECT dblink_exec('mimeo_owner', 'INSERT INTO mimeo_source.logdel_test_source2 VALUES (generate_series(1,10000), ''test''||generate_series(1,10000)::text)');
-- Add another row with only one column of the composite key different to test for edge case
SELECT dblink_exec('mimeo_owner', 'INSERT INTO mimeo_source.logdel_test_source2 VALUES (4, ''test44'')');
SELECT dblink_exec('mimeo_owner', 'INSERT INTO mimeo_source.logdel_test_source2 VALUES (44, ''test4444'')');
SELECT dblink_exec('mimeo_owner', 'INSERT INTO mimeo_source.logdel_test_source2 VALUES (444, ''test4444'')');
-- Test special strings in role names
SELECT dblink_exec('mimeo_owner', 'GRANT SELECT, INSERT, UPDATE, DELETE ON mimeo_source.logdel_test_source2 TO "mimeo-dumber-role"');
SELECT diag('Creating source table: mimeo_source.logdel_test_source_nodata');
SELECT dblink_exec('mimeo_owner', 'CREATE TABLE mimeo_source.logdel_test_source_nodata (
    col1 int UNIQUE NOT NULL,
    col2 text UNIQUE NOT NULL,
    col3 timestamptz DEFAULT clock_timestamp() )');
SELECT dblink_exec('mimeo_owner', 'INSERT INTO mimeo_source.logdel_test_source_nodata VALUES (generate_series(1,10000), ''test''||generate_series(1,10000)::text)');
SELECT diag('Creating source table: mimeo_source.logdel_test_source_filter');
SELECT dblink_exec('mimeo_owner', 'CREATE TABLE mimeo_source.logdel_test_source_filter (
    col1 int UNIQUE NOT NULL,
    col2 text UNIQUE NOT NULL,
    col3 timestamptz DEFAULT clock_timestamp() )');
SELECT dblink_exec('mimeo_owner', 'INSERT INTO mimeo_source.logdel_test_source_filter VALUES (generate_series(1,10000), ''test''||generate_series(1,10000)::text)');
SELECT diag('Creating source table: mimeo_source.logdel_test_source_condition');
SELECT dblink_exec('mimeo_owner', 'CREATE TABLE mimeo_source.logdel_test_source_condition (
    col1 int PRIMARY KEY,
    col2 text UNIQUE NOT NULL,
    col3 timestamptz DEFAULT clock_timestamp() )');
SELECT dblink_exec('mimeo_owner', 'INSERT INTO mimeo_source.logdel_test_source_condition VALUES (generate_series(1,10000), ''test''||generate_series(1,10000)::text)');
SELECT diag('Creating source table: mimeo_source.logdel_test_source_empty');
SELECT dblink_exec('mimeo_owner', 'CREATE TABLE mimeo_source.logdel_test_source_empty (
    col1 int PRIMARY KEY,
    col2 text UNIQUE NOT NULL,
    col3 timestamptz DEFAULT clock_timestamp() )');
-- Test for special charaters, mixed case & reserved words
SELECT diag('Creating source table: mimeo_source."LogDel-Test-Source"');
SELECT dblink_exec('mimeo_owner', 'CREATE TABLE mimeo_source."LogDel-Test-Source" (
        "COL1" int PRIMARY KEY,
        "group" text,
        "Col-3" timestamptz DEFAULT clock_timestamp() )');
SELECT dblink_exec('mimeo_owner', 'CREATE INDEX "LogDel-Test-Source-group-Idx" ON mimeo_source."LogDel-Test-Source" ("group")');
SELECT dblink_exec('mimeo_owner', 'INSERT INTO mimeo_source."LogDel-Test-Source" VALUES (generate_series(1,10000), ''test''||generate_series(1,10000)::text)');


SELECT diag('Setting source table privileges.');
-- Ensure all objects have permissions needed for mimeo_test role to be able to call maker functions
SELECT dblink_exec('mimeo_owner', 'GRANT SELECT, TRIGGER ON ALL TABLES IN SCHEMA mimeo_source TO mimeo_test');
-- Ensure all objects have a non-owner, non-dblink related role that can write to objects for tests
SELECT dblink_exec('mimeo_owner', 'GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA mimeo_source TO mimeo_dumb_role');
SELECT dblink_exec('mimeo_owner', 'GRANT ALL ON ALL SEQUENCES IN SCHEMA mimeo_source TO mimeo_dumb_role');

SELECT dblink_disconnect('mimeo_owner');
--SELECT is('SELECT dblink_get_connections() @> ''{mimeo_owner}''','{}', 'Close remote database connection');

SELECT pass('Completed remote table setup');

SELECT * FROM finish();
