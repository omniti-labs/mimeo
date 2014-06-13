\set ON_ERROR_ROLLBACK 1
\set ON_ERROR_STOP true

DROP DATABASE IF EXISTS mimeo_source;
CREATE DATABASE mimeo_source;

SELECT set_config('search_path','mimeo, dblink, public',false);

-- Plan the tests.
SELECT plan(7);

-- Run the tests.
CREATE ROLE mimeo_owner WITH LOGIN SUPERUSER PASSWORD 'mimeo_owner';
CREATE ROLE mimeo_test WITH LOGIN PASSWORD 'mimeo_test';
SELECT has_role('mimeo_test', 'Create mimeo test role');
CREATE ROLE mimeo_dumb_role WITH LOGIN PASSWORD 'mimeo_test';
SELECT has_role('mimeo_dumb_role', 'Create mimeo dumb role');
CREATE ROLE "mimeo-dumber-role";
SELECT has_role('mimeo-dumber-role', 'Create mimeo dumber role to test nonstandard strings');
CREATE SCHEMA mimeo_source;
SELECT has_schema('mimeo_source', 'Create test schema for default destination tables');
CREATE SCHEMA mimeo_dest;
SELECT has_schema('mimeo_dest', 'Create test schema for named destination tables');
INSERT INTO dblink_mapping_mimeo (data_source, username, pwd) VALUES ('host=localhost port=5432 dbname=mimeo_source', 'mimeo_test', 'mimeo_test');
SELECT is(data_source, 'host=localhost port=5432 dbname=mimeo_source', 'Configure remote host for testing') FROM dblink_mapping_mimeo WHERE username = 'mimeo_test';
SELECT is(pwd, 'mimeo_test', 'Configure password for remote testing role') FROM dblink_mapping_mimeo WHERE username = 'mimeo_test';


-- Finish the tests and clean up.
SELECT * FROM finish();
