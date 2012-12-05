/*
\set ECHO
\set QUIET 1
-- Turn off echo and keep things quiet.

-- Format the output for nice TAP.
\pset format unaligned
\pset tuples_only true
\pset pager

-- Revert all changes on failure.
\set ON_ERROR_ROLLBACK 1
\set ON_ERROR_STOP true
\set QUIET 1 
*/

DROP DATABASE IF EXISTS mimeo_source;
CREATE DATABASE mimeo_source;

-- Load the TAP functions. (only need to do this with final version that ships with mimeo
-- \i pgtap.sql

SELECT set_config('search_path','mimeo, dblink, tap',false);

-- Plan the tests.
SELECT plan(5);

-- Run the tests.
CREATE ROLE mimeo_test WITH LOGIN SUPERUSER PASSWORD 'mimeo_test';
SELECT has_role('mimeo_test', 'Create mimeo test role');
CREATE SCHEMA mimeo_source;
SELECT has_schema('mimeo_source', 'Create test schema for default destination tables');
CREATE SCHEMA mimeo_dest;
SELECT has_schema('mimeo_dest', 'Create test schema for named destination tables');
INSERT INTO dblink_mapping (data_source, username, pwd) VALUES ('host=localhost port=5432 dbname=mimeo_source', 'mimeo_test', 'mimeo_test');
SELECT is(data_source, 'host=localhost port=5432 dbname=mimeo_source', 'Configure remote host for testing') FROM dblink_mapping WHERE username = 'mimeo_test';
SELECT is(pwd, 'mimeo_test', 'Configure password for remote testing role') FROM dblink_mapping WHERE username = 'mimeo_test';


-- Finish the tests and clean up.
SELECT * FROM finish();
