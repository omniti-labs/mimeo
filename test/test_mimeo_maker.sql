-- Must allow 'mimeo_test' role access to connect to the database this function is installed to and the temporary 'mimeo_source' database in the pg_hba.conf file

CREATE OR REPLACE FUNCTION test_mimeo_maker () RETURNS void
    LANGUAGE plpgsql
    AS $$
DECLARE

v_dblink_schema     text;
v_mimeo_schema      text;
v_ds_id        text;
v_old_search_path   text;
v_source_dblink     text;
v_this_dblink       text;

v_inserter_maker_source     text;
v_inserter_maker_dest       text;
v_updater_maker_source      text;
v_updater_maker_dest        text;
v_refresh_inserter_source   text;

BEGIN


SELECT current_setting('search_path') INTO v_old_search_path;
SELECT nspname INTO v_mimeo_schema FROM pg_namespace n, pg_extension e WHERE e.extname = 'mimeo' AND e.extnamespace = n.oid;
SELECT nspname INTO v_dblink_schema FROM pg_namespace n, pg_extension e WHERE e.extname = 'dblink' AND e.extnamespace = n.oid;
EXECUTE 'SELECT set_config(''search_path'','''||v_mimeo_schema||','||v_dblink_schema||''',''false'')';

EXECUTE 'SELECT data_source_id FROM '||v_mimeo_schema||'.dblink_mapping WHERE username = ''mimeo_test''' INTO v_ds_id;

-- Create test source tables in 'remote' database
RAISE NOTICE 'Creating source tables';
v_source_dblink := 'host=localhost port=5432 dbname=mimeo_source user=mimeo_test password=mimeo_test';
--v_this_dblink := 'host=localhost port=5432 dbname='||current_database()||' user=mimeo_test password=mimeo_test';
PERFORM dblink_exec(v_source_dblink, 'CREATE SCHEMA mimeo_source');

PERFORM dblink_exec(v_source_dblink, 'CREATE TABLE mimeo_source.snap_test_source (
    col1 int,
    col2 text,
    col3 timestamptz DEFAULT now())');
PERFORM dblink_exec(v_source_dblink, 'INSERT INTO mimeo_source.snap_test_source VALUES (1, ''test1'')');
PERFORM dblink_exec(v_source_dblink, 'INSERT INTO mimeo_source.snap_test_source VALUES (2, ''test2'')');

PERFORM dblink_exec(v_source_dblink, 'CREATE TABLE mimeo_source.inserter_test_source (
    col1 int,
    col2 text,
    col3 timestamptz DEFAULT now())');
PERFORM dblink_exec(v_source_dblink, 'INSERT INTO mimeo_source.inserter_test_source VALUES (1, ''test1'')');
PERFORM dblink_exec(v_source_dblink, 'INSERT INTO mimeo_source.inserter_test_source VALUES (2, ''test2'')');

PERFORM dblink_exec(v_source_dblink, 'CREATE TABLE mimeo_source.updater_test_source (
    col1 int PRIMARY KEY,
    col2 text,
    col3 timestamptz DEFAULT now())');
PERFORM dblink_exec(v_source_dblink, 'INSERT INTO mimeo_source.updater_test_source VALUES (1, ''test1'')');
PERFORM dblink_exec(v_source_dblink, 'INSERT INTO mimeo_source.updater_test_source VALUES (2, ''test2'')');

PERFORM dblink_exec(v_source_dblink, 'CREATE TABLE mimeo_source.dml_test_source (
    col1 int PRIMARY KEY,
    col2 text,
    col3 timestamptz DEFAULT now())');
PERFORM dblink_exec(v_source_dblink, 'INSERT INTO mimeo_source.dml_test_source VALUES (1, ''test1'')');
PERFORM dblink_exec(v_source_dblink, 'INSERT INTO mimeo_source.dml_test_source VALUES (2, ''test2'')');

PERFORM dblink_exec(v_source_dblink, 'CREATE TABLE mimeo_source.logdel_test_source (
    col1 int PRIMARY KEY,
    col2 text,
    col3 timestamptz DEFAULT now())');
PERFORM dblink_exec(v_source_dblink, 'INSERT INTO mimeo_source.logdel_test_source VALUES (1, ''test1'')');
PERFORM dblink_exec(v_source_dblink, 'INSERT INTO mimeo_source.logdel_test_source VALUES (2, ''test2'')');

-- Run creation tests
RAISE NOTICE 'Running creation tests';
EXECUTE 'SELECT snapshot_maker(''mimeo_source.snap_test_source'','||v_ds_id||')';
EXECUTE 'SELECT snapshot_maker(''mimeo_source.snap_test_source'', ''mimeo_dest.snap_test_dest'', '||v_ds_id||')';

EXECUTE 'SELECT inserter_maker(''mimeo_source.inserter_test_source'', ''col3'', '||v_ds_id||', ''00:00:05''::interval)';
EXECUTE 'SELECT inserter_maker(''mimeo_source.inserter_test_source'', ''mimeo_dest.inserter_test_dest'', ''col3'', '||v_ds_id||',''00:00:05''::interval)';

EXECUTE 'SELECT updater_maker(''mimeo_source.updater_test_source'', ''col3'', '||v_ds_id||', ''{col1}'', ''{int}'', ''00:00:05''::interval)';
EXECUTE 'SELECT updater_maker(''mimeo_source.updater_test_source'',''mimeo_dest.updater_test_dest'', ''col3'', '||v_ds_id||', ''{col1}'', ''{int}'', ''00:00:05''::interval)';

RAISE NOTICE 'Sleeping for 35 seconds to ensure gap for incremental tests...';
PERFORM pg_sleep(35);

-- Insert new data
PERFORM dblink_exec(v_source_dblink, 'INSERT INTO mimeo_source.snap_test_source VALUES (3, ''test3'', now() + ''00:00:15''::interval)');
PERFORM dblink_exec(v_source_dblink, 'INSERT INTO mimeo_source.inserter_test_source VALUES (3, ''test3'', now() + ''00:00:15''::interval)');
PERFORM dblink_exec(v_source_dblink, 'INSERT INTO mimeo_source.updater_test_source VALUES (3, ''test3'', now() + ''00:00:15''::interval)');

EXECUTE 'SELECT set_config(''search_path'','''||v_old_search_path||''',''false'')';

END
$$;

