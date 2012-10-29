-- Must allow 'mimeo_test' role access to connect to the database this function is installed to and the temporary 'mimeo_source' database in the pg_hba.conf file

CREATE OR REPLACE FUNCTION test_mimeo_maker () RETURNS void
    LANGUAGE plpgsql
    AS $$
DECLARE

v_conns             text[];
v_dblink_schema     text;
v_ds_id             text;
v_mimeo_schema      text;
v_old_search_path   text;

BEGIN

SELECT current_setting('search_path') INTO v_old_search_path;
SELECT nspname INTO v_mimeo_schema FROM pg_namespace n, pg_extension e WHERE e.extname = 'mimeo' AND e.extnamespace = n.oid;
SELECT nspname INTO v_dblink_schema FROM pg_namespace n, pg_extension e WHERE e.extname = 'dblink' AND e.extnamespace = n.oid;
EXECUTE 'SELECT set_config(''search_path'','''||v_mimeo_schema||','||v_dblink_schema||''',''false'')';

EXECUTE 'SELECT data_source_id FROM '||v_mimeo_schema||'.dblink_mapping WHERE username = ''mimeo_test''' INTO v_ds_id;

-- Create test source tables in 'remote' database
RAISE NOTICE 'Creating source tables';
PERFORM dblink_connect('mimeo_test', 'host=localhost port=5432 dbname=mimeo_source user=mimeo_test password=mimeo_test');

PERFORM dblink_exec('mimeo_test', 'CREATE SCHEMA mimeo_source');
PERFORM dblink_exec('mimeo_test', 'CREATE SCHEMA mimeo');

PERFORM dblink_exec('mimeo_test', 'CREATE TABLE mimeo_source.snap_test_source (
    col1 int,
    col2 text,
    col3 timestamptz DEFAULT clock_timestamp())');
PERFORM dblink_exec('mimeo_test', 'INSERT INTO mimeo_source.snap_test_source VALUES (generate_series(1,10), ''test''||generate_series(1,10)::text)');

PERFORM dblink_exec('mimeo_test', 'CREATE TABLE mimeo_source.inserter_test_source (
    col1 int,
    col2 text,
    col3 timestamptz DEFAULT clock_timestamp())');
PERFORM dblink_exec('mimeo_test', 'INSERT INTO mimeo_source.inserter_test_source VALUES (generate_series(1,10), ''test''||generate_series(1,10)::text)');

PERFORM dblink_exec('mimeo_test', 'CREATE TABLE mimeo_source.updater_test_source (
    col1 int PRIMARY KEY,
    col2 text,
    col3 timestamptz DEFAULT clock_timestamp())');
PERFORM dblink_exec('mimeo_test', 'INSERT INTO mimeo_source.updater_test_source VALUES (generate_series(1,10), ''test''||generate_series(1,10)::text)');

-- Must do separate tables due to queue table needing to be distinct
PERFORM dblink_exec('mimeo_test', 'CREATE TABLE mimeo_source.dml_test_source (
    col1 int PRIMARY KEY,
    col2 text,
    col3 timestamptz DEFAULT clock_timestamp())');
PERFORM dblink_exec('mimeo_test', 'INSERT INTO mimeo_source.dml_test_source VALUES (generate_series(1,10), ''test''||generate_series(1,10)::text)');
PERFORM dblink_exec('mimeo_test', 'CREATE TABLE mimeo_source.dml_test_source2 (
    col1 int,
    col2 text,
    col3 timestamptz DEFAULT clock_timestamp(),
    PRIMARY KEY (col1, col2) )');
PERFORM dblink_exec('mimeo_test', 'INSERT INTO mimeo_source.dml_test_source2 VALUES (generate_series(1,10), ''test''||generate_series(1,10)::text)');
PERFORM dblink_exec('mimeo_test', 'CREATE TABLE mimeo_source.dml_test_source_nodata (
    col1 int UNIQUE NOT NULL,
    col2 text,
    col3 timestamptz DEFAULT clock_timestamp() )');
PERFORM dblink_exec('mimeo_test', 'INSERT INTO mimeo_source.dml_test_source_nodata VALUES (generate_series(1,10), ''test''||generate_series(1,10)::text)');
PERFORM dblink_exec('mimeo_test', 'CREATE TABLE mimeo_source.dml_test_source_filter (
    col1 int UNIQUE NOT NULL,
    col2 text,
    col3 timestamptz DEFAULT clock_timestamp() )');
PERFORM dblink_exec('mimeo_test', 'INSERT INTO mimeo_source.dml_test_source_filter VALUES (generate_series(1,10), ''test''||generate_series(1,10)::text)');
PERFORM dblink_exec('mimeo_test', 'CREATE TABLE mimeo_source.dml_test_source_condition (
    col1 int PRIMARY KEY,
    col2 text UNIQUE NOT NULL,
    col3 timestamptz DEFAULT clock_timestamp() )');
PERFORM dblink_exec('mimeo_test', 'INSERT INTO mimeo_source.dml_test_source_condition VALUES (generate_series(1,10), ''test''||generate_series(1,10)::text)');

-- Must do separate tables due to queue table needing to be distinct
PERFORM dblink_exec('mimeo_test', 'CREATE TABLE mimeo_source.logdel_test_source (
    col1 int PRIMARY KEY,
    col2 text,
    col3 timestamptz DEFAULT clock_timestamp())');
PERFORM dblink_exec('mimeo_test', 'INSERT INTO mimeo_source.logdel_test_source VALUES (generate_series(1,10), ''test''||generate_series(1,10)::text)');
PERFORM dblink_exec('mimeo_test', 'CREATE TABLE mimeo_source.logdel_test_source2 (
    col1 int,
    col2 text,
    col3 timestamptz DEFAULT clock_timestamp(),
    PRIMARY KEY (col1, col2) )');
PERFORM dblink_exec('mimeo_test', 'INSERT INTO mimeo_source.logdel_test_source2 VALUES (generate_series(1,10), ''test''||generate_series(1,10)::text)');
PERFORM dblink_exec('mimeo_test', 'CREATE TABLE mimeo_source.logdel_test_source_nodata (
    col1 int UNIQUE NOT NULL,
    col2 text UNIQUE NOT NULL,
    col3 timestamptz DEFAULT clock_timestamp() )');
PERFORM dblink_exec('mimeo_test', 'INSERT INTO mimeo_source.logdel_test_source_nodata VALUES (generate_series(1,10), ''test''||generate_series(1,10)::text)');
PERFORM dblink_exec('mimeo_test', 'CREATE TABLE mimeo_source.logdel_test_source_filter (
    col1 int UNIQUE NOT NULL,
    col2 text UNIQUE NOT NULL,
    col3 timestamptz DEFAULT clock_timestamp() )');
PERFORM dblink_exec('mimeo_test', 'INSERT INTO mimeo_source.logdel_test_source_filter VALUES (generate_series(1,10), ''test''||generate_series(1,10)::text)');
PERFORM dblink_exec('mimeo_test', 'CREATE TABLE mimeo_source.logdel_test_source_condition (
    col1 int PRIMARY KEY,
    col2 text UNIQUE NOT NULL,
    col3 timestamptz DEFAULT clock_timestamp() )');
PERFORM dblink_exec('mimeo_test', 'INSERT INTO mimeo_source.logdel_test_source_condition VALUES (generate_series(1,10), ''test''||generate_series(1,10)::text)');

-- Run creation tests
RAISE NOTICE 'Running creation tests';
EXECUTE 'SELECT snapshot_maker(''mimeo_source.snap_test_source'','||v_ds_id||')';
EXECUTE 'SELECT snapshot_maker(''mimeo_source.snap_test_source'', '||v_ds_id||', ''mimeo_dest.snap_test_dest'')';
EXECUTE 'SELECT snapshot_maker(''mimeo_source.snap_test_source'', '||v_ds_id||', ''mimeo_dest.snap_test_dest_nodata'', p_pulldata := false)';
EXECUTE 'SELECT snapshot_maker(''mimeo_source.snap_test_source'', '||v_ds_id||', ''mimeo_dest.snap_test_dest_filter'', p_filter := ''{"col1","col2"}'')';
EXECUTE 'SELECT snapshot_maker(''mimeo_source.snap_test_source'', '||v_ds_id||', ''mimeo_dest.snap_test_dest_condition'', p_condition := ''WHERE col1 > 3 AND col1 < 15'')';

EXECUTE 'SELECT inserter_maker(''mimeo_source.inserter_test_source'', ''col3'', '||v_ds_id||', ''00:00:05''::interval)';
EXECUTE 'SELECT inserter_maker(''mimeo_source.inserter_test_source'', ''col3'', '||v_ds_id||',''00:00:05''::interval, ''mimeo_dest.inserter_test_dest'')';
EXECUTE 'SELECT inserter_maker(''mimeo_source.inserter_test_source'', ''col3'', '||v_ds_id||',''00:00:05''::interval, ''mimeo_dest.inserter_test_dest_nodata'', p_pulldata := false)';
EXECUTE 'SELECT inserter_maker(''mimeo_source.inserter_test_source'', ''col3'', '||v_ds_id||',''00:00:05''::interval, ''mimeo_dest.inserter_test_dest_filter'', p_filter := ''{"col1","col3"}'')';
EXECUTE 'SELECT inserter_maker(''mimeo_source.inserter_test_source'', ''col3'', '||v_ds_id||',''00:00:05''::interval, ''mimeo_dest.inserter_test_dest_condition'', p_condition := ''WHERE col1 > 3 AND col1 < 15'')';

EXECUTE 'SELECT updater_maker(''mimeo_source.updater_test_source'', ''col3'', '||v_ds_id||', ''00:00:05''::interval)';
EXECUTE 'SELECT updater_maker(''mimeo_source.updater_test_source'', ''col3'', '||v_ds_id||', ''00:00:05''::interval, ''mimeo_dest.updater_test_dest'')';
EXECUTE 'SELECT updater_maker(''mimeo_source.updater_test_source'', ''col3'', '||v_ds_id||', ''00:00:05''::interval, ''mimeo_dest.updater_test_dest_nodata'', p_pulldata := false)';
EXECUTE 'SELECT updater_maker(''mimeo_source.updater_test_source'', ''col3'', '||v_ds_id||', ''00:00:05''::interval, ''mimeo_dest.updater_test_dest_filter'', p_filter := ''{"col1","col3"}'')';
EXECUTE 'SELECT updater_maker(''mimeo_source.updater_test_source'', ''col3'', '||v_ds_id||', ''00:00:05''::interval, ''mimeo_dest.updater_test_dest_condition'', p_condition := ''WHERE col1 > 3 AND col1 < 15'')';

EXECUTE 'SELECT dml_maker(''mimeo_source.dml_test_source'', '||v_ds_id||')';
EXECUTE 'SELECT dml_maker(''mimeo_source.dml_test_source2'', '||v_ds_id||', ''mimeo_dest.dml_test_dest'')';
EXECUTE 'SELECT dml_maker(''mimeo_source.dml_test_source_nodata'', '||v_ds_id||', ''mimeo_dest.dml_test_dest_nodata'', p_pulldata := false)';
EXECUTE 'SELECT dml_maker(''mimeo_source.dml_test_source_filter'', '||v_ds_id||', ''mimeo_dest.dml_test_dest_filter'', p_filter := ''{"col1","col2"}'')';
EXECUTE 'SELECT dml_maker(''mimeo_source.dml_test_source_condition'', '||v_ds_id||', ''mimeo_dest.dml_test_dest_condition'', p_condition := ''WHERE col1 > 3 AND col1 < 15'')';


EXECUTE 'SELECT logdel_maker(''mimeo_source.logdel_test_source'', '||v_ds_id||')';
EXECUTE 'SELECT logdel_maker(''mimeo_source.logdel_test_source2'', '||v_ds_id||', ''mimeo_dest.logdel_test_dest'')';
EXECUTE 'SELECT logdel_maker(''mimeo_source.logdel_test_source_nodata'', '||v_ds_id||', ''mimeo_dest.logdel_test_dest_nodata'', p_pulldata := false)';
EXECUTE 'SELECT logdel_maker(''mimeo_source.logdel_test_source_filter'', '||v_ds_id||', ''mimeo_dest.logdel_test_dest_filter'', p_filter := ''{"col1","col2"}'')';
EXECUTE 'SELECT logdel_maker(''mimeo_source.logdel_test_source_condition'', '||v_ds_id||', ''mimeo_dest.logdel_test_dest_condition'', p_condition := ''WHERE col1 > 3 AND col1 < 15'')';

RAISE NOTICE 'Sleeping for 35 seconds to ensure gap for incremental tests...';
PERFORM pg_sleep(35);

-- Insert new data
PERFORM dblink_exec('mimeo_test', 'INSERT INTO mimeo_source.snap_test_source VALUES (generate_series(11,20), ''test''||generate_series(11,20)::text)');
PERFORM dblink_exec('mimeo_test', 'INSERT INTO mimeo_source.inserter_test_source VALUES (generate_series(11,20), ''test''||generate_series(11,20)::text)');
PERFORM dblink_exec('mimeo_test', 'INSERT INTO mimeo_source.updater_test_source VALUES (generate_series(11,20), ''test''||generate_series(11,20)::text)');
PERFORM dblink_exec('mimeo_test', 'INSERT INTO mimeo_source.dml_test_source VALUES (generate_series(11,20), ''test''||generate_series(11,20)::text)');
PERFORM dblink_exec('mimeo_test', 'INSERT INTO mimeo_source.dml_test_source2 VALUES (generate_series(11,20), ''test''||generate_series(11,20)::text)');
PERFORM dblink_exec('mimeo_test', 'INSERT INTO mimeo_source.dml_test_source_nodata VALUES (generate_series(11,20), ''test''||generate_series(11,20)::text)');
PERFORM dblink_exec('mimeo_test', 'INSERT INTO mimeo_source.dml_test_source_filter VALUES (generate_series(11,20), ''test''||generate_series(11,20)::text)');
PERFORM dblink_exec('mimeo_test', 'INSERT INTO mimeo_source.dml_test_source_condition VALUES (generate_series(11,20), ''test''||generate_series(11,20)::text)');
PERFORM dblink_exec('mimeo_test', 'INSERT INTO mimeo_source.logdel_test_source VALUES (generate_series(11,20), ''test''||generate_series(11,20)::text)');
PERFORM dblink_exec('mimeo_test', 'INSERT INTO mimeo_source.logdel_test_source2 VALUES (generate_series(11,20), ''test''||generate_series(11,20)::text)');
PERFORM dblink_exec('mimeo_test', 'INSERT INTO mimeo_source.logdel_test_source_nodata VALUES (generate_series(11,20), ''test''||generate_series(11,20)::text)');
PERFORM dblink_exec('mimeo_test', 'INSERT INTO mimeo_source.logdel_test_source_filter VALUES (generate_series(11,20), ''test''||generate_series(11,20)::text)');
PERFORM dblink_exec('mimeo_test', 'INSERT INTO mimeo_source.logdel_test_source_condition VALUES (generate_series(11,20), ''test''||generate_series(11,20)::text)');

PERFORM dblink_disconnect('mimeo_test');

EXECUTE 'SELECT set_config(''search_path'','''||v_old_search_path||''',''false'')';

EXCEPTION
    WHEN OTHERS THEN
        EXECUTE 'SELECT set_config(''search_path'',''mimeo,'||v_dblink_schema||''',''false'')';
        v_conns := dblink_get_connections();
        IF dblink_get_connections() @> '{mimeo_test}' THEN
            PERFORM dblink_disconnect('mimeo_test');
        END IF;
        EXECUTE 'SELECT set_config(''search_path'','''||v_old_search_path||''',''false'')';
        RAISE EXCEPTION '%', SQLERRM;    

END
$$;

