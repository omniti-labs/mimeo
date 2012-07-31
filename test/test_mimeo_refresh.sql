CREATE OR REPLACE FUNCTION test_mimeo_refresh () RETURNS void
    LANGUAGE plpgsql
    AS $$
DECLARE

v_dblink_schema     text;
v_mimeo_schema      text;
v_ds_id        text;
v_old_search_path   text;
v_source_dblink     text;
v_this_dblink       text;

v_trash             record;
v_refresh_inserter_source   text;

BEGIN

SELECT current_setting('search_path') INTO v_old_search_path;
SELECT nspname INTO v_mimeo_schema FROM pg_namespace n, pg_extension e WHERE e.extname = 'mimeo' AND e.extnamespace = n.oid;
SELECT nspname INTO v_dblink_schema FROM pg_namespace n, pg_extension e WHERE e.extname = 'dblink' AND e.extnamespace = n.oid;
EXECUTE 'SELECT set_config(''search_path'','''||v_mimeo_schema||','||v_dblink_schema||''',''false'')';

v_source_dblink := 'host=localhost port=5432 dbname=mimeo_source user=mimeo_test password=mimeo_test';
v_this_dblink := 'host=localhost port=5432 dbname='||current_database()||' user=mimeo_test password=mimeo_test';



-- Run refresh tests
PERFORM refresh_snap('mimeo_source.snap_test_source', true);
PERFORM refresh_snap('mimeo_dest.snap_test_dest', true);

-- Must be done via dblink otherwise last_value and boundary get mixed up due to function transaction
--v_refresh_inserter_source := 'SELECT '||v_mimeo_schema||'.refresh_inserter(''mimeo_source.inserter_test_source'', true)';
--EXECUTE 'SELECT dblink_exec('||quote_literal(v_this_dblink)||', '||quote_literal(v_refresh_inserter_source)||')' INTO v_trash;
--EXECUTE 'SELECT dblink_exec('''||v_this_dblink||''', ''SELECT '||v_mimeo_schema||'.refresh_inserter(''''mimeo_source.inserter_test_source'''', true)'')';
--v_trash := dblink(v_this_dblink, v_refresh_inserter_source);

PERFORM refresh_inserter('mimeo_source.inserter_test_source', true);
PERFORM refresh_inserter('mimeo_dest.inserter_test_dest', true);

PERFORM refresh_updater('mimeo_source.updater_test_source', true);
PERFORM refresh_updater('mimeo_dest.updater_test_dest', true);

EXECUTE 'SELECT set_config(''search_path'','''||v_old_search_path||''',''false'')';

END
$$;
