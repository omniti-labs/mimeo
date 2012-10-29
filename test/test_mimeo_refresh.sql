CREATE OR REPLACE FUNCTION test_mimeo_refresh () RETURNS void
    LANGUAGE plpgsql
    AS $$
DECLARE

v_dblink_schema     text;
v_mimeo_schema      text;
v_old_search_path   text;

BEGIN

SELECT current_setting('search_path') INTO v_old_search_path;
SELECT nspname INTO v_mimeo_schema FROM pg_namespace n, pg_extension e WHERE e.extname = 'mimeo' AND e.extnamespace = n.oid;
SELECT nspname INTO v_dblink_schema FROM pg_namespace n, pg_extension e WHERE e.extname = 'dblink' AND e.extnamespace = n.oid;
EXECUTE 'SELECT set_config(''search_path'','''||v_mimeo_schema||','||v_dblink_schema||''',''false'')';

-- Run refresh tests
PERFORM refresh_snap('mimeo_source.snap_test_source', p_debug := true);
PERFORM refresh_snap('mimeo_dest.snap_test_dest', p_debug := true);
PERFORM refresh_snap('mimeo_dest.snap_test_dest_nodata', p_debug := true);
PERFORM refresh_snap('mimeo_dest.snap_test_dest_filter', p_debug := true);
PERFORM refresh_snap('mimeo_dest.snap_test_dest_condition', p_debug := true);

PERFORM refresh_inserter('mimeo_source.inserter_test_source', p_debug := true);
PERFORM refresh_inserter('mimeo_dest.inserter_test_dest', p_debug := true);
PERFORM refresh_inserter('mimeo_dest.inserter_test_dest_nodata', p_debug := true);
PERFORM refresh_inserter('mimeo_dest.inserter_test_dest_filter', p_debug := true);
PERFORM refresh_inserter('mimeo_dest.inserter_test_dest_condition', p_debug := true);

PERFORM refresh_updater('mimeo_source.updater_test_source', p_debug := true);
PERFORM refresh_updater('mimeo_dest.updater_test_dest', p_debug := true);
PERFORM refresh_updater('mimeo_dest.updater_test_dest_nodata', p_debug := true);
PERFORM refresh_updater('mimeo_dest.updater_test_dest_filter', p_debug := true);
PERFORM refresh_updater('mimeo_dest.updater_test_dest_condition', p_debug := true);

PERFORM refresh_dml('mimeo_source.dml_test_source', p_debug := true);
PERFORM refresh_dml('mimeo_dest.dml_test_dest', p_debug := true);
PERFORM refresh_dml('mimeo_dest.dml_test_dest_nodata', p_debug := true);
PERFORM refresh_dml('mimeo_dest.dml_test_dest_filter', p_debug := true);
PERFORM refresh_dml('mimeo_dest.dml_test_dest_condition', p_debug := true);

PERFORM refresh_logdel('mimeo_source.logdel_test_source', p_debug := true);
PERFORM refresh_logdel('mimeo_dest.logdel_test_dest', p_debug := true);
PERFORM refresh_logdel('mimeo_dest.logdel_test_dest_nodata', p_debug := true);
PERFORM refresh_logdel('mimeo_dest.logdel_test_dest_filter', p_debug := true);
PERFORM refresh_logdel('mimeo_dest.logdel_test_dest_condition', p_debug := true);

--Add tests to check updates and deletes where needed

EXECUTE 'SELECT set_config(''search_path'','''||v_old_search_path||''',''false'')';

END
$$;
