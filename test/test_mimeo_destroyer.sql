CREATE OR REPLACE FUNCTION test_mimeo_destroyer (p_archive text) RETURNS void
    LANGUAGE plpgsql
    AS $$
DECLARE

v_mimeo_schema      text;
v_old_search_path   text;

BEGIN

SELECT current_setting('search_path') INTO v_old_search_path;
SELECT nspname INTO v_mimeo_schema FROM pg_namespace n, pg_extension e WHERE e.extname = 'mimeo' AND e.extnamespace = n.oid;
EXECUTE 'SELECT set_config(''search_path'','''||v_mimeo_schema||''',''false'')';

PERFORM snapshot_destroyer('mimeo_source.snap_test_source', p_archive);
PERFORM snapshot_destroyer('mimeo_dest.snap_test_dest', p_archive);

PERFORM inserter_destroyer('mimeo_source.inserter_test_source', p_archive);
PERFORM inserter_destroyer('mimeo_dest.inserter_test_dest', p_archive);

PERFORM updater_destroyer('mimeo_source.updater_test_source', p_archive);
PERFORM updater_destroyer('mimeo_dest.updater_test_dest', p_archive);

EXECUTE 'SELECT set_config(''search_path'','''||v_old_search_path||''',''false'')';

END
$$;
