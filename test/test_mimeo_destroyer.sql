CREATE OR REPLACE FUNCTION test_mimeo_destroyer (p_archive text DEFAULT 'n') RETURNS void
    LANGUAGE plpgsql
    AS $$
DECLARE

v_check             int;
v_mimeo_schema      text;
v_old_search_path   text;

BEGIN

SELECT current_setting('search_path') INTO v_old_search_path;
SELECT nspname INTO v_mimeo_schema FROM pg_namespace n, pg_extension e WHERE e.extname = 'mimeo' AND e.extnamespace = n.oid;
EXECUTE 'SELECT set_config(''search_path'','''||v_mimeo_schema||''',''false'')';

SELECT count(*) INTO v_check FROM refresh_config WHERE dest_table = 'mimeo_source.snap_test_source';
IF v_check >= 1 THEN
    PERFORM snapshot_destroyer('mimeo_source.snap_test_source', p_archive);
END IF;
SELECT count(*) INTO v_check FROM refresh_config WHERE dest_table = 'mimeo_dest.snap_test_dest';
IF v_check >= 1 THEN
    PERFORM snapshot_destroyer('mimeo_dest.snap_test_dest', p_archive);
END IF;

SELECT count(*) INTO v_check FROM refresh_config WHERE dest_table = 'mimeo_source.inserter_test_source';
IF v_check >= 1 THEN
    PERFORM inserter_destroyer('mimeo_source.inserter_test_source', p_archive);
END IF;
SELECT count(*) INTO v_check FROM refresh_config WHERE dest_table = 'mimeo_dest.inserter_test_dest';
IF v_check >= 1 THEN
    PERFORM inserter_destroyer('mimeo_dest.inserter_test_dest', p_archive);
END IF;

SELECT count(*) INTO v_check FROM refresh_config WHERE dest_table = 'mimeo_source.updater_test_source';
IF v_check >= 1 THEN
    PERFORM updater_destroyer('mimeo_source.updater_test_source', p_archive);
END IF;
SELECT count(*) INTO v_check FROM refresh_config WHERE dest_table = 'mimeo_dest.updater_test_dest';
IF v_check >= 1 THEN
    PERFORM updater_destroyer('mimeo_dest.updater_test_dest', p_archive);
END IF;

SELECT count(*) INTO v_check FROM refresh_config WHERE dest_table = 'mimeo_source.dml_test_source';
IF v_check >= 1 THEN
    PERFORM dml_destroyer('mimeo_source.dml_test_source', p_archive);
END IF;
SELECT count(*) INTO v_check FROM refresh_config WHERE dest_table = 'mimeo_dest.dml_test_dest';
IF v_check >= 1 THEN
    PERFORM dml_destroyer('mimeo_dest.dml_test_dest', p_archive);
END IF;

SELECT count(*) INTO v_check FROM refresh_config WHERE dest_table = 'mimeo_source.logdel_test_source';
IF v_check >= 1 THEN
    PERFORM logdel_destroyer('mimeo_source.logdel_test_source', p_archive);
END IF;
SELECT count(*) INTO v_check FROM refresh_config WHERE dest_table = 'mimeo_dest.logdel_test_dest';
IF v_check >= 1 THEN
    PERFORM logdel_destroyer('mimeo_dest.logdel_test_dest', p_archive);
END IF;

EXECUTE 'SELECT set_config(''search_path'','''||v_old_search_path||''',''false'')';

END
$$;
