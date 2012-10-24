/*
 *  Logdel destroyer function. Pass ARCHIVE to keep table intact.
 */
CREATE FUNCTION logdel_destroyer(p_dest_table text, p_archive_option text) RETURNS void
    LANGUAGE plpgsql
    AS $$
    
DECLARE

v_conns             text[];
v_dblink            int;
v_dblink_schema     text;
v_dest_table        text;
v_drop_function     text;
v_drop_q_table      text;
v_drop_trigger      text;
v_old_search_path   text;
v_src_table         text;
v_table_name        text;
    
BEGIN

SELECT nspname INTO v_dblink_schema FROM pg_namespace n, pg_extension e WHERE e.extname = 'dblink' AND e.extnamespace = n.oid;
SELECT current_setting('search_path') INTO v_old_search_path;
EXECUTE 'SELECT set_config(''search_path'',''@extschema@,'||v_dblink_schema||',public'',''false'')';

SELECT source_table, dest_table, dblink INTO v_src_table, v_dest_table, v_dblink
		FROM @extschema@.refresh_config_logdel WHERE dest_table = p_dest_table;
IF NOT FOUND THEN
		RAISE NOTICE 'This table is not set up for logdel replication: %', v_dest_table;
ELSE
    IF position('.' in v_src_table) > 0 THEN 
        v_table_name := substring(v_src_table from position('.' in v_src_table)+1);
    END IF;

    v_drop_function := 'DROP FUNCTION IF EXISTS @extschema@.'||v_table_name||'_mimeo_queue()';
    v_drop_trigger := 'DROP TRIGGER IF EXISTS '||v_table_name||'_mimeo_trig ON '||v_src_table;
    v_drop_q_table := 'DROP TABLE IF EXISTS @extschema@.'||v_table_name||'_pgq';

    RAISE NOTICE 'Removing mimeo objects from source database (trigger, function, queue table)';
    PERFORM dblink_connect('mimeo_logdel_destroy', @extschema@.auth(v_dblink));
    PERFORM dblink_exec('mimeo_logdel_destroy', v_drop_trigger);
    PERFORM dblink_exec('mimeo_logdel_destroy', v_drop_function);
    PERFORM dblink_exec('mimeo_logdel_destroy', v_drop_q_table);
    PERFORM dblink_disconnect('mimeo_logdel_destroy');

    IF p_archive_option != 'ARCHIVE' THEN 
        RAISE NOTICE 'Destination table destroyed: %', v_dest_table;
        EXECUTE 'DROP TABLE IF EXISTS ' || v_dest_table;
    ELSE
        RAISE NOTICE 'Archive option set. Destination table NOT destroyed: %', v_dest_table; 
    END IF;

    RAISE NOTICE 'Removing config data';
    EXECUTE 'DELETE FROM @extschema@.refresh_config_logdel WHERE dest_table = ' || quote_literal(v_dest_table);	

    RAISE NOTICE 'Done';
END IF;

EXECUTE 'SELECT set_config(''search_path'','''||v_old_search_path||''',''false'')';

EXCEPTION
    WHEN OTHERS THEN
        EXECUTE 'SELECT set_config(''search_path'',''@extschema@,'||v_dblink_schema||''',''false'')';
        v_conns := dblink_get_connections();
        IF dblink_get_connections() @> '{mimeo_logdel_destroy}' THEN
            PERFORM dblink_disconnect('mimeo_logdel_destroy');
        END IF;
        EXECUTE 'SELECT set_config(''search_path'','''||v_old_search_path||''',''false'')';
        RAISE EXCEPTION '%', SQLERRM;    

END
$$;
