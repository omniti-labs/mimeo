/*
 *  DML destroyer function. 
 */
CREATE FUNCTION dml_destroyer(p_dest_table text, p_keep_table boolean DEFAULT true, p_debug boolean DEFAULT false) RETURNS void
    LANGUAGE plpgsql
    AS $$
DECLARE

v_dblink                int;
v_dblink_name           text;
v_dblink_schema         text;
v_dest_schema_name      text;
v_dest_table            text;
v_dest_table_name       text;
v_drop_dest_table       text;
v_drop_function         text;
v_drop_q_table          text;
v_drop_trigger          text;
v_link_exists           text;
v_old_search_path       text;
v_source_queue_function text;
v_source_queue_table    text;
v_source_queue_trigger  text;
v_sql                   text;
v_src_schema_name       text;
v_src_table             text;
v_src_table_name        text;
v_src_table_template    text;
v_table_name            text;
v_table_owner           text;
v_username              text;

BEGIN

SELECT nspname INTO v_dblink_schema FROM pg_namespace n, pg_extension e WHERE e.extname = 'dblink' AND e.extnamespace = n.oid;
SELECT current_setting('search_path') INTO v_old_search_path;
EXECUTE 'SELECT set_config(''search_path'',''@extschema@,'||v_dblink_schema||',public'',''false'')';

SELECT source_table
    , dest_table
    , dblink 
INTO v_src_table
    , v_dest_table
    , v_dblink
FROM @extschema@.refresh_config_dml 
WHERE dest_table = p_dest_table;

IF NOT FOUND THEN
    RAISE NOTICE 'This table is not set up for dml replication: %', v_dest_table;
ELSE
    SELECT schemaname, tablename 
    INTO v_dest_schema_name, v_dest_table_name
    FROM pg_catalog.pg_tables
    WHERE schemaname||'.'||tablename = v_dest_table;

    SELECT username INTO v_username FROM @extschema@.dblink_mapping_mimeo;

    v_dblink_name := 'mimeo_dml_destroy';
    PERFORM dblink_connect(v_dblinK_name, @extschema@.auth(v_dblink));

    SELECT schemaname ||'_'|| tablename, schemaname, tablename, tableowner
    INTO v_src_table_template, v_src_schema_name, v_src_table_name, v_table_owner
    FROM dblink(v_dblink_name, 'SELECT schemaname, tablename, tableowner FROM pg_catalog.pg_tables WHERE schemaname ||''.''|| tablename = '||quote_literal(v_src_table)) t (schemaname text, tablename text, tableowner text);

    IF v_table_owner <> v_username THEN
        RAISE EXCEPTION 'Unable to drop the mimeo trigger on source table (%). Mimeo role must be the owner of the table to automatically drop it. Manually drop the mimeo trigger first, then run destroyer function again.', v_src_table;
    END IF;

    v_source_queue_table :=  check_name_length(v_src_table_template, '_q');
    v_source_queue_function := check_name_length(v_src_table_template, '_mimeo_queue');
    v_source_queue_trigger := check_name_length(v_src_table_template, '_mimeo_trig');

    v_drop_trigger := format('DROP TRIGGER IF EXISTS %I ON %I.%I', v_source_queue_trigger, v_src_schema_name, v_src_table_name);
    v_drop_function := format('DROP FUNCTION IF EXISTS %I.%I()', '@extschema@', v_source_queue_function);
    v_drop_q_table := format('DROP TABLE IF EXISTS %I.%I', '@extschema@', v_source_queue_table);

    RAISE NOTICE 'Removing mimeo objects from source database if they exist (trigger, function, queue table)';
    PERFORM dblink_exec(v_dblink_name, v_drop_trigger);
    PERFORM gdb(p_debug, v_drop_trigger);
    PERFORM dblink_exec(v_dblink_name, v_drop_function);
    PERFORM gdb(p_debug, v_drop_function);
    PERFORM dblink_exec(v_dblink_name, v_drop_q_table);
    PERFORM gdb(p_debug, v_drop_q_table);
    PERFORM dblink_disconnect(v_dblink_name);

    IF p_keep_table THEN 
        RAISE NOTICE 'Destination table NOT destroyed (if it existed): %', v_dest_table; 
    ELSE
        IF v_dest_schema_name IS NOT NULL AND v_dest_table_name IS NOT NULL THEN
            v_drop_dest_table := format('DROP TABLE IF EXISTS %I.%I', v_dest_schema_name, v_dest_table_name);
            PERFORM gdb(p_debug, v_drop_dest_table);
            EXECUTE v_drop_dest_table;
            RAISE NOTICE 'Destination table destroyed: %', v_dest_table;
        ELSE
            RAISE NOTICE 'Destination table did not exist: %', v_dest_table;
        END IF;
    END IF;

    RAISE NOTICE 'Removing config data';
    EXECUTE 'DELETE FROM @extschema@.refresh_config_dml WHERE dest_table = ' || quote_literal(v_dest_table);

    RAISE NOTICE 'Done';
END IF;

EXECUTE 'SELECT set_config(''search_path'','''||v_old_search_path||''',''false'')';

EXCEPTION
    WHEN OTHERS THEN
        EXECUTE 'SELECT '||v_dblink_schema||'.dblink_get_connections() @> ARRAY['||quote_literal(v_dblink_name)||']' INTO v_link_exists;
        IF v_link_exists THEN
            EXECUTE 'SELECT '||v_dblink_schema||'.dblink_disconnect('||quote_literal(v_dblink_name)||')';
        END IF;
        RAISE EXCEPTION '%', SQLERRM;
END
$$;

