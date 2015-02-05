/*
 * Simple row count compare. 
 * For any replication type other than inserter/updater, this will fail to run if replication is currently running.
 * For any replication type other than inserter/updater, this will pause replication for the given table until validation is complete
 */
CREATE FUNCTION validate_rowcount(p_destination text, p_src_incr_less boolean DEFAULT false, p_debug boolean DEFAULT false, OUT match boolean, OUT source_count bigint, OUT dest_count bigint, OUT min_source_value text, OUT max_source_value text) RETURNS record
    LANGUAGE plpgsql
    AS $$
DECLARE

v_adv_lock          boolean := true;
v_condition         text;
v_control           text;
v_dblink            int;
v_dblink_name       text;
v_dblink_schema     text;
v_dest_table        text;
v_link_exists       boolean;
v_local_sql         text;
v_max_dest_serial   bigint;
v_max_dest_time     timestamptz;
v_old_search_path   text;
v_remote_sql        text;
v_remote_min_sql    text;
v_source_min_serial bigint;
v_source_min_time   timestamptz;
v_source_table      text;
v_type              text;

BEGIN

SELECT nspname INTO v_dblink_schema FROM pg_namespace n, pg_extension e WHERE e.extname = 'dblink' AND e.extnamespace = n.oid;

SELECT current_setting('search_path') INTO v_old_search_path;
EXECUTE 'SELECT set_config(''search_path'',''@extschema@,'||v_dblink_schema||',public'',''true'')';

v_dblink_name := @extschema@.check_name_length('mimeo_data_validation_'||p_destination);

SELECT dest_table
    , type
    , dblink
    , condition
INTO v_dest_table
    , v_type
    , v_dblink
    , v_condition
FROM refresh_config
WHERE dest_table = p_destination; 
IF NOT FOUND THEN
   RAISE EXCEPTION 'ERROR: This table is not set up for replication: %', p_destination; 
END IF;

CASE v_type
WHEN 'snap' THEN
    v_adv_lock := @extschema@.concurrent_lock_check(v_dest_table);
    SELECT source_table INTO v_source_table FROM refresh_config_snap WHERE dest_table = v_dest_table;
WHEN 'inserter_time' THEN
    SELECT source_table, control INTO v_source_table, v_control FROM refresh_config_inserter WHERE dest_table = v_dest_table;
WHEN 'inserter_serial' THEN
    SELECT source_table, control INTO v_source_table, v_control FROM refresh_config_inserter WHERE dest_table = v_dest_table;
WHEN 'updater_time' THEN
    SELECT source_table, control INTO v_source_table, v_control FROM refresh_config_updater WHERE dest_table = v_dest_table;
WHEN 'updater_serial' THEN
    SELECT source_table, control INTO v_source_table, v_control FROM refresh_config_updater WHERE dest_table = v_dest_table;
WHEN 'dml' THEN
    v_adv_lock := @extschema@.concurrent_lock_check(v_dest_table);
    SELECT source_table INTO v_source_table FROM refresh_config_dml WHERE dest_table = v_dest_table;
WHEN 'logdel' THEN
    v_adv_lock := @extschema@.concurrent_lock_check(v_dest_table);
    SELECT source_table INTO v_source_table FROM refresh_config_logdel WHERE dest_table = v_dest_table;
WHEN 'table' THEN
    v_adv_lock := @extschema@.concurrent_lock_check(v_dest_table);
    SELECT source_table INTO v_source_table FROM refresh_config_table WHERE dest_table = v_dest_table;
END CASE;

IF v_adv_lock = 'false' THEN
    RAISE EXCEPTION 'Validation cannot run while refresh for given table is running: %', v_dest_table;
    RETURN;
END IF;

PERFORM dblink_connect(v_dblink_name, auth(v_dblink));

v_remote_sql := 'SELECT count(*) as row_count FROM '||v_source_table;
v_local_sql := 'SELECT count(*) FROM '||v_dest_table;
IF v_control IS NOT NULL THEN
    IF p_src_incr_less THEN  
        v_remote_min_sql := 'SELECT min('||v_control||') AS min_source FROM '||v_source_table;
        IF v_condition IS NOT NULL THEN
            v_remote_min_sql := v_remote_min_sql ||' '||v_condition;
        END IF;
        IF v_type = 'inserter_time' OR v_type = 'updater_time' THEN
            v_remote_min_sql := 'SELECT min_source FROM dblink('||quote_literal(v_dblink_name)||','||quote_literal(v_remote_min_sql)||') t (min_source timestamptz)';
            PERFORM gdb(p_debug, 'v_remote_min_sql: '||v_remote_min_sql);
            EXECUTE v_remote_min_sql INTO v_source_min_time;
            v_local_sql := v_local_sql || ' WHERE '||v_control|| ' >= '||quote_literal(v_source_min_time);
            min_source_value := v_source_min_time::text;
        ELSIF v_type = 'inserter_serial' OR v_type = 'updater_serial' THEN
            v_remote_min_sql := 'SELECT min_source FROM dblink('||quote_literal(v_dblink_name)||','||quote_literal(v_remote_min_sql)||') t (min_source bigint)';
            PERFORM gdb(p_debug, 'v_remote_min_sql: '||v_remote_min_sql);
            EXECUTE v_remote_min_sql INTO v_source_min_serial;
            v_local_sql := v_local_sql || ' WHERE '||v_control|| ' >= '||quote_literal(v_source_min_serial);
            min_source_value := v_source_min_serial::text;
        END IF;
    END IF;

    IF v_condition IS NOT NULL THEN
        v_remote_sql := v_remote_sql ||' '|| v_condition || ' AND ';
    ELSE
        v_remote_sql := v_remote_sql ||' WHERE ';
    END IF;

    IF v_type = 'inserter_time' OR v_type = 'updater_time' THEN
        EXECUTE 'SELECT max('||quote_ident(v_control)||') FROM '||v_dest_table INTO v_max_dest_time;
        v_remote_sql := v_remote_sql ||v_control||' <= '||quote_literal(v_max_dest_time);
        max_source_value := v_max_dest_time::text;
    ELSIF v_type = 'inserter_serial' OR v_type = 'updater_serial' THEN
        EXECUTE 'SELECT max('||quote_ident(v_control)||') FROM '||v_dest_table INTO v_max_dest_serial;
        v_remote_sql := v_remote_sql ||v_control||' <= '||quote_literal(v_max_dest_serial);
        max_source_value := v_max_dest_serial::text;
    END IF;
    
ELSIF v_condition IS NOT NULL THEN
    v_remote_sql := v_remote_sql ||' '|| v_condition;
END IF;

v_remote_sql := 'SELECT row_count FROM dblink('||quote_literal(v_dblink_name)||','||quote_literal(v_remote_sql)||') t (row_count bigint)';
PERFORM gdb(p_debug, 'v_remote_sql: '||v_remote_sql);
EXECUTE v_remote_sql INTO source_count;
PERFORM gdb(p_debug, 'v_local_sql: '||v_local_sql);
EXECUTE v_local_sql INTO dest_count;

IF source_count = dest_count THEN
    match = true;
ELSE
    match = false;
END IF;

PERFORM dblink_disconnect(v_dblink_name);

EXCEPTION
    WHEN QUERY_CANCELED OR OTHERS THEN
        EXECUTE 'SELECT '||v_dblink_schema||'.dblink_get_connections() @> ARRAY['||quote_literal(v_dblink_name)||']' INTO v_link_exists;
        IF v_link_exists THEN
            EXECUTE 'SELECT '||v_dblink_schema||'.dblink_disconnect('||quote_literal(v_dblink_name)||')';
        END IF;
        RAISE EXCEPTION '%', SQLERRM;
END
$$;

