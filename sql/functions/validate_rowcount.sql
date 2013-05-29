/*
 * Simple row count compare. 
 * For any replication type other than inserter/updater, this will fail to run if replication is currently running.
 * For any replication type other than inserter/updater, this will pause replication for the given table until validation is complete
 */
CREATE FUNCTION validate_rowcount(p_destination text, p_src_incr_less boolean DEFAULT false, p_debug boolean DEFAULT false, OUT match boolean, OUT source_count bigint, OUT dest_count bigint, OUT min_timestamp timestamptz, OUT max_timestamp timestamptz) RETURNS record
    LANGUAGE plpgsql
    AS $$
DECLARE

v_adv_lock          boolean;
v_adv_lock_hash1    text;
v_adv_lock_hash2    text;
v_condition         text;
v_control           text;
v_dblink            int;
v_dblink_name       text;
v_dblink_schema     text;
v_dest_table        text;
v_link_exists       boolean;
v_local_sql         text;
v_max_dest          timestamptz;
v_old_search_path   text;
v_remote_sql        text;
v_remote_min_sql    text;
v_source_min        timestamptz;
v_source_table      text;
v_type              text;

BEGIN

SELECT nspname INTO v_dblink_schema FROM pg_namespace n, pg_extension e WHERE e.extname = 'dblink' AND e.extnamespace = n.oid;

SELECT current_setting('search_path') INTO v_old_search_path;
EXECUTE 'SELECT set_config(''search_path'',''@extschema@,'||v_dblink_schema||',public'',''true'')';

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
    v_adv_lock_hash1 := 'refresh_snap';
    v_adv_lock_hash2 := 'Refresh Snap: '||v_dest_table;
    SELECT source_table INTO v_source_table FROM refresh_config_snap WHERE dest_table = v_dest_table;
WHEN 'inserter' THEN
    SELECT source_table, control INTO v_source_table, v_control FROM refresh_config_inserter WHERE dest_table = v_dest_table;
WHEN 'updater' THEN
    SELECT source_table, control INTO v_source_table, v_control FROM refresh_config_updater WHERE dest_table = v_dest_table;
WHEN 'dml' THEN
    v_adv_lock_hash1 := 'refresh_dml';
    v_adv_lock_hash2 := 'Refresh DML: '||v_dest_table;
    SELECT source_table INTO v_source_table FROM refresh_config_dml WHERE dest_table = v_dest_table;
WHEN 'logdel' THEN
    v_adv_lock_hash1 := 'refresh_logdel';
    v_adv_lock_hash2 := 'Refresh Log Del: '||v_dest_table;
    SELECT source_table INTO v_source_table FROM refresh_config_logdel WHERE dest_table = v_dest_table;
WHEN 'table' THEN
    v_adv_lock_hash1 := 'refresh_table';
    v_adv_lock_hash2 := v_dest_table;
    SELECT source_table INTO v_source_table FROM refresh_config_table WHERE dest_table = v_dest_table;
END CASE;

IF v_adv_lock_hash1 IS NOT NULL AND v_adv_lock_hash2 IS NOT NULL THEN
    v_adv_lock := pg_try_advisory_lock(hashtext(v_adv_lock_hash1), hashtext(v_adv_lock_hash2));
    IF v_adv_lock = 'false' THEN
        RAISE EXCEPTION 'Validation cannot run while refresh for given table is running: %', v_dest_table;
        RETURN;
    END IF;
END IF;

v_dblink_name := 'mimeo_data_validation_'||v_dest_table;
PERFORM dblink_connect(v_dblink_name, auth(v_dblink));

v_remote_sql := 'SELECT count(*) as row_count FROM '||v_source_table;
v_local_sql := 'SELECT count(*) FROM '||v_dest_table;
IF v_control IS NOT NULL THEN
    IF p_src_incr_less THEN  
        v_remote_min_sql := 'SELECT min('||v_control||') AS min_source FROM '||v_source_table;
        IF v_condition IS NOT NULL THEN
            v_remote_min_sql := v_remote_min_sql ||' '||v_condition;
        END IF;
        v_remote_min_sql := 'SELECT min_source FROM dblink('||quote_literal(v_dblink_name)||','||quote_literal(v_remote_min_sql)||') t (min_source timestamptz)';
        PERFORM gdb(p_debug, 'v_remote_min_sql: '||v_remote_min_sql);
        EXECUTE v_remote_min_sql INTO v_source_min;
        v_local_sql := v_local_sql || ' WHERE '||v_control|| ' >= '||quote_literal(v_source_min);
        min_timestamp := v_source_min;
    END IF;
    EXECUTE 'SELECT max('||quote_ident(v_control)||') FROM '||v_dest_table INTO v_max_dest;
    IF v_condition IS NOT NULL THEN
        v_remote_sql := v_remote_sql ||' '|| v_condition || ' AND ';
    ELSE
        v_remote_sql := v_remote_sql ||' WHERE ';
    END IF;
    v_remote_sql := v_remote_sql ||v_control||' <= '||quote_literal(v_max_dest);
    max_timestamp := v_max_dest;
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

IF v_adv_lock_hash1 IS NOT NULL AND v_adv_lock_hash2 IS NOT NULL THEN
    PERFORM pg_advisory_unlock(hashtext(v_adv_lock_hash1), hashtext(v_adv_lock_hash2));
END IF;

EXCEPTION
    WHEN QUERY_CANCELED OR OTHERS THEN
        EXECUTE 'SELECT '||v_dblink_schema||'.dblink_get_connections() @> ARRAY['||quote_literal(v_dblink_name)||']' INTO v_link_exists;
        IF v_link_exists THEN
            EXECUTE 'SELECT '||v_dblink_schema||'.dblink_disconnect('||quote_literal(v_dblink_name)||')';
        END IF;
        IF v_adv_lock_hash1 IS NOT NULL AND v_adv_lock_hash2 IS NOT NULL THEN
            PERFORM pg_advisory_unlock(hashtext(v_adv_lock_hash1), hashtext(v_adv_lock_hash2));
        END IF;
        RAISE EXCEPTION '%', SQLERRM;
END
$$;
