-- Added interval option to validate_rowcount() function. This allows the validation to ignore more recent data with incremental replication. For example, if you'd like to ignore the most recent 2 days of data (for a time-based control column), you'd set this parameter to '2 days'. 
    -- The current max value of the destination table is used as the baseline value when subtracting the given interval.
    -- Note the parameter is of type text but the value must be able to be cast to either an interval or integer data type.

-- Preserve privileges of dropped functions
CREATE TEMP TABLE mimeo_preserve_privs_temp (statement text);

INSERT INTO mimeo_preserve_privs_temp 
SELECT 'GRANT EXECUTE ON FUNCTION @extschema@.validate_rowcount(text, boolean, text, boolean) TO '||string_agg(grantee::text, ',')||';' 
FROM information_schema.routine_privileges
WHERE routine_schema = '@extschema@'
AND routine_name = 'validate_rowcount'; 

DROP FUNCTION validate_rowcount(text, boolean, boolean);


/*
 * Simple row count compare. 
 * For any replication type other than inserter/updater, this will fail to run if replication is currently running.
 * For any replication type other than inserter/updater, this will pause replication for the given table until validation is complete
 */
CREATE FUNCTION validate_rowcount(p_destination text, p_src_incr_less boolean DEFAULT false, p_incr_interval text DEFAULT NULL, p_debug boolean DEFAULT false, OUT match boolean, OUT source_count bigint, OUT dest_count bigint, OUT min_source_value text, OUT max_source_value text) RETURNS record
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
v_dest_schemaname   text;
v_dest_tablename    text;
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
v_src_schemaname    text;
v_src_tablename     text;
v_type              text;

BEGIN

SELECT nspname INTO v_dblink_schema FROM pg_catalog.pg_namespace n, pg_catalog.pg_extension e WHERE e.extname = 'dblink' AND e.extnamespace = n.oid;

SELECT current_setting('search_path') INTO v_old_search_path;
EXECUTE 'SELECT set_config(''search_path'',''@extschema@,'||v_dblink_schema||',public'',''true'')';

v_dblink_name := @extschema@.check_name_length('mimeo_data_validation_'||p_destination);

SELECT dest_table
    , type
    , dblink
    , condition
    , source_table
INTO v_dest_table
    , v_type
    , v_dblink
    , v_condition
    , v_source_table
FROM refresh_config
WHERE dest_table = p_destination; 
IF NOT FOUND THEN
   RAISE EXCEPTION 'ERROR: This table is not set up for replication: %', p_destination; 
END IF;

IF v_type = 'snap' OR v_type = 'dml' OR v_type = 'logdel' OR v_type = 'table' THEN
    v_adv_lock := @extschema@.concurrent_lock_check(v_dest_table);
END IF;

IF v_type = 'inserter_time' OR v_type = 'inserter_serial' THEN
    SELECT control INTO v_control FROM refresh_config_inserter WHERE dest_table = v_dest_table;
ELSIF v_type = 'updater_time' OR v_type = 'updater_serial' THEN
    SELECT control INTO v_control FROM refresh_config_updater WHERE dest_table = v_dest_table;
END IF;

IF v_adv_lock = 'false' THEN
    RAISE EXCEPTION 'Validation cannot run while refresh for given table is running: %', v_dest_table;
    RETURN;
END IF;

PERFORM dblink_connect(v_dblink_name, auth(v_dblink));

v_remote_sql := format('
            SELECT schemaname, tablename
            FROM (
                SELECT schemaname, tablename 
                FROM pg_catalog.pg_tables 
                WHERE schemaname ||''.''|| tablename = %L
                UNION
                SELECT schemaname, viewname AS tablename
                FROM pg_catalog.pg_views
                WHERE schemaname || ''.'' || viewname = %L
            ) tables LIMIT 1'
        , v_source_table, v_source_table);
v_remote_sql := format('SELECT schemaname, tablename FROM dblink(%L, %L) t (schemaname text, tablename text)', v_dblink_name, v_remote_sql);
EXECUTE v_remote_sql INTO v_src_schemaname, v_src_tablename;

SELECT schemaname, tablename
FROM pg_catalog.pg_tables WHERE schemaname||'.'||tablename = v_dest_table
UNION
SELECT schemaname, viewname AS tablename
FROM pg_catalog.pg_views WHERE schemaname||'.'||viewname = v_dest_table
INTO v_dest_schemaname, v_dest_tablename;

v_remote_sql := format('SELECT count(*) as row_count FROM %I.%I', v_src_schemaname, v_src_tablename);
v_local_sql := format('SELECT count(*) FROM %I.%I', v_dest_schemaname, v_dest_tablename);
IF v_control IS NOT NULL THEN

    IF v_condition IS NOT NULL THEN
        v_remote_sql := v_remote_sql ||' '|| v_condition || ' AND ';
    ELSE
        v_remote_sql := v_remote_sql ||' WHERE ';
    END IF;

    IF p_src_incr_less THEN  
        v_remote_min_sql := format('SELECT min(%I) AS min_source FROM %I.%I', v_control, v_src_schemaname, v_src_tablename);
        PERFORM gdb(p_debug, 'v_remote_min_sql: '||v_remote_min_sql);
        IF v_condition IS NOT NULL THEN
            v_remote_min_sql := v_remote_min_sql ||' '||v_condition;
        END IF;
        IF v_type = 'inserter_time' OR v_type = 'updater_time' THEN
            v_remote_min_sql := 'SELECT min_source FROM dblink('||quote_literal(v_dblink_name)||','||quote_literal(v_remote_min_sql)||') t (min_source timestamptz)';
            PERFORM gdb(p_debug, 'v_remote_min_sql: '||v_remote_min_sql);
            EXECUTE v_remote_min_sql INTO v_source_min_time;
            v_local_sql := format(v_local_sql || ' WHERE %I >= %L', v_control, COALESCE(v_source_min_time, '-infinity'));
            min_source_value := v_source_min_time::text;
        ELSIF v_type = 'inserter_serial' OR v_type = 'updater_serial' THEN
            v_remote_min_sql := 'SELECT min_source FROM dblink('||quote_literal(v_dblink_name)||','||quote_literal(v_remote_min_sql)||') t (min_source bigint)';
            PERFORM gdb(p_debug, 'v_remote_min_sql: '||v_remote_min_sql);
            EXECUTE v_remote_min_sql INTO v_source_min_serial;
            v_local_sql := format(v_local_sql || ' WHERE %I >= %L', v_control, COALESCE(v_source_min_serial, 0));
            min_source_value := v_source_min_serial::text;
        END IF;
    END IF;

    IF v_type = 'inserter_time' OR v_type = 'updater_time' THEN
        EXECUTE format('SELECT max(%I) FROM %I.%I', v_control, v_dest_schemaname, v_dest_tablename) INTO v_max_dest_time;
        -- Reduce the max value being compared by the given interval value
        IF p_incr_interval IS NOT NULL AND v_max_dest_time IS NOT NULL THEN
            v_max_dest_time := v_max_dest_time - p_incr_interval::interval;
            IF p_src_incr_less THEN
                v_local_sql := v_local_sql || ' AND ';
            ELSE
                v_local_sql := v_local_sql || ' WHERE ';
            END IF;
            v_local_sql := format(v_local_sql || ' %I <= %L', v_control, v_max_dest_time);
        END IF;
        v_remote_sql := format(v_remote_sql ||' %I <= %L', v_control, COALESCE(v_max_dest_time, 'infinity'));
        max_source_value := v_max_dest_time::text;
    ELSIF v_type = 'inserter_serial' OR v_type = 'updater_serial' THEN
        EXECUTE format('SELECT max(%I) FROM %I.%I', v_control, v_dest_schemaname, v_dest_tablename) INTO v_max_dest_serial;
        -- Reduce the max value being compared by the given interval value
        IF p_incr_interval IS NOT NULL AND v_max_dest_serial IS NOT NULL THEN
            v_max_dest_serial := v_max_dest_serial - p_incr_interval::bigint;
            IF p_src_incr_less THEN
                v_local_sql := v_local_sql || ' AND ';
            ELSE
                v_local_sql := v_local_sql || ' WHERE ';
            END IF;
            v_local_sql := format(v_local_sql || ' %I <= %L', v_control, v_max_dest_serial);
        END IF;
        v_remote_sql := format(v_remote_sql ' %I <= %L', v_control, COALESCE(v_max_dest_serial, 0));
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
        SELECT nspname INTO v_dblink_schema FROM pg_catalog.pg_namespace n, pg_catalog.pg_extension e WHERE e.extname = 'dblink' AND e.extnamespace = n.oid;
        EXECUTE format('SELECT %I.dblink_get_connections() @> ARRAY[%L]', v_dblink_schema, v_dblink_name) INTO v_link_exists;
        IF v_link_exists THEN
            EXECUTE format('SELECT %I.dblink_disconnect(%L)', v_dblink_schema, v_dblink_name);
        END IF;
        RAISE EXCEPTION '%', SQLERRM;
END
$$;

-- Restore dropped object privileges
DO $$
DECLARE
v_row   record;
BEGIN
    FOR v_row IN SELECT statement FROM mimeo_preserve_privs_temp LOOP
        IF v_row.statement IS NOT NULL THEN
            EXECUTE v_row.statement;
        END IF;
    END LOOP;
END
$$;

DROP TABLE mimeo_preserve_privs_temp;
