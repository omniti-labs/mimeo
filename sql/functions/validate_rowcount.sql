/*
 * Simple row count compare. 
 * Upper & lower interval values allow you to limit the block of rows that is compared for inserter or updater replication. The boundary is calculated from the max control column value on the destination backwards. So the rowcount is done as: WHERE control > (max - lower_interval) AND control < (max - upper_interval). If either is left NULL, then that compares all relevant rows.
 * For any replication type other than inserter/updater, this will fail to run if replication is currently running.
 * For any replication type other than inserter/updater, this will pause replication for the given table until validation is complete
 */
CREATE FUNCTION validate_rowcount(p_destination text, p_lower_interval text DEFAULT NULL, p_upper_interval text DEFAULT NULL, p_debug boolean DEFAULT false, OUT match boolean, OUT source_count bigint, OUT dest_count bigint, OUT min_dest_value text, OUT max_dest_value text) RETURNS record
    LANGUAGE plpgsql
    AS $$
DECLARE

v_adv_lock              boolean := true;
v_boundary              text;
v_condition             text;
v_control               text;
v_dblink                int;
v_dblink_name           text;
v_dblink_schema         text;
v_dest_table            text;
v_dest_schemaname       text;
v_dest_tablename        text;
v_link_exists           boolean;
v_local_sql             text;
v_lower_interval_time   interval;
v_lower_interval_id     bigint;
v_max_dest_serial       bigint;
v_max_dest_time         timestamptz;
v_min_dest_serial       bigint;
v_min_dest_time         timestamptz;
v_old_search_path       text;
v_remote_sql            text;
v_remote_min_sql        text;
v_source_table          text;
v_src_schemaname        text;
v_src_tablename         text;
v_type                  text;
v_upper_interval_time   interval;
v_upper_interval_id     bigint;

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

IF v_adv_lock = 'false' THEN
    RAISE EXCEPTION 'Validation cannot run while refresh for given table is running: %', v_dest_table;
    RETURN;
END IF;

IF v_type = 'inserter_time' OR v_type = 'inserter_serial' THEN
    SELECT control, boundary::text INTO v_control, v_boundary FROM refresh_config_inserter_time WHERE dest_table = v_dest_table;
ELSIF v_type = 'updater_time' OR v_type = 'updater_serial' THEN
    SELECT control, boundary::text INTO v_control, v_boundary FROM refresh_config_updater_serial WHERE dest_table = v_dest_table;
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

v_remote_sql := format('SELECT count(*) AS row_count FROM %I.%I', v_src_schemaname, v_src_tablename);
v_local_sql := format('SELECT count(*) AS row_count FROM %I.%I', v_dest_schemaname, v_dest_tablename);

IF v_condition IS NOT NULL THEN
    v_remote_sql := v_remote_sql ||' '|| v_condition;
END IF;

IF v_control IS NOT NULL THEN

    IF p_lower_interval IS NOT NULL THEN 
        
        IF v_condition IS NOT NULL THEN
            v_remote_sql := v_remote_sql || ' AND ';
            v_local_sql := v_local_sql || ' AND ';
        ELSE
            v_remote_sql := v_remote_sql ||' WHERE ';
            v_local_sql := v_local_sql ||' WHERE ';
        END IF;

        IF v_type IN ('inserter_time', 'updater_time') THEN
            EXECUTE format('SELECT max(%I) max_dest FROM %I.%I', v_control, v_dest_schemaname, v_dest_tablename) 
                INTO v_max_dest_time;

            v_min_dest_time := v_max_dest_time - p_lower_interval::interval; 
            v_remote_sql := v_remote_sql || format(' %I > %L ', v_control, v_min_dest_time);
            v_local_sql := v_local_sql || format(' %I > %L ', v_control, v_min_dest_time);
            min_dest_value := v_min_dest_time::text;

        ELSIF v_type IN ('inserter_serial', 'updater_serial') THEN
            EXECUTE format('SELECT max(%I) max_dest FROM %I.%I', v_control, v_dest_schemaname, v_dest_tablename) 
                INTO v_max_dest_serial;

            v_min_dest_serial := v_max_dest_serial - p_lower_interval::bigint; 
            v_remote_sql := v_remote_sql || format(' %I > %L ', v_control, v_min_dest_serial);
            v_local_sql := v_local_sql || format(' %I > %L ', v_control, v_min_dest_serial);
            min_dest_value := v_min_dest_serial::text;

        END IF;

    END IF; -- end p_lower_interval

    IF p_upper_interval IS NOT NULL OR v_boundary IS NOT NULL THEN

        IF p_upper_interval IS NULL THEN
            -- Just use existing boundary value to text to make following code easier (it's cast to text above)
            p_upper_interval := v_boundary;
        END IF;

        IF v_condition IS NOT NULL OR p_lower_interval IS NOT NULL THEN
            v_remote_sql := v_remote_sql || ' AND ';
            v_local_sql := v_local_sql || ' AND ';
        ELSE
            v_remote_sql := v_remote_sql ||' WHERE ';
            v_local_sql := v_local_sql ||' WHERE ';
        END IF;

        IF v_type IN ('inserter_time', 'updater_time') THEN
            EXECUTE format('SELECT max(%I) max_dest FROM %I.%I', v_control, v_dest_schemaname, v_dest_tablename) 
                INTO v_max_dest_time;

            v_max_dest_time := v_max_dest_time - p_upper_interval::interval;
            v_remote_sql := v_remote_sql || format(' %I < %L ', v_control, v_max_dest_time);
            v_local_sql := v_local_sql || format(' %I < %L ', v_control, v_max_dest_time);
            max_dest_value := v_max_dest_time::text;
        ELSIF v_type ('inserter-serial', 'updater-serial') THEN
            EXECUTE format('SELECT max(%I) max_dest FROM %I.%I', v_control, v_dest_schemaname, v_dest_tablename) 
                INTO v_max_dest_serial;

            v_max_dest_serial := v_max_dest_serial - p_upper_interval::bigint;
            v_remote_sql := v_remote_sql || format(' %I < %L ', v_control, v_max_dest_serial);
            v_local_sql := v_local_sql || format(' %I < %L ', v_control, v_max_dest_serial);
            max_dest_value := v_max_dest_serial::text;
        END IF;

    END IF; -- end p_upper_interval
    
END IF; -- end v_control check

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

