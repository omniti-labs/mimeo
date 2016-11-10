-- Snapshot maker now gives a clearer error when the destination table already exists and clarifies that it cannot (Github Issue #18).
-- Further fixes to validate_rowcount() for incremental replication. May have been giving incorrect match failure before due to not always setting the proper upper boundary value.


/*
 *  Snapshot maker function.
 */
CREATE OR REPLACE FUNCTION snapshot_maker(
    p_src_table text
    , p_dblink_id int
    , p_dest_table text DEFAULT NULL
    , p_index boolean DEFAULT true
    , p_filter text[] DEFAULT NULL
    , p_condition text DEFAULT NULL
    , p_pulldata boolean DEFAULT true
    , p_jobmon boolean DEFAULT NULL
    , p_debug boolean DEFAULT false) 
RETURNS void
    LANGUAGE plpgsql
    AS $$
DECLARE

v_data_source               text;
v_dest_exists               text;
v_insert_refresh_config     text;
v_job_id                    bigint;
v_job_name                  text;
v_jobmon                    boolean;
v_jobmon_schema             text;
v_old_search_path           text;
v_step_id                   bigint;

BEGIN

SELECT nspname INTO v_jobmon_schema FROM pg_namespace n, pg_extension e WHERE e.extname = 'pg_jobmon' AND e.extnamespace = n.oid;
SELECT current_setting('search_path') INTO v_old_search_path;
EXECUTE 'SELECT set_config(''search_path'',''@extschema@,'||COALESCE(v_jobmon_schema||',', '')||'public'',''false'')';

SELECT data_source INTO v_data_source FROM @extschema@.dblink_mapping_mimeo WHERE data_source_id = p_dblink_id; 
IF NOT FOUND THEN
	RAISE EXCEPTION 'ERROR: Database link ID does not exist in @extschema@.dblink_mapping_mimeo: %', p_dblink_id; 
END IF;  

IF p_dest_table IS NULL THEN
    p_dest_table := p_src_table;
END IF;

IF position('.' in p_dest_table) = 0 AND position('.' in p_src_table) = 0 THEN
    RAISE EXCEPTION 'Source (and destination) table must be schema qualified';
END IF;

SELECT tablename INTO v_dest_exists
FROM pg_catalog.pg_tables
WHERE schemaname = split_part(p_dest_table, '.', 1)::name
AND tablename = split_part(p_dest_table, '.', 2)::name;
    IF v_dest_exists IS NOT NULL THEN
        RAISE EXCEPTION 'Destination table cannot exist before running snapshot_maker(): %', p_dest_table;
    END IF;

IF p_jobmon IS TRUE AND v_jobmon_schema IS NULL THEN
    RAISE EXCEPTION 'p_jobmon parameter set to TRUE, but unable to determine if pg_jobmon extension is installed';
ELSIF (p_jobmon IS TRUE OR p_jobmon IS NULL) AND v_jobmon_schema IS NOT NULL THEN
    v_jobmon := true;
ELSE
    v_jobmon := false;
END IF;

v_job_name := 'Snapshot Maker: '||p_src_table;

IF v_jobmon THEN
    v_job_id := add_job(v_job_name);
    PERFORM gdb(p_debug,'Job ID: '||v_job_id::text);
    v_step_id := add_step(v_job_id,'Inserting config data');
END IF;

v_insert_refresh_config := 'INSERT INTO @extschema@.refresh_config_snap(
        source_table
        , dest_table
        , dblink
        , filter
        , condition
        , jobmon) 
    VALUES('
        ||quote_literal(p_src_table)
        ||', '||quote_literal(p_dest_table)
        ||', '||p_dblink_id
        ||', '||COALESCE(quote_literal(p_filter), 'NULL')
        ||', '||COALESCE(quote_literal(p_condition), 'NULL')
        ||', '||v_jobmon||')';

EXECUTE v_insert_refresh_config;

IF v_jobmon THEN
    PERFORM update_step(v_step_id, 'OK','Done');
END IF;

IF v_jobmon THEN
    v_step_id := add_step(v_job_id,'Running first snapshot. See separate refresh job for more details.');
END IF;

RAISE NOTICE 'attempting first snapshot';
EXECUTE 'SELECT @extschema@.refresh_snap('||quote_literal(p_dest_table)||', p_index := '||p_index||', p_pulldata := '||p_pulldata||', p_debug := '||p_debug||')'; 

IF v_jobmon THEN
    PERFORM update_step(v_step_id, 'OK','Done');
    v_step_id := add_step(v_job_id,'Running second snapshot. See separate refresh job for more details.');
END IF;

RAISE NOTICE 'attempting second snapshot';
EXECUTE 'SELECT @extschema@.refresh_snap('||quote_literal(p_dest_table)||', p_index := '||p_index||', p_pulldata := '||p_pulldata||', p_debug := '||p_debug||')';

IF v_jobmon THEN
    PERFORM update_step(v_step_id, 'OK','Done');
END IF;

IF v_jobmon THEN
    PERFORM close_job(v_job_id);
END IF;

EXECUTE 'SELECT set_config(''search_path'','''||v_old_search_path||''',''false'')';

RAISE NOTICE 'Done';

RETURN;

EXCEPTION
    WHEN OTHERS THEN
        SELECT nspname INTO v_jobmon_schema FROM pg_namespace n, pg_extension e WHERE e.extname = 'pg_jobmon' AND e.extnamespace = n.oid;
        SELECT jobmon INTO v_jobmon FROM @extschema@.refresh_config_snap WHERE dest_table = p_src_table;
        v_jobmon := COALESCE(p_jobmon, v_jobmon);
        IF v_jobmon_schema IS NULL THEN
            v_jobmon := false;
        END IF;
        IF v_jobmon THEN
            IF v_job_id IS NULL THEN
                EXECUTE format('SELECT %I.add_job(%L)', v_jobmon_schema, 'Snapshot Maker: '||p_src_table) INTO v_job_id;
                EXECUTE format('SELECT %I.add_step(%L, %L)', v_jobmon_schema, v_job_id, 'EXCEPTION before job logging started') INTO v_step_id;
            END IF;
            IF v_step_id IS NULL THEN
                EXECUTE format('SELECT %I.add_step(%L, %L)', v_jobmon_schema, v_job_id, 'EXCEPTION before first step logged') INTO v_step_id;
            END IF;
                  EXECUTE format('SELECT %I.update_step(%L, %L, %L)', v_jobmon_schema, v_step_id, 'CRITICAL', 'ERROR: '||COALESCE(SQLERRM,'unknown'));
            EXECUTE format('SELECT %I.fail_job(%L)', v_jobmon_schema, v_job_id);
        END IF;
        RAISE EXCEPTION '%', SQLERRM;
END
$$;


/*
 * Simple row count compare. 
 * For any replication type other than inserter/updater, this will fail to run if replication is currently running.
 * For any replication type other than inserter/updater, this will pause replication for the given table until validation is complete
 */
CREATE OR REPLACE FUNCTION validate_rowcount(p_destination text, p_src_incr_less boolean DEFAULT false, p_incr_interval text DEFAULT NULL, p_debug boolean DEFAULT false, OUT match boolean, OUT source_count bigint, OUT dest_count bigint, OUT min_source_value text, OUT max_source_value text) RETURNS record
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

