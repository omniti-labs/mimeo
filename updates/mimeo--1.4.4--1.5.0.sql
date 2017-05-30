-- Fixed incremental time refresh to work properly when the source table is initially empty and later starts getting data. Previously the lower boundary was always being reset to the value of CURRENT_TIMESTAMP if the destination remained empty after setup. This could cause data that is later added on the source to then be missed if it happens to be outside the current window. Now the lower boundary remains the timestamp at the time of replication setup (as the documentation states) until newer data is added to the source. Then replication begins to work as normal and obeys the boundary configuration as well.
    -- NOTE: Recommended to do a data validation check on any incremental replication tables that started out empty when they were created.

-- Reworked validate_rowcount() function significantly to allow more customizable checks for inserter & updater validation. Please review changes carefully if you regularly use this function as both the input & output parameters have changed.
    -- There are now lower and upper interval parameters to determine the block of data to compare. The value is always calculated from the maximum control value on the destination. So the lower boundary value is (max - lower) and the upper boundary is (max - upper).
    -- If these values are left NULL, then all rows are compared.
    -- The output values are the conditional values used in the count query and they are now obtained from the destination, not the source.

-- Added new configuration option to dml & logdel replication to reduce transaction times on the source database at the expense of higher disk usage on the destination: insert_on_fetch. Default behavior has not changed so this is only relevant if the option is manually set. See documentation for further explanation of this option.

-- Added new function: snapshot_monitor(). This can monitor for when snapshot tables are possibly growing too large to be easily replicated in their entirety every refresh run. Tables being returned can then be considered for another replication method (incremental or dml). Paramenters are available for setting minimum size and rowcount amounts. Thanks to Nicole Daley (nicoledaley) for the work on this feature.

ALTER TABLE @extschema@.refresh_config_dml ADD insert_on_fetch boolean NOT NULL DEFAULT true;
ALTER TABLE @extschema@.refresh_config_logdel ADD insert_on_fetch boolean NOT NULL DEFAULT true;

-- Preserve privileges of dropped functions
CREATE TEMP TABLE mimeo_preserve_privs_temp (statement text);

INSERT INTO mimeo_preserve_privs_temp 
SELECT 'GRANT EXECUTE ON FUNCTION @extschema@.validate_rowcount(text, text, text, boolean) TO '||string_agg(grantee::text, ',')||';' 
FROM information_schema.routine_privileges
WHERE routine_schema = '@extschema@'
AND routine_name = 'validate_rowcount'; 

INSERT INTO mimeo_preserve_privs_temp 
SELECT 'GRANT EXECUTE ON FUNCTION @extschema@.refresh_dml(text, int, boolean, boolean, int, boolean, boolean) TO '||string_agg(grantee::text, ',')||';' 
FROM information_schema.routine_privileges
WHERE routine_schema = '@extschema@'
AND routine_name = 'refresh_dml'; 

INSERT INTO mimeo_preserve_privs_temp 
SELECT 'GRANT EXECUTE ON FUNCTION @extschema@.refresh_logdel(text, int, boolean, boolean, int, boolean, boolean) TO '||string_agg(grantee::text, ',')||';' 
FROM information_schema.routine_privileges
WHERE routine_schema = '@extschema@'
AND routine_name = 'refresh_logdel'; 

DROP FUNCTION validate_rowcount(text, boolean, text, boolean);
DROP FUNCTION refresh_dml(text, int, boolean, boolean, int, boolean);
DROP FUNCTION refresh_logdel(text, int, boolean, boolean, int, boolean);

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



/*
 *  Inserter maker function. 
 */
CREATE OR REPLACE FUNCTION inserter_maker(
    p_src_table text
    , p_type text
    , p_control_field text
    , p_dblink_id int
    , p_boundary text DEFAULT NULL
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

v_boundary_serial           int;
v_boundary_time             interval;
v_data_source               text;
v_dest_schema_name          text;
v_dest_table_name           text;
v_dst_active                boolean;
v_job_id                    bigint;
v_jobmon                    boolean;
v_job_name                  text;
v_jobmon_schema             text;
v_insert_refresh_config     text;
v_max_id                    bigint;
v_max_timestamp             timestamptz;
v_old_search_path           text;
v_sql                       text;
v_src_schema_name           text;
v_src_table_name            text;
v_step_id                   bigint;
v_table_exists              boolean;

BEGIN

SELECT nspname INTO v_jobmon_schema FROM pg_namespace n, pg_extension e WHERE e.extname = 'pg_jobmon' AND e.extnamespace = n.oid;
SELECT current_setting('search_path') INTO v_old_search_path;
EXECUTE 'SELECT set_config(''search_path'',''@extschema@,'||COALESCE(v_jobmon_schema||',', '')||'public'',''false'')';

SELECT data_source INTO v_data_source FROM @extschema@.dblink_mapping_mimeo WHERE data_source_id = p_dblink_id; 
IF NOT FOUND THEN
    RAISE EXCEPTION 'Database link ID is incorrect %', p_dblink_id; 
END IF;

IF (p_type <> 'time' AND p_type <> 'serial') OR p_type IS NULL THEN
    RAISE EXCEPTION 'Invalid inserter type: %. Must be either "time" or "serial"', p_type;
END IF;

IF p_dest_table IS NULL THEN
    p_dest_table := p_src_table;
END IF;

IF position('.' in p_dest_table) > 0 AND position('.' in p_src_table) > 0 THEN
    -- Do nothing. Schema & table variable names set below after table is created
ELSE
    RAISE EXCEPTION 'Source (and destination) table must be schema qualified';
END IF;

IF p_jobmon IS TRUE AND v_jobmon_schema IS NULL THEN
    RAISE EXCEPTION 'p_jobmon parameter set to TRUE, but unable to determine if pg_jobmon extension is installed';
ELSIF (p_jobmon IS TRUE OR p_jobmon IS NULL) AND v_jobmon_schema IS NOT NULL THEN
    v_jobmon := true;
ELSE
    v_jobmon := false;
END IF;

v_job_name := 'Inserter Maker: '||p_src_table;

IF v_jobmon THEN
    v_job_id := add_job(v_job_name);
    PERFORM gdb(p_debug,'Job ID: '||v_job_id::text);
    v_step_id := add_step(v_job_id,'Adding configuration data');
END IF;

IF p_type = 'time' THEN
    v_dst_active := @extschema@.dst_utc_check();
    IF p_boundary IS NULL THEN
        v_boundary_time = '10 minutes'::interval;
    ELSE
        v_boundary_time = p_boundary::interval;
    END IF;
    v_insert_refresh_config := 'INSERT INTO @extschema@.refresh_config_inserter_time (
            source_table
            , type
            , dest_table
            , dblink
            , control
            , boundary
            , last_value
            , last_run
            , dst_active
            , filter
            , condition
            , jobmon ) 
        VALUES('
            ||quote_literal(p_src_table)
            ||', '||quote_literal('inserter_time')
            ||', '||quote_literal(p_dest_table)
            ||', '||quote_literal(p_dblink_id)
            ||', '||quote_literal(p_control_field)
            ||', '||quote_literal(v_boundary_time)
            ||', '||quote_literal('-infinity')
            ||', '||quote_literal(CURRENT_TIMESTAMP)
            ||', '||v_dst_active
            ||', '||COALESCE(quote_literal(p_filter), 'NULL')
            ||', '||COALESCE(quote_literal(p_condition), 'NULL')
            ||', '||v_jobmon||')';
ELSIF p_type = 'serial' THEN
    IF p_boundary IS NULL THEN
        v_boundary_serial = 10;
    ELSE
        v_boundary_serial = p_boundary::int;
    END IF;
    v_insert_refresh_config := 'INSERT INTO @extschema@.refresh_config_inserter_serial (
            source_table
            , type
            , dest_table
            , dblink
            , control
            , boundary
            , last_value
            , last_run
            , filter
            , condition
            , jobmon ) 
        VALUES('
            ||quote_literal(p_src_table)
            ||', '||quote_literal('inserter_serial')
            ||', '||quote_literal(p_dest_table)
            ||', '||quote_literal(p_dblink_id)
            ||', '||quote_literal(p_control_field)
            ||', '||quote_literal(v_boundary_serial)
            ||', '||quote_literal(0)
            ||', '||quote_literal(CURRENT_TIMESTAMP)
            ||', '||COALESCE(quote_literal(p_filter), 'NULL')
            ||', '||COALESCE(quote_literal(p_condition), 'NULL')
            ||', '||v_jobmon||')';
ELSE
    RAISE EXCEPTION 'Invalid inserter type: %. Must be either "time" or "serial"', p_type;
END IF;

PERFORM @extschema@.gdb(p_debug, 'v_insert_refresh_config: '||v_insert_refresh_config);
EXECUTE v_insert_refresh_config;

IF v_jobmon THEN
    PERFORM update_step(v_step_id, 'OK','Done');
END IF;

SELECT p_table_exists, p_source_schema_name, p_source_table_name INTO v_table_exists, v_src_schema_name, v_src_table_name
FROM @extschema@.manage_dest_table(p_dest_table, NULL, NULL, p_debug);

SELECT schemaname, tablename 
INTO v_dest_schema_name, v_dest_table_name 
FROM pg_catalog.pg_tables 
WHERE schemaname||'.'||tablename = p_dest_table;

IF p_pulldata AND v_table_exists = false THEN
    IF v_jobmon THEN
        v_step_id := add_step(v_job_id,'Pulling data from source. Check for new job entry under REFRESH INSERTER for current status if this step has not finished');
    END IF;
    RAISE NOTICE 'Pulling all data from source...';
    EXECUTE 'SELECT @extschema@.refresh_inserter('||quote_literal(p_dest_table)||', p_repull := true, p_debug := '||p_debug||')';
    IF v_jobmon THEN
        PERFORM update_step(v_step_id, 'OK','Done');
    END IF;
END IF;

IF p_index AND v_table_exists = false THEN
    IF v_jobmon THEN
        v_step_id := add_step(v_job_id,'Creating indexes on destination table');
    END IF;
    PERFORM @extschema@.create_index(p_dest_table, v_src_schema_name, v_src_table_name, NULL, p_debug);
    IF v_jobmon THEN
        PERFORM update_step(v_step_id, 'OK','Done');
    END IF;
END IF;

IF v_table_exists THEN
    RAISE NOTICE 'Destination table % already exists. No data or indexes were pulled from source', p_dest_table;
    IF v_jobmon THEN
        v_step_id := add_step(v_job_id, format('Destination table %s.%s already exists. No data or indexes were pulled from source', v_dest_schema_name, v_dest_table_name));
        PERFORM update_step(v_step_id, 'OK','Done');
    END IF;
END IF;

RAISE NOTICE 'Analyzing destination table...';
IF v_jobmon THEN
    v_step_id := add_step(v_job_id, 'Analyzing destination table');
END IF;
EXECUTE format('ANALYZE %I.%I', v_dest_schema_name, v_dest_table_name);
IF v_jobmon THEN
    PERFORM update_step(v_step_id, 'OK','Done');
END IF;

IF v_jobmon THEN
    v_step_id := add_step(v_job_id, 'Obtaining max destination value of control column for config table');
END IF;
v_sql := format('SELECT max(%I) FROM %I.%I', p_control_field, v_dest_schema_name, v_dest_table_name);
PERFORM @extschema@.gdb(p_debug, v_sql);
IF p_type = 'time' THEN
    RAISE NOTICE 'Getting the maximum destination timestamp...';
    EXECUTE v_sql INTO v_max_timestamp;
    v_sql := format('UPDATE %I.refresh_config_inserter_time SET last_value = %L WHERE dest_table = %L'
                    , '@extschema@'
                    , COALESCE(v_max_timestamp, CURRENT_TIMESTAMP)
                    , p_dest_table);
    PERFORM @extschema@.gdb(p_debug, v_sql);
    EXECUTE v_sql;
ELSIF p_type = 'serial' THEN
    RAISE NOTICE 'Getting the maximum destination id...';
    EXECUTE v_sql INTO v_max_id;
    v_sql := format('UPDATE %I.refresh_config_inserter_serial SET last_value = %L WHERE dest_table = %L'
                    , '@extschema@'
                    , COALESCE(v_max_id, 0)
                    , p_dest_table);
    PERFORM @extschema@.gdb(p_debug, v_sql);
    EXECUTE v_sql;
END IF;
IF v_jobmon THEN
    PERFORM update_step(v_step_id, 'OK','Done');
    PERFORM close_job(v_job_id);
END IF;

EXECUTE 'SELECT set_config(''search_path'','''||v_old_search_path||''',''false'')';

RAISE NOTICE 'Done';

EXCEPTION
    WHEN OTHERS THEN
        SELECT nspname INTO v_jobmon_schema FROM pg_namespace n, pg_extension e WHERE e.extname = 'pg_jobmon' AND e.extnamespace = n.oid;
        SELECT jobmon INTO v_jobmon FROM @extschema@.refresh_config_inserter WHERE dest_table = p_src_table;
        v_jobmon := COALESCE(p_jobmon, v_jobmon);
        IF v_jobmon_schema IS NULL THEN
            v_jobmon := false;
        END IF;
        IF v_jobmon THEN
            IF v_job_id IS NULL THEN
                EXECUTE format('SELECT %I.add_job(%L)', v_jobmon_schema, 'Inserter Maker: '||p_src_table) INTO v_job_id;
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
 *  Updater maker function.
 */
CREATE OR REPLACE FUNCTION updater_maker(
    p_src_table text
    , p_type text
    , p_control_field text
    , p_dblink_id int
    , p_boundary text DEFAULT NULL
    , p_dest_table text DEFAULT NULL
    , p_index boolean DEFAULT true
    , p_filter text[] DEFAULT NULL
    , p_condition text DEFAULT NULL
    , p_pulldata boolean DEFAULT true
    , p_pk_name text[] DEFAULT NULL
    , p_pk_type text[] DEFAULT NULL
    , p_jobmon boolean DEFAULT NULL
    , p_debug boolean DEFAULT false) 
RETURNS void
    LANGUAGE plpgsql
    AS $$
DECLARE

v_boundary_serial           int;
v_boundary_time             interval;
v_data_source               text;
v_dblink_name               text;
v_dblink_schema             text;
v_dest_schema_name          text;
v_dest_table_name           text;
v_dst_active                boolean;
v_field                     text;
v_insert_refresh_config     text;
v_job_id                    bigint;
v_job_name                  text;
v_jobmon                    boolean;
v_jobmon_schema             text;
v_key_type                  text;
v_link_exists               boolean;
v_max_id                    bigint;
v_max_timestamp             timestamptz;
v_old_search_path           text;
v_pk_name                   text[] := p_pk_name;
v_pk_type                   text[] := p_pk_type;
v_remote_key_sql            text;
v_sql                       text;
v_src_schema_name           text;
v_src_table_name            text;
v_step_id                   bigint;
v_table_exists              boolean;
v_update_refresh_config     text;

BEGIN

v_dblink_name := @extschema@.check_name_length('mimeo_updater_maker_'||p_src_table);

SELECT nspname INTO v_dblink_schema FROM pg_namespace n, pg_extension e WHERE e.extname = 'dblink' AND e.extnamespace = n.oid;
SELECT nspname INTO v_jobmon_schema FROM pg_namespace n, pg_extension e WHERE e.extname = 'pg_jobmon' AND e.extnamespace = n.oid;
SELECT current_setting('search_path') INTO v_old_search_path;
EXECUTE 'SELECT set_config(''search_path'',''@extschema@,'||COALESCE(v_jobmon_schema||',', '')||v_dblink_schema||',public'',''false'')';

IF (p_pk_name IS NULL AND p_pk_type IS NOT NULL) OR (p_pk_name IS NOT NULL AND p_pk_type IS NULL) THEN
    RAISE EXCEPTION 'Cannot manually set primary/unique key field(s) without defining type(s) or vice versa';
END IF;

SELECT data_source INTO v_data_source FROM @extschema@.dblink_mapping_mimeo WHERE data_source_id = p_dblink_id; 
IF NOT FOUND THEN
    RAISE EXCEPTION 'ERROR: database link ID is incorrect %', p_dblink_id; 
END IF;

IF p_dest_table IS NULL THEN
    p_dest_table := p_src_table;
END IF;

IF position('.' in p_dest_table) > 0 AND position('.' in p_src_table) > 0 THEN
    -- Do nothing. Schema & table variable names set below after table is created
ELSE
    RAISE EXCEPTION 'Source (and destination) table must be schema qualified';
END IF;

IF p_jobmon IS TRUE AND v_jobmon_schema IS NULL THEN
    RAISE EXCEPTION 'p_jobmon parameter set to TRUE, but unable to determine if pg_jobmon extension is installed';
ELSIF (p_jobmon IS TRUE OR p_jobmon IS NULL) AND v_jobmon_schema IS NOT NULL THEN
    v_jobmon := true;
ELSE
    v_jobmon := false;
END IF;

v_job_name := 'Updater Maker: '||p_src_table;

IF v_jobmon THEN
    v_job_id := add_job(v_job_name);
    PERFORM gdb(p_debug,'Job ID: '||v_job_id::text);
    v_step_id := add_step(v_job_id,'Connecting to remote source');
END IF;

PERFORM dblink_connect(v_dblink_name, @extschema@.auth(p_dblink_id));

SELECT schemaname, tablename INTO v_src_schema_name, v_src_table_name 
FROM dblink(v_dblink_name, format('
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
        , p_src_table, p_src_table) )
    t (schemaname text, tablename text);


IF v_src_table_name IS NULL THEN
    RAISE EXCEPTION 'Source table missing (%)', p_src_table;
END IF;

-- Automatically get source primary/unique key if none given
IF p_pk_name IS NULL AND p_pk_type IS NULL THEN
    SELECT v_key_type, indkey_names, indkey_types INTO v_key_type, v_pk_name, v_pk_type FROM fetch_replication_key(v_src_schema_name, v_src_table_name, v_dblink_name);
END IF;

IF v_pk_name IS NULL OR v_pk_type IS NULL THEN
    RAISE EXCEPTION 'Source table has no valid primary key or unique index';
END IF;

IF p_filter IS NOT NULL THEN
    FOREACH v_field IN ARRAY v_pk_name LOOP
        IF v_field = ANY(p_filter) THEN
            CONTINUE;
        ELSE
            RAISE EXCEPTION 'ERROR: filter list did not contain all columns that compose primary/unique key for source table %', p_src_table;
        END IF;
    END LOOP;
END IF;

IF v_jobmon THEN
    PERFORM update_step(v_step_id, 'OK','Done');
    v_step_id := add_step(v_job_id,'Adding configuration data');
END IF;

IF p_type = 'time' THEN
    v_dst_active := @extschema@.dst_utc_check();
    IF p_boundary IS NULL THEN
        v_boundary_time = '10 minutes'::interval;
    ELSE
        v_boundary_time = p_boundary::interval;
    END IF;
    v_insert_refresh_config := 'INSERT INTO @extschema@.refresh_config_updater_time(
            source_table
            , type
            , dest_table
            , dblink
            , control
            , boundary
            , pk_name
            , pk_type
            , last_value
            , last_run
            , dst_active
            , filter
            , condition
            , jobmon) 
        VALUES('
            ||quote_literal(p_src_table)
            ||', '||quote_literal('updater_time')
            ||', '||quote_literal(p_dest_table)
            ||', '||quote_literal(p_dblink_id)
            ||', '||quote_literal(p_control_field)
            ||', '||quote_literal(v_boundary_time)
            ||', '||quote_literal(v_pk_name)
            ||', '||quote_literal(v_pk_type)
            ||', '||quote_literal('-infinity')
            ||', '||quote_literal(CURRENT_TIMESTAMP)
            ||', '||v_dst_active
            ||', '||COALESCE(quote_literal(p_filter), 'NULL')
            ||', '||COALESCE(quote_literal(p_condition), 'NULL')
            ||', '||v_jobmon||')';
    PERFORM gdb(p_debug, 'v_insert_refresh_config: '||v_insert_refresh_config);
    EXECUTE v_insert_refresh_config;
ELSIF p_type = 'serial' THEN
    IF p_boundary IS NULL THEN
        v_boundary_serial = 10;
    ELSE
        v_boundary_serial = p_boundary::int;
    END IF;
    v_insert_refresh_config := 'INSERT INTO @extschema@.refresh_config_updater_serial(
            source_table
            , type
            , dest_table
            , dblink
            , control
            , boundary
            , pk_name
            , pk_type
            , last_value
            , last_run
            , filter
            , condition
            , jobmon) 
        VALUES('
            ||quote_literal(p_src_table)
            ||', '||quote_literal('updater_serial')
            ||', '||quote_literal(p_dest_table)
            ||', '||quote_literal(p_dblink_id)
            ||', '||quote_literal(p_control_field)
            ||', '||quote_literal(v_boundary_serial)
            ||', '||quote_literal(v_pk_name)
            ||', '||quote_literal(v_pk_type)
            ||', '||quote_literal(0)
            ||', '||quote_literal(CURRENT_TIMESTAMP)
            ||', '||COALESCE(quote_literal(p_filter), 'NULL')
            ||', '||COALESCE(quote_literal(p_condition), 'NULL')
            ||', '||v_jobmon||')';
    PERFORM gdb(p_debug, 'v_insert_refresh_config: '||v_insert_refresh_config);
    EXECUTE v_insert_refresh_config;
END IF;

SELECT p_table_exists FROM @extschema@.manage_dest_table(p_dest_table, NULL, NULL,  p_debug) INTO v_table_exists;

SELECT schemaname, tablename 
INTO v_dest_schema_name, v_dest_table_name
FROM pg_catalog.pg_tables
WHERE schemaname||'.'||tablename = p_dest_table;

IF v_jobmon THEN
    PERFORM update_step(v_step_id, 'OK','Done');
END IF;

IF p_pulldata AND v_table_exists = false THEN
    RAISE NOTICE 'Pulling all data from source...';
    IF v_jobmon THEN
        v_step_id := add_step(v_job_id,'Pulling data from source. Check for new job entry under REFRESH UPDATER for current status if this step has not finished');
    END IF;
    EXECUTE 'SELECT @extschema@.refresh_updater('||quote_literal(p_dest_table)||', p_repull := true, p_debug := '||p_debug||')';
END IF;

IF p_index AND v_table_exists = false THEN
    IF v_jobmon THEN
        v_step_id := add_step(v_job_id,'Creating indexes on destination table');
    END IF;
    PERFORM create_index(p_dest_table, v_src_schema_name, v_src_table_name, NULL, p_debug);
    IF v_jobmon THEN
        PERFORM update_step(v_step_id, 'OK','Done');
    END IF;
ELSIF v_table_exists = false THEN
-- Ensure destination indexes that are needed for efficient replication are created even if p_index is set false
    IF v_jobmon THEN
        v_step_id := add_step(v_job_id,'Creating indexes needed for replication');
    END IF;
    PERFORM gdb(p_debug, 'Creating indexes needed for replication');
    IF v_key_type = 'primary' THEN
        EXECUTE format('ALTER TABLE %I.%I', v_dest_schema_name, v_dest_table_name)||' ADD PRIMARY KEY('||array_to_string(v_pk_name, ',')||')';
    ELSE
        EXECUTE format('CREATE UNIQUE INDEX ON %I.%I', v_dest_schema_name, v_dest_table_name)||' ('||array_to_string(v_pk_name, ',')||')';
    END IF;
    IF v_jobmon THEN
        PERFORM update_step(v_step_id, 'OK','Done');
    END IF;
END IF;

IF v_table_exists THEN
    IF v_jobmon THEN
        v_step_id := add_step(v_job_id, format('Destination table %s.%s already exists. No data or indexes were pulled from source', v_dest_schema_name, v_dest_table_name));
        PERFORM update_step(v_step_id, 'OK','Done');
    END IF;
    RAISE NOTICE 'Destination table % already exists. No data or indexes was pulled from source', p_dest_table;
END IF;

RAISE NOTICE 'Analyzing destination table...';
IF v_jobmon THEN
    v_step_id := add_step(v_job_id, 'Analyzing destination table');
END IF;
EXECUTE format('ANALYZE %I.%I', v_dest_schema_name, v_dest_table_name);
IF v_jobmon THEN
    PERFORM update_step(v_step_id, 'OK','Done');
END IF;

PERFORM dblink_disconnect(v_dblink_name);

IF v_jobmon THEN
    v_step_id := add_step(v_job_id, 'Obtaining max destination value of control column for config table');
END IF;
v_sql := format('SELECT max(%I) FROM %I.%I', p_control_field, v_dest_schema_name, v_dest_table_name);
PERFORM @extschema@.gdb(p_debug, v_sql);
IF p_type = 'time' THEN
    RAISE NOTICE 'Getting the maximum destination timestamp...';
    EXECUTE v_sql INTO v_max_timestamp;
    v_sql := format('UPDATE %I.refresh_config_updater_time SET last_value = %L WHERE dest_table = %L'
                    , '@extschema@'
                    , COALESCE(v_max_timestamp, CURRENT_TIMESTAMP)
                    , p_dest_table);
    PERFORM @extschema@.gdb(p_debug, v_sql);
    EXECUTE v_sql;
ELSIF p_type = 'serial' THEN
    RAISE NOTICE 'Getting the maximum destination id...';
    EXECUTE v_sql INTO v_max_id;
    v_sql := format('UPDATE %I.refresh_config_updater_serial SET last_value = %L WHERE dest_table = %L'
                    , '@extschema@'
                    , COALESCE(v_max_id, 0)
                    , p_dest_table);
    PERFORM @extschema@.gdb(p_debug, v_sql);
    EXECUTE v_sql;
END IF;
IF v_jobmon THEN
    PERFORM update_step(v_step_id, 'OK','Done');
    PERFORM close_job(v_job_id);
END IF;

EXECUTE 'SELECT set_config(''search_path'','''||v_old_search_path||''',''false'')';

RAISE NOTICE 'Done';

RETURN;

EXCEPTION
    WHEN QUERY_CANCELED THEN
        SELECT nspname INTO v_dblink_schema FROM pg_namespace n, pg_extension e WHERE e.extname = 'dblink' AND e.extnamespace = n.oid;
        EXECUTE format('SELECT %I.dblink_get_connections() @> ARRAY[%L]', v_dblink_schema, v_dblink_name) INTO v_link_exists;
        IF v_link_exists THEN
            EXECUTE format('SELECT %I.dblink_disconnect(%L)', v_dblink_schema, v_dblink_name);
        END IF;
        RAISE EXCEPTION '%', SQLERRM;
    WHEN OTHERS THEN
        SELECT nspname INTO v_dblink_schema FROM pg_namespace n, pg_extension e WHERE e.extname = 'dblink' AND e.extnamespace = n.oid;
        SELECT nspname INTO v_jobmon_schema FROM pg_namespace n, pg_extension e WHERE e.extname = 'pg_jobmon' AND e.extnamespace = n.oid;
        SELECT jobmon INTO v_jobmon FROM @extschema@.refresh_config_updater WHERE dest_table = p_src_table;
        v_jobmon := COALESCE(p_jobmon, v_jobmon);
        EXECUTE format('SELECT %I.dblink_get_connections() @> ARRAY[%L]', v_dblink_schema, v_dblink_name) INTO v_link_exists;
        IF v_link_exists THEN
            EXECUTE format('SELECT %I.dblink_disconnect(%L)', v_dblink_schema, v_dblink_name);
        END IF;
        IF v_jobmon AND v_jobmon_schema IS NOT NULL THEN
            IF v_job_id IS NULL THEN
                EXECUTE format('SELECT %I.add_job(%L)', v_jobmon_schema, 'Inserter Maker: '||p_src_table) INTO v_job_id;
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
 *  Refresh insert only table based on timestamp control field
 */
CREATE OR REPLACE FUNCTION refresh_inserter_time(p_destination text, p_limit integer DEFAULT NULL, p_repull boolean DEFAULT false, p_repull_start timestamp DEFAULT NULL, p_repull_end timestamp DEFAULT NULL, p_jobmon boolean DEFAULT NULL, p_lock_wait int DEFAULT NULL, p_debug boolean DEFAULT false) RETURNS void
    LANGUAGE plpgsql SECURITY DEFINER
    AS $$
DECLARE

v_adv_lock              boolean;
v_batch_limit_reached   int := 0; 
v_boundary              timestamptz;
v_cols_n_types          text;
v_cols                  text;
v_condition             text;
v_control               text;
v_create_sql            text;
v_dblink                int;
v_dblink_name           text;
v_dblink_schema         text;
v_delete_sql            text;
v_dest_schema_name      text;
v_dest_table            text;
v_dest_table_name       text;
v_dst_active            boolean;
v_dst_check             boolean;
v_dst_start             int;
v_dst_end               int;
v_fetch_sql             text;
v_filter                text[]; 
v_full_refresh          boolean := false;
v_insert_sql            text;
v_job_id                int;
v_jobmon                boolean;
v_jobmon_schema         text;
v_job_name              text;
v_last_fetched          timestamptz;
v_last_value            timestamptz;
v_limit                 int;
v_link_exists           boolean;
v_old_search_path       text;
v_remote_sql            text;
v_rowcount              bigint := 0; 
v_source_table          text;
v_src_schema_name       text;
v_src_table_name        text;
v_step_id               int;
v_total                 bigint := 0;

BEGIN

-- Take advisory lock to prevent multiple calls to function overlapping
v_adv_lock := @extschema@.concurrent_lock_check(p_destination, p_lock_wait);
IF v_adv_lock = 'false' THEN
    -- This code is known duplication of code below.
    -- This is done in order to keep advisory lock as early in the code as possible to avoid race conditions and still log if issues are encountered.
    v_job_name := 'Refresh Inserter: '||p_destination;
    SELECT nspname INTO v_jobmon_schema FROM pg_namespace n, pg_extension e WHERE e.extname = 'pg_jobmon' AND e.extnamespace = n.oid;
    SELECT jobmon INTO v_jobmon FROM @extschema@.refresh_config_inserter WHERE dest_table = p_destination;
    v_jobmon := COALESCE(p_jobmon, v_jobmon);
    IF v_jobmon IS TRUE AND v_jobmon_schema IS NULL THEN
        RAISE EXCEPTION 'jobmon config set to TRUE, but unable to determine if pg_jobmon extension is installed';
    END IF;

    IF v_jobmon THEN
        EXECUTE format('SELECT %I.add_job(%L)', v_jobmon_schema, v_job_name) INTO v_job_id;
        EXECUTE format('SELECT %I.add_step(%L, %L)', v_jobmon_schema, v_job_id, 'Obtaining advisory lock for job: '||v_job_name) INTO v_step_id;
        EXECUTE format('SELECT %I.update_step(%L, %L, %L)', v_jobmon_schema, v_step_id, 'WARNING', 'Found concurrent job. Exiting gracefully');
        EXECUTE format('SELECT %I.fail_job(%L, %L)', v_jobmon_schema, v_job_id, 2);
    END IF;
    PERFORM @extschema@.gdb(p_debug,'Obtaining advisory lock FAILED for job: '||v_job_name);
    RAISE DEBUG 'Found concurrent job. Exiting gracefully';
    RETURN;
END IF;

IF p_debug IS DISTINCT FROM true THEN
    PERFORM set_config( 'client_min_messages', 'warning', true );
END IF;

v_job_name := 'Refresh Inserter: '||p_destination;

SELECT nspname INTO v_dblink_schema FROM pg_namespace n, pg_extension e WHERE e.extname = 'dblink' AND e.extnamespace = n.oid;
SELECT nspname INTO v_jobmon_schema FROM pg_namespace n, pg_extension e WHERE e.extname = 'pg_jobmon' AND e.extnamespace = n.oid;
IF p_jobmon IS TRUE AND v_jobmon_schema IS NULL THEN
    RAISE EXCEPTION 'p_jobmon parameter set to TRUE, but unable to determine if pg_jobmon extension is installed';
END IF;

v_dblink_name := @extschema@.check_name_length('mimeo_inserter_refresh_'||p_destination);

-- Set custom search path to allow easier calls to other functions, especially job logging
SELECT current_setting('search_path') INTO v_old_search_path;
EXECUTE 'SELECT set_config(''search_path'',''@extschema@,'||COALESCE(v_jobmon_schema||',', '')||v_dblink_schema||',public'',''false'')';

SELECT source_table
    , dest_table
    , dblink
    , control
    , last_value
    , now() - boundary::interval
    , filter
    , condition
    , dst_active
    , dst_start
    , dst_end
    , batch_limit 
    , jobmon
INTO v_source_table
    , v_dest_table
    , v_dblink
    , v_control
    , v_last_value
    , v_boundary
    , v_filter
    , v_condition
    , v_dst_active
    , v_dst_start
    , v_dst_end
    , v_limit
    , v_jobmon
FROM refresh_config_inserter_time
WHERE dest_table = p_destination; 
IF NOT FOUND THEN
    RAISE EXCEPTION 'Destination table given in argument (%) is not managed by mimeo.', p_destination; 
END IF;  

-- Allow override with parameter
v_jobmon := COALESCE(p_jobmon, v_jobmon);

SELECT schemaname, tablename 
INTO v_dest_schema_name, v_dest_table_name
FROM pg_catalog.pg_tables
WHERE schemaname||'.'||tablename = v_dest_table;

IF v_jobmon THEN
    v_job_id := add_job(v_job_name);
    PERFORM gdb(p_debug,'Job ID: '||v_job_id::text);
END IF;

-- Do not allow this function to run during DST time change if config option is true. Otherwise will miss data from source
IF v_dst_active THEN
    v_dst_check := @extschema@.dst_change(CURRENT_TIMESTAMP);
    IF v_dst_check THEN 
        IF to_number(to_char(CURRENT_TIMESTAMP, 'HH24MM'), '0000') > v_dst_start AND to_number(to_char(CURRENT_TIMESTAMP, 'HH24MM'), '0000') < v_dst_end THEN
            IF v_jobmon THEN
                v_step_id := add_step( v_job_id, 'DST Check');
                PERFORM update_step(v_step_id, 'OK', 'Job CANCELLED - Does not run during DST time change');
                PERFORM close_job(v_job_id);
            END IF;
            PERFORM gdb(p_debug, 'Cannot run during DST time change');
            UPDATE refresh_config_inserter SET last_run = CURRENT_TIMESTAMP WHERE dest_table = p_destination;  
            EXECUTE 'SELECT set_config(''search_path'','''||v_old_search_path||''',''false'')';
            RETURN;
        END IF;
    END IF;
END IF;

IF v_jobmon THEN
    v_step_id := add_step(v_job_id,'Building SQL');
END IF;

PERFORM dblink_connect(v_dblink_name, auth(v_dblink));

SELECT array_to_string(p_cols, ',')
    , array_to_string(p_cols_n_types, ',') 
    , p_source_schema_name
    , p_source_table_name
INTO v_cols
    , v_cols_n_types 
    , v_src_schema_name
    , v_src_table_name
FROM manage_dest_table(v_dest_table, NULL, v_dblink_name, p_debug);

IF v_src_table_name IS NULL THEN
    RAISE EXCEPTION 'Source table missing (%)', v_source_table;
END IF;

IF p_limit IS NOT NULL THEN
    v_limit := p_limit;
END IF;

IF p_repull THEN
    -- Repull ALL data if no start and end values set
    IF p_repull_start IS NULL AND p_repull_end IS NULL THEN
        IF v_jobmon THEN
            PERFORM update_step(v_step_id, 'OK','Request to repull ALL data from source. This could take a while...');
        END IF;
        EXECUTE format('TRUNCATE %I.%I', v_dest_schema_name, v_dest_table_name);
        v_remote_sql := format('SELECT '||v_cols||' FROM %I.%I', v_src_schema_name, v_src_table_name);
        -- Use upper boundary to avoid edge case of multiple upper boundary values inserting during refresh
        IF v_condition IS NOT NULL THEN
            v_remote_sql := v_remote_sql || ' ' || v_condition || ' AND ';
        ELSE
            v_remote_sql := v_remote_sql || ' WHERE ';
        END IF;
        v_remote_sql := v_remote_sql ||format('%I < %L', v_control, v_boundary);
    ELSE
        IF v_jobmon THEN
            PERFORM update_step(v_step_id, 'OK','Request to repull data from '||COALESCE(p_repull_start, '-infinity')||' to '||COALESCE(p_repull_end, v_boundary));
        END IF;
        PERFORM gdb(p_debug,'Request to repull data from '||COALESCE(p_repull_start, '-infinity')||' to '||COALESCE(p_repull_end, v_boundary));
        v_remote_sql := format('SELECT '||v_cols||' FROM %I.%I', v_src_schema_name, v_src_table_name);
        IF v_condition IS NOT NULL THEN
            v_remote_sql := v_remote_sql || ' ' || v_condition || ' AND ';
        ELSE
            v_remote_sql := v_remote_sql || ' WHERE ';
        END IF;
        -- Use upper boundary to avoid edge case of multiple upper boundary values inserting during refresh
        v_remote_sql := v_remote_sql || format('%I > %L AND %I < %L'
                                                , v_control
                                                , COALESCE(p_repull_start, '-infinity')
                                                , v_control
                                                , COALESCE(p_repull_end, v_boundary));
        -- Delete the old local data. Use higher than upper boundary to ensure all old data is deleted
        v_delete_sql := format('DELETE FROM %I.%I WHERE %I > %L AND %I < %L'
                                , v_dest_schema_name
                                , v_dest_table_name
                                , v_control
                                , COALESCE(p_repull_start, '-infinity')
                                , v_control
                                , COALESCE(p_repull_end, 'infinity'));
        IF v_jobmon THEN
            v_step_id := add_step(v_job_id, 'Deleting current, local data');
        END IF;
        PERFORM gdb(p_debug,'Deleting current, local data: '||v_delete_sql);
        EXECUTE v_delete_sql;
        GET DIAGNOSTICS v_rowcount = ROW_COUNT;
        IF v_jobmon THEN
            PERFORM update_step(v_step_id, 'OK', v_rowcount || ' rows removed');
        END IF;
    END IF;
ELSE
    -- does < for upper boundary to keep missing data from happening on rare edge case where a newly inserted row outside the transaction batch
    -- has the exact same timestamp as the previous batch's max timestamp
    v_remote_sql := format('SELECT %s FROM %I.%I', v_cols, v_src_schema_name, v_src_table_name);
    IF v_condition IS NOT NULL THEN
        v_remote_sql := v_remote_sql || ' ' || v_condition || ' AND ';
    ELSE
        v_remote_sql := v_remote_sql || ' WHERE ';
    END IF;
    v_remote_sql := v_remote_sql || format('%I > %L AND %I < %L ORDER BY %I ASC LIMIT '||COALESCE(v_limit::text, 'ALL')
                                            , v_control
                                            , v_last_value
                                            , v_control
                                            , v_boundary
                                            , v_control);

    IF v_jobmon THEN
        PERFORM update_step(v_step_id, 'OK','Grabbing rows from '||v_last_value::text||' to '||v_boundary::text);
    END IF;
    PERFORM gdb(p_debug,'Grabbing rows from '||v_last_value::text||' to '||v_boundary::text);

END IF;

EXECUTE 'CREATE TEMP TABLE mimeo_refresh_inserter_temp ('||v_cols_n_types||')';
PERFORM gdb(p_debug,v_remote_sql);
PERFORM dblink_open(v_dblink_name, 'mimeo_cursor', v_remote_sql);
IF v_jobmon THEN
    v_step_id := add_step(v_job_id, 'Inserting new records into local table');
END IF;
v_rowcount := 0;
v_total := 0;
LOOP
    v_fetch_sql := format('INSERT INTO mimeo_refresh_inserter_temp(%s) SELECT %s FROM dblink_fetch(%L, %L, %s) AS (%s)'
        , v_cols
        , v_cols
        , v_dblink_name
        , 'mimeo_cursor'
        , '50000'
        , v_cols_n_types);
    EXECUTE v_fetch_sql;
    GET DIAGNOSTICS v_rowcount = ROW_COUNT;
    v_total := v_total + coalesce(v_rowcount, 0);
    EXECUTE format('SELECT max(%I) FROM mimeo_refresh_inserter_temp', v_control) INTO v_last_fetched;
    IF v_limit IS NULL THEN -- insert into the real table in batches if no limit to avoid excessively large temp tables
        EXECUTE format('INSERT INTO %I.%I (%s) SELECT %s FROM mimeo_refresh_inserter_temp', v_dest_schema_name, v_dest_table_name, v_cols, v_cols);
        TRUNCATE mimeo_refresh_inserter_temp;
    END IF;
    EXIT WHEN v_rowcount = 0;
    PERFORM gdb(p_debug,'Fetching rows in batches: '||v_total||' done so far. Last fetched: '||v_last_fetched);
    IF v_jobmon THEN
        PERFORM update_step(v_step_id, 'PENDING', 'Fetching rows in batches: '||v_total||' done so far. Last fetched: '||v_last_fetched);
    END IF;
END LOOP;
PERFORM dblink_close(v_dblink_name, 'mimeo_cursor');
IF v_jobmon THEN
    PERFORM update_step(v_step_id, 'OK','Rows fetched: '||v_total);
END IF;

IF v_limit IS NULL THEN
    -- nothing else to do
ELSE
    -- When using batch limits, entire batch must be pulled to temp table before inserting to real table to catch edge cases 
    IF v_jobmon THEN
        v_step_id := add_step(v_job_id,'Checking for batch limit issues');
    END IF;
    PERFORM gdb(p_debug, 'Checking for batch limit issues');
    -- Not recommended that the batch actually equal the limit set if possible. Handle all edge cases to keep data consistent
    IF v_total >= v_limit THEN
        IF v_jobmon THEN
            PERFORM update_step(v_step_id, 'WARNING','Row count fetched equal to or greater than limit set: '||v_limit||'. Recommend increasing batch limit if possible.');
        END IF;
        PERFORM gdb(p_debug, 'Row count fetched equal to or greater than limit set: '||v_limit||'. Recommend increasing batch limit if possible.'); 
        EXECUTE format('SELECT max(%I) FROM mimeo_refresh_inserter_temp', v_control) INTO v_last_value;
        IF v_jobmon THEN
            v_step_id := add_step(v_job_id, 'Removing high boundary rows from this batch to avoid missing data');
        END IF;
        EXECUTE format('DELETE FROM mimeo_refresh_inserter_temp WHERE %I = %L', v_control, v_last_value);
        GET DIAGNOSTICS v_rowcount = ROW_COUNT;
        IF v_jobmon THEN
            PERFORM update_step(v_step_id, 'OK', 'Removed '||v_rowcount||' rows. Batch now contains '||v_limit - v_rowcount||' records');
        END IF;
        PERFORM gdb(p_debug, 'Removed '||v_rowcount||' rows from batch. Batch table now contains '||v_limit - v_rowcount||' records');
        v_batch_limit_reached = 2;
        v_total := v_total - v_rowcount;
        IF (v_limit - v_rowcount) < 1 THEN
            IF v_jobmon THEN
                v_step_id := add_step(v_job_id, 'Reached inconsistent state');
                PERFORM update_step(v_step_id, 'CRITICAL', 'Batch contained max rows ('||v_limit||') or greater and all contained the same timestamp value. Unable to guarentee rows will ever be replicated consistently. Increase row limit parameter to allow a consistent batch.');
            END IF;
            PERFORM gdb(p_debug, 'Batch contained max rows desired ('||v_limit||') or greaer and all contained the same timestamp value. Unable to guarentee rows will be replicated consistently. Increase row limit parameter to allow a consistent batch.');
            v_batch_limit_reached = 3;
        END IF;
    ELSE
        IF v_jobmon THEN
            PERFORM update_step(v_step_id, 'OK','No issues found');
        END IF;
        PERFORM gdb(p_debug, 'No issues found');
    END IF;

    IF v_batch_limit_reached <> 3 THEN
        IF v_jobmon THEN
            v_step_id := add_step(v_job_id,'Inserting new records into local table');
        END IF;
        EXECUTE format('INSERT INTO %I.%I (%s) SELECT %s FROM mimeo_refresh_inserter_temp', v_dest_schema_name, v_dest_table_name, v_cols, v_cols);
        IF v_jobmon THEN
            PERFORM update_step(v_step_id, 'OK','Inserted '||v_total||' records');
        END IF;
        PERFORM gdb(p_debug, 'Inserted '||v_total||' records');
    END IF;

END IF; -- end v_limit IF

IF v_batch_limit_reached <> 3 THEN
    IF v_jobmon THEN
        v_step_id := add_step(v_job_id, 'Setting next lower boundary');
    END IF;
    EXECUTE format('SELECT max(%I) FROM %I.%I', v_control, v_dest_schema_name, v_dest_table_name) INTO v_last_value;
    IF v_last_value IS NOT NULL THEN
        UPDATE refresh_config_inserter_time SET last_value = v_last_value, last_run = CURRENT_TIMESTAMP WHERE dest_table = p_destination;  
    END IF;
    IF v_jobmon THEN
        PERFORM update_step(v_step_id, 'OK','Lower boundary value is: '|| coalesce(v_last_value::text, 'NULL'));
    END IF;
        PERFORM gdb(p_debug, 'Lower boundary value is: '||coalesce(v_last_value::text, 'NULL'));
END IF;

DROP TABLE IF EXISTS mimeo_refresh_inserter_temp;

PERFORM dblink_disconnect(v_dblink_name);

IF v_jobmon THEN
    IF v_batch_limit_reached = 0 THEN
        PERFORM close_job(v_job_id);
    ELSIF v_batch_limit_reached = 2 THEN
        -- Set final job status to level 2 (WARNING) to bring notice that the batch limit was reached and may need adjusting.
        -- Preventive warning to keep replication from falling behind.
        PERFORM fail_job(v_job_id, 2);
    ELSIF v_batch_limit_reached = 3 THEN
        -- Really bad. Critical alert!
        PERFORM fail_job(v_job_id);
    END IF;
END IF;

-- Ensure old search path is reset for the current session
EXECUTE 'SELECT set_config(''search_path'','''||v_old_search_path||''',''false'')';

EXCEPTION
    WHEN QUERY_CANCELED THEN
        SELECT nspname INTO v_dblink_schema FROM pg_namespace n, pg_extension e WHERE e.extname = 'dblink' AND e.extnamespace = n.oid;
        EXECUTE format('SELECT %I.dblink_get_connections() @> ARRAY[%L]', v_dblink_schema, v_dblink_name) INTO v_link_exists;
        IF v_link_exists THEN
            EXECUTE format('SELECT %I.dblink_disconnect(%L)', v_dblink_schema, v_dblink_name);
        END IF;
        RAISE EXCEPTION '%', SQLERRM;
    WHEN OTHERS THEN
        SELECT nspname INTO v_dblink_schema FROM pg_namespace n, pg_extension e WHERE e.extname = 'dblink' AND e.extnamespace = n.oid;
        SELECT nspname INTO v_jobmon_schema FROM pg_namespace n, pg_extension e WHERE e.extname = 'pg_jobmon' AND e.extnamespace = n.oid;
        SELECT jobmon INTO v_jobmon FROM @extschema@.refresh_config_inserter WHERE dest_table = p_destination;
        v_jobmon := COALESCE(p_jobmon, v_jobmon);
        EXECUTE format('SELECT %I.dblink_get_connections() @> ARRAY[%L]', v_dblink_schema, v_dblink_name) INTO v_link_exists;
        IF v_link_exists THEN
            EXECUTE format('SELECT %I.dblink_disconnect(%L)', v_dblink_schema, v_dblink_name);
        END IF;
        IF v_jobmon AND v_jobmon_schema IS NOT NULL THEN
            IF v_job_id IS NULL THEN
                EXECUTE format('SELECT %I.add_job(%L)', v_jobmon_schema, 'Refresh Inserter: '||p_destination) INTO v_job_id;
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
 *  Refresh insert/update only table based on timestamp control field
 */
CREATE OR REPLACE FUNCTION refresh_updater_time(p_destination text, p_limit integer DEFAULT NULL, p_repull boolean DEFAULT false, p_repull_start timestamp DEFAULT NULL, p_repull_end timestamp DEFAULT NULL, p_jobmon boolean DEFAULT NULL, p_lock_wait int DEFAULT NULL, p_debug boolean DEFAULT false) RETURNS void
    LANGUAGE plpgsql SECURITY DEFINER
    AS $$
DECLARE

v_adv_lock              boolean;
v_batch_limit_reached   int := 0;
v_boundary_sql          text;
v_boundary              timestamptz;
v_cols_n_types          text;
v_cols                  text;
v_condition             text;
v_control               text;
v_create_sql            text;
v_dblink                int;
v_dblink_name           text;
v_dblink_schema         text;
v_delete_sql            text;
v_dest_schema_name      text;
v_dest_table            text;
v_dest_table_name       text;
v_dst_active            boolean;
v_dst_check             boolean;
v_dst_start             int;
v_dst_end               int;
v_fetch_sql             text;
v_field                 text;
v_filter                text[];
v_full_refresh          boolean := false;
v_insert_sql            text;
v_job_id                int;
v_jobmon                boolean;
v_jobmon_schema         text;
v_job_name              text;
v_last_fetched          timestamptz;
v_last_value            timestamptz;
v_limit                 int;
v_link_exists           boolean;
v_old_search_path       text;
v_pk_counter            int := 1;
v_pk_name               text[];
v_remote_boundry_sql    text;
v_remote_boundry        timestamptz;
v_remote_sql            text;
v_rowcount              bigint := 0; 
v_source_table          text;
v_src_schema_name       text;
v_src_table_name        text;
v_step_id               int;
v_total                 bigint := 0;

BEGIN

-- Take advisory lock to prevent multiple calls to function overlapping
v_adv_lock := @extschema@.concurrent_lock_check(p_destination, p_lock_wait);
IF v_adv_lock = 'false' THEN
    -- This code is known duplication of code below.
    -- This is done in order to keep advisory lock as early in the code as possible to avoid race conditions and still log if issues are encountered.
    v_job_name := 'Refresh Updater: '||p_destination;
    SELECT jobmon INTO v_jobmon FROM @extschema@.refresh_config_updater WHERE dest_table = p_destination;
    SELECT nspname INTO v_jobmon_schema FROM pg_namespace n, pg_extension e WHERE e.extname = 'pg_jobmon' AND e.extnamespace = n.oid;
    v_jobmon := COALESCE(p_jobmon, v_jobmon);
    IF v_jobmon IS TRUE AND v_jobmon_schema IS NULL THEN
        RAISE EXCEPTION 'jobmon config set to TRUE, but unable to determine if pg_jobmon extension is installed';
    END IF;

    IF v_jobmon THEN
        EXECUTE format('SELECT %I.add_job(%L)', v_jobmon_schema, v_job_name) INTO v_job_id;
        EXECUTE format('SELECT %I.add_step(%L, %L)', v_jobmon_schema, v_job_id, 'Obtaining advisory lock for job: '||v_job_name) INTO v_step_id;
        EXECUTE format('SELECT %I.update_step(%L, %L, %L)', v_jobmon_schema, v_step_id, 'WARNING', 'Found concurrent job. Exiting gracefully');
        EXECUTE format('SELECT %I.fail_job(%L, %L)', v_jobmon_schema, v_job_id, 2);
    END IF;
    PERFORM @extschema@.gdb(p_debug,'Obtaining advisory lock FAILED for job: '||v_job_name);
    RAISE DEBUG 'Found concurrent job. Exiting gracefully';
    RETURN;
END IF;

IF p_debug IS DISTINCT FROM true THEN
    PERFORM set_config( 'client_min_messages', 'warning', true );
END IF;

v_job_name := 'Refresh Updater: '||p_destination;
v_dblink_name := @extschema@.check_name_length('mimeo_updater_refresh_'||p_destination);

SELECT nspname INTO v_dblink_schema FROM pg_namespace n, pg_extension e WHERE e.extname = 'dblink' AND e.extnamespace = n.oid;
SELECT nspname INTO v_jobmon_schema FROM pg_namespace n, pg_extension e WHERE e.extname = 'pg_jobmon' AND e.extnamespace = n.oid;
IF p_jobmon IS TRUE AND v_jobmon_schema IS NULL THEN
    RAISE EXCEPTION 'p_jobmon parameter set to TRUE, but unable to determine if pg_jobmon extension is installed';
END IF;

-- Set custom search path to allow easier calls to other functions, especially job logging
SELECT current_setting('search_path') INTO v_old_search_path;
EXECUTE 'SELECT set_config(''search_path'',''@extschema@,'||COALESCE(v_jobmon_schema||',', '')||v_dblink_schema||',public'',''false'')';

SELECT source_table
    , dest_table
    , dblink
    , control
    , last_value
    , now() - boundary::interval
    , pk_name
    , filter
    , condition
    , dst_active
    , dst_start
    , dst_end
    , batch_limit  
    , jobmon
INTO v_source_table
    , v_dest_table
    , v_dblink
    , v_control
    , v_last_value
    , v_boundary
    , v_pk_name
    , v_filter
    , v_condition
    , v_dst_active
    , v_dst_start
    , v_dst_end
    , v_limit
    , v_jobmon
FROM refresh_config_updater_time
WHERE dest_table = p_destination;
IF NOT FOUND THEN
    RAISE EXCEPTION 'Destination table given in argument (%) is not managed by mimeo.', p_destination; 
END IF;

-- Allow override with parameter
v_jobmon := COALESCE(p_jobmon, v_jobmon);

IF v_jobmon THEN
    v_job_id := add_job(v_job_name);
    PERFORM gdb(p_debug,'Job ID: '||v_job_id::text);
END IF;

SELECT schemaname, tablename 
INTO v_dest_schema_name, v_dest_table_name
FROM pg_catalog.pg_tables
WHERE schemaname||'.'||tablename = v_dest_table;

IF v_dest_table_name IS NULL THEN
    RAISE EXCEPTION 'Destination table is missing (%)', v_dest_table;
END IF;

-- Do not allow this function to run during DST time change if config option is true. Otherwise will miss data from source
IF v_dst_active THEN
    v_dst_check := @extschema@.dst_change(CURRENT_TIMESTAMP);
    IF v_dst_check THEN 
        IF to_number(to_char(CURRENT_TIMESTAMP, 'HH24MM'), '0000') > v_dst_start AND to_number(to_char(CURRENT_TIMESTAMP, 'HH24MM'), '0000') < v_dst_end THEN
            IF v_jobmon THEN
                v_step_id := add_step( v_job_id, 'DST Check');
                PERFORM update_step(v_step_id, 'OK', 'Job CANCELLED - Does not run during DST time change');
                PERFORM close_job(v_job_id);
            END IF;
            UPDATE refresh_config_updater SET last_run = CURRENT_TIMESTAMP WHERE dest_table = p_destination;  
            PERFORM gdb(p_debug, 'Cannot run during DST time change');
            EXECUTE 'SELECT set_config(''search_path'','''||v_old_search_path||''',''false'')';
            RETURN;
        END IF;
    END IF;
END IF;

IF v_jobmon THEN
    v_step_id := add_step(v_job_id,'Building SQL');
END IF;

-- ensure all primary key columns are included in any column filters
IF v_filter IS NOT NULL THEN
    FOREACH v_field IN ARRAY v_pk_name LOOP
        IF v_field = ANY(v_filter) THEN
            CONTINUE;
        ELSE
            RAISE EXCEPTION 'Filter list did not contain all columns that compose primary/unique key for %',v_job_name; 
        END IF;
    END LOOP;
END IF;

PERFORM dblink_connect(v_dblink_name, auth(v_dblink));

SELECT array_to_string(p_cols, ',')
    , array_to_string(p_cols_n_types, ',') 
    , p_source_schema_name
    , p_source_table_name
INTO v_cols
    , v_cols_n_types 
    , v_src_schema_name
    , v_src_table_name
FROM manage_dest_table(v_dest_table, NULL, v_dblink_name, p_debug);

IF v_src_table_name IS NULL THEN
    RAISE EXCEPTION 'Source table missing (%)', v_source_table;
END IF;

IF p_limit IS NOT NULL THEN
    v_limit := p_limit;
END IF;

-- Repull old data instead of normal new data pull
IF p_repull THEN
    -- Repull ALL data if no start and end values set
    IF p_repull_start IS NULL AND p_repull_end IS NULL THEN
        IF v_jobmon THEN
            PERFORM update_step(v_step_id, 'OK','Request to repull ALL data from source. This could take a while...');
        END IF;
        EXECUTE format('TRUNCATE %I.%I', v_dest_schema_name, v_dest_table_name);
        -- Use upper boundary to avoid edge case of multiple upper boundary values inserting during refresh
        v_remote_sql := format('SELECT %s FROM %I.%I', v_cols, v_src_schema_name, v_src_table_name);
        IF v_condition IS NOT NULL THEN
            v_remote_sql := v_remote_sql || ' ' || v_condition || ' AND ';
        ELSE
            v_remote_sql := v_remote_sql || ' WHERE ';
        END IF;
        v_remote_sql := v_remote_sql ||format('%I < %L', v_control, v_boundary);
    ELSE
        IF v_jobmon THEN
            PERFORM update_step(v_step_id, 'OK','Request to repull data from '||COALESCE(p_repull_start, '-infinity')||' to '||COALESCE(p_repull_end, v_boundary));
        END IF;
        PERFORM gdb(p_debug,'Request to repull data from '||COALESCE(p_repull_start, '-infinity')||' to '||COALESCE(p_repull_end, v_boundary));
        v_remote_sql := format('SELECT %s FROM %I.%I', v_cols, v_src_schema_name, v_src_table_name);
        IF v_condition IS NOT NULL THEN
            v_remote_sql := v_remote_sql || ' ' || v_condition || ' AND ';
        ELSE
            v_remote_sql := v_remote_sql || ' WHERE ';
        END IF;
        -- Use upper boundary to avoid edge case of multiple upper boundary values inserting during refresh
        v_remote_sql := v_remote_sql || format('%I > %L AND %I < %L'
                                                , v_control
                                                , COALESCE(p_repull_start, '-infinity')
                                                , v_control
                                                , COALESCE(p_repull_end, v_boundary));
        -- Delete the old local data. Use higher than upper boundary to ensure all old data is deleted
        v_delete_sql := format('DELETE FROM %I.%I WHERE %I > %L AND %I < %L'
                                , v_dest_schema_name
                                , v_dest_table_name
                                , v_control
                                , COALESCE(p_repull_start, '-infinity')
                                , v_control
                                , COALESCE(p_repull_end, 'infinity'));
        IF v_jobmon THEN
            v_step_id := add_step(v_job_id, 'Deleting current, local data');
        END IF;
        PERFORM gdb(p_debug,'Deleting current, local data: '||v_delete_sql);
        EXECUTE v_delete_sql;
        GET DIAGNOSTICS v_rowcount = ROW_COUNT;
        IF v_jobmon THEN
            PERFORM update_step(v_step_id, 'OK', v_rowcount || ' rows removed');
        END IF;

    END IF;
ELSE
    -- does < for upper boundary to keep missing data from happening on rare edge case where a newly inserted row outside the transaction batch
    -- has the exact same timestamp as the previous batch's max timestamp
    v_remote_sql := format('SELECT %s FROM %I.%I', v_cols, v_src_schema_name, v_src_table_name);
    IF v_condition IS NOT NULL THEN
        v_remote_sql := v_remote_sql || ' ' || v_condition || ' AND ';
    ELSE
        v_remote_sql := v_remote_sql || ' WHERE ';
    END IF;
    v_remote_sql := v_remote_sql || format('%I > %L AND %I < %L ORDER BY %I ASC LIMIT '||COALESCE(v_limit::text, 'ALL')
                                    , v_control
                                    , v_last_value
                                    , v_control
                                    , v_boundary
                                    , v_control);

    v_delete_sql := format('DELETE FROM %I.%I a USING mimeo_refresh_updater_temp t WHERE ', v_dest_schema_name, v_dest_table_name);

    WHILE v_pk_counter <= array_length(v_pk_name,1) LOOP
        IF v_pk_counter > 1 THEN
            v_delete_sql := v_delete_sql ||' AND ';
        END IF;
            v_delete_sql := v_delete_sql ||'a."'||v_pk_name[v_pk_counter]||'" = t."'||v_pk_name[v_pk_counter]||'"';
        v_pk_counter := v_pk_counter + 1;
    END LOOP;

    IF v_jobmon THEN
        PERFORM update_step(v_step_id, 'OK','Grabbing rows from '||v_last_value::text||' to '||v_boundary::text);
    END IF;
    PERFORM gdb(p_debug,'Grabbing rows from '||v_last_value::text||' to '||v_boundary::text);
END IF;

v_insert_sql := format('INSERT INTO %I.%I (%s) SELECT %s FROM mimeo_refresh_updater_temp', v_dest_schema_name, v_dest_table_name, v_cols, v_cols); 

PERFORM gdb(p_debug,v_remote_sql);
PERFORM dblink_open(v_dblink_name, 'mimeo_cursor', v_remote_sql);
IF v_jobmon THEN
    v_step_id := add_step(v_job_id, 'Inserting new/updated records into local table');
END IF;
v_rowcount := 0;

EXECUTE format('CREATE TEMP TABLE mimeo_refresh_updater_temp (%s)', v_cols_n_types); 
LOOP
    v_fetch_sql := format('INSERT INTO mimeo_refresh_updater_temp (%s) SELECT %s FROM dblink_fetch(%L, %L, %s) AS (%s)'
        , v_cols
        , v_cols
        , v_dblink_name
        , 'mimeo_cursor'
        , '50000'
        , v_cols_n_types);
    EXECUTE v_fetch_sql;
    GET DIAGNOSTICS v_rowcount = ROW_COUNT;
    v_total := v_total + coalesce(v_rowcount, 0);
    EXECUTE format('SELECT max(%I) FROM mimeo_refresh_updater_temp', v_control) INTO v_last_fetched;
    IF v_limit IS NULL OR p_repull IS TRUE THEN -- insert into the real table in batches if no limit or repull to avoid excessively large temp tables
        IF p_repull IS FALSE THEN   -- Delete any rows that exist in the current temp table batch. repull delete is done above.
            EXECUTE v_delete_sql;
        END IF;
        EXECUTE v_insert_sql;
        TRUNCATE mimeo_refresh_updater_temp;
    END IF;
    EXIT WHEN v_rowcount = 0;
    PERFORM gdb(p_debug,'Fetching rows in batches: '||v_total||' done so far. Last fetched: '||v_last_fetched);
    IF v_jobmon THEN
        PERFORM update_step(v_step_id, 'PENDING', 'Fetching rows in batches: '||v_total||' done so far. Last fetched: '||v_last_fetched);
    END IF;
END LOOP;
PERFORM dblink_close(v_dblink_name, 'mimeo_cursor');
IF v_jobmon THEN
    PERFORM update_step(v_step_id, 'OK','Rows fetched: '||v_total);
END IF;

IF v_limit IS NULL THEN
    -- nothing else to do
ELSIF p_repull IS FALSE THEN -- don't care about limits when doing a repull
    -- When using batch limits, entire batch must be pulled to temp table before inserting to real table to catch edge cases 
    IF v_jobmon THEN
        v_step_id := add_step(v_job_id,'Checking for batch limit issues');
    END IF;
    -- Not recommended that the batch actually equal the limit set if possible.
    IF v_total >= v_limit THEN
        PERFORM gdb(p_debug, 'Row count fetched equal to or greater than limit set: '||v_limit||'. Recommend increasing batch limit if possible.'); 
        IF v_jobmon THEN
            PERFORM update_step(v_step_id, 'WARNING','Row count fetched equal to or greater than limit set: '||v_limit||'. Recommend increasing batch limit if possible.');
            v_step_id := add_step(v_job_id, 'Removing high boundary rows from this batch to avoid missing data');       
        END IF;
        EXECUTE format('SELECT max(%I) FROM mimeo_refresh_updater_temp', v_control) INTO v_last_value;
        EXECUTE format('DELETE FROM mimeo_refresh_updater_temp WHERE %I = %L', v_control, v_last_value);
        GET DIAGNOSTICS v_rowcount = ROW_COUNT;
        IF v_jobmon THEN
            PERFORM update_step(v_step_id, 'OK', 'Removed '||v_rowcount||' rows. Batch now contains '||v_limit - v_rowcount||' records');
        END IF;
        PERFORM gdb(p_debug, 'Removed '||v_rowcount||' rows from batch. Batch table now contains '||v_limit - v_rowcount||' records');
        v_batch_limit_reached := 2;
        IF (v_limit - v_rowcount) < 1 THEN
            IF v_jobmon THEN
                v_step_id := add_step(v_job_id, 'Reached inconsistent state');
                PERFORM update_step(v_step_id, 'CRITICAL', 'Batch contained max rows ('||v_limit||') or greater and all contained the same timestamp value. Unable to guarentee rows will ever be replicated consistently. Increase row limit parameter to allow a consistent batch.');
            END IF;
            PERFORM gdb(p_debug, 'Batch contained max rows ('||v_limit||') or greater and all contained the same timestamp value. Unable to guarentee rows will be replicated consistently. Increase row limit parameter to allow a consistent batch.');
            v_batch_limit_reached := 3;
        END IF;
    ELSE
        IF v_jobmon THEN
            PERFORM update_step(v_step_id, 'OK','No issues found');
        END IF;
        PERFORM gdb(p_debug, 'No issues found');
    END IF;

    IF v_batch_limit_reached <> 3 THEN
        EXECUTE format('CREATE INDEX ON mimeo_refresh_updater_temp ("%s")', array_to_string(v_pk_name, '","')); -- incase of large batch limit
        ANALYZE mimeo_refresh_updater_temp;
        IF v_jobmon THEN
            v_step_id := add_step(v_job_id,'Deleting records marked for update in local table');
        END IF;
        PERFORM gdb(p_debug,v_delete_sql);
        EXECUTE v_delete_sql; 
        GET DIAGNOSTICS v_rowcount = ROW_COUNT;
        IF v_jobmon THEN
            PERFORM update_step(v_step_id, 'OK','Deleted '||v_rowcount||' records');
        END IF;

        IF v_jobmon THEN
            v_step_id := add_step(v_job_id,'Inserting new records into local table');
        END IF;
        perform gdb(p_debug,v_insert_sql);
        EXECUTE v_insert_sql; 
        GET DIAGNOSTICS v_rowcount = ROW_COUNT;
        IF v_jobmon THEN
            PERFORM update_step(v_step_id, 'OK','Inserted '||v_rowcount||' records');
        END IF;
    END IF;

END IF; -- end v_limit IF

IF v_batch_limit_reached <> 3 THEN
    IF v_jobmon THEN
        v_step_id := add_step(v_job_id, 'Setting next lower boundary');
    END IF;
    EXECUTE format('SELECT max(%I) FROM %I.%I', v_control, v_dest_schema_name, v_dest_table_name) INTO v_last_value;
    IF v_last_value IS NOT NULL THEN
        UPDATE refresh_config_updater_time set last_value = v_last_value, last_run = CURRENT_TIMESTAMP WHERE dest_table = p_destination;  
    END IF;
    IF v_jobmon THEN
        PERFORM update_step(v_step_id, 'OK','Lower boundary value is: '||coalesce(v_last_value::text, 'NULL'));
    END IF;
    PERFORM gdb(p_debug, 'Lower boundary value is: '||coalesce(v_last_value::text, 'NULL'));
END IF;

DROP TABLE IF EXISTS mimeo_refresh_updater_temp;

PERFORM dblink_disconnect(v_dblink_name);

IF v_jobmon THEN
    IF v_batch_limit_reached = 0 THEN
        PERFORM close_job(v_job_id);
    ELSIF v_batch_limit_reached = 2 THEN
        -- Set final job status to level 2 (WARNING) to bring notice that the batch limit was reached and may need adjusting.
        -- Preventive warning to keep replication from falling behind.
        PERFORM fail_job(v_job_id, 2);
    ELSIF v_batch_limit_reached = 3 THEN
        -- Really bad. Critical alert!
        PERFORM fail_job(v_job_id);
    END IF;
END IF;

-- Ensure old search path is reset for the current session
EXECUTE 'SELECT set_config(''search_path'','''||v_old_search_path||''',''false'')';

EXCEPTION
    WHEN QUERY_CANCELED THEN
        SELECT nspname INTO v_dblink_schema FROM pg_namespace n, pg_extension e WHERE e.extname = 'dblink' AND e.extnamespace = n.oid;
        EXECUTE format('SELECT %I.dblink_get_connections() @> ARRAY[%L]', v_dblink_schema, v_dblink_name) INTO v_link_exists;
        IF v_link_exists THEN
            EXECUTE format('SELECT %I.dblink_disconnect(%L)', v_dblink_schema, v_dblink_name);
        END IF;
        RAISE EXCEPTION '%', SQLERRM;
    WHEN OTHERS THEN
        SELECT nspname INTO v_dblink_schema FROM pg_namespace n, pg_extension e WHERE e.extname = 'dblink' AND e.extnamespace = n.oid;
        SELECT nspname INTO v_jobmon_schema FROM pg_namespace n, pg_extension e WHERE e.extname = 'pg_jobmon' AND e.extnamespace = n.oid;
        SELECT jobmon INTO v_jobmon FROM @extschema@.refresh_config_updater WHERE dest_table = p_destination;
        v_jobmon := COALESCE(p_jobmon, v_jobmon);
        EXECUTE format('SELECT %I.dblink_get_connections() @> ARRAY[%L]', v_dblink_schema, v_dblink_name) INTO v_link_exists;
        IF v_link_exists THEN
            EXECUTE format('SELECT %I.dblink_disconnect(%L)', v_dblink_schema, v_dblink_name);
        END IF;
        IF v_jobmon AND v_jobmon_schema IS NOT NULL THEN
            IF v_job_id IS NULL THEN
                EXECUTE format('SELECT %I.add_job(%L)', v_jobmon_schema, 'Refresh Updater: '||p_destination) INTO v_job_id;
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
 *  Refresh based on DML (Insert, Update, Delete)
 */
CREATE FUNCTION refresh_dml(p_destination text, p_limit int default NULL, p_repull boolean DEFAULT false, p_jobmon boolean DEFAULT NULL, p_lock_wait int DEFAULT NULL, p_insert_on_fetch boolean DEFAULT NULL, p_debug boolean DEFAULT false) RETURNS void
    LANGUAGE plpgsql SECURITY DEFINER
    AS $$
DECLARE

v_adv_lock              boolean;
v_batch_limit_reached   boolean := false;
v_cols_n_types          text;
v_cols                  text;
v_condition             text;
v_control               text;
v_dblink                int;
v_dblink_name           text;
v_dblink_schema         text;
v_delete_sql            text;
v_dest_schema_name      text;
v_dest_table            text;
v_dest_table_name       text;
v_exec_status           text;
v_fetch_sql             text;
v_field                 text;
v_filter                text[];
v_insert_on_fetch       boolean;
v_job_id                int;
v_jobmon_schema         text;
v_job_name              text;
v_jobmon                boolean;
v_limit                 int; 
v_link_exists           boolean;
v_local_insert_sql      text;
v_old_search_path       text;
v_pk_counter            int;
v_pk_name_csv           text;
v_pk_name_type_csv      text := '';
v_pk_name               text[];
v_pk_type               text[];
v_pk_where              text := '';
v_q_schema_name         text;
v_q_table_name          text;
v_remote_f_sql          text;
v_remote_q_sql          text;
v_rowcount              bigint := 0; 
v_source_table          text;
v_src_schema_name       text;
v_src_table_name        text;
v_step_id               int;
v_total                 bigint := 0;
v_trigger_delete        text; 
v_trigger_update        text;
v_delete_remote_q       text;
v_with_update           text;

BEGIN

-- Take advisory lock to prevent multiple calls to function overlapping
v_adv_lock := @extschema@.concurrent_lock_check(p_destination, p_lock_wait);
IF v_adv_lock = 'false' THEN
    -- This code is known duplication of code found below
    -- This is done in order to keep advisory lock as early in the code as possible to avoid race conditions and still log if issues are encountered.
    v_job_name := 'Refresh DML: '||p_destination;
    SELECT nspname INTO v_jobmon_schema FROM pg_namespace n, pg_extension e WHERE e.extname = 'pg_jobmon' AND e.extnamespace = n.oid;
    SELECT jobmon INTO v_jobmon FROM @extschema@.refresh_config_dml WHERE dest_table = p_destination;
    v_jobmon := COALESCE(p_jobmon, v_jobmon);
    IF v_jobmon IS TRUE AND v_jobmon_schema IS NULL THEN
        RAISE EXCEPTION 'jobmon config set to TRUE, but unable to determine if pg_jobmon extension is installed';
    END IF;

    IF v_jobmon THEN
        EXECUTE format('SELECT %I.add_job(%L)', v_jobmon_schema, v_job_name) INTO v_job_id;
        EXECUTE format('SELECT %I.add_step(%L, %L)', v_jobmon_schema, v_job_id, 'Obtaining advisory lock for job: '||v_job_name) INTO v_step_id;
        EXECUTE format('SELECT %I.update_step(%L, %L, %L)', v_jobmon_schema, v_step_id, 'WARNING', 'Found concurrent job. Exiting gracefully');
        EXECUTE format('SELECT %I.fail_job(%L, %L)', v_jobmon_schema, v_job_id, 2);
    END IF;
    PERFORM @extschema@.gdb(p_debug,'Obtaining advisory lock FAILED for job: '||v_job_name);
    RAISE DEBUG 'Found concurrent job. Exiting gracefully';
    RETURN;
END IF;

IF p_debug IS DISTINCT FROM true THEN
    PERFORM set_config( 'client_min_messages', 'warning', true );
END IF;

v_job_name := 'Refresh DML: '||p_destination;
v_dblink_name := @extschema@.check_name_length('mimeo_dml_refresh_'||p_destination);

SELECT nspname INTO v_dblink_schema FROM pg_namespace n, pg_extension e WHERE e.extname = 'dblink' AND e.extnamespace = n.oid;
SELECT nspname INTO v_jobmon_schema FROM pg_namespace n, pg_extension e WHERE e.extname = 'pg_jobmon' AND e.extnamespace = n.oid;
IF p_jobmon IS TRUE AND v_jobmon_schema IS NULL THEN
    RAISE EXCEPTION 'p_jobmon parameter set to TRUE, but unable to determine if pg_jobmon extension is installed';
END IF;

-- Set custom search path to allow easier calls to other functions, especially job logging
SELECT current_setting('search_path') INTO v_old_search_path;
EXECUTE 'SELECT set_config(''search_path'',''@extschema@,'||COALESCE(v_jobmon_schema||',', '')||v_dblink_schema||',public'',''false'')';

SELECT source_table
    , dest_table
    , dblink
    , control
    , pk_name
    , pk_type
    , filter
    , condition
    , batch_limit 
    , jobmon
    , insert_on_fetch
INTO v_source_table
    , v_dest_table
    , v_dblink
    , v_control
    , v_pk_name
    , v_pk_type
    , v_filter
    , v_condition
    , v_limit
    , v_jobmon
    , v_insert_on_fetch
FROM refresh_config_dml 
WHERE dest_table = p_destination; 
IF NOT FOUND THEN
    RAISE EXCEPTION 'Destination table given in argument (%) is not managed by mimeo.', p_destination; 
END IF;

-- Allow override with parameter
v_jobmon := COALESCE(p_jobmon, v_jobmon);
v_insert_on_fetch := COALESCE(p_insert_on_fetch, v_insert_on_fetch);

SELECT schemaname, tablename 
INTO v_dest_schema_name, v_dest_table_name
FROM pg_catalog.pg_tables
WHERE schemaname||'.'||tablename = v_dest_table;

IF v_dest_table_name IS NULL THEN
    RAISE EXCEPTION 'Destination table is missing (%)', v_dest_table;
END IF;

IF v_jobmon THEN
    v_job_id := add_job(v_job_name);
    PERFORM gdb(p_debug,'Job ID: '||v_job_id::text);
    v_step_id := add_step(v_job_id,'Sanity check primary/unique key values');
END IF;

IF v_pk_name IS NULL OR v_pk_type IS NULL THEN
    RAISE EXCEPTION 'Primary key fields in refresh_config_dml must be defined';
END IF;

-- ensure all primary key columns are included in any column filters
IF v_filter IS NOT NULL THEN
    FOREACH v_field IN ARRAY v_pk_name LOOP
        IF v_field = ANY(v_filter) THEN
            CONTINUE;
        ELSE
            RAISE EXCEPTION 'Filter list did not contain all columns that compose primary/unique key for %',v_job_name; 
        END IF;
    END LOOP;
END IF;

PERFORM dblink_connect(v_dblink_name, auth(v_dblink));

SELECT array_to_string(p_cols, ',')
    , array_to_string(p_cols_n_types, ',') 
    , p_source_schema_name
    , p_source_table_name
INTO v_cols
    , v_cols_n_types 
    , v_src_schema_name
    , v_src_table_name
FROM manage_dest_table(v_dest_table, NULL, v_dblink_name, p_debug);

IF v_src_table_name IS NULL THEN
    RAISE EXCEPTION 'Source table missing (%)', v_source_table;
END IF;

IF p_limit IS NOT NULL THEN
    v_limit := p_limit;
END IF;

v_pk_name_csv := '"'||array_to_string(v_pk_name, '","')||'"';
v_pk_counter := 1;
WHILE v_pk_counter <= array_length(v_pk_name,1) LOOP
    IF v_pk_counter > 1 THEN
        v_pk_name_type_csv := v_pk_name_type_csv || ', ';
        v_pk_where := v_pk_where ||' AND ';
    END IF;
    v_pk_name_type_csv := v_pk_name_type_csv||'"'||v_pk_name[v_pk_counter]||'" '||v_pk_type[v_pk_counter];
    v_pk_where := v_pk_where || ' a."'||v_pk_name[v_pk_counter]||'" = b."'||v_pk_name[v_pk_counter]||'"';
    v_pk_counter := v_pk_counter + 1;
END LOOP;

IF v_jobmon THEN
    PERFORM update_step(v_step_id, 'OK','Done');
END IF;

SELECT schemaname, tablename INTO v_q_schema_name, v_q_table_name 
    FROM dblink(v_dblink_name, 'SELECT schemaname, tablename FROM pg_catalog.pg_tables WHERE schemaname ||''.''|| tablename = '||quote_literal(v_control)) t (schemaname text, tablename text);

IF v_q_table_name IS NULL THEN
    RAISE EXCEPTION 'Source queue table missing (%)', v_control;
END IF;

-- update remote entries
IF v_jobmon THEN
    v_step_id := add_step(v_job_id,'Updating remote trigger table');
END IF;
v_with_update := format('
        WITH a AS (
            SELECT %s FROM %I.%I ORDER BY %s LIMIT %s)
        UPDATE %I.%I b SET processed = true 
        FROM a 
        WHERE %s'
    , v_pk_name_csv
    , v_q_schema_name
    , v_q_table_name
    , v_pk_name_csv
    , COALESCE(v_limit::text, 'ALL')
    , v_q_schema_name
    , v_q_table_name
    , v_pk_where);
PERFORM gdb(p_debug, v_with_update);
v_trigger_update := 'SELECT dblink_exec('||quote_literal(v_dblink_name)||','|| quote_literal(v_with_update)||')';
PERFORM gdb(p_debug,v_trigger_update);
EXECUTE v_trigger_update INTO v_exec_status;
IF v_jobmon THEN
    PERFORM update_step(v_step_id, 'OK','Result was '||v_exec_status);
END IF;

IF p_repull THEN
    IF v_jobmon THEN
        PERFORM update_step(v_step_id, 'OK','Request to repull ALL data from source. This could take a while...');
    END IF;
    PERFORM gdb(p_debug, 'Request to repull ALL data from source. This could take a while...');

    IF v_jobmon THEN
        v_step_id := add_step(v_job_id,'Truncating local table');
    END IF;
    PERFORM gdb(p_debug,'Truncating local table');
    EXECUTE format('TRUNCATE %I.%I', v_dest_schema_name, v_dest_table_name);
    IF v_jobmon THEN
        PERFORM update_step(v_step_id, 'OK','Done');
    END IF;
    -- Define cursor query
    v_remote_f_sql := format('SELECT %s FROM %I.%I', v_cols, v_src_schema_name, v_src_table_name);
    IF v_condition IS NOT NULL THEN
        v_remote_f_sql := v_remote_f_sql || ' ' || v_condition;
    END IF;
ELSE
    EXECUTE format('CREATE TEMP TABLE refresh_dml_queue (%s, PRIMARY KEY (%s))', v_pk_name_type_csv, v_pk_name_csv);
    -- Copy queue locally for use in removing updated/deleted rows
    v_remote_q_sql := format('SELECT DISTINCT %s FROM %I.%I WHERE processed = true', v_pk_name_csv, v_q_schema_name, v_q_table_name);
    PERFORM dblink_open(v_dblink_name, 'mimeo_cursor', v_remote_q_sql);
    IF v_jobmon THEN
        v_step_id := add_step(v_job_id, 'Creating local queue temp table');
    END IF;
    v_rowcount := 0;
    LOOP
        v_fetch_sql := format('INSERT INTO refresh_dml_queue (%s) SELECT %s FROM dblink_fetch(%L, %L, %s) AS (%s)'
                , v_pk_name_csv
                , v_pk_name_csv
                , v_dblink_name
                , 'mimeo_cursor'
                , '50000'
                , v_pk_name_type_csv);
        EXECUTE v_fetch_sql;
        GET DIAGNOSTICS v_rowcount = ROW_COUNT;
        EXIT WHEN v_rowcount = 0;
        v_total := v_total + coalesce(v_rowcount, 0);
        PERFORM gdb(p_debug,'Fetching rows in batches: '||v_total||' done so far.');
        IF v_jobmon THEN
            PERFORM update_step(v_step_id, 'PENDING', 'Fetching rows in batches: '||v_total||' done so far.');
        END IF;
    END LOOP;
    PERFORM dblink_close(v_dblink_name, 'mimeo_cursor');
    EXECUTE format('CREATE INDEX ON refresh_dml_queue (%s)', v_pk_name_csv);
    ANALYZE refresh_dml_queue;
    IF v_jobmon THEN
        PERFORM update_step(v_step_id, 'OK','Number of rows inserted: '||v_total);
    END IF;
    PERFORM gdb(p_debug,'Temp queue table row count '||v_total::text);

    IF v_jobmon THEN
        v_step_id := add_step(v_job_id,'Deleting records from local table');
    END IF;
    v_delete_sql := format('DELETE FROM %I.%I a USING refresh_dml_queue b WHERE %s', v_dest_schema_name, v_dest_table_name, v_pk_where);
    PERFORM gdb(p_debug,v_delete_sql);
    EXECUTE v_delete_sql; 
    GET DIAGNOSTICS v_rowcount = ROW_COUNT;
    PERFORM gdb(p_debug,'Rows removed from local table before applying changes: '||v_rowcount::text);
    IF v_jobmon THEN
        PERFORM update_step(v_step_id, 'OK','Removed '||v_rowcount||' records');
    END IF;
    -- Define cursor query
    v_remote_f_sql := format('SELECT %s FROM %I.%I JOIN (%s) x USING (%s)', v_cols, v_src_schema_name, v_src_table_name, v_remote_q_sql, v_pk_name_csv);
    IF v_condition IS NOT NULL THEN
        v_remote_f_sql := v_remote_f_sql || ' ' || v_condition;
    END IF;
END IF;

-- insert records to local table. Have to do temp table in case destination table is non-natively partitioned (returns 0 when inserting to parent). Also allows for when insert_on_fetch is false to reduce the open cursor time on the source.
IF v_jobmon THEN
    v_step_id := add_step(v_job_id, 'Inserting new records into local table');
END IF;
EXECUTE format('CREATE TEMP TABLE refresh_dml_full (%s)', v_cols_n_types);
v_rowcount := 0;
v_total := 0;
v_local_insert_sql := format('INSERT INTO %I.%I (%s) SELECT %s FROM refresh_dml_full', v_dest_schema_name, v_dest_table_name, v_cols, v_cols);
PERFORM dblink_open(v_dblink_name, 'mimeo_cursor', v_remote_f_sql);
LOOP
    v_fetch_sql := format('INSERT INTO refresh_dml_full (%s) SELECT %s FROM dblink_fetch(%L, %L, %s) AS (%s)'
            , v_cols
            , v_cols
            , v_dblink_name
            , 'mimeo_cursor'
            , '50000'
            , v_cols_n_types);
    EXECUTE v_fetch_sql;
    GET DIAGNOSTICS v_rowcount = ROW_COUNT;
    v_total := v_total + coalesce(v_rowcount, 0);
    PERFORM gdb(p_debug,'Fetching rows in batches: '||v_total||' done so far.');
    IF v_jobmon AND v_insert_on_fetch THEN
        -- Avoid the overhead of jobmon logging each batch step to minimize transaction time on source when insert_on_fetch is false
        PERFORM update_step(v_step_id, 'PENDING', 'Fetching rows in batches: '||v_total||' done so far.');
    END IF;

    IF v_insert_on_fetch THEN
        EXECUTE v_local_insert_sql;
        EXECUTE 'TRUNCATE refresh_dml_full';
    END IF;

    IF v_rowcount = 0 THEN
        -- Above rowcount variable is saved after temp table inserts. 
        -- So when temp table has the whole queue, this block of code should be reached
        PERFORM dblink_close(v_dblink_name, 'mimeo_cursor');
        IF v_insert_on_fetch = false THEN
            PERFORM gdb(p_debug,'Inserting into destination table in single batch (insert_on_fetch set to false)');
            IF v_jobmon THEN
                PERFORM update_step(v_step_id, 'PENDING', 'Inserting into destination table in single batch (insert_on_fetch set to false)...');
            END IF;
            EXECUTE v_local_insert_sql;
            IF v_jobmon THEN
                PERFORM update_step(v_step_id, 'OK', 'Inserted into destination table in single batch (insert_on_fetch set to false)');
            END IF;
        END IF;
        EXIT; -- leave insert loop
    END IF;

END LOOP;
IF v_jobmon THEN
    IF v_insert_on_fetch THEN
        PERFORM update_step(v_step_id, 'OK','Number of rows inserted: '||v_total);
    ELSE
        PERFORM update_step(v_step_id, 'OK','Number of rows inserted: '||v_total||'. Final insert to destination done as single batch (insert_on_fetch set to false)');
    END IF;
END IF;

IF p_repull = false AND v_total > (v_limit * .75) THEN
    IF v_jobmon THEN
        v_step_id := add_step(v_job_id, 'Row count warning');
        PERFORM update_step(v_step_id, 'WARNING','Row count fetched ('||v_total||') greater than 75% of batch limit ('||v_limit||'). Recommend increasing batch limit if possible.');
    END IF;
    v_batch_limit_reached := true;
END IF;

-- clean out rows from remote queue table
IF v_jobmon THEN
    v_step_id := add_step(v_job_id,'Cleaning out rows from remote queue table');
END IF;
v_trigger_delete := format('SELECT dblink_exec(%L, ''DELETE FROM %I.%I WHERE processed = true'')', v_dblink_name, v_q_schema_name, v_q_table_name);
PERFORM gdb(p_debug,v_trigger_delete);
EXECUTE v_trigger_delete INTO v_exec_status;
IF v_jobmon THEN
    PERFORM update_step(v_step_id, 'OK','Result was '||v_exec_status);
END IF;
-- update activity status
IF v_jobmon THEN
    v_step_id := add_step(v_job_id,'Updating last_run in config table');
END IF;
UPDATE refresh_config_dml SET last_run = CURRENT_TIMESTAMP WHERE dest_table = v_dest_table; 
IF v_jobmon THEN
    PERFORM update_step(v_step_id, 'OK','Last run was '||CURRENT_TIMESTAMP);
END IF;
DROP TABLE IF EXISTS refresh_dml_full;
DROP TABLE IF EXISTS refresh_dml_queue;

PERFORM dblink_disconnect(v_dblink_name);

IF v_jobmon THEN
    IF v_batch_limit_reached = false THEN
        PERFORM close_job(v_job_id);
    ELSE
        -- Set final job status to level 2 (WARNING) to bring notice that the batch limit was reached and may need adjusting.
        -- Preventive warning to keep replication from falling behind.
        PERFORM fail_job(v_job_id, 2);
    END IF;
END IF;

-- Ensure old search path is reset for the current session
EXECUTE 'SELECT set_config(''search_path'','''||v_old_search_path||''',''false'')';

EXCEPTION
    WHEN QUERY_CANCELED THEN
        SELECT nspname INTO v_dblink_schema FROM pg_namespace n, pg_extension e WHERE e.extname = 'dblink' AND e.extnamespace = n.oid;
        EXECUTE format('SELECT %I.dblink_get_connections() @> ARRAY[%L]', v_dblink_schema, v_dblink_name) INTO v_link_exists;
        IF v_link_exists THEN
            EXECUTE format('SELECT %I.dblink_disconnect(%L)', v_dblink_schema, v_dblink_name);
        END IF;
        RAISE EXCEPTION '%', SQLERRM;  
    WHEN OTHERS THEN
        SELECT nspname INTO v_dblink_schema FROM pg_namespace n, pg_extension e WHERE e.extname = 'dblink' AND e.extnamespace = n.oid;
        SELECT nspname INTO v_jobmon_schema FROM pg_namespace n, pg_extension e WHERE e.extname = 'pg_jobmon' AND e.extnamespace = n.oid;
        SELECT jobmon INTO v_jobmon FROM @extschema@.refresh_config_dml WHERE dest_table = p_destination;
        v_jobmon := COALESCE(p_jobmon, v_jobmon);
        EXECUTE format('SELECT %I.dblink_get_connections() @> ARRAY[%L]', v_dblink_schema, v_dblink_name) INTO v_link_exists;
        IF v_link_exists THEN
            EXECUTE format('SELECT %I.dblink_disconnect(%L)', v_dblink_schema, v_dblink_name);
        END IF;
        IF v_jobmon AND v_jobmon_schema IS NOT NULL THEN
            IF v_job_id IS NULL THEN
                EXECUTE format('SELECT %I.add_job(%L)', v_jobmon_schema, 'Refresh DML: '||p_destination) INTO v_job_id;
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
 *  Refresh based on DML (Insert, Update, Delete), but logs all deletes on the destination table
 *  Destination table requires extra column: mimeo_source_deleted timestamptz
 */
CREATE FUNCTION refresh_logdel(p_destination text, p_limit int DEFAULT NULL, p_repull boolean DEFAULT false, p_jobmon boolean DEFAULT NULL, p_lock_wait int DEFAULT NULL, p_insert_on_fetch boolean DEFAULT true, p_debug boolean DEFAULT false) RETURNS void
    LANGUAGE plpgsql SECURITY DEFINER
    AS $$
DECLARE

v_adv_lock              boolean;
v_batch_limit_reached   boolean := false;
v_cols_n_types          text;
v_cols                  text;
v_condition             text;
v_control               text;
v_dblink                int;
v_dblink_name           text;
v_dblink_schema         text;
v_delete_d_sql          text;
v_delete_f_sql          text;
v_delete_remote_q       text;
v_dest_schema_name      text;
v_dest_table            text;
v_dest_table_name       text;
v_exec_status           text;
v_fetch_sql             text;
v_field                 text;
v_filter                text[];
v_insert_deleted_sql    text;
v_insert_on_fetch       boolean;
v_job_id                int;
v_jobmon_schema         text;
v_jobmon                boolean;
v_job_name              text;
v_limit                 int; 
v_link_exists           boolean;
v_local_insert_sql      text;
v_old_search_path       text;
v_pk_counter            int;
v_pk_name               text[];
v_pk_name_csv           text;
v_pk_name_type_csv      text := '';
v_pk_type               text[];
v_pk_where              text := '';
v_q_schema_name         text;
v_q_table_name          text;
v_remote_d_sql          text;
v_remote_f_sql          text;
v_remote_q_sql          text;
v_rowcount              bigint := 0; 
v_source_table          text;
v_src_schema_name       text;
v_src_table_name        text;
v_step_id               int;
v_total                 bigint := 0;
v_trigger_delete        text; 
v_trigger_update        text;
v_with_update           text;

BEGIN

-- Take advisory lock to prevent multiple calls to function overlapping
v_adv_lock := @extschema@.concurrent_lock_check(p_destination, p_lock_wait);
IF v_adv_lock = 'false' THEN
    -- This code is known duplication of code below.
    -- This is done in order to keep advisory lock as early in the code as possible to avoid race conditions and still log if issues are encountered.
    v_job_name := 'Refresh Log Del: '||p_destination;
    SELECT jobmon INTO v_jobmon FROM @extschema@.refresh_config_logdel WHERE dest_table = p_destination;
    SELECT nspname INTO v_jobmon_schema FROM pg_namespace n, pg_extension e WHERE e.extname = 'pg_jobmon' AND e.extnamespace = n.oid;
    v_jobmon := COALESCE(p_jobmon, v_jobmon);
    IF v_jobmon IS TRUE AND v_jobmon_schema IS NULL THEN
        RAISE EXCEPTION 'jobmon config set to TRUE, but unable to determine if pg_jobmon extension is installed';
    END IF;

    IF v_jobmon THEN
        EXECUTE format('SELECT %I.add_job(%L)', v_jobmon_schema, v_job_name) INTO v_job_id;
        EXECUTE format('SELECT %I.add_step(%L, %L)', v_jobmon_schema, v_job_id, 'Obtaining advisory lock for job: '||v_job_name) INTO v_step_id;
        EXECUTE format('SELECT %I.update_step(%L, %L, %L)', v_jobmon_schema, v_step_id, 'WARNING', 'Found concurrent job. Exiting gracefully');
        EXECUTE format('SELECT %I.fail_job(%L, %L)', v_jobmon_schema, v_job_id, 2);
    END IF;
    PERFORM @extschema@.gdb(p_debug,'Obtaining advisory lock FAILED for job: '||v_job_name);
    RAISE DEBUG 'Found concurrent job. Exiting gracefully';
    RETURN;
END IF;

IF p_debug IS DISTINCT FROM true THEN
    PERFORM set_config( 'client_min_messages', 'warning', true );
END IF;

v_job_name := 'Refresh Log Del: '||p_destination;
v_dblink_name := @extschema@.check_name_length('mimeo_logdel_refresh_'||p_destination);

SELECT nspname INTO v_dblink_schema FROM pg_namespace n, pg_extension e WHERE e.extname = 'dblink' AND e.extnamespace = n.oid;
SELECT nspname INTO v_jobmon_schema FROM pg_namespace n, pg_extension e WHERE e.extname = 'pg_jobmon' AND e.extnamespace = n.oid;
IF p_jobmon IS TRUE AND v_jobmon_schema IS NULL THEN
    RAISE EXCEPTION 'p_jobmon parameter set to TRUE, but unable to determine if pg_jobmon extension is installed';
END IF;

-- Set custom search path to allow easier calls to other functions, especially job logging
SELECT current_setting('search_path') INTO v_old_search_path;
EXECUTE 'SELECT set_config(''search_path'',''@extschema@,'||COALESCE(v_jobmon_schema||',', '')||v_dblink_schema||',public'',''false'')';

SELECT source_table
    , dest_table
    , dblink
    , control
    , pk_name
    , pk_type
    , filter
    , condition
    , batch_limit 
    , jobmon
    , insert_on_fetch
INTO v_source_table
    , v_dest_table
    , v_dblink
    , v_control
    , v_pk_name
    , v_pk_type
    , v_filter
    , v_condition
    , v_limit
    , v_jobmon
    , v_insert_on_fetch
FROM refresh_config_logdel 
WHERE dest_table = p_destination; 
IF NOT FOUND THEN
    RAISE EXCEPTION 'Destination table given in argument (%) is not managed by mimeo.', p_destination; 
END IF;

-- Allow override with parameter
v_jobmon := COALESCE(p_jobmon, v_jobmon);
v_insert_on_fetch := COALESCE(p_insert_on_fetch, v_insert_on_fetch);

SELECT schemaname, tablename 
INTO v_dest_schema_name, v_dest_table_name
FROM pg_catalog.pg_tables
WHERE schemaname||'.'||tablename = v_dest_table;

IF v_dest_table_name IS NULL THEN
    RAISE EXCEPTION 'Destination table is missing (%)', v_dest_table;
END IF;

IF v_jobmon THEN
    v_job_id := add_job(v_job_name);
    PERFORM gdb(p_debug,'Job ID: '||v_job_id::text);
    v_step_id := add_step(v_job_id,'Sanity check primary/unique key values');
END IF;

IF v_pk_name IS NULL OR v_pk_type IS NULL THEN
    RAISE EXCEPTION 'Primary key fields in refresh_config_logdel must be defined';
END IF;

-- ensure all primary key columns are included in any column filters
IF v_filter IS NOT NULL THEN
    FOREACH v_field IN ARRAY v_pk_name LOOP
        IF v_field = ANY(v_filter) THEN
            CONTINUE;
        ELSE
            RAISE EXCEPTION 'Filter list did not contain all columns that compose primary/unique key for %',v_job_name; 
        END IF;
    END LOOP;
END IF;

PERFORM dblink_connect(v_dblink_name, auth(v_dblink));

SELECT array_to_string(p_cols, ',')
    , array_to_string(p_cols_n_types, ',') 
    , p_source_schema_name
    , p_source_table_name
INTO v_cols
    , v_cols_n_types 
    , v_src_schema_name
    , v_src_table_name
FROM manage_dest_table(v_dest_table, NULL, v_dblink_name, p_debug);

IF p_limit IS NOT NULL THEN
    v_limit := p_limit;
END IF;

v_pk_name_csv := '"'||array_to_string(v_pk_name,'","')||'"';
v_pk_counter := 1;
WHILE v_pk_counter <= array_length(v_pk_name,1) LOOP
    IF v_pk_counter > 1 THEN
        v_pk_name_type_csv := v_pk_name_type_csv || ', ';
        v_pk_where := v_pk_where ||' AND ';
    END IF;
    v_pk_name_type_csv := v_pk_name_type_csv ||'"'||v_pk_name[v_pk_counter]||'" '||v_pk_type[v_pk_counter];
    v_pk_where := v_pk_where || ' a."'||v_pk_name[v_pk_counter]||'" = b."'||v_pk_name[v_pk_counter]||'"';
    v_pk_counter := v_pk_counter + 1;
END LOOP;

IF v_jobmon THEN
    PERFORM update_step(v_step_id, 'OK','Done');
END IF;

SELECT schemaname, tablename INTO v_src_schema_name, v_src_table_name 
    FROM dblink(v_dblink_name, 'SELECT schemaname, tablename FROM pg_catalog.pg_tables WHERE schemaname ||''.''|| tablename = '||quote_literal(v_source_table)) t (schemaname text, tablename text);

IF v_src_table_name IS NULL THEN
    RAISE EXCEPTION 'Source table missing (%)', v_source_table;
END IF;

SELECT schemaname, tablename INTO v_q_schema_name, v_q_table_name 
    FROM dblink(v_dblink_name, 'SELECT schemaname, tablename FROM pg_catalog.pg_tables WHERE schemaname ||''.''|| tablename = '||quote_literal(v_control)) t (schemaname text, tablename text);

IF v_q_table_name IS NULL THEN
    RAISE EXCEPTION 'Source queue table missing (%)', v_control;
END IF;

-- update remote entries
IF v_jobmon THEN
    v_step_id := add_step(v_job_id,'Updating remote trigger table');
END IF;
v_with_update := format('
        WITH a AS (
            SELECT %s FROM %I.%I ORDER BY %s LIMIT %s)
        UPDATE %I.%I b SET processed = true 
        FROM a 
        WHERE %s'
    , v_pk_name_csv
    , v_q_schema_name
    , v_q_table_name
    , v_pk_name_csv
    , COALESCE(v_limit::text, 'ALL')
    , v_q_schema_name
    , v_q_table_name
    , v_pk_where);
v_trigger_update := format('SELECT dblink_exec(%L, %L)', v_dblink_name, v_with_update);
PERFORM gdb(p_debug,v_trigger_update);
EXECUTE v_trigger_update INTO v_exec_status;
IF v_jobmon THEN
    PERFORM update_step(v_step_id, 'OK','Result was '||v_exec_status);
END IF;

-- create temp table for recording deleted rows
EXECUTE format('CREATE TEMP TABLE refresh_logdel_deleted (%s, mimeo_source_deleted timestamptz)', v_cols_n_types);
v_remote_d_sql := format('SELECT %s, mimeo_source_deleted FROM %I.%I WHERE processed = true and mimeo_source_deleted IS NOT NULL', v_cols, v_q_schema_name, v_q_table_name);
PERFORM dblink_open(v_dblink_name, 'mimeo_cursor', v_remote_d_sql);
IF v_jobmon THEN
    v_step_id := add_step(v_job_id, 'Creating local queue temp table for deleted rows on source');
END IF;
v_rowcount := 0;
v_total := 0;
LOOP
    v_fetch_sql := format('INSERT INTO refresh_logdel_deleted (%s, mimeo_source_deleted) 
                            SELECT %s, mimeo_source_deleted FROM dblink_fetch(%L, %L, %s) AS (%s, mimeo_source_deleted timestamptz)'
        , v_cols
        , v_cols
        , v_dblink_name
        , 'mimeo_cursor'
        , '50000'
        , v_cols_n_types);
    EXECUTE v_fetch_sql;
    GET DIAGNOSTICS v_rowcount = ROW_COUNT;
    EXIT WHEN v_rowcount = 0;
    v_total := v_total + coalesce(v_rowcount, 0);
    PERFORM gdb(p_debug,'Fetching rows in batches: '||v_total||' done so far.');
IF v_jobmon THEN
    PERFORM update_step(v_step_id, 'PENDING', 'Fetching rows in batches: '||v_total||' done so far.');
END IF;
END LOOP;
PERFORM dblink_close(v_dblink_name, 'mimeo_cursor');
EXECUTE format('CREATE INDEX ON refresh_logdel_deleted (%s)', v_pk_name_csv);
ANALYZE refresh_logdel_deleted;
IF v_jobmon THEN
    PERFORM update_step(v_step_id, 'OK','Number of rows inserted: '||v_total);
END IF;
PERFORM gdb(p_debug,'Temp deleted queue table row count '||v_total::text);  

IF p_repull THEN
    -- Do delete instead of truncate to avoid missing deleted rows that may have been inserted after the above queue fetch.
    IF v_jobmon THEN
        PERFORM update_step(v_step_id, 'OK','Request to repull ALL data from source. This could take a while...');
    END IF;
    PERFORM gdb(p_debug, 'Request to repull ALL data from source. This could take a while...');
    v_delete_remote_q := format('SELECT dblink_exec(%L, ''DELETE FROM %I.%I WHERE processed = true'')', v_dblink_name, v_q_schema_name, v_q_table_name);
    PERFORM gdb(p_debug, v_delete_remote_q);
    EXECUTE v_delete_remote_q;

    IF v_jobmon THEN
        v_step_id := add_step(v_job_id,'Removing local, undeleted rows');
    END IF;
    PERFORM gdb(p_debug,'Removing local, undeleted rows');
    EXECUTE format('DELETE FROM %I.%I WHERE mimeo_source_deleted IS NULL', v_dest_schema_name, v_dest_table_name);
    IF v_jobmon THEN
        PERFORM update_step(v_step_id, 'OK','Done');
    END IF;

    -- Define cursor query
    v_remote_f_sql := format('SELECT %s FROM %I.%I', v_cols, v_src_schema_name, v_src_table_name);
    IF v_condition IS NOT NULL THEN
        v_remote_f_sql := v_remote_f_sql || ' ' || v_condition;
    END IF;
ELSE
    -- Do normal stuff here
    EXECUTE format('CREATE TEMP TABLE refresh_logdel_queue (%s)', v_pk_name_type_csv);
    v_remote_q_sql := format('SELECT DISTINCT %s FROM %I.%I WHERE processed = true and mimeo_source_deleted IS NULL', v_pk_name_csv, v_q_schema_name, v_q_table_name);
    PERFORM dblink_open(v_dblink_name, 'mimeo_cursor', v_remote_q_sql);
    IF v_jobmon THEN
        v_step_id := add_step(v_job_id, 'Creating local queue temp table for inserts/updates');
    END IF;
    v_rowcount := 0;
    LOOP
        v_fetch_sql := format('INSERT INTO refresh_logdel_queue(%s) SELECT %s FROM dblink_fetch(%L, %L, %s) AS (%s)'
            , v_pk_name_csv
            , v_pk_name_csv
            , v_dblink_name
            , 'mimeo_cursor'
            , '50000'
            , v_pk_name_type_csv);
        EXECUTE v_fetch_sql;
        GET DIAGNOSTICS v_rowcount = ROW_COUNT;
        EXIT WHEN v_rowcount = 0;
        v_total := v_total + coalesce(v_rowcount, 0);
        PERFORM gdb(p_debug,'Fetching rows in batches: '||v_total||' done so far.');
        IF v_jobmon THEN
            PERFORM update_step(v_step_id, 'PENDING', 'Fetching rows in batches: '||v_total||' done so far.');
        END IF;
    END LOOP;
    PERFORM dblink_close(v_dblink_name, 'mimeo_cursor');
    EXECUTE format('CREATE INDEX ON refresh_logdel_queue (%s)', v_pk_name_csv);
    ANALYZE refresh_logdel_queue;
    IF v_jobmon THEN
        PERFORM update_step(v_step_id, 'OK','Number of rows inserted: '||v_total);
    END IF;
    PERFORM gdb(p_debug,'Temp inserts/updates queue table row count '||v_total::text);

    -- remove records from local table (inserts/updates)
    IF v_jobmon THEN
        v_step_id := add_step(v_job_id,'Removing insert/update records from local table');
    END IF;
    v_delete_f_sql := format('DELETE FROM %I.%I a USING refresh_logdel_queue b WHERE '|| v_pk_where, v_dest_schema_name, v_dest_table_name);
    PERFORM gdb(p_debug,v_delete_f_sql);
    EXECUTE v_delete_f_sql; 
    GET DIAGNOSTICS v_rowcount = ROW_COUNT;
    PERFORM gdb(p_debug,'Insert/Update rows removed from local table before applying changes: '||v_rowcount::text);
    IF v_jobmon THEN
        PERFORM update_step(v_step_id, 'OK','Removed '||v_rowcount||' records');
    END IF;

    -- remove records from local table (deleted rows)
    IF v_jobmon THEN
        v_step_id := add_step(v_job_id,'Removing deleted records from local table');
    END IF;
    v_delete_d_sql := format('DELETE FROM %I.%I a USING refresh_logdel_deleted b WHERE '|| v_pk_where, v_dest_schema_name, v_dest_table_name);
    PERFORM gdb(p_debug,v_delete_d_sql);
    EXECUTE v_delete_d_sql; 
    GET DIAGNOSTICS v_rowcount = ROW_COUNT;
    PERFORM gdb(p_debug,'Deleted rows removed from local table before applying changes: '||v_rowcount::text);
    IF v_jobmon THEN
        PERFORM update_step(v_step_id, 'OK','Removed '||v_rowcount||' records');
    END IF;

    -- Remote full query for normal replication 
    v_remote_f_sql := format('SELECT %s FROM %I.%I JOIN (%s) x USING (%s)', v_cols, v_src_schema_name, v_src_table_name, v_remote_q_sql, v_pk_name_csv);
    IF v_condition IS NOT NULL THEN
        v_remote_f_sql := v_remote_f_sql || ' ' || v_condition;
    END IF;
END IF;

-- insert records to local table. Have to do temp table in case destination table is non-natively partitioned (returns 0 when inserting to parent). Also allows for when insert_on_fetch is false to reduce the open cursor time on the source.

IF v_jobmon THEN
    v_step_id := add_step(v_job_id, 'Inserting new/updated records into local table');
END IF;
EXECUTE format('CREATE TEMP TABLE refresh_logdel_full (%s)', v_cols_n_types); 
v_rowcount := 0;
v_total := 0;
v_local_insert_sql := format('INSERT INTO %I.%I (%s) SELECT %s FROM refresh_logdel_full', v_dest_schema_name, v_dest_table_name, v_cols, v_cols);
PERFORM dblink_open(v_dblink_name, 'mimeo_cursor', v_remote_f_sql);
LOOP
    v_fetch_sql := format('INSERT INTO refresh_logdel_full(%s) SELECT %s FROM dblink_fetch(%L, %L, %s) AS (%s)'
        , v_cols
        , v_cols
        , v_dblink_name
        , 'mimeo_cursor'
        , '50000'
        , v_cols_n_types);
    EXECUTE v_fetch_sql;
    GET DIAGNOSTICS v_rowcount = ROW_COUNT;
    v_total := v_total + coalesce(v_rowcount, 0);
    PERFORM gdb(p_debug,'Fetching rows in batches: '||v_total||' done so far.');
    IF v_jobmon AND v_insert_on_fetch THEN
        -- Avoid the overhead of jobmon logging each batch step to minimize transaction time on source when insert_on_fetch is false
        PERFORM update_step(v_step_id, 'PENDING', 'Fetching rows in batches: '||v_total||' done so far.');
    END IF;

    IF v_insert_on_fetch THEN
        EXECUTE v_local_insert_sql;
        TRUNCATE refresh_logdel_full;
    END IF;

    IF v_rowcount = 0 THEN
        -- Above rowcount variable is saved after temp table inserts. 
        -- So when temp table has the whole queue, this block of code should be reached
        PERFORM dblink_close(v_dblink_name, 'mimeo_cursor');
        IF v_insert_on_fetch = false THEN
            PERFORM gdb(p_debug,'Inserting into destination table in single batch (insert_on_fetch set to false)');
            IF v_jobmon THEN
                PERFORM update_step(v_step_id, 'PENDING', 'Inserting into destination table in single batch (insert_on_fetch set to false)...');
            END IF;
            EXECUTE v_local_insert_sql;
            IF v_jobmon THEN
                PERFORM update_step(v_step_id, 'OK', 'Inserted into destination table in single batch (insert_on_fetch set to false)');
            END IF;
        END IF;
        EXIT; -- leave insert loop
    END IF;

END LOOP;
IF v_jobmon THEN
    IF v_insert_on_fetch THEN
        PERFORM update_step(v_step_id, 'OK','Number of rows inserted: '||v_total);
    ELSE
        PERFORM update_step(v_step_id, 'OK','Number of rows inserted: '||v_total||'. Final insert to destination done as single batch (insert_on_fetch set to false)');
    END IF;
END IF;

-- insert records to local table (deleted rows to be kept)
IF v_jobmon THEN
    v_step_id := add_step(v_job_id,'Inserting deleted records into local table');
END IF;
v_insert_deleted_sql := format('INSERT INTO %I.%I (%s, mimeo_source_deleted) 
                                SELECT %s, mimeo_source_deleted FROM refresh_logdel_deleted', v_dest_schema_name, v_dest_table_name, v_cols, v_cols); 
PERFORM gdb(p_debug,v_insert_deleted_sql);
EXECUTE v_insert_deleted_sql;
GET DIAGNOSTICS v_rowcount = ROW_COUNT;
PERFORM gdb(p_debug,'Deleted rows inserted: '||v_rowcount::text);
IF v_jobmon THEN
    PERFORM update_step(v_step_id, 'OK','Inserted '||v_rowcount||' records');
END IF;
IF (v_total + v_rowcount) > (v_limit * .75) THEN
    IF v_jobmon THEN
        v_step_id := add_step(v_job_id, 'Row count warning');
        PERFORM update_step(v_step_id, 'WARNING','Row count fetched ('||v_total||') greater than 75% of batch limit ('||v_limit||'). Recommend increasing batch limit if possible.');
    END IF;
    v_batch_limit_reached := true;
END IF;

-- clean out rows from remote queue table
IF v_jobmon THEN
    v_step_id := add_step(v_job_id,'Cleaning out rows from remote queue table');
END IF;
v_trigger_delete := format('SELECT dblink_exec(%L,''DELETE FROM %I.%I WHERE processed = true'')', v_dblink_name, v_q_schema_name, v_q_table_name); 
PERFORM gdb(p_debug,v_trigger_delete);
EXECUTE v_trigger_delete INTO v_exec_status;
IF v_jobmon THEN
    PERFORM update_step(v_step_id, 'OK','Result was '||v_exec_status);
END IF;

-- update activity status
IF v_jobmon THEN
    v_step_id := add_step(v_job_id,'Updating last_run in config table');
END IF;
UPDATE refresh_config_logdel SET last_run = CURRENT_TIMESTAMP WHERE dest_table = p_destination; 
IF v_jobmon THEN
    PERFORM update_step(v_step_id, 'OK','Last Value was '||current_timestamp);
END IF;

PERFORM dblink_disconnect(v_dblink_name);

DROP TABLE IF EXISTS refresh_logdel_full;
DROP TABLE IF EXISTS refresh_logdel_queue;
DROP TABLE IF EXISTS refresh_logdel_deleted;

IF v_jobmon THEN
    IF v_batch_limit_reached = false THEN
        PERFORM close_job(v_job_id);
    ELSE
        -- Set final job status to level 2 (WARNING) to bring notice that the batch limit was reached and may need adjusting.
        -- Preventive warning to keep replication from falling behind.
        PERFORM fail_job(v_job_id, 2);
    END IF;
END IF;
-- Ensure old search path is reset for the current session
EXECUTE 'SELECT set_config(''search_path'','''||v_old_search_path||''',''false'')';

EXCEPTION
    WHEN QUERY_CANCELED THEN
        SELECT nspname INTO v_dblink_schema FROM pg_namespace n, pg_extension e WHERE e.extname = 'dblink' AND e.extnamespace = n.oid;
        EXECUTE format('SELECT %I.dblink_get_connections() @> ARRAY[%L]', v_dblink_schema, v_dblink_name) INTO v_link_exists;
        IF v_link_exists THEN
            EXECUTE format('SELECT %I.dblink_disconnect(%L)', v_dblink_schema, v_dblink_name);
        END IF;
        RAISE EXCEPTION '%', SQLERRM;
    WHEN OTHERS THEN
        SELECT nspname INTO v_dblink_schema FROM pg_namespace n, pg_extension e WHERE e.extname = 'dblink' AND e.extnamespace = n.oid;
        SELECT nspname INTO v_jobmon_schema FROM pg_namespace n, pg_extension e WHERE e.extname = 'pg_jobmon' AND e.extnamespace = n.oid;
        SELECT jobmon INTO v_jobmon FROM @extschema@.refresh_config_logdel WHERE dest_table = p_destination;
        v_jobmon := COALESCE(p_jobmon, v_jobmon);
        EXECUTE format('SELECT %I.dblink_get_connections() @> ARRAY[%L]', v_dblink_schema, v_dblink_name) INTO v_link_exists;
        IF v_link_exists THEN
            EXECUTE format('SELECT %I.dblink_disconnect(%L)', v_dblink_schema, v_dblink_name);
        END IF;
        IF v_jobmon AND v_jobmon_schema IS NOT NULL THEN
            IF v_job_id IS NULL THEN
                EXECUTE format('SELECT %I.add_job(%L)', v_jobmon_schema, 'Refresh Log Del: '||p_destination) INTO v_job_id;
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
 * Function that can monitor if snapshot tables may be growing too large for full table replication
 * Pass a minimum row count and/or byte size to return any table that grows larger than that one the source
 * Checks all snapshot tables unless the p_destination parameter is set to check a specific one
 */
CREATE FUNCTION snapshot_monitor(p_rowcount bigint DEFAULT NULL::bigint, p_size bigint DEFAULT NULL::bigint, p_destination text DEFAULT NULL::text, p_debug boolean DEFAULT false)
    RETURNS TABLE(dest_tablename text, source_rowcount bigint, source_size bigint)
    LANGUAGE plpgsql
AS $$
DECLARE

v_dblink            int;
v_dblink_name       text;
v_dblink_schema     text;
v_dest_table        text;
v_remote_sql        text;
v_result            bigint;
v_source_table      text;
v_sql               text;
v_table             record;

BEGIN

SELECT nspname INTO v_dblink_schema FROM pg_catalog.pg_namespace n, pg_catalog.pg_extension e WHERE e.extname = 'dblink' AND e.extnamespace = n.oid;

IF p_destination IS NOT NULL THEN
    v_sql := format('SELECT dest_table, dblink, source_table FROM @extschema@.refresh_config_snap WHERE dest_table= %L;' , p_destination); 
ELSE
    v_sql:= 'SELECT dest_table, dblink, source_table FROM @extschema@.refresh_config_snap';
END IF;


FOR v_table IN EXECUTE v_sql
LOOP
    IF p_debug THEN
        RAISE NOTICE 'v_table: %', v_table;
    END IF;
    v_dblink_name := mimeo.check_name_length('mimeo_snap_validation_'||v_table.source_table);

    EXECUTE format('SELECT %I.dblink_connect(%L, @extschema@.auth(%L))', v_dblink_schema, v_dblink_name, v_table.dblink);

    v_remote_sql := format('SELECT pg_total_relation_size(%L);', v_table.source_table);
    v_remote_sql := format('SELECT table_size FROM dblink.dblink(%L, %L) t (table_size bigint)', v_dblink_name, v_remote_sql);

    EXECUTE v_remote_sql INTO v_result;

    source_size := v_result::bigint;

    v_remote_sql := format('SELECT count(*) FROM %s;', v_table.source_table);
    v_remote_sql := format('SELECT table_count FROM %I.dblink(%L, %L) t (table_count int)', v_dblink_schema, v_dblink_name, v_remote_sql);

    EXECUTE v_remote_sql INTO v_result;

    source_rowcount := v_result::bigint;
    dest_tablename:= v_table.source_table;

    EXECUTE format('SELECT %I.dblink_disconnect(%L)', v_dblink_schema, v_dblink_name);

    IF p_debug THEN
        RAISE NOTICE 'p_rowcount: %, source_rowcount: % ', p_rowcount, source_rowcount;
        RAISE NOTICE 'p_size: %, source_size: % ', p_size, source_size;
    END IF;

    IF (p_rowcount IS NULL AND p_size IS NULL) THEN
        RETURN NEXT;
    ELSIF (p_rowcount IS NULL AND p_size IS NOT NULL) 
    AND (source_size >= p_size) THEN 
        RETURN NEXT;
    ELSIF (p_rowcount IS NOT NULL AND p_size IS NULL) 
    AND (source_rowcount >= p_rowcount) THEN 
        RETURN NEXT;
    ELSIF (p_rowcount IS NOT NULL AND p_size IS NOT NULL) 
    AND (source_rowcount >= p_rowcount OR source_size >= p_size) THEN 
        RETURN NEXT; 
    END IF;

END LOOP;

EXCEPTION 
WHEN QUERY_CANCELED THEN
    EXECUTE format('SELECT %I.dblink_disconnect(%L)', v_dblink_schema, v_dblink_name);
    RAISE EXCEPTION '%', SQLERRM;
WHEN OTHERS THEN 
    EXECUTE format('SELECT %I.dblink_disconnect(%L)', v_dblink_schema, v_dblink_name);
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
