/*
 *  Inserter maker function. 
 */
CREATE FUNCTION inserter_maker(
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


