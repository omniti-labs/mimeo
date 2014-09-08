/*
 *  Updater maker function.
 */
CREATE FUNCTION updater_maker(
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
v_jobmon                    boolean;
v_key_type                  text;
v_link_exists               boolean;
v_max_id                    bigint;
v_max_timestamp             timestamptz;
v_old_search_path           text;
v_pk_name                   text[] := p_pk_name;
v_pk_type                   text[] := p_pk_type;
v_remote_key_sql            text;
v_src_schema_name           text;
v_src_table_name            text;
v_table_exists              boolean;
v_update_refresh_config     text;

BEGIN

v_dblink_name := @extschema@.check_name_length('mimeo_updater_maker_'||p_src_table);

SELECT nspname INTO v_dblink_schema FROM pg_namespace n, pg_extension e WHERE e.extname = 'dblink' AND e.extnamespace = n.oid;
SELECT current_setting('search_path') INTO v_old_search_path;
EXECUTE 'SELECT set_config(''search_path'',''@extschema@,'||v_dblink_schema||',public'',''false'')';

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

PERFORM dblink_connect(v_dblink_name, @extschema@.auth(p_dblink_id));

SELECT schemaname, tablename INTO v_src_schema_name, v_src_table_name 
    FROM dblink(v_dblink_name, 'SELECT schemaname, tablename FROM pg_catalog.pg_tables WHERE schemaname ||''.''|| tablename = '||quote_literal(p_src_table)) t (schemaname text, tablename text);

IF v_src_table_name IS NULL THEN
    RAISE EXCEPTION 'Source table missing (%)', v_source_table;
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


-- Determine if pg_jobmon is installed to set config table option below
SELECT 
    CASE 
        WHEN count(nspname) > 0 THEN true
        ELSE false
    END AS jobmon_schema
INTO v_jobmon 
FROM pg_namespace n, pg_extension e WHERE e.extname = 'pg_jobmon' AND e.extnamespace = n.oid;

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
            ||', '||quote_literal('0001-01-01'::date)
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

SELECT p_table_exists FROM @extschema@.manage_dest_table(p_dest_table, NULL, p_debug) INTO v_table_exists;

SELECT schemaname, tablename 
INTO v_dest_schema_name, v_dest_table_name
FROM pg_catalog.pg_tables
WHERE schemaname||'.'||tablename = p_dest_table;

IF p_pulldata AND v_table_exists = false THEN
    RAISE NOTICE 'Pulling all data from source...';
    EXECUTE 'SELECT @extschema@.refresh_updater('||quote_literal(p_dest_table)||', p_repull := true, p_debug := '||p_debug||')';
END IF;

IF p_index AND v_table_exists = false THEN
    PERFORM create_index(p_dest_table, NULL, p_debug);
ELSIF v_table_exists = false THEN
-- Ensure destination indexes that are needed for efficient replication are created even if p_index is set false
    PERFORM gdb(p_debug, 'Creating indexes needed for replication');
    IF v_key_type = 'primary' THEN
        EXECUTE format('ALTER TABLE %I.%I', v_dest_schema_name, v_dest_table_name)||' ADD PRIMARY KEY('||array_to_string(v_pk_name, ',')||')';
    ELSE
        EXECUTE format('CREATE UNIQUE INDEX ON %I.%I', v_dest_schema_name, v_dest_table_name)||' ('||array_to_string(v_pk_name, ',')||')';
    END IF;
END IF;

IF v_table_exists THEN
    RAISE NOTICE 'Destination table % already exists. No data or indexes was pulled from source', p_dest_table;
END IF;

RAISE NOTICE 'Analyzing destination table...';
EXECUTE format('ANALYZE %I.%I', v_dest_schema_name, v_dest_table_name);

PERFORM dblink_disconnect(v_dblink_name);

IF p_type = 'time' THEN
    RAISE NOTICE 'Getting the maximum destination timestamp...';
    EXECUTE format('SELECT max(%I) FROM %I.%I', p_control_field, v_dest_schema_name, v_dest_table_name) INTO v_max_timestamp;
    EXECUTE format('UPDATE @extschema@.refresh_config_updater_time SET last_value = %L WHERE dest_table = %L', COALESCE(v_max_timestamp, CURRENT_TIMESTAMP), p_dest_table);
ELSIF p_type = 'serial' THEN
    RAISE NOTICE 'Getting the maximum destination id...';
    EXECUTE format('SELECT max(%I) FROM %I.%I', p_control_field, v_dest_schema_name, v_dest_table_name) INTO v_max_id;
    EXECUTE format('UPDATE @extschema@.refresh_config_updater_serial SET last_value = %L WHERE dest_table = %L', COALESCE(v_max_id, 0), p_dest_table);
END IF;

EXECUTE 'SELECT set_config(''search_path'','''||v_old_search_path||''',''false'')';

RAISE NOTICE 'Done';

RETURN;

EXCEPTION
    WHEN QUERY_CANCELED OR OTHERS THEN
        EXECUTE 'SELECT '||v_dblink_schema||'.dblink_get_connections() @> ARRAY['||quote_literal(v_dblink_name)||']' INTO v_link_exists;
        IF v_link_exists THEN
            EXECUTE 'SELECT '||v_dblink_schema||'.dblink_disconnect('||quote_literal(v_dblink_name)||')';
        END IF;
        RAISE EXCEPTION '%', SQLERRM;
END
$$;


