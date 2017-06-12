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
v_cursor_name           text;
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

-- Ensure name is consistent in case it would get truncated by maximium object name length
v_cursor_name := @extschema@.check_name_length('mimeo_cursor_' || v_src_table_name, p_convert_standard := true);

-- create temp table for recording deleted rows
EXECUTE format('CREATE TEMP TABLE refresh_logdel_deleted (%s, mimeo_source_deleted timestamptz)', v_cols_n_types);
v_remote_d_sql := format('SELECT %s, mimeo_source_deleted FROM %I.%I WHERE processed = true and mimeo_source_deleted IS NOT NULL', v_cols, v_q_schema_name, v_q_table_name);
PERFORM dblink_open(v_dblink_name, v_cursor_name, v_remote_d_sql);
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
        , v_cursor_name 
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
PERFORM dblink_close(v_dblink_name, v_cursor_name);
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
    PERFORM dblink_open(v_dblink_name, v_cursor_name, v_remote_q_sql);
    IF v_jobmon THEN
        v_step_id := add_step(v_job_id, 'Creating local queue temp table for inserts/updates');
    END IF;
    v_rowcount := 0;
    LOOP
        v_fetch_sql := format('INSERT INTO refresh_logdel_queue(%s) SELECT %s FROM dblink_fetch(%L, %L, %s) AS (%s)'
            , v_pk_name_csv
            , v_pk_name_csv
            , v_dblink_name
            , v_cursor_name 
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
    PERFORM dblink_close(v_dblink_name, v_cursor_name);
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
PERFORM dblink_open(v_dblink_name, v_cursor_name, v_remote_f_sql);
LOOP
    v_fetch_sql := format('INSERT INTO refresh_logdel_full(%s) SELECT %s FROM dblink_fetch(%L, %L, %s) AS (%s)'
        , v_cols
        , v_cols
        , v_dblink_name
        , v_cursor_name 
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
        PERFORM dblink_close(v_dblink_name, v_cursor_name);
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

