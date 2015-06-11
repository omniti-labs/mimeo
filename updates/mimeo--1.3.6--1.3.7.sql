-- Fixed bug in snapshot replication where it would not always catch every column change on the source.

/*
 *  Snap refresh to repull all table data
 */
CREATE OR REPLACE FUNCTION refresh_snap(p_destination text, p_index boolean DEFAULT true, p_pulldata boolean DEFAULT true, p_jobmon boolean DEFAULT NULL, p_lock_wait int DEFAULT NULL, p_debug boolean DEFAULT false) RETURNS void
    LANGUAGE plpgsql SECURITY DEFINER
    AS $$
DECLARE

v_adv_lock          boolean; 
v_cols_n_types      text[];
v_cols              text;
v_condition         text;
v_create_sql        text;
v_dblink            int;
v_dblink_name       text;
v_dblink_schema     text;
v_dest_schema_name  text;
v_dest_table        text;
v_dest_table_name   text;
v_exists            int;
v_fetch_sql         text;
v_filter            text[];
v_insert_sql        text;
v_job_id            int;
v_jobmon            boolean;
v_jobmon_schema     text;
v_job_name          text;
v_lcols_array       text[];
v_link_exists       boolean;
v_local_sql         text;
v_l                 text;
v_match             boolean = true;
v_old_grant         record;
v_old_owner         text;
v_old_search_path   text;
v_old_snap          text;
v_old_snap_table    text;
v_parts             record;
v_post_script       text[];
v_refresh_snap      text;
v_remote_index_sql  text;
v_remote_sql        text;
v_row               record;
v_rowcount          bigint;
v_r                 text;
v_snap              text;
v_source_table      text;
v_src_schema_name   text;
v_src_table_name    text;
v_step_id           int;
v_table_exists      boolean;
v_total             bigint := 0;
v_tup_del           bigint;
v_tup_ins           bigint;
v_tup_upd           bigint;
v_tup_del_new       bigint;
v_tup_ins_new       bigint;
v_tup_upd_new       bigint;
v_view_definition   text;

BEGIN

IF p_debug IS DISTINCT FROM true THEN
    PERFORM set_config( 'client_min_messages', 'notice', true );
END IF;

v_job_name := 'Refresh Snap: '||p_destination;
v_dblink_name := @extschema@.check_name_length('mimeo_snap_refresh_'||p_destination);

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
    , filter
    , condition
    , n_tup_ins
    , n_tup_upd
    , n_tup_del
    , post_script 
    , jobmon
INTO v_source_table
    , v_dest_table
    , v_dblink
    , v_filter
    , v_condition
    , v_tup_ins
    , v_tup_upd
    , v_tup_del
    , v_post_script 
    , v_jobmon
FROM refresh_config_snap
WHERE dest_table = p_destination; 
IF NOT FOUND THEN
   RAISE EXCEPTION 'No configuration found for %',v_job_name; 
END IF;  

-- Allow override with parameter
v_jobmon := COALESCE(p_jobmon, v_jobmon);

-- Take advisory lock to prevent multiple calls to function overlapping and causing possible deadlock
v_adv_lock := @extschema@.concurrent_lock_check(v_dest_table, p_lock_wait);
IF v_adv_lock = 'false' THEN
    IF v_jobmon THEN
        v_job_id := add_job(v_job_name);
        v_step_id := add_step(v_job_id,'Obtaining advisory lock for job: '||v_job_name);
        PERFORM update_step(v_step_id, 'WARNING','Found concurrent job. Exiting gracefully');
        PERFORM fail_job(v_job_id, 2);
    END IF;
    PERFORM gdb(p_debug,'Obtaining advisory lock FAILED for job: '||v_job_name);
    RAISE NOTICE 'Found concurrent job. Exiting gracefully';
    EXECUTE 'SELECT set_config(''search_path'','''||v_old_search_path||''',''false'')';
    RETURN;
END IF;

IF v_jobmon THEN
    v_job_id := add_job(v_job_name);
    PERFORM gdb(p_debug,'Job ID: '||v_job_id::text);
    v_step_id := add_step(v_job_id,'Grabbing Mapping, Building SQL');
END IF;

-- checking for current view
SELECT definition
INTO v_view_definition
FROM pg_views
WHERE schemaname ||'.'|| viewname = v_dest_table;

PERFORM dblink_connect(v_dblink_name, @extschema@.auth(v_dblink));

IF v_jobmon THEN
    PERFORM update_step(v_step_id, 'OK','Done');
    v_step_id := add_step(v_job_id,'Truncate non-active snap table');
END IF;

v_exists := strpos(v_view_definition, 'snap1');
  IF v_exists > 0 THEN
    v_snap := 'snap2';
    v_old_snap := 'snap1';
    ELSE
    v_snap := 'snap1';
    v_old_snap := 'snap2';
 END IF;
v_refresh_snap := v_dest_table||'_'||v_snap;
v_old_snap_table := v_dest_table||'_'||v_old_snap;
PERFORM gdb(p_debug,'v_refresh_snap: '||v_refresh_snap::text);

-- Split schemas and table names into their own variables
v_dest_schema_name := split_part(v_dest_table, '.', 1); 
v_dest_table_name := split_part(v_dest_table, '.', 2); 
v_refresh_snap := split_part(v_refresh_snap, '.', 2);
v_old_snap_table := split_part(v_old_snap_table, '.', 2);

-- Create snap table if it doesn't exist
PERFORM gdb(p_debug, 'Getting table columns and creating destination table if it doesn''t exist');
-- v_cols is never used as an array in this function. v_cols_n_types is used as both.
SELECT p_table_exists
    , array_to_string(p_cols, ',')
    , p_cols_n_types
    , p_source_schema_name
    , p_source_table_name
INTO v_table_exists
    , v_cols
    , v_cols_n_types 
    , v_src_schema_name
    , v_src_table_name
FROM manage_dest_table(v_dest_table, v_snap, v_dblink_name, p_debug);

IF v_table_exists THEN 
/* Check local column definitions against remote and recreate table if different. 
Allows automatic recreation of snap tables if columns change (add, drop type change)  */  
    v_local_sql := 'SELECT array_agg(''"''||a.attname||''"''||'' ''||format_type(a.atttypid, a.atttypmod)::text) as cols_n_types 
                    FROM pg_catalog.pg_attribute a
                    JOIN pg_catalog.pg_class c ON a.attrelid = c.oid
                    JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid
                    WHERE n.nspname = '||quote_literal(v_dest_schema_name)||'
                    AND c.relname = '||quote_literal(v_refresh_snap)||'
                    AND a.attnum > 0 
                    AND a.attisdropped is false';
    PERFORM gdb(p_debug, v_local_sql);

    EXECUTE v_local_sql INTO v_lcols_array;
    PERFORM gdb(p_debug, 'v_lcols_array: {'||array_to_string(v_lcols_array,',')||'}');
    -- Check to see if there's a change in the column structure on the remote
    FOREACH v_r IN ARRAY v_cols_n_types LOOP
        v_match := false;
        FOREACH v_l IN ARRAY v_lcols_array LOOP
            IF v_r = v_l THEN
                v_match := true;
                EXIT;
            END IF;
        END LOOP;
        IF v_match = false THEN
            EXIT;
        END IF;
    END LOOP;

    IF v_match = false THEN
        CREATE TEMP TABLE mimeo_snapshot_grants_tmp (statement text);
        -- Grab old table privileges. They are applied later after the view is recreated/swapped
        FOR v_old_grant IN 
            SELECT table_schema ||'.'|| table_name AS tablename
                , array_agg(privilege_type::text) AS types
                , grantee
            FROM information_schema.table_privileges 
            WHERE table_schema = v_dest_schema_name
            AND table_name = v_refresh_snap
            GROUP BY grantee, table_schema, table_name 
        LOOP
            INSERT INTO mimeo_snapshot_grants_tmp VALUES ( 
                format('GRANT '||array_to_string(v_old_grant.types, ',')||' ON %I.%I TO %I', v_dest_schema_name, v_refresh_snap, v_old_grant.grantee)
            );
        END LOOP;
        -- Grab old view privileges. They are applied later after the view is recreated/swapped
        FOR v_old_grant IN 
            SELECT table_schema ||'.'|| table_name AS tablename
                , array_agg(privilege_type::text) AS types
                , grantee
            FROM information_schema.table_privileges 
            WHERE table_schema = v_dest_schema_name
            AND table_name = v_dest_table_name
            GROUP BY grantee, table_schema, table_name 
        LOOP
            INSERT INTO mimeo_snapshot_grants_tmp VALUES ( 
                format('GRANT '||array_to_string(v_old_grant.types, ',')||' ON %I.%I TO %I', v_dest_schema_name, v_dest_table_name, v_old_grant.grantee)
            );
        END LOOP;
        SELECT viewowner INTO v_old_owner FROM pg_views WHERE schemaname ||'.'|| viewname = v_dest_table;

        EXECUTE format('DROP TABLE %I.%I', v_dest_schema_name, v_refresh_snap);
        EXECUTE format('DROP VIEW %I.%I', v_dest_schema_name, v_dest_table_name);
        PERFORM manage_dest_table(v_dest_table, v_snap, v_dblink_name, p_debug);

        IF v_jobmon THEN
            v_step_id := add_step(v_job_id,'Source table structure changed.');
            PERFORM update_step(v_step_id, 'OK','Tables and view dropped and recreated. Please double-check snap table attributes (permissions, indexes, etc');
        END IF;
        PERFORM gdb(p_debug,'Source table structure changed. Tables and view dropped and recreated. Please double-check snap table attributes (permissions, indexes, etc)');
    END IF;
    -- truncate non-active snap table
    EXECUTE format('TRUNCATE TABLE %I.%I', v_dest_schema_name, v_refresh_snap);

    IF v_jobmon THEN
        PERFORM update_step(v_step_id, 'OK','Done');
    END IF;
END IF;

-- Only check the remote data if there have been no column changes and snap table actually exists. 
-- Otherwise maker functions won't work if source is empty & view switch won't happen properly.
IF  v_table_exists AND v_match THEN
    v_remote_sql := format('SELECT n_tup_ins, n_tup_upd, n_tup_del 
                    FROM pg_catalog.pg_stat_all_tables 
                    WHERE schemaname = %L
                    AND relname = %L',
                    v_src_schema_name, v_src_table_name);
    v_remote_sql := 'SELECT n_tup_ins, n_tup_upd, n_tup_del 
                    FROM dblink('||quote_literal(v_dblink_name)||', ' || quote_literal(v_remote_sql) || ') AS
                    t (n_tup_ins bigint, n_tup_upd bigint, n_tup_del bigint)';
    PERFORM gdb(p_debug,'v_remote_sql: '||v_remote_sql);
    EXECUTE v_remote_sql INTO v_tup_ins_new, v_tup_upd_new, v_tup_del_new;
    IF v_tup_ins_new = v_tup_ins AND v_tup_upd_new = v_tup_upd AND v_tup_del_new = v_tup_del THEN
        PERFORM gdb(p_debug,'Remote table has not had any writes. Skipping data pull');
        IF v_jobmon THEN
            PERFORM update_step(v_step_id, 'OK', 'Remote table has not had any writes. Skipping data pull');
        END IF;
        UPDATE refresh_config_snap SET last_run = CURRENT_TIMESTAMP WHERE dest_table = p_destination;
        PERFORM dblink_disconnect(v_dblink_name);
        IF v_jobmon THEN
            PERFORM close_job(v_job_id);
        END IF;
        EXECUTE 'SELECT set_config(''search_path'','''||v_old_search_path||''',''false'')';
        RETURN;
    END IF;
END IF;

v_remote_sql := format('SELECT '|| v_cols ||' FROM %I.%I', v_src_schema_name, v_src_table_name);
-- Used by p_pulldata parameter in maker function
IF p_pulldata = false THEN
    v_remote_sql := v_remote_sql || ' LIMIT 0';
ELSIF v_condition IS NOT NULL THEN
    v_remote_sql := v_remote_sql || ' ' || v_condition;
END IF;

IF v_jobmon THEN
    v_step_id := add_step(v_job_id,'Inserting records into local table');
END IF;
PERFORM dblink_open(v_dblink_name, 'mimeo_cursor', v_remote_sql);

v_rowcount := 0;
LOOP
    v_fetch_sql := format('INSERT INTO %I.%I ('|| v_cols ||')', v_dest_schema_name, v_refresh_snap);
    v_fetch_sql := v_fetch_sql || 'SELECT '|| v_cols ||' FROM dblink_fetch('||quote_literal(v_dblink_name)||', ''mimeo_cursor'', 50000) AS ('||array_to_string(v_cols_n_types, ',')||')';
    PERFORM gdb(p_debug, 'v_fetch_sql: '||v_fetch_sql);
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

IF v_jobmon THEN
    PERFORM update_step(v_step_id, 'OK','Inserted '||v_total||' rows');
END IF;

-- Create indexes if new table was created
IF (v_table_exists = false OR v_match = 'f') AND p_index = true THEN
    PERFORM gdb(p_debug, 'Creating indexes');
    PERFORM create_index(v_dest_table, v_src_schema_name, v_src_table_name, v_snap, p_debug);
END IF;

EXECUTE format('ANALYZE %I.%I', v_dest_schema_name, v_refresh_snap);

-- swap view
IF v_jobmon THEN
    v_step_id := add_step(v_job_id,'Swap view to '||v_refresh_snap);
END IF;
PERFORM gdb(p_debug,'Swapping view to '||v_refresh_snap);
EXECUTE format('CREATE OR REPLACE VIEW %I.%I AS SELECT * FROM %I.%I', v_dest_schema_name, v_dest_table_name, v_dest_schema_name, v_refresh_snap);
IF v_jobmon THEN
    PERFORM update_step(v_step_id, 'OK','View Swapped');
END IF;

IF v_match = false THEN
    -- Actually apply the original privileges if the table was recreated
    IF v_jobmon THEN
        v_step_id := add_step(v_job_id,'Applying original privileges to recreated snap table');
    END IF;
    FOR v_old_grant IN SELECT statement FROM mimeo_snapshot_grants_tmp
    LOOP
        PERFORM gdb(p_debug, 'v_old_grant.statement: '|| v_old_grant.statement);
        EXECUTE v_old_grant.statement;
    END LOOP;
    DROP TABLE IF EXISTS mimeo_snapshot_grants_tmp;
    EXECUTE format('ALTER VIEW %I.%I OWNER TO %I', v_dest_schema_name, v_dest_table_name, v_old_owner);
    EXECUTE format('ALTER TABLE %I.%I OWNER TO %I', v_dest_schema_name, v_refresh_snap, v_old_owner);
    IF v_jobmon THEN
        PERFORM update_step(v_step_id, 'OK','Done');
    END IF;

    -- Run any special sql to fix anything that was done to destination tables (extra indexes, etc)
    IF v_post_script IS NOT NULL THEN
        IF v_jobmon THEN
            v_step_id := add_step(v_job_id,'Applying post_script sql commands due to schema change');
        END IF;
        PERFORM @extschema@.post_script(v_dest_table);
        IF v_jobmon THEN
            PERFORM update_step(v_step_id, 'OK','Done');
        END IF;
    END IF;
END IF;

SELECT
    CASE
        WHEN count(1) > 0 THEN true
        ELSE false 
    END
INTO v_table_exists 
FROM pg_catalog.pg_tables 
WHERE schemaname = v_dest_schema_name
AND tablename = v_old_snap_table;

IF v_table_exists THEN
    IF v_jobmon THEN
        v_step_id := add_step(v_job_id,'Truncating old snap table');
    END IF;
    EXECUTE format('TRUNCATE TABLE %I.%I', v_dest_schema_name, v_old_snap_table);
    IF v_jobmon THEN
        PERFORM update_step(v_step_id, 'OK','Done');
    END IF;
END IF;

IF v_jobmon THEN
    v_step_id := add_step(v_job_id,'Updating last_run & tuple change values');
END IF;
UPDATE refresh_config_snap SET
    last_run = CURRENT_TIMESTAMP
    , n_tup_ins = v_tup_ins_new
    , n_tup_upd = v_tup_upd_new
    , n_tup_del = v_tup_del_new
WHERE dest_table = p_destination;
IF v_jobmon THEN
    PERFORM update_step(v_step_id, 'OK','Done');
END IF;

PERFORM dblink_disconnect(v_dblink_name);

IF v_jobmon THEN
    PERFORM close_job(v_job_id);
END IF;

-- Ensure old search path is reset for the current session
EXECUTE 'SELECT set_config(''search_path'','''||v_old_search_path||''',''false'')';

EXCEPTION
    WHEN QUERY_CANCELED THEN
        EXECUTE 'SELECT '||v_dblink_schema||'.dblink_get_connections() @> ARRAY['||quote_literal(v_dblink_name)||']' INTO v_link_exists;
        IF v_link_exists THEN
            EXECUTE 'SELECT '||v_dblink_schema||'.dblink_disconnect('||quote_literal(v_dblink_name)||')';
        END IF;
        RAISE EXCEPTION '%', SQLERRM;   
    WHEN OTHERS THEN
        EXECUTE 'SELECT '||v_dblink_schema||'.dblink_get_connections() @> ARRAY['||quote_literal(v_dblink_name)||']' INTO v_link_exists;
        IF v_link_exists THEN
            EXECUTE 'SELECT '||v_dblink_schema||'.dblink_disconnect('||quote_literal(v_dblink_name)||')';
        END IF;
        IF v_jobmon THEN
            IF v_job_id IS NULL THEN
                EXECUTE 'SELECT '||v_jobmon_schema||'.add_job(''Refresh Snap: '||p_destination||''')' INTO v_job_id;
                EXECUTE 'SELECT '||v_jobmon_schema||'.add_step('||v_job_id||', ''EXCEPTION before job logging started'')' INTO v_step_id;
            END IF;
            IF v_step_id IS NULL THEN
                EXECUTE 'SELECT '||v_jobmon_schema||'.add_step('||v_job_id||', ''EXCEPTION before first step logged'')' INTO v_step_id;
            END IF;
            EXECUTE 'SELECT '||v_jobmon_schema||'.update_step('||v_step_id||', ''CRITICAL'', ''ERROR: '||COALESCE(SQLERRM,'unknown')||''')';
            EXECUTE 'SELECT '||v_jobmon_schema||'.fail_job('||v_job_id||')';
        END IF;
        RAISE EXCEPTION '%', SQLERRM;
END
$$;


