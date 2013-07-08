-- Made the pg_jobmon extension optional. It can be turned on or off on a per replication table basis using the new "jobmon" boolean column in the config table. If pg_jobmon is installed it will be set to true by default for all replication types, otherwise it will be set false. You can also set whether it is used at runtime with the "p_jobmon" parameter to the refresh functions.
-- Jobmon logging has been added to the table replication method.
-- Added row count validation function. Just does a basic row count comparison between the source and destination. Snapshot & incremental should match rows exactly. DML/Logdel most likely will not match exactly due to the nature of their replication methods, but they should be close.
-- Changed the parameter in the destroyer functions for keeping/destroying the destination. It is now a boolean and defaults to true for KEEPING the table. Before, that parameter would default to destroying the table unless you passed a specific value. This should hopefully be safer stop accidentally destroying the destination table unless you specifically ask to do that. 

ALTER TABLE @extschema@.refresh_config ADD COLUMN jobmon boolean NOT NULL DEFAULT false;
UPDATE @extschema@.refresh_config_dml SET jobmon = true;
UPDATE @extschema@.refresh_config_inserter SET jobmon = true;
UPDATE @extschema@.refresh_config_logdel SET jobmon = true;
UPDATE @extschema@.refresh_config_snap SET jobmon = true;
UPDATE @extschema@.refresh_config_table SET jobmon = true;
UPDATE @extschema@.refresh_config_updater SET jobmon = true;

-- Preserve dropped function privileges. Re-applied at the end of this update.
CREATE TEMP TABLE mimeo_preserve_privs_temp (statement text);

INSERT INTO mimeo_preserve_privs_temp 
SELECT 'GRANT EXECUTE ON FUNCTION @extschema@.refresh_dml(text, int, boolean, boolean, boolean) TO '||array_to_string(array_agg(grantee::text), ',')||';' 
FROM information_schema.routine_privileges
WHERE routine_schema = '@extschema@'
AND routine_name = 'refresh_dml'; 

INSERT INTO mimeo_preserve_privs_temp 
SELECT 'GRANT EXECUTE ON FUNCTION @extschema@.refresh_inserter(text, integer, boolean, text, text, boolean, boolean) TO '||array_to_string(array_agg(grantee::text), ',')||';' 
FROM information_schema.routine_privileges
WHERE routine_schema = '@extschema@'
AND routine_name = 'refresh_inserter'; 

INSERT INTO mimeo_preserve_privs_temp 
SELECT 'GRANT EXECUTE ON FUNCTION @extschema@.refresh_logdel(text, int, boolean, boolean, boolean) TO '||array_to_string(array_agg(grantee::text), ',')||';' 
FROM information_schema.routine_privileges
WHERE routine_schema = '@extschema@'
AND routine_name = 'refresh_logdel'; 

INSERT INTO mimeo_preserve_privs_temp 
SELECT 'GRANT EXECUTE ON FUNCTION @extschema@.refresh_snap(text, boolean, boolean, boolean, boolean) TO '||array_to_string(array_agg(grantee::text), ',')||';' 
FROM information_schema.routine_privileges
WHERE routine_schema = '@extschema@'
AND routine_name = 'refresh_snap'; 

INSERT INTO mimeo_preserve_privs_temp 
SELECT 'GRANT EXECUTE ON FUNCTION @extschema@.refresh_table(text, boolean, boolean, boolean) TO '||array_to_string(array_agg(grantee::text), ',')||';' 
FROM information_schema.routine_privileges
WHERE routine_schema = '@extschema@'
AND routine_name = 'refresh_table'; 

INSERT INTO mimeo_preserve_privs_temp 
SELECT 'GRANT EXECUTE ON FUNCTION @extschema@.refresh_updater(text, integer, boolean, text, text, boolean, boolean) TO '||array_to_string(array_agg(grantee::text), ',')||';' 
FROM information_schema.routine_privileges
WHERE routine_schema = '@extschema@'
AND routine_name = 'refresh_updater'; 

INSERT INTO mimeo_preserve_privs_temp 
SELECT 'GRANT EXECUTE ON FUNCTION @extschema@.dml_destroyer(text, boolean) TO '||array_to_string(array_agg(grantee::text), ',')||';' 
FROM information_schema.routine_privileges
WHERE routine_schema = '@extschema@'
AND routine_name = 'dml_destroyer'; 

INSERT INTO mimeo_preserve_privs_temp 
SELECT 'GRANT EXECUTE ON FUNCTION @extschema@.inserter_destroyer(text, boolean) TO '||array_to_string(array_agg(grantee::text), ',')||';' 
FROM information_schema.routine_privileges
WHERE routine_schema = '@extschema@'
AND routine_name = 'inserter_destroyer'; 

INSERT INTO mimeo_preserve_privs_temp 
SELECT 'GRANT EXECUTE ON FUNCTION @extschema@.logdel_destroyer(text, boolean) TO '||array_to_string(array_agg(grantee::text), ',')||';' 
FROM information_schema.routine_privileges
WHERE routine_schema = '@extschema@'
AND routine_name = 'logdel_destroyer'; 

INSERT INTO mimeo_preserve_privs_temp 
SELECT 'GRANT EXECUTE ON FUNCTION @extschema@.snapshot_destroyer(text, boolean) TO '||array_to_string(array_agg(grantee::text), ',')||';' 
FROM information_schema.routine_privileges
WHERE routine_schema = '@extschema@'
AND routine_name = 'snapshot_destroyer'; 

INSERT INTO mimeo_preserve_privs_temp 
SELECT 'GRANT EXECUTE ON FUNCTION @extschema@.table_destroyer(text, boolean) TO '||array_to_string(array_agg(grantee::text), ',')||';' 
FROM information_schema.routine_privileges
WHERE routine_schema = '@extschema@'
AND routine_name = 'table_destroyer'; 

INSERT INTO mimeo_preserve_privs_temp 
SELECT 'GRANT EXECUTE ON FUNCTION @extschema@.updater_destroyer(text, boolean) TO '||array_to_string(array_agg(grantee::text), ',')||';' 
FROM information_schema.routine_privileges
WHERE routine_schema = '@extschema@'
AND routine_name = 'updater_destroyer'; 

CREATE FUNCTION replay_preserved_privs() RETURNS void
    LANGUAGE plpgsql
    AS $$
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

DROP FUNCTION refresh_dml(text, int, boolean, boolean);
DROP FUNCTION refresh_inserter(text, integer, boolean, text, text, boolean);
DROP FUNCTION refresh_logdel(text, int, boolean, boolean);
DROP FUNCTION refresh_snap(text, boolean, boolean, boolean);
DROP FUNCTION refresh_table(text, boolean, boolean);
DROP FUNCTION refresh_updater(text, integer, boolean, text, text, boolean);
DROP FUNCTION dml_destroyer(text, text);
DROP FUNCTION inserter_destroyer(text, text);
DROP FUNCTION logdel_destroyer(text, text);
DROP FUNCTION snapshot_destroyer(text, text);
DROP FUNCTION table_destroyer(text, text);
DROP FUNCTION updater_destroyer(text, text);

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


/*
 *  Refresh based on DML (Insert, Update, Delete)
 */
CREATE FUNCTION refresh_dml(p_destination text, p_limit int default NULL, p_repull boolean DEFAULT false, p_jobmon boolean DEFAULT NULL, p_debug boolean DEFAULT false) RETURNS void
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
v_dest_table            text;
v_exec_status           text;
v_fetch_sql             text;
v_field                 text;
v_filter                text[];
v_insert_sql            text;
v_job_id                int;
v_jobmon_schema         text;
v_job_name              text;
v_jobmon                boolean;
v_limit                 int; 
v_link_exists           boolean;
v_old_search_path       text;
v_pk_counter            int;
v_pk_name_csv           text;
v_pk_name_type_csv      text := '';
v_pk_name               text[];
v_pk_type               text[];
v_pk_where              text := '';
v_remote_f_sql          text;
v_remote_q_sql          text;
v_rowcount              bigint := 0; 
v_source_table          text;
v_step_id               int;
v_tmp_table             text;
v_total                 bigint := 0;
v_trigger_delete        text; 
v_trigger_update        text;
v_truncate_remote_q     text;
v_with_update           text;

BEGIN

IF p_debug IS DISTINCT FROM true THEN
    PERFORM set_config( 'client_min_messages', 'warning', true );
END IF;

v_job_name := 'Refresh DML: '||p_destination;
v_dblink_name := 'mimeo_dml_refresh_'||p_destination;

SELECT nspname INTO v_dblink_schema FROM pg_namespace n, pg_extension e WHERE e.extname = 'dblink' AND e.extnamespace = n.oid;
SELECT nspname INTO v_jobmon_schema FROM pg_namespace n, pg_extension e WHERE e.extname = 'pg_jobmon' AND e.extnamespace = n.oid;

-- Set custom search path to allow easier calls to other functions, especially job logging
SELECT current_setting('search_path') INTO v_old_search_path;
EXECUTE 'SELECT set_config(''search_path'',''@extschema@,'||COALESCE(v_jobmon_schema||',', '')||v_dblink_schema||',public'',''false'')';

SELECT source_table
    , dest_table
    , 'tmp_'||replace(dest_table,'.','_')
    , dblink
    , control
    , pk_name
    , pk_type
    , filter
    , condition
    , batch_limit 
    , jobmon
INTO v_source_table
    , v_dest_table
    , v_tmp_table
    , v_dblink
    , v_control
    , v_pk_name
    , v_pk_type
    , v_filter
    , v_condition
    , v_limit
    , v_jobmon
FROM refresh_config_dml 
WHERE dest_table = p_destination; 
IF NOT FOUND THEN
   RAISE EXCEPTION 'No configuration found for %',v_job_name; 
END IF;

-- Allow override with parameter
v_jobmon := COALESCE(p_jobmon, v_jobmon);

-- Take advisory lock to prevent multiple calls to function overlapping
v_adv_lock := pg_try_advisory_lock(hashtext('refresh_dml'), hashtext(v_job_name));
IF v_adv_lock = 'false' THEN
    IF v_jobmon THEN
        v_job_id := add_job(v_job_name);
        v_step_id := add_step(v_job_id,'Obtaining advisory lock for job: '||v_job_name);
        PERFORM update_step(v_step_id, 'WARNING','Found concurrent job. Exiting gracefully');
        PERFORM fail_job(v_job_id, 2);
    END IF;
    PERFORM gdb(p_debug,'Obtaining advisory lock FAILED for job: '||v_job_name);
    EXECUTE 'SELECT set_config(''search_path'','''||v_old_search_path||''',''false'')';
    RETURN;
END IF;

IF v_jobmon THEN
    v_job_id := add_job(v_job_name);
    PERFORM gdb(p_debug,'Job ID: '||v_job_id::text);
    v_step_id := add_step(v_job_id,'Sanity check primary/unique key values');
END IF;

IF v_pk_name IS NULL OR v_pk_type IS NULL THEN
    RAISE EXCEPTION 'Primary key fields in refresh_config_dml must be defined';
END IF;

-- determine column list, column type list
IF v_filter IS NULL THEN 
    SELECT array_to_string(array_agg(attname),','), array_to_string(array_agg(attname||' '||format_type(atttypid, atttypmod)::text),',') 
        INTO v_cols, v_cols_n_types
        FROM pg_attribute WHERE attrelid = p_destination::regclass AND attnum > 0 AND attisdropped is false;
ELSE
    -- ensure all primary key columns are included in any column filters
    FOREACH v_field IN ARRAY v_pk_name LOOP
        IF v_field = ANY(v_filter) THEN
            CONTINUE;
        ELSE
            RAISE EXCEPTION 'Filter list did not contain all columns that compose primary/unique key for %',v_job_name; 
        END IF;
    END LOOP;
    SELECT array_to_string(array_agg(attname),','), array_to_string(array_agg(attname||' '||format_type(atttypid, atttypmod)::text),',') 
        INTO v_cols, v_cols_n_types
        FROM pg_attribute WHERE attrelid = p_destination::regclass AND ARRAY[attname::text] <@ v_filter AND attnum > 0 AND attisdropped is false;
END IF;    

IF p_limit IS NOT NULL THEN
    v_limit := p_limit;
END IF;

v_pk_name_csv := array_to_string(v_pk_name, ',');
v_pk_counter := 1;
WHILE v_pk_counter <= array_length(v_pk_name,1) LOOP
    IF v_pk_counter > 1 THEN
        v_pk_name_type_csv := v_pk_name_type_csv || ', ';
        v_pk_where := v_pk_where ||' AND ';
    END IF;
    v_pk_name_type_csv := v_pk_name_type_csv ||v_pk_name[v_pk_counter]||' '||v_pk_type[v_pk_counter];
    v_pk_where := v_pk_where || ' a.'||v_pk_name[v_pk_counter]||' = b.'||v_pk_name[v_pk_counter];
    v_pk_counter := v_pk_counter + 1;
END LOOP;

IF v_jobmon THEN
    PERFORM update_step(v_step_id, 'OK','Done');
END IF;

PERFORM dblink_connect(v_dblink_name, auth(v_dblink));

-- update remote entries
IF v_jobmon THEN
    v_step_id := add_step(v_job_id,'Updating remote trigger table');
END IF;
v_with_update := 'WITH a AS (SELECT '||v_pk_name_csv||' FROM '|| v_control ||' ORDER BY '||v_pk_name_csv||' LIMIT '|| COALESCE(v_limit::text, 'ALL') ||') UPDATE '||v_control||' b SET processed = true FROM a WHERE '||v_pk_where;
v_trigger_update := 'SELECT dblink_exec('||quote_literal(v_dblink_name)||','|| quote_literal(v_with_update)||')';
PERFORM gdb(p_debug,v_trigger_update);
EXECUTE v_trigger_update INTO v_exec_status;    
IF v_jobmon THEN
    PERFORM update_step(v_step_id, 'OK','Result was '||v_exec_status);
END IF;

IF p_repull THEN
    -- Do truncate of remote queue table here before full data pull is actually started to ensure all new changes are recorded
    IF v_jobmon THEN
        PERFORM update_step(v_step_id, 'OK','Request to repull ALL data from source. This could take a while...');
    END IF;
    PERFORM gdb(p_debug, 'Request to repull ALL data from source. This could take a while...');
    v_truncate_remote_q := 'SELECT dblink_exec('||quote_literal(v_dblink_name)||','||quote_literal('TRUNCATE TABLE '||v_control)||')';
    EXECUTE v_truncate_remote_q;

    IF v_jobmon THEN
        v_step_id := add_step(v_job_id,'Truncating local table');
    END IF;
    PERFORM gdb(p_debug,'Truncating local table');
    EXECUTE 'TRUNCATE '||v_dest_table;
    IF v_jobmon THEN
        PERFORM update_step(v_step_id, 'OK','Done');
    END IF;
    -- Define cursor query
    v_remote_f_sql := 'SELECT '||v_cols||' FROM '||v_source_table;
    IF v_condition IS NOT NULL THEN
        v_remote_f_sql := v_remote_f_sql || ' ' || v_condition;
    END IF;
ELSE
    EXECUTE 'CREATE TEMP TABLE '||v_tmp_table||'_queue ('||v_pk_name_type_csv||', PRIMARY KEY ('||v_pk_name_csv||'))';
    -- Copy queue locally for use in removing updated/deleted rows
    v_remote_q_sql := 'SELECT DISTINCT '||v_pk_name_csv||' FROM '||v_control||' WHERE processed = true';
    PERFORM dblink_open(v_dblink_name, 'mimeo_cursor', v_remote_q_sql);
    IF v_jobmon THEN
        v_step_id := add_step(v_job_id, 'Creating local queue temp table');
    END IF;
    v_rowcount := 0;
    LOOP
        v_fetch_sql := 'INSERT INTO '||v_tmp_table||'_queue ('||v_pk_name_csv||') 
            SELECT '||v_pk_name_csv||' FROM dblink_fetch('||quote_literal(v_dblink_name)||', ''mimeo_cursor'', 50000) AS ('||v_pk_name_type_csv||')';
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
    EXECUTE 'CREATE INDEX ON '||v_tmp_table||'_queue ('||v_pk_name_csv||')';
    EXECUTE 'ANALYZE '||v_tmp_table||'_queue';
    IF v_jobmon THEN
        PERFORM update_step(v_step_id, 'OK','Number of rows inserted: '||v_total);
    END IF;
    PERFORM gdb(p_debug,'Temp queue table row count '||v_total::text);

    IF v_jobmon THEN
        v_step_id := add_step(v_job_id,'Deleting records from local table');
    END IF;
    v_delete_sql := 'DELETE FROM '||v_dest_table||' a USING '||v_tmp_table||'_queue b WHERE '|| v_pk_where; 
    PERFORM gdb(p_debug,v_delete_sql);
    EXECUTE v_delete_sql; 
    GET DIAGNOSTICS v_rowcount = ROW_COUNT;
    PERFORM gdb(p_debug,'Rows removed from local table before applying changes: '||v_rowcount::text);
    IF v_jobmon THEN
        PERFORM update_step(v_step_id, 'OK','Removed '||v_rowcount||' records');
    END IF;
    -- Define cursor query
    v_remote_f_sql := 'SELECT '||v_cols||' FROM '||v_source_table||' JOIN ('||v_remote_q_sql||') x USING ('||v_pk_name_csv||')';
    IF v_condition IS NOT NULL THEN
        v_remote_f_sql := v_remote_f_sql || ' ' || v_condition;
    END IF;
END IF;

-- insert records to local table. Have to do temp table in case destination table is partitioned (returns 0 when inserting to parent)
PERFORM dblink_open(v_dblink_name, 'mimeo_cursor', v_remote_f_sql);
IF v_jobmon THEN
    v_step_id := add_step(v_job_id, 'Inserting new records into local table');
END IF;
EXECUTE 'CREATE TEMP TABLE '||v_tmp_table||'_full ('||v_cols_n_types||')';
v_rowcount := 0;
v_total := 0;
LOOP
    v_fetch_sql := 'INSERT INTO '||v_tmp_table||'_full ('||v_cols||') 
        SELECT '||v_cols||' FROM dblink_fetch('||quote_literal(v_dblink_name)||', ''mimeo_cursor'', 50000) AS ('||v_cols_n_types||')';
    EXECUTE v_fetch_sql;
    GET DIAGNOSTICS v_rowcount = ROW_COUNT;
    v_total := v_total + coalesce(v_rowcount, 0);
    EXECUTE 'INSERT INTO '||v_dest_table||' ('||v_cols||') SELECT '||v_cols||' FROM '||v_tmp_table||'_full';
    EXECUTE 'TRUNCATE '||v_tmp_table||'_full';
    EXIT WHEN v_rowcount = 0;
    PERFORM gdb(p_debug,'Fetching rows in batches: '||v_total||' done so far.');
    IF v_jobmon THEN
        PERFORM update_step(v_step_id, 'PENDING', 'Fetching rows in batches: '||v_total||' done so far.');
    END IF;
END LOOP;
PERFORM dblink_close(v_dblink_name, 'mimeo_cursor');
IF v_jobmon THEN
    PERFORM update_step(v_step_id, 'OK','Number of rows inserted: '||v_total);
END IF;

IF p_repull = false AND v_total > (v_limit * .75) THEN
    IF v_jobmon THEN
        v_step_id := add_step(v_job_id, 'Row count warning');
        PERFORM update_step(v_step_id, 'WARNING','Row count fetched ('||v_total||') greater than 75% of batch limit ('||v_limit||'). Recommend increasing batch limit if possible.');
    END IF;
    v_batch_limit_reached := true;
END IF;

-- clean out rows from txn table
IF v_jobmon THEN
    v_step_id := add_step(v_job_id,'Cleaning out rows from txn table');
END IF;
v_trigger_delete := 'SELECT dblink_exec('||quote_literal(v_dblink_name)||','||quote_literal('DELETE FROM '||v_control||' WHERE processed = true')||')'; 
PERFORM gdb(p_debug,v_trigger_delete);
EXECUTE v_trigger_delete INTO v_exec_status;
IF v_jobmon THEN
    PERFORM update_step(v_step_id, 'OK','Result was '||v_exec_status);
END IF;
-- update activity status
IF v_jobmon THEN
    v_step_id := add_step(v_job_id,'Updating last_run in config table');
END IF;
UPDATE refresh_config_dml SET last_run = CURRENT_TIMESTAMP WHERE dest_table = p_destination; 
IF v_jobmon THEN
    PERFORM update_step(v_step_id, 'OK','Last run was '||CURRENT_TIMESTAMP);
END IF;
EXECUTE 'DROP TABLE IF EXISTS '||v_tmp_table||'_full';
EXECUTE 'DROP TABLE IF EXISTS '||v_tmp_table||'_queue';

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

PERFORM pg_advisory_unlock(hashtext('refresh_dml'), hashtext(v_job_name));

EXCEPTION
    WHEN QUERY_CANCELED THEN
        EXECUTE 'SELECT '||v_dblink_schema||'.dblink_get_connections() @> ARRAY['||quote_literal(v_dblink_name)||']' INTO v_link_exists;
        IF v_link_exists THEN
            EXECUTE 'SELECT '||v_dblink_schema||'.dblink_disconnect('||quote_literal(v_dblink_name)||')';
        END IF;
        PERFORM pg_advisory_unlock(hashtext('refresh_dml'), hashtext(v_job_name));
        RAISE EXCEPTION '%', SQLERRM;  
    WHEN OTHERS THEN
        EXECUTE 'SELECT '||v_dblink_schema||'.dblink_get_connections() @> ARRAY['||quote_literal(v_dblink_name)||']' INTO v_link_exists;
        IF v_link_exists THEN
            EXECUTE 'SELECT '||v_dblink_schema||'.dblink_disconnect('||quote_literal(v_dblink_name)||')';
        END IF;
        IF v_jobmon THEN
            IF v_job_id IS NULL THEN
                EXECUTE 'SELECT '||v_jobmon_schema||'.add_job(''Refresh DML: '||p_destination||''')' INTO v_job_id;
                EXECUTE 'SELECT '||v_jobmon_schema||'.add_step('||v_job_id||', ''EXCEPTION before job logging started'')' INTO v_step_id;
            END IF;
            IF v_step_id IS NULL THEN
                EXECUTE 'SELECT '||v_jobmon_schema||'.add_step('||v_job_id||', ''EXCEPTION before first step logged'')' INTO v_step_id;
            END IF;
            EXECUTE 'SELECT '||v_jobmon_schema||'.update_step('||v_step_id||', ''CRITICAL'', ''ERROR: '||coalesce(SQLERRM,'unknown')||''')';
            EXECUTE 'SELECT '||v_jobmon_schema||'.fail_job('||v_job_id||')';
        END IF;
        PERFORM pg_advisory_unlock(hashtext('refresh_dml'), hashtext(v_job_name));
        RAISE EXCEPTION '%', SQLERRM;
END
$$;


/*
 *  Refresh insert only table based on timestamp control field
 */
CREATE FUNCTION refresh_inserter(p_destination text, p_limit integer DEFAULT NULL, p_repull boolean DEFAULT false, p_repull_start text DEFAULT NULL, p_repull_end text DEFAULT NULL, p_jobmon boolean DEFAULT NULL, p_debug boolean DEFAULT false) RETURNS void
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
v_dest_table            text;
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
v_now                   timestamptz := now();
v_old_search_path       text;
v_remote_sql            text;
v_rowcount              bigint := 0; 
v_source_table          text;
v_step_id               int;
v_tmp_table             text;
v_total                 bigint := 0;

BEGIN

IF p_debug IS DISTINCT FROM true THEN
    PERFORM set_config( 'client_min_messages', 'warning', true );
END IF;

v_job_name := 'Refresh Inserter: '||p_destination;
v_dblink_name := 'mimeo_inserter_refresh_'||p_destination;

SELECT nspname INTO v_dblink_schema FROM pg_namespace n, pg_extension e WHERE e.extname = 'dblink' AND e.extnamespace = n.oid;
SELECT nspname INTO v_jobmon_schema FROM pg_namespace n, pg_extension e WHERE e.extname = 'pg_jobmon' AND e.extnamespace = n.oid;

-- Set custom search path to allow easier calls to other functions, especially job logging
SELECT current_setting('search_path') INTO v_old_search_path;
EXECUTE 'SELECT set_config(''search_path'',''@extschema@,'||COALESCE(v_jobmon_schema||',', '')||v_dblink_schema||',public'',''false'')';

SELECT source_table
    , dest_table
    , 'tmp_'||replace(dest_table,'.','_')
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
    , v_tmp_table
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
FROM refresh_config_inserter 
WHERE dest_table = p_destination; 
IF NOT FOUND THEN
   RAISE EXCEPTION 'No configuration found for %',v_job_name; 
END IF;  

-- Allow override with parameter
v_jobmon := COALESCE(p_jobmon, v_jobmon);

-- Take advisory lock to prevent multiple calls to function overlapping
v_adv_lock := pg_try_advisory_lock(hashtext('refresh_inserter'), hashtext(v_job_name));
IF v_adv_lock = 'false' THEN
    IF v_jobmon THEN
        v_job_id := add_job(v_job_name);
        v_step_id := add_step(v_job_id,'Obtaining advisory lock for job: '||v_job_name);
        PERFORM update_step(v_step_id, 'WARNING','Found concurrent job. Exiting gracefully');
        PERFORM fail_job(v_job_id, 2);
    END IF;
    PERFORM gdb(p_debug,'Obtaining advisory lock FAILED for job: '||v_job_name);
    EXECUTE 'SELECT set_config(''search_path'','''||v_old_search_path||''',''false'')';
    RETURN;
END IF;

IF v_jobmon THEN
    v_job_id := add_job(v_job_name);
    PERFORM gdb(p_debug,'Job ID: '||v_job_id::text);
END IF;

-- Do not allow this function to run during DST time change if config option is true. Otherwise will miss data from source
IF v_dst_active THEN
    v_dst_check := @extschema@.dst_change(v_now);
    IF v_dst_check THEN 
        IF to_number(to_char(v_now, 'HH24MM'), '0000') > v_dst_start AND to_number(to_char(v_now, 'HH24MM'), '0000') < v_dst_end THEN
            IF v_jobmon THEN
                v_step_id := add_step( v_job_id, 'DST Check');
                PERFORM update_step(v_step_id, 'OK', 'Job CANCELLED - Does not run during DST time change');
                PERFORM close_job(v_job_id);
            END IF;
            PERFORM gdb(p_debug, 'Cannot run during DST time change');
            UPDATE refresh_config_inserter SET last_run = CURRENT_TIMESTAMP WHERE dest_table = p_destination;  
            EXECUTE 'SELECT set_config(''search_path'','''||v_old_search_path||''',''false'')';
            PERFORM pg_advisory_unlock(hashtext('refresh_inserter'), hashtext(v_job_name));
            RETURN;
        END IF;
    END IF;
END IF;

IF v_jobmon THEN
    v_step_id := add_step(v_job_id,'Building SQL');
END IF;

IF v_filter IS NULL THEN 
    SELECT array_to_string(array_agg(attname),','), array_to_string(array_agg(attname||' '||format_type(atttypid, atttypmod)::text),',') 
        INTO v_cols, v_cols_n_types
        FROM pg_attribute WHERE attrelid = p_destination::regclass AND attnum > 0 AND attisdropped is false;
ELSE
    SELECT array_to_string(array_agg(attname),','), array_to_string(array_agg(attname||' '||format_type(atttypid, atttypmod)::text),',') 
        INTO v_cols, v_cols_n_types
        FROM pg_attribute WHERE attrelid = p_destination::regclass AND ARRAY[attname::text] <@ v_filter AND attnum > 0 AND attisdropped is false;
END IF;

PERFORM dblink_connect(v_dblink_name, auth(v_dblink));

IF p_limit IS NOT NULL THEN
    v_limit := p_limit;
END IF;

IF p_repull THEN
    -- Repull ALL data if no start and end values set
    IF p_repull_start IS NULL AND p_repull_end IS NULL THEN
        IF v_jobmon THEN
            PERFORM update_step(v_step_id, 'OK','Request to repull ALL data from source. This could take a while...');
        END IF;
        EXECUTE 'TRUNCATE '||v_dest_table;
        v_remote_sql := 'SELECT '||v_cols||' FROM '||v_source_table;
        IF v_condition IS NOT NULL THEN
            v_remote_sql := v_remote_sql || ' ' || v_condition;
        END IF;
    ELSE
        IF v_jobmon THEN
            PERFORM update_step(v_step_id, 'OK','Request to repull data from '||COALESCE(p_repull_start, '-infinity')||' to '||COALESCE(p_repull_end, 'infinity'));
        END IF;
        PERFORM gdb(p_debug,'Request to repull data from '||COALESCE(p_repull_start, '-infinity')||' to '||COALESCE(p_repull_end, 'infinity'));
        v_remote_sql := 'SELECT '||v_cols||' FROM '||v_source_table;
        IF v_condition IS NOT NULL THEN
            v_remote_sql := v_remote_sql || ' ' || v_condition || ' AND ';
        ELSE
            v_remote_sql := v_remote_sql || ' WHERE ';
        END IF;
        v_remote_sql := v_remote_sql ||v_control||' > '||quote_literal(COALESCE(p_repull_start, '-infinity'))||' AND '
            ||v_control||' < '||quote_literal(COALESCE(p_repull_end, 'infinity'));
        -- Delete the old local data
        v_delete_sql := 'DELETE FROM '||v_dest_table||' WHERE '||v_control||' > '||quote_literal(COALESCE(p_repull_start, '-infinity'))||' AND '
            ||v_control||' < '||quote_literal(COALESCE(p_repull_end, 'infinity'));
        IF v_jobmon THEN
            v_step_id := add_step(v_job_id, 'Deleting current, local data');
        END IF;
        PERFORM gdb(p_debug,'Deleting current, local data: '||v_delete_sql);
        EXECUTE v_delete_sql;
        GET DIAGNOSTICS v_rowcount = ROW_COUNT;
        IF v_jobmon THEN
            PERFORM update_step(v_step_id, 'OK', v_rowcount || 'rows removed');
        END IF;
    END IF;
ELSE
    -- does < for upper boundary to keep missing data from happening on rare edge case where a newly inserted row outside the transaction batch
    -- has the exact same timestamp as the previous batch's max timestamp
    v_remote_sql := 'SELECT '||v_cols||' FROM '||v_source_table;
    IF v_condition IS NOT NULL THEN
        v_remote_sql := v_remote_sql || ' ' || v_condition || ' AND ';
    ELSE
        v_remote_sql := v_remote_sql || ' WHERE ';
    END IF;
    v_remote_sql := v_remote_sql ||v_control||' > '||quote_literal(v_last_value)||' AND '||v_control||' < '||quote_literal(v_boundary)||' ORDER BY '||v_control||' ASC LIMIT '|| COALESCE(v_limit::text, 'ALL');

    IF v_jobmon THEN
        PERFORM update_step(v_step_id, 'OK','Grabbing rows from '||v_last_value::text||' to '||v_boundary::text);
    END IF;
    PERFORM gdb(p_debug,'Grabbing rows from '||v_last_value::text||' to '||v_boundary::text);

END IF;

EXECUTE 'CREATE TEMP TABLE '||v_tmp_table||' ('||v_cols_n_types||')';
PERFORM gdb(p_debug,v_remote_sql);
PERFORM dblink_open(v_dblink_name, 'mimeo_cursor', v_remote_sql);
IF v_jobmon THEN
    v_step_id := add_step(v_job_id, 'Inserting new records into local table');
END IF;
v_rowcount := 0;
v_total := 0;
LOOP
    v_fetch_sql := 'INSERT INTO '||v_tmp_table||'('||v_cols||') 
        SELECT '||v_cols||' FROM dblink_fetch('||quote_literal(v_dblink_name)||', ''mimeo_cursor'', 50000) AS ('||v_cols_n_types||')';
    EXECUTE v_fetch_sql;
    GET DIAGNOSTICS v_rowcount = ROW_COUNT;
    v_total := v_total + coalesce(v_rowcount, 0);
    EXECUTE 'SELECT max('||v_control||') FROM '||v_tmp_table INTO v_last_fetched;
    IF v_limit IS NULL THEN -- insert into the real table in batches if no limit to avoid excessively large temp tables
        EXECUTE 'INSERT INTO '||v_dest_table||' ('||v_cols||') SELECT '||v_cols||' FROM '||v_tmp_table;
        EXECUTE 'TRUNCATE '||v_tmp_table;
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
        EXECUTE 'SELECT max('||v_control||') FROM '||v_tmp_table INTO v_last_value;
        IF v_jobmon THEN
            v_step_id := add_step(v_job_id, 'Removing high boundary rows from this batch to avoid missing data');       
        END IF;
        EXECUTE 'DELETE FROM '||v_tmp_table||' WHERE '||v_control||' = '||quote_literal(v_last_value);
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
        EXECUTE 'INSERT INTO '||v_dest_table||' ('||v_cols||') SELECT '||v_cols||' FROM '||v_tmp_table;
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
    EXECUTE 'SELECT max('||v_control||') FROM '|| v_dest_table INTO v_last_value;
    UPDATE refresh_config_inserter set last_value = coalesce(v_last_value, CURRENT_TIMESTAMP), last_run = CURRENT_TIMESTAMP WHERE dest_table = p_destination;  
    IF v_jobmon THEN
        PERFORM update_step(v_step_id, 'OK','Lower boundary value is: '|| coalesce(v_last_value, CURRENT_TIMESTAMP));
        PERFORM gdb(p_debug, 'Lower boundary value is: '||coalesce(v_last_value, CURRENT_TIMESTAMP));
    END IF;
END IF;

EXECUTE 'DROP TABLE IF EXISTS ' || v_tmp_table;

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

PERFORM pg_advisory_unlock(hashtext('refresh_inserter'), hashtext(v_job_name));

EXCEPTION
    WHEN QUERY_CANCELED THEN
        EXECUTE 'SELECT '||v_dblink_schema||'.dblink_get_connections() @> ARRAY['||quote_literal(v_dblink_name)||']' INTO v_link_exists;
        IF v_link_exists THEN
            EXECUTE 'SELECT '||v_dblink_schema||'.dblink_disconnect('||quote_literal(v_dblink_name)||')';
        END IF;
        PERFORM pg_advisory_unlock(hashtext('refresh_inserter'), hashtext(v_job_name));
        RAISE EXCEPTION '%', SQLERRM;    
    WHEN OTHERS THEN
        EXECUTE 'SELECT '||v_dblink_schema||'.dblink_get_connections() @> ARRAY['||quote_literal(v_dblink_name)||']' INTO v_link_exists;
        IF v_link_exists THEN
            EXECUTE 'SELECT '||v_dblink_schema||'.dblink_disconnect('||quote_literal(v_dblink_name)||')';
        END IF;
        IF v_jobmon THEN
            IF v_job_id IS NULL THEN
                EXECUTE 'SELECT '||v_jobmon_schema||'.add_job(''Refresh Inserter: '||p_destination||''')' INTO v_job_id;
                EXECUTE 'SELECT '||v_jobmon_schema||'.add_step('||v_job_id||', ''EXCEPTION before job logging started'')' INTO v_step_id;
            END IF;
            IF v_step_id IS NULL THEN
                EXECUTE 'SELECT '||v_jobmon_schema||'.add_step('||v_job_id||', ''EXCEPTION before first step logged'')' INTO v_step_id;
            END IF;
                  EXECUTE 'SELECT '||v_jobmon_schema||'.update_step('||v_step_id||', ''CRITICAL'', ''ERROR: '||COALESCE(SQLERRM,'unknown')||''')';
            EXECUTE 'SELECT '||v_jobmon_schema||'.fail_job('||v_job_id||')';
        END IF;
        PERFORM pg_advisory_unlock(hashtext('refresh_inserter'), hashtext(v_job_name));
        RAISE EXCEPTION '%', SQLERRM;    
END
$$;


/*
 *  Refresh based on DML (Insert, Update, Delete), but logs all deletes on the destination table
 *  Destination table requires extra column: mimeo_source_deleted timestamptz
 */
CREATE FUNCTION refresh_logdel(p_destination text, p_limit int DEFAULT NULL, p_repull boolean DEFAULT false, p_jobmon boolean DEFAULT NULL, p_debug boolean DEFAULT false) RETURNS void
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
v_dest_table            text;
v_exec_status           text;
v_fetch_sql             text;
v_field                 text;
v_filter                text[];
v_insert_deleted_sql    text;
v_insert_sql            text;
v_job_id                int;
v_jobmon_schema         text;
v_jobmon                boolean;
v_job_name              text;
v_limit                 int; 
v_link_exists           boolean;
v_old_search_path       text;
v_pk_counter            int;
v_pk_name               text[];
v_pk_name_csv           text;
v_pk_name_type_csv      text := '';
v_pk_type               text[];
v_pk_where              text := '';
v_remote_d_sql          text;
v_remote_f_sql          text;
v_remote_q_sql          text;
v_rowcount              bigint := 0; 
v_source_table          text;
v_step_id               int;
v_tmp_table             text;
v_total                 bigint := 0;
v_trigger_delete        text; 
v_trigger_update        text;
v_truncate_remote_q     text;
v_with_update           text;

BEGIN

IF p_debug IS DISTINCT FROM true THEN
    PERFORM set_config( 'client_min_messages', 'warning', true );
END IF;

v_job_name := 'Refresh Log Del: '||p_destination;
v_dblink_name := 'mimeo_logdel_refresh_'||p_destination;

SELECT nspname INTO v_dblink_schema FROM pg_namespace n, pg_extension e WHERE e.extname = 'dblink' AND e.extnamespace = n.oid;
SELECT nspname INTO v_jobmon_schema FROM pg_namespace n, pg_extension e WHERE e.extname = 'pg_jobmon' AND e.extnamespace = n.oid;

-- Set custom search path to allow easier calls to other functions, especially job logging
SELECT current_setting('search_path') INTO v_old_search_path;
EXECUTE 'SELECT set_config(''search_path'',''@extschema@,'||COALESCE(v_jobmon_schema||',', '')||v_dblink_schema||',public'',''false'')';

SELECT source_table
    , dest_table
    , 'tmp_'||replace(dest_table,'.','_')
    , dblink
    , control
    , pk_name
    , pk_type
    , filter
    , condition
    , batch_limit 
    , jobmon
INTO v_source_table
    , v_dest_table
    , v_tmp_table
    , v_dblink
    , v_control
    , v_pk_name
    , v_pk_type
    , v_filter
    , v_condition
    , v_limit
    , v_jobmon
FROM refresh_config_logdel 
WHERE dest_table = p_destination; 
IF NOT FOUND THEN
   RAISE EXCEPTION 'No configuration found for %',v_job_name; 
END IF;

-- Allow override with parameter
v_jobmon := COALESCE(p_jobmon, v_jobmon);

-- Take advisory lock to prevent multiple calls to function overlapping
v_adv_lock := pg_try_advisory_lock(hashtext('refresh_logdel'), hashtext(v_job_name));
IF v_adv_lock = 'false' THEN
    IF v_jobmon THEN
        v_job_id := add_job(v_job_name);
        v_step_id := add_step(v_job_id,'Obtaining advisory lock for job: '||v_job_name);
        PERFORM update_step(v_step_id, 'WARNING','Found concurrent job. Exiting gracefully');
        PERFORM fail_job(v_job_id, 2);
    END IF;
    PERFORM gdb(p_debug,'Obtaining advisory lock FAILED for job: '||v_job_name);
    EXECUTE 'SELECT set_config(''search_path'','''||v_old_search_path||''',''false'')';
    RETURN;
END IF;


IF v_jobmon THEN
    v_job_id := add_job(v_job_name);
    PERFORM gdb(p_debug,'Job ID: '||v_job_id::text);
    v_step_id := add_step(v_job_id,'Sanity check primary/unique key values');
END IF;

IF v_pk_name IS NULL OR v_pk_type IS NULL THEN
    RAISE EXCEPTION 'Primary key fields in refresh_config_logdel must be defined';
END IF;

-- determine column list, column type list
IF v_filter IS NULL THEN 
    SELECT array_to_string(array_agg(attname),','), array_to_string(array_agg(attname||' '||format_type(atttypid, atttypmod)::text),',')
        INTO v_cols, v_cols_n_types
        FROM pg_attribute WHERE attnum > 0 AND attisdropped is false AND attrelid = p_destination::regclass AND attname != 'mimeo_source_deleted';
ELSE
    -- ensure all primary key columns are included in any column filters
    FOREACH v_field IN ARRAY v_pk_name LOOP
        IF v_field = ANY(v_filter) THEN
            CONTINUE;
        ELSE
            RAISE EXCEPTION 'Filter list did not contain all columns that compose primary key for %',v_job_name; 
        END IF;
    END LOOP;
    SELECT array_to_string(array_agg(attname),','), array_to_string(array_agg(attname||' '||format_type(atttypid, atttypmod)::text),',') 
        INTO v_cols, v_cols_n_types
        FROM pg_attribute WHERE attrelid = p_destination::regclass AND ARRAY[attname::text] <@ v_filter AND attnum > 0 AND attisdropped is false AND attname != 'mimeo_source_deleted' ;
END IF;    

IF p_limit IS NOT NULL THEN
    v_limit := p_limit;
END IF;

v_pk_name_csv := array_to_string(v_pk_name,',');
v_pk_counter := 1;
WHILE v_pk_counter <= array_length(v_pk_name,1) LOOP
    IF v_pk_counter > 1 THEN
        v_pk_name_type_csv := v_pk_name_type_csv || ', ';
        v_pk_where := v_pk_where ||' AND ';
    END IF;
    v_pk_name_type_csv := v_pk_name_type_csv ||v_pk_name[v_pk_counter]||' '||v_pk_type[v_pk_counter];
    v_pk_where := v_pk_where || ' a.'||v_pk_name[v_pk_counter]||' = b.'||v_pk_name[v_pk_counter];
    v_pk_counter := v_pk_counter + 1;
END LOOP;

IF v_jobmon THEN
    PERFORM update_step(v_step_id, 'OK','Done');
END IF;

PERFORM dblink_connect(v_dblink_name, auth(v_dblink));

-- update remote entries
IF v_jobmon THEN
    v_step_id := add_step(v_job_id,'Updating remote trigger table');
END IF;
v_with_update := 'WITH a AS (SELECT '||v_pk_name_csv||' FROM '|| v_control ||' ORDER BY '||v_pk_name_csv||' LIMIT '|| COALESCE(v_limit::text, 'ALL') ||') UPDATE '||v_control||' b SET processed = true FROM a WHERE '|| v_pk_where;
v_trigger_update := 'SELECT dblink_exec('||quote_literal(v_dblink_name)||','|| quote_literal(v_with_update)||')';
PERFORM gdb(p_debug,v_trigger_update);
EXECUTE v_trigger_update INTO v_exec_status;    
IF v_jobmon THEN
    PERFORM update_step(v_step_id, 'OK','Result was '||v_exec_status);
END IF;

-- create temp table for recording deleted rows
EXECUTE 'CREATE TEMP TABLE '||v_tmp_table||'_deleted ('||v_cols_n_types||', mimeo_source_deleted timestamptz)';
v_remote_d_sql := 'SELECT '||v_cols||', mimeo_source_deleted FROM '||v_control||' WHERE processed = true and mimeo_source_deleted IS NOT NULL';
PERFORM dblink_open(v_dblink_name, 'mimeo_cursor', v_remote_d_sql);
IF v_jobmon THEN
    v_step_id := add_step(v_job_id, 'Creating local queue temp table for deleted rows on source');
END IF;
v_rowcount := 0;
v_total := 0;
LOOP
    v_fetch_sql := 'INSERT INTO '||v_tmp_table||'_deleted ('||v_cols||', mimeo_source_deleted) 
        SELECT '||v_cols||', mimeo_source_deleted FROM dblink_fetch('||quote_literal(v_dblink_name)||', ''mimeo_cursor'', 50000) AS ('||v_cols_n_types||', mimeo_source_deleted timestamptz)';
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
EXECUTE 'CREATE INDEX ON '||v_tmp_table||'_deleted ('||v_pk_name_csv||')';
EXECUTE 'ANALYZE '||v_tmp_table||'_deleted';
IF v_jobmon THEN
    PERFORM update_step(v_step_id, 'OK','Number of rows inserted: '||v_total);
END IF;
PERFORM gdb(p_debug,'Temp deleted queue table row count '||v_total::text);  

IF p_repull THEN
    -- Do delete instead of truncate like refresh_dml to avoid missing rows between the above deleted queue fetch and here.
    IF v_jobmon THEN
        PERFORM update_step(v_step_id, 'OK','Request to repull ALL data from source. This could take a while...');
    END IF;
    PERFORM gdb(p_debug, 'Request to repull ALL data from source. This could take a while...');
    v_truncate_remote_q := 'SELECT dblink_exec('||quote_literal(v_dblink_name)||','||quote_literal('DELETE FROM '||v_control||' WHERE processed = true')||')';
    PERFORM gdb(p_debug, v_truncate_remote_q);
    EXECUTE v_truncate_remote_q;

    IF v_jobmon THEN
        v_step_id := add_step(v_job_id,'Removing local, undeleted rows');
    END IF;
    PERFORM gdb(p_debug,'Removing local, undeleted rows');
    EXECUTE 'DELETE FROM '||v_dest_table||' WHERE mimeo_source_deleted IS NULL';
    IF v_jobmon THEN
        PERFORM update_step(v_step_id, 'OK','Done');
    END IF;

    -- Define cursor query
    v_remote_f_sql := 'SELECT '||v_cols||' FROM '||v_source_table;
    IF v_condition IS NOT NULL THEN
        v_remote_f_sql := v_remote_f_sql || ' ' || v_condition;
    END IF;
ELSE
    -- Do normal stuff here
    EXECUTE 'CREATE TEMP TABLE '||v_tmp_table||'_queue ('||v_pk_name_type_csv||')';
    v_remote_q_sql := 'SELECT DISTINCT '||v_pk_name_csv||' FROM '||v_control||' WHERE processed = true and mimeo_source_deleted IS NULL';
    PERFORM dblink_open(v_dblink_name, 'mimeo_cursor', v_remote_q_sql);
    IF v_jobmon THEN
        v_step_id := add_step(v_job_id, 'Creating local queue temp table for inserts/updates');
    END IF;
    v_rowcount := 0;
    LOOP
        v_fetch_sql := 'INSERT INTO '||v_tmp_table||'_queue ('||v_pk_name_csv||') 
            SELECT '||v_pk_name_csv||' FROM dblink_fetch('||quote_literal(v_dblink_name)||', ''mimeo_cursor'', 50000) AS ('||v_pk_name_type_csv||')';
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
    EXECUTE 'CREATE INDEX ON '||v_tmp_table||'_queue ('||v_pk_name_csv||')';
    EXECUTE 'ANALYZE '||v_tmp_table||'_queue';
    IF v_jobmon THEN
        PERFORM update_step(v_step_id, 'OK','Number of rows inserted: '||v_total);
    END IF;
    PERFORM gdb(p_debug,'Temp inserts/updates queue table row count '||v_total::text);

    -- remove records from local table (inserts/updates)
    IF v_jobmon THEN
        v_step_id := add_step(v_job_id,'Removing insert/update records from local table');
    END IF;
    v_delete_f_sql := 'DELETE FROM '||v_dest_table||' a USING '||v_tmp_table||'_queue b WHERE '|| v_pk_where;
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
    v_delete_d_sql := 'DELETE FROM '||v_dest_table||' a USING '||v_tmp_table||'_deleted b WHERE '|| v_pk_where;
    PERFORM gdb(p_debug,v_delete_d_sql);
    EXECUTE v_delete_d_sql; 
    GET DIAGNOSTICS v_rowcount = ROW_COUNT;
    PERFORM gdb(p_debug,'Deleted rows removed from local table before applying changes: '||v_rowcount::text);
    IF v_jobmon THEN
        PERFORM update_step(v_step_id, 'OK','Removed '||v_rowcount||' records');
    END IF;

    -- Remote full query for normal replication 
    v_remote_f_sql := 'SELECT '||v_cols||' FROM '||v_source_table||' JOIN ('||v_remote_q_sql||') x USING ('||v_pk_name_csv||')';
    IF v_condition IS NOT NULL THEN
        v_remote_f_sql := v_remote_f_sql || ' ' || v_condition;
    END IF;
END IF;

-- insert records to local table (inserts/updates). Have to do temp table in case destination table is partitioned (returns 0 when inserting to parent)
PERFORM dblink_open(v_dblink_name, 'mimeo_cursor', v_remote_f_sql);
IF v_jobmon THEN
    v_step_id := add_step(v_job_id, 'Inserting new/updated records into local table');
END IF;
EXECUTE 'CREATE TEMP TABLE '||v_tmp_table||'_full ('||v_cols_n_types||')'; 
v_rowcount := 0;
v_total := 0;
LOOP
    v_fetch_sql := 'INSERT INTO '||v_tmp_table||'_full ('||v_cols||') 
        SELECT '||v_cols||' FROM dblink_fetch('||quote_literal(v_dblink_name)||', ''mimeo_cursor'', 50000) AS ('||v_cols_n_types||')';
    EXECUTE v_fetch_sql;
    GET DIAGNOSTICS v_rowcount = ROW_COUNT;
    v_total := v_total + coalesce(v_rowcount, 0);
    EXECUTE 'INSERT INTO '||v_dest_table||' ('||v_cols||') SELECT '||v_cols||' FROM '||v_tmp_table||'_full';
    EXECUTE 'TRUNCATE '||v_tmp_table||'_full';
    EXIT WHEN v_rowcount = 0;    
    PERFORM gdb(p_debug,'Fetching rows in batches: '||v_total||' done so far.');
    IF v_jobmon THEN
        PERFORM update_step(v_step_id, 'PENDING', 'Fetching rows in batches: '||v_total||' done so far.');
    END IF;
END LOOP;
PERFORM dblink_close(v_dblink_name, 'mimeo_cursor');
IF v_jobmon THEN
    PERFORM update_step(v_step_id, 'OK','New/updated rows inserted: '||v_total);
END IF;

-- insert records to local table (deleted rows to be kept)
IF v_jobmon THEN
    v_step_id := add_step(v_job_id,'Inserting deleted records into local table');
END IF;
v_insert_deleted_sql := 'INSERT INTO '||v_dest_table||'('||v_cols||', mimeo_source_deleted) SELECT '||v_cols||', mimeo_source_deleted FROM '||v_tmp_table||'_deleted'; 
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

-- clean out rows from txn table
IF v_jobmon THEN
    v_step_id := add_step(v_job_id,'Cleaning out rows from txn table');
END IF;
v_trigger_delete := 'SELECT dblink_exec('||quote_literal(v_dblink_name)||','||quote_literal('DELETE FROM '||v_control||' WHERE processed = true')||')'; 
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

EXECUTE 'DROP TABLE IF EXISTS '||v_tmp_table||'_full';
EXECUTE 'DROP TABLE IF EXISTS '||v_tmp_table||'_queue';
EXECUTE 'DROP TABLE IF EXISTS '||v_tmp_table||'_deleted';

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

PERFORM pg_advisory_unlock(hashtext('refresh_logdel'), hashtext(v_job_name));

EXCEPTION
    WHEN QUERY_CANCELED THEN
        EXECUTE 'SELECT '||v_dblink_schema||'.dblink_get_connections() @> ARRAY['||quote_literal(v_dblink_name)||']' INTO v_link_exists;
        IF v_link_exists THEN
            EXECUTE 'SELECT '||v_dblink_schema||'.dblink_disconnect('||quote_literal(v_dblink_name)||')';
        END IF;
        PERFORM pg_advisory_unlock(hashtext('refresh_logdel'), hashtext(v_job_name));
        RAISE EXCEPTION '%', SQLERRM;  
    WHEN OTHERS THEN
        EXECUTE 'SELECT '||v_dblink_schema||'.dblink_get_connections() @> ARRAY['||quote_literal(v_dblink_name)||']' INTO v_link_exists;
        IF v_link_exists THEN
            EXECUTE 'SELECT '||v_dblink_schema||'.dblink_disconnect('||quote_literal(v_dblink_name)||')';
        END IF;
        IF v_jobmon THEN
            IF v_job_id IS NULL THEN
                EXECUTE 'SELECT '||v_jobmon_schema||'.add_job(''Refresh Log Del: '||p_destination||''')' INTO v_job_id;
                EXECUTE 'SELECT '||v_jobmon_schema||'.add_step('||v_job_id||', ''EXCEPTION before job logging started'')' INTO v_step_id;
            END IF;
            IF v_step_id IS NULL THEN
                EXECUTE 'SELECT '||v_jobmon_schema||'.add_step('||v_job_id||', ''EXCEPTION before first step logged'')' INTO v_step_id;
            END IF;
                  EXECUTE 'SELECT '||v_jobmon_schema||'.update_step('||v_step_id||', ''CRITICAL'', ''ERROR: '||coalesce(SQLERRM,'unknown')||''')';
            EXECUTE 'SELECT '||v_jobmon_schema||'.fail_job('||v_job_id||')';
        END IF;
        PERFORM pg_advisory_unlock(hashtext('refresh_logdel'), hashtext(v_job_name));
        RAISE EXCEPTION '%', SQLERRM;
END
$$;


/*
 *  Snap refresh to repull all table data
 */
CREATE FUNCTION refresh_snap(p_destination text, p_index boolean DEFAULT true, p_pulldata boolean DEFAULT true, p_jobmon boolean DEFAULT NULL, p_debug boolean DEFAULT false) RETURNS void
    LANGUAGE plpgsql SECURITY DEFINER
    AS $$
DECLARE

v_adv_lock          boolean; 
v_cols_n_types      text[];
v_cols              text[];
v_condition         text;
v_create_sql        text;
v_dblink            int;
v_dblink_name       text;
v_dblink_schema     text;
v_dest_table        text;
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
v_dblink_name := 'mimeo_snap_refresh_'||p_destination;

SELECT nspname INTO v_dblink_schema FROM pg_namespace n, pg_extension e WHERE e.extname = 'dblink' AND e.extnamespace = n.oid;
SELECT nspname INTO v_jobmon_schema FROM pg_namespace n, pg_extension e WHERE e.extname = 'pg_jobmon' AND e.extnamespace = n.oid;

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
v_adv_lock := pg_try_advisory_lock(hashtext('refresh_snap'), hashtext(v_job_name));
IF v_adv_lock = 'false' THEN
    IF v_jobmon THEN
        v_job_id := add_job(v_job_name);
        v_step_id := add_step(v_job_id,'Obtaining advisory lock for job: '||v_job_name);
        PERFORM update_step(v_step_id, 'WARNING','Found concurrent job. Exiting gracefully');
        PERFORM fail_job(v_job_id, 2);
    END IF;
    PERFORM gdb(p_debug,'Obtaining advisory lock FAILED for job: '||v_job_name);
    EXECUTE 'SELECT set_config(''search_path'','''||v_old_search_path||''',''false'')';
    RETURN;
END IF;

IF v_jobmon THEN
    v_job_id := add_job(v_job_name);
    PERFORM gdb(p_debug,'Job ID: '||v_job_id::text);
    v_step_id := add_step(v_job_id,'Grabbing Mapping, Building SQL');
END IF;

-- checking for current view
SELECT definition INTO v_view_definition FROM pg_views where
      ((schemaname || '.') || viewname)=v_dest_table;

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

-- Create snap table if it doesn't exist
PERFORM gdb(p_debug, 'Getting table columns and creating destination table if it doesn''t exist');
SELECT p_table_exists, p_cols, p_cols_n_types FROM manage_dest_table(v_dest_table, v_snap, p_debug) INTO v_table_exists, v_cols, v_cols_n_types;
IF v_table_exists THEN 
/* Check local column definitions against remote and recreate table if different. Allows automatic recreation of
        snap tables if columns change (add, drop type change)  */  
    v_local_sql := 'SELECT array_agg(attname||'' ''||format_type(atttypid, atttypmod)::text) as cols_n_types FROM pg_attribute WHERE attnum > 0 AND attisdropped is false AND attrelid = ' || quote_literal(v_refresh_snap) || '::regclass'; 
    PERFORM gdb(p_debug, v_local_sql);

    EXECUTE v_local_sql INTO v_lcols_array;
    -- Check to see if there's a change in the column structure on the remote
    FOREACH v_r IN ARRAY v_cols_n_types LOOP
        v_match := false;
        FOREACH v_l IN ARRAY v_lcols_array LOOP
            IF v_r = v_l THEN
                v_match := true;
                EXIT;
            END IF;
        END LOOP;
    END LOOP;

    IF v_match = false THEN
        -- Grab old table & view privileges. They are applied later after the view is recreated/swapped
        CREATE TEMP TABLE mimeo_snapshot_grants_tmp (statement text);
        FOR v_old_grant IN 
            SELECT table_schema ||'.'|| table_name AS tablename
                , array_agg(privilege_type::text) AS types
                , grantee
            FROM information_schema.table_privileges 
            WHERE table_schema ||'.'|| table_name IN (v_refresh_snap, v_dest_table)
            GROUP BY grantee, table_schema, table_name 
        LOOP
            INSERT INTO mimeo_snapshot_grants_tmp VALUES ( 
                'GRANT '||array_to_string(v_old_grant.types, ',')||' ON '||v_old_grant.tablename||' TO '||v_old_grant.grantee
            );
        END LOOP;
        SELECT viewowner INTO v_old_owner FROM pg_views WHERE schemaname ||'.'|| viewname = v_dest_table;

        EXECUTE 'DROP TABLE ' || v_refresh_snap;
        EXECUTE 'DROP VIEW ' || v_dest_table;
        PERFORM manage_dest_table(v_dest_table, v_snap, p_debug);

        IF v_jobmon THEN
            v_step_id := add_step(v_job_id,'Source table structure changed.');
            PERFORM update_step(v_step_id, 'OK','Tables and view dropped and recreated. Please double-check snap table attributes (permissions, indexes, etc');
        END IF;
        PERFORM gdb(p_debug,'Source table structure changed. Tables and view dropped and recreated. Please double-check snap table attributes (permissions, indexes, etc)');
    END IF;
    -- truncate non-active snap table
    EXECUTE 'TRUNCATE TABLE ' || v_refresh_snap;

    IF v_jobmon THEN
        PERFORM update_step(v_step_id, 'OK','Done');
    END IF;
END IF;

-- Only check the remote data if there have been no column changes and snap table actually exists. 
-- Otherwise maker functions won't work if source is empty & view switch won't happen properly.
IF  v_table_exists AND v_match THEN
    v_remote_sql := 'SELECT n_tup_ins, n_tup_upd, n_tup_del FROM pg_catalog.pg_stat_all_tables WHERE relid::regclass = '||quote_literal(v_source_table)||'::regclass';
    v_remote_sql := 'SELECT n_tup_ins, n_tup_upd, n_tup_del FROM dblink('||quote_literal(v_dblink_name)||', ' || quote_literal(v_remote_sql) || ') t (n_tup_ins bigint, n_tup_upd bigint, n_tup_del bigint)';
    perform gdb(p_debug,'v_remote_sql: '||v_remote_sql);
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
        PERFORM pg_advisory_unlock(hashtext('refresh_snap'), hashtext(v_job_name));
        RETURN;
    END IF;
END IF;

v_remote_sql := 'SELECT '|| array_to_string(v_cols, ',') ||' FROM '||v_source_table;
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
    v_fetch_sql := 'INSERT INTO '|| v_refresh_snap ||' ('|| array_to_string(v_cols, ',') ||') 
        SELECT '||array_to_string(v_cols, ',')||' FROM dblink_fetch('||quote_literal(v_dblink_name)||', ''mimeo_cursor'', 50000) AS ('||array_to_string(v_cols_n_types, ',')||')';
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
    PERFORM create_index(v_dest_table, v_snap, p_debug);
END IF;

EXECUTE 'ANALYZE ' ||v_refresh_snap;

-- swap view
IF v_jobmon THEN
    v_step_id := add_step(v_job_id,'Swap view to '||v_refresh_snap);
END IF;
PERFORM gdb(p_debug,'Swapping view to '||v_refresh_snap);
EXECUTE 'CREATE OR REPLACE VIEW '||v_dest_table||' AS SELECT * FROM '||v_refresh_snap;
IF v_jobmon THEN
    PERFORM update_step(v_step_id, 'OK','View Swapped');
END IF;

IF v_match = false THEN
    -- Actually apply the original privileges if the table was recreated
    FOR v_old_grant IN SELECT statement FROM mimeo_snapshot_grants_tmp
    LOOP
        EXECUTE v_old_grant.statement;
    END LOOP;
    DROP TABLE IF EXISTS mimeo_snapshot_grants_tmp;
    EXECUTE 'ALTER VIEW '||v_dest_table||' OWNER TO '||v_old_owner;
    EXECUTE 'ALTER TABLE '||v_refresh_snap||' OWNER TO '||v_old_owner;

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
INTO v_table_exists FROM pg_tables WHERE schemaname ||'.'|| tablename = v_old_snap_table;
IF v_table_exists THEN
    IF v_jobmon THEN
        v_step_id := add_step(v_job_id,'Truncating old snap table');
    END IF;
    EXECUTE 'TRUNCATE TABLE '||v_old_snap_table;
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

PERFORM pg_advisory_unlock(hashtext('refresh_snap'), hashtext(v_job_name));

EXCEPTION
    WHEN QUERY_CANCELED THEN
        EXECUTE 'SELECT '||v_dblink_schema||'.dblink_get_connections() @> ARRAY['||quote_literal(v_dblink_name)||']' INTO v_link_exists;
        IF v_link_exists THEN
            EXECUTE 'SELECT '||v_dblink_schema||'.dblink_disconnect('||quote_literal(v_dblink_name)||')';
        END IF;
        PERFORM pg_advisory_unlock(hashtext('refresh_snap'), hashtext(v_job_name));
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
        PERFORM pg_advisory_unlock(hashtext('refresh_snap'), hashtext(v_job_name));
        RAISE EXCEPTION '%', SQLERRM;
END
$$;


/*
 *  Plain table refresh function. 
 */
CREATE FUNCTION refresh_table(p_destination text, p_truncate_cascade boolean DEFAULT NULL, p_jobmon boolean DEFAULT NULL, p_debug boolean DEFAULT false) RETURNS void
    LANGUAGE plpgsql
    AS $$
DECLARE

v_adv_lock              boolean;
v_cols                  text;
v_cols_n_types          text;
v_condition             text;
v_dblink                int;
v_dblink_name           text;
v_dblink_schema         text;
v_dest_table            text;
v_fetch_sql             text;
v_filter                text[];
v_job_id                bigint;
v_job_name              text;
v_jobmon                boolean;
v_jobmon_schema         text;
v_link_exists           boolean;
v_old_search_path       text;
v_post_script           text[];
v_remote_sql            text;
v_rowcount              bigint := 0;
v_seq                   text;
v_seq_max               bigint;
v_sequences             text[];
v_source_table          text;
v_step_id               bigint;
v_total                 bigint := 0;
v_truncate_cascade      boolean;
v_truncate_sql          text;

BEGIN

SELECT nspname INTO v_dblink_schema FROM pg_namespace n, pg_extension e WHERE e.extname = 'dblink' AND e.extnamespace = n.oid;
SELECT nspname INTO v_jobmon_schema FROM pg_namespace n, pg_extension e WHERE e.extname = 'pg_jobmon' AND e.extnamespace = n.oid;

v_dblink_name := 'mimeo_table_refresh_'||p_destination;
v_job_name := 'Refresh Table: '||p_destination;

SELECT current_setting('search_path') INTO v_old_search_path;
EXECUTE 'SELECT set_config(''search_path'',''@extschema@,'||COALESCE(v_jobmon_schema||',', '')||v_dblink_schema||',public'',''false'')';

SELECT source_table
    , dest_table
    , dblink
    , filter
    , condition
    , sequences
    , truncate_cascade
    , jobmon
INTO v_source_table
    , v_dest_table
    , v_dblink
    , v_filter
    , v_condition
    , v_sequences
    , v_truncate_cascade
    , v_jobmon
FROM refresh_config_table
WHERE dest_table = p_destination; 
IF NOT FOUND THEN
   RAISE EXCEPTION 'No configuration found for Refresh Table: %',p_destination; 
END IF;

-- Allow override with parameter
v_jobmon := COALESCE(p_jobmon, v_jobmon);

v_adv_lock := pg_try_advisory_lock(hashtext('refresh_table'), hashtext(p_destination));
IF v_adv_lock = 'false' THEN
    IF v_jobmon THEN
        v_job_id := add_job(v_job_name);
        v_step_id := add_step(v_job_id,'Obtaining advisory lock for job: '||v_job_name);
        PERFORM update_step(v_step_id, 'WARNING','Found concurrent job. Exiting gracefully');
        PERFORM fail_job(v_job_id, 2);
    END IF;
    RAISE NOTICE 'Found concurrent job. Exiting gracefully';
    EXECUTE 'SELECT set_config(''search_path'','''||v_old_search_path||''',''false'')';
    RETURN;
END IF;

IF v_jobmon THEN
    v_job_id := add_job(v_job_name);
    PERFORM gdb(p_debug,'Job ID: '||v_job_id::text);
END IF;

IF p_truncate_cascade IS NOT NULL THEN
    v_truncate_cascade := p_truncate_cascade;
END IF;

IF v_jobmon THEN
    v_step_id := add_step(v_job_id, 'Truncating destination table');
END IF;
v_truncate_sql := 'TRUNCATE TABLE '||v_dest_table;
IF v_truncate_cascade THEN
    v_truncate_sql := v_truncate_sql || ' CASCADE';
    RAISE NOTICE 'WARNING! If this table had foreign keys, you have just truncated all referencing tables as well!';
    IF v_jobmon THEN
        PERFORM update_step(v_step_id, 'OK', 'If this table had foreign keys, you have just truncated all referencing tables as well!');
    END IF;
ELSE
    IF v_jobmon THEN
        PERFORM update_step(v_step_id, 'OK', 'Done');
    END IF;
END IF;
EXECUTE v_truncate_sql;

PERFORM dblink_connect(v_dblink_name, @extschema@.auth(v_dblink));

IF v_jobmon THEN
    v_step_id := add_step(v_job_id,'Grabbing Mapping, Building SQL');
END IF;

IF v_filter IS NULL THEN 
    SELECT array_to_string(array_agg(attname),','), array_to_string(array_agg(attname||' '||format_type(atttypid, atttypmod)::text),',') 
        INTO v_cols, v_cols_n_types
        FROM pg_attribute WHERE attrelid = p_destination::regclass AND attnum > 0 AND attisdropped is false;
ELSE
    SELECT array_to_string(array_agg(attname),','), array_to_string(array_agg(attname||' '||format_type(atttypid, atttypmod)::text),',') 
        INTO v_cols, v_cols_n_types
        FROM pg_attribute WHERE attrelid = p_destination::regclass AND ARRAY[attname::text] <@ v_filter AND attnum > 0 AND attisdropped is false;
END IF;

IF v_jobmon THEN
    PERFORM update_step(v_step_id, 'OK', 'Done');
    v_step_id := add_step(v_job_id,'Inserting records into local table');
END IF;

v_remote_sql := 'SELECT '||v_cols||' FROM '||v_source_table;
IF v_condition IS NOT NULL THEN
    v_remote_sql := v_remote_sql || ' ' || v_condition;
END IF;  
PERFORM dblink_open(v_dblink_name, 'mimeo_cursor', v_remote_sql);
v_rowcount := 0;
LOOP
    v_fetch_sql := 'INSERT INTO '|| v_dest_table ||' ('||v_cols||') 
        SELECT '||v_cols||' FROM dblink_fetch('||quote_literal(v_dblink_name)||', ''mimeo_cursor'', 50000) AS ('||v_cols_n_types||')';
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

PERFORM dblink_disconnect(v_dblink_name);

-- Reset any sequences given in the parameter to their new value. 
-- Checks all tables that use the given sequence to ensure it's the max for the entire database.
IF v_sequences IS NOT NULL THEN
    IF v_jobmon THEN
        v_step_id := add_step(v_job_id, 'Resetting sequences');
    END IF;
    FOREACH v_seq IN ARRAY v_sequences LOOP
        SELECT sequence_max_value(c.oid) INTO v_seq_max FROM pg_class c JOIN pg_namespace n ON c.relnamespace = n.oid WHERE n.nspname ||'.'|| c.relname = v_seq;
        IF v_seq_max IS NOT NULL THEN
            PERFORM setval(v_seq, v_seq_max);
        END IF;
    END LOOP;
    IF v_jobmon THEN
        PERFORM update_step(v_step_id, 'OK', 'Done');
    END IF;
END IF;

IF v_jobmon THEN
    v_step_id := add_step(v_job_id,'Updating last_run in config table');
END IF;
UPDATE refresh_config_table set last_run = CURRENT_TIMESTAMP WHERE dest_table = v_dest_table;
IF v_jobmon THEN
    PERFORM update_step(v_step_id, 'OK','Last run was '||CURRENT_TIMESTAMP);
END IF;

IF v_jobmon THEN
    PERFORM close_job(v_job_id);
END IF;

PERFORM pg_advisory_unlock(hashtext('refresh_table'), hashtext(p_destination));

EXECUTE 'SELECT set_config(''search_path'','''||v_old_search_path||''',''false'')';

EXCEPTION
    WHEN QUERY_CANCELED THEN
        EXECUTE 'SELECT '||v_dblink_schema||'.dblink_get_connections() @> ARRAY['||quote_literal(v_dblink_name)||']' INTO v_link_exists;
        IF v_link_exists THEN
            EXECUTE 'SELECT '||v_dblink_schema||'.dblink_disconnect('||quote_literal(v_dblink_name)||')';
        END IF;
        PERFORM pg_advisory_unlock(hashtext('refresh_dml'), hashtext(v_job_name));
        RAISE EXCEPTION '%', SQLERRM;  
    WHEN OTHERS THEN
        EXECUTE 'SELECT '||v_dblink_schema||'.dblink_get_connections() @> ARRAY['||quote_literal(v_dblink_name)||']' INTO v_link_exists;
        IF v_link_exists THEN
            EXECUTE 'SELECT '||v_dblink_schema||'.dblink_disconnect('||quote_literal(v_dblink_name)||')';
        END IF;
        IF v_jobmon THEN
            IF v_job_id IS NULL THEN
                EXECUTE 'SELECT '||v_jobmon_schema||'.add_job(''Refresh DML: '||p_destination||''')' INTO v_job_id;
                EXECUTE 'SELECT '||v_jobmon_schema||'.add_step('||v_job_id||', ''EXCEPTION before job logging started'')' INTO v_step_id;
            END IF;
            IF v_step_id IS NULL THEN
                EXECUTE 'SELECT '||v_jobmon_schema||'.add_step('||v_job_id||', ''EXCEPTION before first step logged'')' INTO v_step_id;
            END IF;
            EXECUTE 'SELECT '||v_jobmon_schema||'.update_step('||v_step_id||', ''CRITICAL'', ''ERROR: '||coalesce(SQLERRM,'unknown')||''')';
            EXECUTE 'SELECT '||v_jobmon_schema||'.fail_job('||v_job_id||')';
        END IF;
        PERFORM pg_advisory_unlock(hashtext('refresh_dml'), hashtext(v_job_name));
        RAISE EXCEPTION '%', SQLERRM;
END
$$;


/*
 *  Refresh insert/update only table based on timestamp control field
 */
CREATE FUNCTION refresh_updater(p_destination text, p_limit integer DEFAULT NULL, p_repull boolean DEFAULT false, p_repull_start text DEFAULT NULL, p_repull_end text DEFAULT NULL, p_jobmon boolean DEFAULT NULL, p_debug boolean DEFAULT false) RETURNS void
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
v_dest_table            text;
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
v_now                   timestamptz := now(); 
v_old_search_path       text;
v_pk_counter            int := 1;
v_pk_name               text[];
v_remote_boundry_sql    text;
v_remote_boundry        timestamptz;
v_remote_sql            text;
v_rowcount              bigint := 0; 
v_source_table          text;
v_step_id               int;
v_tmp_table             text;
v_total                 bigint := 0;

BEGIN

IF p_debug IS DISTINCT FROM true THEN
    PERFORM set_config( 'client_min_messages', 'warning', true );
END IF;

v_job_name := 'Refresh Updater: '||p_destination;
v_dblink_name := 'mimeo_updater_refresh_'||p_destination;

SELECT nspname INTO v_dblink_schema FROM pg_namespace n, pg_extension e WHERE e.extname = 'dblink' AND e.extnamespace = n.oid;
SELECT nspname INTO v_jobmon_schema FROM pg_namespace n, pg_extension e WHERE e.extname = 'pg_jobmon' AND e.extnamespace = n.oid;

-- Set custom search path to allow easier calls to other functions, especially job logging
SELECT current_setting('search_path') INTO v_old_search_path;
EXECUTE 'SELECT set_config(''search_path'',''@extschema@,'||COALESCE(v_jobmon_schema||',', '')||v_dblink_schema||',public'',''false'')';

SELECT source_table
    , dest_table
    , 'tmp_'||replace(dest_table,'.','_')
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
    , v_tmp_table
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
FROM refresh_config_updater
WHERE dest_table = p_destination;
IF NOT FOUND THEN
   RAISE EXCEPTION 'No configuration found for %',v_job_name;
END IF;

-- Allow override with parameter
v_jobmon := COALESCE(p_jobmon, v_jobmon);

-- Take advisory lock to prevent multiple calls to function overlapping
v_adv_lock := pg_try_advisory_lock(hashtext('refresh_updater'), hashtext(v_job_name));
IF v_adv_lock = 'false' THEN
    IF v_jobmon THEN
        v_job_id := add_job(v_job_name);
        v_step_id := add_step(v_job_id,'Obtaining advisory lock for job: '||v_job_name);
        PERFORM update_step(v_step_id, 'WARNING','Found concurrent job. Exiting gracefully');
        PERFORM fail_job(v_job_id, 2);
    END IF;
    PERFORM gdb(p_debug,'Obtaining advisory lock FAILED for job: '||v_job_name);
    EXECUTE 'SELECT set_config(''search_path'','''||v_old_search_path||''',''false'')';
    RETURN;
END IF;

IF v_jobmon THEN
    v_job_id := add_job(v_job_name);
    PERFORM gdb(p_debug,'Job ID: '||v_job_id::text);
END IF;

-- Do not allow this function to run during DST time change if config option is true. Otherwise will miss data from source
IF v_dst_active THEN
    v_dst_check := @extschema@.dst_change(v_now);
    IF v_dst_check THEN 
        IF to_number(to_char(v_now, 'HH24MM'), '0000') > v_dst_start AND to_number(to_char(v_now, 'HH24MM'), '0000') < v_dst_end THEN
            IF v_jobmon THEN
                v_step_id := add_step( v_job_id, 'DST Check');
                PERFORM update_step(v_step_id, 'OK', 'Job CANCELLED - Does not run during DST time change');
                PERFORM close_job(v_job_id);
            END IF;
            UPDATE refresh_config_updater SET last_run = CURRENT_TIMESTAMP WHERE dest_table = p_destination;  
            PERFORM gdb(p_debug, 'Cannot run during DST time change');
            EXECUTE 'SELECT set_config(''search_path'','''||v_old_search_path||''',''false'')';
            PERFORM pg_advisory_unlock(hashtext('refresh_updater'), hashtext(v_job_name));
            RETURN;
        END IF;
    END IF;
END IF;

IF v_jobmon THEN
    v_step_id := add_step(v_job_id,'Building SQL');
END IF;

-- determine column list, column type list
IF v_filter IS NULL THEN 
    SELECT array_to_string(array_agg(attname),','), array_to_string(array_agg(attname||' '||format_type(atttypid, atttypmod)::text),',')
        INTO v_cols, v_cols_n_types
        FROM pg_attribute WHERE attrelid = p_destination::regclass AND attnum > 0 AND attisdropped is false;
ELSE
    -- ensure all primary key columns are included in any column filters
    FOREACH v_field IN ARRAY v_pk_name LOOP
        IF v_field = ANY(v_filter) THEN
            CONTINUE;
        ELSE
            RAISE EXCEPTION 'Filter list did not contain all columns that compose primary/unique key for %',v_job_name; 
        END IF;
    END LOOP;
    SELECT array_to_string(array_agg(attname),','), array_to_string(array_agg(attname||' '||format_type(atttypid, atttypmod)::text),',') 
        INTO v_cols, v_cols_n_types
        FROM pg_attribute WHERE attrelid = p_destination::regclass AND ARRAY[attname::text] <@ v_filter AND attnum > 0 AND attisdropped is false;
END IF;

PERFORM dblink_connect(v_dblink_name, auth(v_dblink));

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
        EXECUTE 'TRUNCATE '||v_dest_table;
        v_remote_sql := 'SELECT '||v_cols||' FROM '||v_source_table;
        IF v_condition IS NOT NULL THEN
            v_remote_sql := v_remote_sql || ' ' || v_condition;
        END IF;
    ELSE
        IF v_jobmon THEN
            PERFORM update_step(v_step_id, 'OK','Request to repull data from '||p_repull_start||' to '||p_repull_end);
        END IF;
        PERFORM gdb(p_debug,'Request to repull data from '||p_repull_start||' to '||p_repull_end);
        v_remote_sql := 'SELECT '||v_cols||' FROM '||v_source_table;
        IF v_condition IS NOT NULL THEN
            v_remote_sql := v_remote_sql || ' ' || v_condition || ' AND ';
        ELSE
            v_remote_sql := v_remote_sql || ' WHERE ';
        END IF;
        v_remote_sql := v_remote_sql ||v_control||' > '||quote_literal(COALESCE(p_repull_start, '-infinity'))||' AND '
            ||v_control||' < '||quote_literal(COALESCE(p_repull_end, 'infinity'));
        EXECUTE 'DELETE FROM '||v_dest_table||' WHERE '||v_control||' > '||quote_literal(COALESCE(p_repull_start, '-infinity'))||' AND '
            ||v_control||' < '||quote_literal(COALESCE(p_repull_end, 'infinity'));
    END IF;
ELSE
    -- does < for upper boundary to keep missing data from happening on rare edge case where a newly inserted row outside the transaction batch
    -- has the exact same timestamp as the previous batch's max timestamp
    v_remote_sql := 'SELECT '||v_cols||' FROM '||v_source_table;
    IF v_condition IS NOT NULL THEN
        v_remote_sql := v_remote_sql || ' ' || v_condition || ' AND ';
    ELSE
        v_remote_sql := v_remote_sql || ' WHERE ';
    END IF;
    v_remote_sql := v_remote_sql ||v_control||' > '||quote_literal(v_last_value)||' AND '||v_control||' < '||quote_literal(v_boundary)||' ORDER BY '||v_control||' ASC LIMIT '|| COALESCE(v_limit::text, 'ALL');

    v_delete_sql := 'DELETE FROM '||v_dest_table||' USING '||v_tmp_table||' t WHERE ';

    WHILE v_pk_counter <= array_length(v_pk_name,1) LOOP
        IF v_pk_counter > 1 THEN
            v_delete_sql := v_delete_sql ||' AND ';
        END IF;
        v_delete_sql := v_delete_sql ||v_dest_table||'.'||v_pk_name[v_pk_counter]||' = t.'||v_pk_name[v_pk_counter];
        v_pk_counter := v_pk_counter + 1;
    END LOOP;

    IF v_jobmon THEN
        PERFORM update_step(v_step_id, 'OK','Grabbing rows from '||v_last_value::text||' to '||v_boundary::text);
    END IF;
    PERFORM gdb(p_debug,'Grabbing rows from '||v_last_value::text||' to '||v_boundary::text);
END IF;

v_insert_sql := 'INSERT INTO '||v_dest_table||' ('||v_cols||') SELECT '||v_cols||' FROM '||v_tmp_table; 

PERFORM gdb(p_debug,v_remote_sql);
PERFORM dblink_open(v_dblink_name, 'mimeo_cursor', v_remote_sql);
IF v_jobmon THEN
    v_step_id := add_step(v_job_id, 'Inserting new/updated records into local table');
END IF;
v_rowcount := 0;

EXECUTE 'CREATE TEMP TABLE '||v_tmp_table||' ('||v_cols_n_types||')'; 
LOOP            
    v_fetch_sql := 'INSERT INTO '||v_tmp_table||'('||v_cols||') 
        SELECT '||v_cols||' FROM dblink_fetch('||quote_literal(v_dblink_name)||', ''mimeo_cursor'', 50000) AS ('||v_cols_n_types||')';
    EXECUTE v_fetch_sql;
    GET DIAGNOSTICS v_rowcount = ROW_COUNT;
    v_total := v_total + coalesce(v_rowcount, 0);
    EXECUTE 'SELECT max('||v_control||') FROM '||v_tmp_table INTO v_last_fetched;
    IF v_limit IS NULL THEN -- insert into the real table in batches if no limit to avoid excessively large temp tables
        IF p_repull IS FALSE THEN   -- Delete any rows that exist in the current temp table batch. repull delete is done above.
            EXECUTE v_delete_sql;
        END IF;
        EXECUTE v_insert_sql;
        EXECUTE 'TRUNCATE '||v_tmp_table;
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
    -- Not recommended that the batch actually equal the limit set if possible.
    IF v_total >= v_limit THEN
        PERFORM gdb(p_debug, 'Row count fetched equal to or greater than limit set: '||v_limit||'. Recommend increasing batch limit if possible.'); 
        IF v_jobmon THEN
            PERFORM update_step(v_step_id, 'WARNING','Row count fetched equal to or greater than limit set: '||v_limit||'. Recommend increasing batch limit if possible.');
            v_step_id := add_step(v_job_id, 'Removing high boundary rows from this batch to avoid missing data');       
        END IF;
        EXECUTE 'SELECT max('||v_control||') FROM '||v_tmp_table INTO v_last_value;
        EXECUTE 'DELETE FROM '||v_tmp_table||' WHERE '||v_control||' = '||quote_literal(v_last_value);
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
        EXECUTE 'CREATE INDEX ON '||v_tmp_table||' ('||array_to_string(v_pk_name, ',')||')'; -- incase of large batch limit
        EXECUTE 'ANALYZE '||v_tmp_table;       
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
    EXECUTE 'SELECT max('||v_control||') FROM '|| v_dest_table INTO v_last_value;
    UPDATE refresh_config_updater set last_value = coalesce(v_last_value, CURRENT_TIMESTAMP), last_run = CURRENT_TIMESTAMP WHERE dest_table = p_destination;  
    IF v_jobmon THEN
        PERFORM update_step(v_step_id, 'OK','Lower boundary value is: '||coalesce(v_last_value, CURRENT_TIMESTAMP));
    END IF;
    PERFORM gdb(p_debug, 'Lower boundary value is: '||coalesce(v_last_value, CURRENT_TIMESTAMP));
END IF;

EXECUTE 'DROP TABLE IF EXISTS '||v_tmp_table;

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

PERFORM pg_advisory_unlock(hashtext('refresh_updater'), hashtext(v_job_name));

EXCEPTION
    WHEN QUERY_CANCELED THEN
        EXECUTE 'SELECT '||v_dblink_schema||'.dblink_get_connections() @> ARRAY['||quote_literal(v_dblink_name)||']' INTO v_link_exists;
        IF v_link_exists THEN
            EXECUTE 'SELECT '||v_dblink_schema||'.dblink_disconnect('||quote_literal(v_dblink_name)||')';
        END IF;
        PERFORM pg_advisory_unlock(hashtext('refresh_updater'), hashtext(v_job_name));
        RAISE EXCEPTION '%', SQLERRM;  
    WHEN OTHERS THEN
        EXECUTE 'SELECT '||v_dblink_schema||'.dblink_get_connections() @> ARRAY['||quote_literal(v_dblink_name)||']' INTO v_link_exists;
        IF v_link_exists THEN
            EXECUTE 'SELECT '||v_dblink_schema||'.dblink_disconnect('||quote_literal(v_dblink_name)||')';
        END IF;
        IF v_jobmon THEN
            IF v_job_id IS NULL THEN
                EXECUTE 'SELECT '||v_jobmon_schema||'.add_job(''Refresh Updater: '||p_destination||''')' INTO v_job_id;
                EXECUTE 'SELECT '||v_jobmon_schema||'.add_step('||v_job_id||', ''EXCEPTION before job logging started'')' INTO v_step_id;
            END IF;
            IF v_step_id IS NULL THEN
                EXECUTE 'SELECT '||v_jobmon_schema||'.add_step('||v_job_id||', ''EXCEPTION before first step logged'')' INTO v_step_id;
            END IF;
            EXECUTE 'SELECT '||v_jobmon_schema||'.update_step('||v_step_id||', ''CRITICAL'', ''ERROR: '||COALESCE(SQLERRM,'unknown')||''')';
            EXECUTE 'SELECT '||v_jobmon_schema||'.fail_job('||v_job_id||')';
        END IF;
        PERFORM pg_advisory_unlock(hashtext('refresh_updater'), hashtext(v_job_name));
        RAISE EXCEPTION '%', SQLERRM;
END
$$;


/*
 *  DML maker function.
 */
CREATE OR REPLACE FUNCTION dml_maker(
    p_src_table text
    , p_dblink_id int
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

v_create_trig               text;
v_data_source               text;
v_dblink_schema             text;
v_dest_schema_name          text;
v_dest_table_name           text;
v_exists                    int := 0;
v_field                     text;
v_insert_refresh_config     text;
v_jobmon                    boolean;
v_key_type                  text;
v_old_search_path           text;
v_pk_counter                int := 1;
v_pk_name                   text[] := p_pk_name;
v_pk_name_n_type            text[];
v_pk_type                   text[] := p_pk_type;
v_pk_value                  text := '';
v_remote_exists             int := 0;
v_remote_grants_sql         text;
v_remote_key_sql            text;
v_remote_q_index            text;
v_remote_q_table            text;
v_row                       record;
v_src_table_name            text;
v_table_exists              boolean;
v_trigger_func              text;

BEGIN

SELECT nspname INTO v_dblink_schema FROM pg_namespace n, pg_extension e WHERE e.extname = 'dblink' AND e.extnamespace = n.oid;
SELECT current_setting('search_path') INTO v_old_search_path;
EXECUTE 'SELECT set_config(''search_path'',''@extschema@,'||v_dblink_schema||',public'',''false'')';

IF (p_pk_name IS NULL AND p_pk_type IS NOT NULL) OR (p_pk_name IS NOT NULL AND p_pk_type IS NULL) THEN
    RAISE EXCEPTION 'Cannot manually set primary/unique key field(s) without defining type(s) or vice versa';
END IF;

SELECT data_source INTO v_data_source FROM @extschema@.dblink_mapping WHERE data_source_id = p_dblink_id; 
IF NOT FOUND THEN
	RAISE EXCEPTION 'Database link ID is incorrect %', p_dblink_id; 
END IF;

IF p_dest_table IS NULL THEN
    p_dest_table := p_src_table;
END IF;

IF position('.' in p_dest_table) > 0 AND position('.' in p_src_table) > 0 THEN
    v_dest_schema_name := split_part(p_dest_table, '.', 1); 
    v_dest_table_name := split_part(p_dest_table, '.', 2);
ELSE
    RAISE EXCEPTION 'Source (and destination) table must be schema qualified';
END IF;

-- Substring avoids some issues with tables near max length
v_src_table_name := substring(replace(p_src_table, '.', '_') for 61);

PERFORM dblink_connect('mimeo_dml', @extschema@.auth(p_dblink_id));

-- Automatically get source primary/unique key if none given
IF p_pk_name IS NULL AND p_pk_type IS NULL THEN
    SELECT v_key_type, indkey_names, indkey_types INTO v_key_type, v_pk_name, v_pk_type FROM fetch_replication_key(p_src_table, 'mimeo_dml');
END IF;

PERFORM gdb(p_debug, 'v_key_type: '||v_key_type);
PERFORM gdb(p_debug, 'v_pk_name: '||array_to_string(v_pk_name, ','));
PERFORM gdb(p_debug, 'v_pk_type: '||array_to_string(v_pk_type, ','));

IF v_pk_name IS NULL OR v_pk_type IS NULL THEN
    RAISE EXCEPTION 'Source table has no valid primary key or unique index';
END IF;

IF p_filter IS NOT NULL THEN
    FOREACH v_field IN ARRAY v_pk_name LOOP
        IF v_field = ANY(p_filter) THEN
            CONTINUE;
        ELSE
            RAISE EXCEPTION 'ERROR: filter list did not contain all columns that compose primary/unique key for source table %',p_src_table; 
        END IF;
    END LOOP;
END IF;

v_remote_q_table := 'CREATE TABLE @extschema@.'||v_src_table_name||'_q (';
WHILE v_pk_counter <= array_length(v_pk_name,1) LOOP
    v_remote_q_table := v_remote_q_table || v_pk_name[v_pk_counter]||' '||v_pk_type[v_pk_counter];
    v_pk_counter := v_pk_counter + 1;
    IF v_pk_counter <= array_length(v_pk_name,1) THEN
        v_remote_q_table := v_remote_q_table || ', ';
    END IF;
END LOOP;
v_remote_q_table := v_remote_q_table || ', processed boolean)';
v_remote_q_index := 'CREATE INDEX '||v_src_table_name||'_q_'||array_to_string(v_pk_name, '_')||'_idx ON @extschema@.'||v_src_table_name||'_q ('||array_to_string(v_pk_name, ',')||')';

v_pk_counter := 1;
v_trigger_func := 'CREATE FUNCTION @extschema@.'||v_src_table_name||'_mimeo_queue() RETURNS trigger LANGUAGE plpgsql AS $_$ ';
    v_trigger_func := v_trigger_func || ' 
        BEGIN IF TG_OP = ''INSERT'' THEN ';
    v_pk_value := array_to_string(v_pk_name, ', NEW.');
    v_pk_value := 'NEW.'||v_pk_value;
    v_trigger_func := v_trigger_func || ' 
            INSERT INTO @extschema@.'||v_src_table_name||'_q ('||array_to_string(v_pk_name, ',')||') VALUES ('||v_pk_value||'); ';
    v_trigger_func := v_trigger_func || ' 
        ELSIF TG_OP = ''UPDATE'' THEN ';
    -- UPDATE needs to insert the NEW values so reuse v_pk_value from INSERT operation
    v_trigger_func := v_trigger_func || ' 
            INSERT INTO @extschema@.'||v_src_table_name||'_q ('||array_to_string(v_pk_name, ',')||') VALUES ('||v_pk_value||'); ';
    -- Only insert the old row if the new key doesn't match the old key. This handles edge case when only one column of a composite key is updated
    v_trigger_func := v_trigger_func || ' 
            IF ';
    FOREACH v_field IN ARRAY v_pk_name LOOP
        IF v_pk_counter > 1 THEN
            v_trigger_func := v_trigger_func || ' OR ';
        END IF;
        v_trigger_func := v_trigger_func || ' NEW.'||v_field||' != OLD.'||v_field||' ';
        v_pk_counter := v_pk_counter + 1;
    END LOOP;
    v_trigger_func := v_trigger_func || ' THEN ';
    v_pk_value := array_to_string(v_pk_name, ', OLD.');
    v_pk_value := 'OLD.'||v_pk_value;
    v_trigger_func := v_trigger_func || ' 
                INSERT INTO @extschema@.'||v_src_table_name||'_q ('||array_to_string(v_pk_name, ',')||') VALUES ('||v_pk_value||'); ';
    v_trigger_func := v_trigger_func || ' 
            END IF;';
    v_trigger_func := v_trigger_func || ' 
        ELSIF TG_OP = ''DELETE'' THEN ';
    -- DELETE needs to insert the OLD values so reuse v_pk_value from UPDATE operation
    v_trigger_func := v_trigger_func || ' 
            INSERT INTO @extschema@.'||v_src_table_name||'_q ('||array_to_string(v_pk_name, ',')||') VALUES ('||v_pk_value||'); ';
v_trigger_func := v_trigger_func || ' 
        END IF; RETURN NULL; END $_$;';

v_create_trig := 'CREATE TRIGGER '||v_src_table_name||'_mimeo_trig AFTER INSERT OR DELETE OR UPDATE';
IF p_filter IS NOT NULL THEN
    v_create_trig := v_create_trig || ' OF '||array_to_string(p_filter, ',');
END IF;
v_create_trig := v_create_trig || ' ON '||p_src_table||' FOR EACH ROW EXECUTE PROCEDURE @extschema@.'||v_src_table_name||'_mimeo_queue()';

RAISE NOTICE 'Creating objects on source database (function, trigger & queue table)...';

PERFORM gdb(p_debug, 'v_remote_q_table: '||v_remote_q_table);
PERFORM dblink_exec('mimeo_dml', v_remote_q_table);
PERFORM gdb(p_debug, 'v_remote_q_index: '||v_remote_q_index);
PERFORM dblink_exec('mimeo_dml', v_remote_q_index);
PERFORM gdb(p_debug, 'v_trigger_func: '||v_trigger_func);
PERFORM dblink_exec('mimeo_dml', v_trigger_func);
-- Grant any current role with write privileges on source table INSERT on the queue table before the trigger is actually created
v_remote_grants_sql := 'SELECT DISTINCT grantee FROM information_schema.table_privileges WHERE table_schema ||''.''|| table_name = '||quote_literal(p_dest_table)||' and privilege_type IN (''INSERT'',''UPDATE'',''DELETE'')';
FOR v_row IN SELECT grantee FROM dblink('mimeo_dml', v_remote_grants_sql) t (grantee text)
LOOP
    PERFORM dblink_exec('mimeo_dml', 'GRANT USAGE ON SCHEMA @extschema@ TO '||v_row.grantee);
    PERFORM dblink_exec('mimeo_dml', 'GRANT INSERT ON TABLE @extschema@.'||v_src_table_name||'_q TO '||v_row.grantee);
    PERFORM dblink_exec('mimeo_dml', 'GRANT EXECUTE ON FUNCTION @extschema@.'||v_src_table_name||'_mimeo_queue() TO '||v_row.grantee);
END LOOP;
PERFORM gdb(p_debug, 'v_create_trig: '||v_create_trig);
PERFORM dblink_exec('mimeo_dml', v_create_trig);

SELECT 
    CASE 
        WHEN count(nspname) > 0 THEN true
        ELSE false
    END AS jobmon_schema
INTO v_jobmon 
FROM pg_namespace n, pg_extension e WHERE e.extname = 'pg_jobmon' AND e.extnamespace = n.oid;

v_insert_refresh_config := 'INSERT INTO @extschema@.refresh_config_dml(
        source_table
        , dest_table
        , dblink
        , control
        , pk_name
        , pk_type
        , last_run
        , filter
        , condition
        , jobmon) 
    VALUES('||quote_literal(p_src_table)
        ||', '||quote_literal(p_dest_table)
        ||', '||p_dblink_id
        ||', '||quote_literal('@extschema@.'||v_src_table_name||'_q')
        ||', '||quote_literal(v_pk_name)
        ||', '||quote_literal(v_pk_type)
        ||', '||quote_literal(CURRENT_TIMESTAMP)
        ||', '||COALESCE(quote_literal(p_filter), 'NULL')
        ||', '||COALESCE(quote_literal(p_condition), 'NULL')
        ||', '||v_jobmon||')';
PERFORM gdb(p_debug, 'v_insert_refresh_config: '||v_insert_refresh_config);
EXECUTE v_insert_refresh_config;

SELECT p_table_exists FROM manage_dest_table(p_dest_table, NULL, p_debug) INTO v_table_exists;

IF p_pulldata AND v_table_exists = false THEN
    RAISE NOTICE 'Pulling data from source...';
    EXECUTE 'SELECT refresh_dml('||quote_literal(p_dest_table)||', p_repull := true, p_debug := '||p_debug||')';
END IF;

IF p_index AND v_table_exists = false THEN
    PERFORM create_index(p_dest_table, NULL, p_debug);
ELSIF v_table_exists = false THEN
-- Ensure destination indexes that are needed for efficient replication are created even if p_index is set false
    PERFORM gdb(p_debug, 'Creating indexes needed for replication');
    IF v_key_type = 'primary' THEN
        EXECUTE 'ALTER TABLE '||p_dest_table||' ADD PRIMARY KEY('||array_to_string(v_pk_name, ',')||')';
    ELSE
        EXECUTE 'CREATE UNIQUE INDEX ON '||p_dest_table||' ('||array_to_string(v_pk_name, ',')||')';
    END IF;
END IF;

IF v_table_exists THEN
    RAISE NOTICE 'Destination table % already exists. No data or indexes were pulled from source', p_src_table;
END IF;

PERFORM dblink_disconnect('mimeo_dml');

EXECUTE 'SELECT set_config(''search_path'','''||v_old_search_path||''',''false'')';

RAISE NOTICE 'Done';

RETURN;

EXCEPTION
    WHEN OTHERS THEN
        EXECUTE 'SELECT set_config(''search_path'',''@extschema@,'||v_dblink_schema||''',''false'')';
        -- Only cleanup remote objects if replication doesn't exist at all for source table
        EXECUTE 'SELECT count(*) FROM @extschema@.refresh_config_dml WHERE source_table = '||quote_literal(p_src_table) INTO v_exists;
        IF dblink_get_connections() @> '{mimeo_dml}' THEN
            IF v_exists = 0 THEN
                PERFORM dblink_exec('mimeo_dml', 'DROP TABLE IF EXISTS @extschema@.'||v_src_table_name||'_q');
                PERFORM dblink_exec('mimeo_dml', 'DROP TRIGGER IF EXISTS '||v_src_table_name||'_mimeo_trig ON '||p_src_table);
                PERFORM dblink_exec('mimeo_dml', 'DROP FUNCTION IF EXISTS @extschema@.'||v_src_table_name||'_mimeo_queue()');
            END IF;
            PERFORM dblink_disconnect('mimeo_dml');
        END IF;
        IF v_exists = 0 AND dblink_get_connections() @> '{mimeo_dml}' THEN
            EXECUTE 'SELECT set_config(''search_path'','''||v_old_search_path||''',''false'')';
            RAISE EXCEPTION 'dml_maker() failure. Cleaned up source table mimeo objects (queue table, function & trigger) if they existed. SQLERRM: %', SQLERRM;
        ELSE
            EXECUTE 'SELECT set_config(''search_path'','''||v_old_search_path||''',''false'')';
            RAISE EXCEPTION 'dml_maker() failure. Unable to clean up source database objects (trigger/queue table) if they were made. SQLERRM: % ', SQLERRM;
        END IF;
END
$$;


/*
 *  Inserter maker function. 
 */
CREATE OR REPLACE FUNCTION inserter_maker(
    p_src_table text
    , p_control_field text
    , p_dblink_id int
    , p_boundary interval DEFAULT '00:10:00'
    , p_dest_table text DEFAULT NULL
    , p_index boolean DEFAULT true
    , p_filter text[] DEFAULT NULL
    , p_condition text DEFAULT NULL
    , p_pulldata boolean DEFAULT true
    , p_debug boolean DEFAULT false) 
RETURNS void
    LANGUAGE plpgsql
    AS $$
DECLARE

v_data_source               text;
v_dest_schema_name          text;
v_dest_table_name           text;
v_dst_active                boolean;
v_jobmon                    boolean;
v_insert_refresh_config     text;
v_max_timestamp             timestamptz;
v_table_exists              boolean;

BEGIN

SELECT data_source INTO v_data_source FROM @extschema@.dblink_mapping WHERE data_source_id = p_dblink_id; 
IF NOT FOUND THEN
	RAISE EXCEPTION 'ERROR: database link ID is incorrect %', p_dblink_id; 
END IF;  

IF p_dest_table IS NULL THEN
    p_dest_table := p_src_table;
END IF;

IF position('.' in p_dest_table) > 0 AND position('.' in p_src_table) > 0 THEN
    v_dest_schema_name := split_part(p_dest_table, '.', 1); 
    v_dest_table_name := split_part(p_dest_table, '.', 2);
ELSE
    RAISE EXCEPTION 'Source (and destination) table must be schema qualified';
END IF;

-- Determine if pg_jobmon is installed to set config table option below
SELECT 
    CASE 
        WHEN count(nspname) > 0 THEN true
        ELSE false
    END AS jobmon_schema
INTO v_jobmon 
FROM pg_namespace n, pg_extension e WHERE e.extname = 'pg_jobmon' AND e.extnamespace = n.oid;

v_dst_active := @extschema@.dst_utc_check();

v_insert_refresh_config := 'INSERT INTO @extschema@.refresh_config_inserter(
        source_table
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
        ||', '||quote_literal(p_dest_table)
        ||', '|| p_dblink_id
        ||', '||quote_literal(p_control_field)
        ||', '||quote_literal(p_boundary)
        ||', ''0001-01-01''::date, '
              ||quote_literal(CURRENT_TIMESTAMP)
        ||', '||v_dst_active
        ||', '||COALESCE(quote_literal(p_filter), 'NULL')
        ||', '||COALESCE(quote_literal(p_condition), 'NULL')
        ||', '||v_jobmon||')';

PERFORM @extschema@.gdb(p_debug, 'v_insert_refresh_config: '||v_insert_refresh_config);
EXECUTE v_insert_refresh_config;

SELECT p_table_exists FROM @extschema@.manage_dest_table(p_dest_table, NULL, p_debug) INTO v_table_exists;

IF p_pulldata AND v_table_exists = false THEN
    RAISE NOTICE 'Pulling all data from source...';
    EXECUTE 'SELECT @extschema@.refresh_inserter('||quote_literal(p_dest_table)||', p_repull := true, p_debug := '||p_debug||')';
END IF;

IF p_index AND v_table_exists = false THEN
    PERFORM @extschema@.create_index(p_dest_table, NULL, p_debug);
END IF;

IF v_table_exists THEN
    RAISE NOTICE 'Destination table % already exists. No data or indexes were pulled from source', p_dest_table;
END IF;

RAISE NOTICE 'Getting the maximum destination timestamp...';
EXECUTE 'SELECT max('||p_control_field||') FROM '||p_dest_table||';' INTO v_max_timestamp;

EXECUTE 'UPDATE @extschema@.refresh_config_inserter SET last_value = '||quote_literal(COALESCE(v_max_timestamp, CURRENT_TIMESTAMP))||' WHERE dest_table = '||quote_literal(p_dest_table);

RAISE NOTICE 'Done';
END
$$;


/*
 *  Logdel maker function.
 */
CREATE OR REPLACE FUNCTION logdel_maker(
    p_src_table text
    , p_dblink_id int
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

v_col_exists                int;
v_cols                      text[];
v_cols_n_types              text[];
v_counter                   int := 1;
v_create_trig               text;
v_data_source               text;
v_dblink_schema             text;
v_dest_schema_name          text;
v_dest_table_name           text;
v_exists                    int := 0;
v_field                     text;
v_insert_refresh_config     text;
v_jobmon                    boolean;
v_key_type                  text;
v_old_search_path           text;
v_pk_name                   text[] := p_pk_name;
v_pk_type                   text[] := p_pk_type;
v_q_value                   text := '';
v_remote_grants_sql         text;
v_remote_key_sql            text;
v_remote_sql                text;
v_remote_q_index            text;
v_remote_q_table            text;
v_row                       record;
v_src_table_name            text;
v_table_exists              boolean;
v_trigger_func              text;
v_types                     text[];

BEGIN

SELECT nspname INTO v_dblink_schema FROM pg_namespace n, pg_extension e WHERE e.extname = 'dblink' AND e.extnamespace = n.oid;
SELECT current_setting('search_path') INTO v_old_search_path;
EXECUTE 'SELECT set_config(''search_path'',''@extschema@,'||v_dblink_schema||',public'',''false'')';

IF (p_pk_name IS NULL AND p_pk_type IS NOT NULL) OR (p_pk_name IS NOT NULL AND p_pk_type IS NULL) THEN
    RAISE EXCEPTION 'Cannot manually set primary/unique key field(s) without defining type(s) or vice versa';
END IF;

SELECT data_source INTO v_data_source FROM @extschema@.dblink_mapping WHERE data_source_id = p_dblink_id; 
IF NOT FOUND THEN
	RAISE EXCEPTION 'Database link ID is incorrect %', p_dblink_id; 
END IF;

IF p_dest_table IS NULL THEN
    p_dest_table := p_src_table;
END IF;

IF position('.' in p_dest_table) > 0 AND position('.' in p_src_table) > 0 THEN
    v_dest_schema_name := split_part(p_dest_table, '.', 1); 
    v_dest_table_name := split_part(p_dest_table, '.', 2);
ELSE
    RAISE EXCEPTION 'Source (and destination) table must be schema qualified';
END IF;

-- Substring avoids some issues with tables near max length
v_src_table_name := substring(replace(p_src_table, '.', '_') for 61);

PERFORM dblink_connect('mimeo_logdel', @extschema@.auth(p_dblink_id));

-- Automatically get source primary/unique key if none given
IF p_pk_name IS NULL AND p_pk_type IS NULL THEN
   SELECT v_key_type, indkey_names, indkey_types INTO v_key_type, v_pk_name, v_pk_type FROM fetch_replication_key(p_src_table, 'mimeo_logdel');
END IF;

PERFORM gdb(p_debug, 'v_key_type: '||v_key_type);
PERFORM gdb(p_debug, 'v_pk_name: '||array_to_string(v_pk_name, ','));
PERFORM gdb(p_debug, 'v_pk_type: '||array_to_string(v_pk_type, ','));

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

SELECT 
    CASE 
        WHEN count(nspname) > 0 THEN true
        ELSE false
    END AS jobmon_schema
INTO v_jobmon 
FROM pg_namespace n, pg_extension e WHERE e.extname = 'pg_jobmon' AND e.extnamespace = n.oid;

-- Unlike dml, config table insertion has to go first so that remote queue table creation step can have full column list
v_insert_refresh_config := 'INSERT INTO @extschema@.refresh_config_logdel(
        source_table
        , dest_table
        , dblink
        , control
        , pk_name
        , pk_type
        , last_run
        , filter
        , condition
        , jobmon ) 
    VALUES('
        ||quote_literal(p_src_table)
        ||', '||quote_literal(p_dest_table)
        ||', '|| p_dblink_id
        ||', '||quote_literal('@extschema@.'||v_src_table_name||'_q')
        ||', '||quote_literal(v_pk_name)
        ||', '||quote_literal(v_pk_type)
        ||', '||quote_literal(CURRENT_TIMESTAMP)
        ||', '||COALESCE(quote_literal(p_filter), 'NULL')
        ||', '||COALESCE(quote_literal(p_condition), 'NULL')
        ||', '||v_jobmon||')';
RAISE NOTICE 'Inserting data into config table';
PERFORM gdb(p_debug, 'v_insert_refresh_config: '||v_insert_refresh_config);
EXECUTE v_insert_refresh_config;

SELECT p_table_exists, p_cols, p_cols_n_types FROM manage_dest_table(p_dest_table, NULL, p_debug) INTO v_table_exists, v_cols, v_cols_n_types;

v_remote_q_table := 'CREATE TABLE @extschema@.'||v_src_table_name||'_q ('||array_to_string(v_cols_n_types, ',')||', mimeo_source_deleted timestamptz, processed boolean)';
-- Indexes on queue table created below so the variable can be reused

v_trigger_func := 'CREATE FUNCTION @extschema@.'||v_src_table_name||'_mimeo_queue() RETURNS trigger LANGUAGE plpgsql AS $_$ DECLARE ';
    v_trigger_func := v_trigger_func || ' 
        v_del_time timestamptz := clock_timestamp(); ';
    v_trigger_func := v_trigger_func || ' 
        BEGIN IF TG_OP = ''INSERT'' THEN ';
    v_q_value := array_to_string(v_pk_name, ', NEW.');
    v_q_value := 'NEW.'||v_q_value;
    v_trigger_func := v_trigger_func || ' 
            INSERT INTO @extschema@.'||v_src_table_name||'_q ('||array_to_string(v_pk_name, ',')||') VALUES ('||v_q_value||');';
    v_trigger_func := v_trigger_func || ' 
        ELSIF TG_OP = ''UPDATE'' THEN  ';
    -- UPDATE needs to insert the NEW values so reuse v_q_value from INSERT operation
    v_trigger_func := v_trigger_func || ' 
            INSERT INTO @extschema@.'||v_src_table_name||'_q ('||array_to_string(v_pk_name, ',')||') VALUES ('||v_q_value||');';
    -- Only insert the old row if the new key doesn't match the old key. This handles edge case when only one column of a composite key is updated
    v_trigger_func := v_trigger_func || ' 
            IF ';
    FOREACH v_field IN ARRAY v_pk_name LOOP
        IF v_counter > 1 THEN
            v_trigger_func := v_trigger_func || ' OR ';
        END IF;
        v_trigger_func := v_trigger_func || ' NEW.'||v_field||' != OLD.'||v_field||' ';
        v_counter := v_counter + 1;
    END LOOP;
    v_trigger_func := v_trigger_func || ' THEN ';
    v_q_value := array_to_string(v_pk_name, ', OLD.');
    v_q_value := 'OLD.'||v_q_value;
    v_trigger_func := v_trigger_func || ' 
                INSERT INTO @extschema@.'||v_src_table_name||'_q ('||array_to_string(v_pk_name, ',')||') VALUES ('||v_q_value||'); ';
    v_trigger_func := v_trigger_func || ' 
            END IF;';
    v_trigger_func := v_trigger_func || ' 
        ELSIF TG_OP = ''DELETE'' THEN  ';
    v_q_value := array_to_string(v_cols, ', OLD.');
    v_q_value := 'OLD.'||v_q_value;
    v_trigger_func := v_trigger_func || ' 
            INSERT INTO @extschema@.'||v_src_table_name||'_q ('||array_to_string(v_cols, ',')||', mimeo_source_deleted) VALUES ('||v_q_value||', v_del_time);';
v_trigger_func := v_trigger_func ||' 
        END IF; RETURN NULL; END $_$;';

v_create_trig := 'CREATE TRIGGER '||v_src_table_name||'_mimeo_trig AFTER INSERT OR DELETE OR UPDATE';
IF p_filter IS NOT NULL THEN
    v_create_trig := v_create_trig || ' OF '||array_to_string(p_filter, ',');
END IF;
v_create_trig := v_create_trig || ' ON '||p_src_table||' FOR EACH ROW EXECUTE PROCEDURE @extschema@.'||v_src_table_name||'_mimeo_queue()';

RAISE NOTICE 'Creating objects on source database (function, trigger & queue table)...';

PERFORM gdb(p_debug, 'v_remote_q_table: '||v_remote_q_table); 
PERFORM dblink_exec('mimeo_logdel', v_remote_q_table);
v_remote_q_index := 'CREATE INDEX '||v_src_table_name||'_q_'||array_to_string(v_pk_name, '_')||'_idx ON @extschema@.'||v_src_table_name||'_q ('||array_to_string(v_pk_name, ',')||')';
PERFORM gdb(p_debug, 'v_remote_q_index: '||v_remote_q_index);
PERFORM dblink_exec('mimeo_logdel', v_remote_q_index);
v_remote_q_index := 'CREATE INDEX '||v_src_table_name||'_q_processed_deleted'||'_idx ON @extschema@.'||v_src_table_name||'_q (processed, mimeo_source_deleted)';
PERFORM gdb(p_debug, 'v_remote_q_index: '||v_remote_q_index);
PERFORM dblink_exec('mimeo_logdel', v_remote_q_index);
PERFORM gdb(p_debug, 'v_trigger_func: '||v_trigger_func);
PERFORM dblink_exec('mimeo_logdel', v_trigger_func);
-- Grant any current role with write privileges on source table INSERT on the queue table before the trigger is actually created
v_remote_grants_sql := 'SELECT DISTINCT grantee FROM information_schema.table_privileges WHERE table_schema ||''.''|| table_name = '||quote_literal(p_dest_table)||' and privilege_type IN (''INSERT'',''UPDATE'',''DELETE'')';
FOR v_row IN SELECT grantee FROM dblink('mimeo_logdel', v_remote_grants_sql) t (grantee text)
LOOP
    PERFORM dblink_exec('mimeo_logdel', 'GRANT USAGE ON SCHEMA @extschema@ TO '||v_row.grantee);
    PERFORM dblink_exec('mimeo_logdel', 'GRANT INSERT ON TABLE @extschema@.'||v_src_table_name||'_q TO '||v_row.grantee);
    PERFORM dblink_exec('mimeo_logdel', 'GRANT EXECUTE ON FUNCTION @extschema@.'||v_src_table_name||'_mimeo_queue() TO '||v_row.grantee);
END LOOP;
PERFORM gdb(p_debug, 'v_create_trig: '||v_create_trig); 
PERFORM dblink_exec('mimeo_logdel', v_create_trig);

IF p_pulldata AND v_table_exists = false THEN
    RAISE NOTICE 'Pulling all data from source...';
    EXECUTE 'SELECT refresh_logdel('||quote_literal(p_dest_table)||', p_repull := true, p_debug := '||p_debug||')';
END IF;

IF p_index AND v_table_exists = false THEN
    PERFORM create_index(p_dest_table, NULL, p_debug);
    -- Create index on special column for logdel
    EXECUTE 'CREATE INDEX '||v_dest_table_name||'_mimeo_source_deleted ON '||p_dest_table||' (mimeo_source_deleted)';
ELSIF v_table_exists = false THEN
    -- Ensure destination indexes that are needed for efficient replication are created even if p_index is set false
    RAISE NOTICE 'Adding primary/unique key to table...';
    IF v_key_type = 'primary' THEN
        EXECUTE 'ALTER TABLE '||p_dest_table||' ADD PRIMARY KEY('||array_to_string(v_pk_name, ',')||')';
    ELSE
        EXECUTE 'CREATE UNIQUE INDEX ON '||p_dest_table||' ('||array_to_string(v_pk_name, ',')||')';
    END IF;    
END IF;

IF v_table_exists THEN
    RAISE NOTICE 'Destination table % already exists. No data or indexes were pulled from source: %. Recommend making index on special column mimeo_source_deleted if it doesn''t have one', p_dest_table;
END IF;

PERFORM dblink_disconnect('mimeo_logdel');

EXECUTE 'SELECT set_config(''search_path'','''||v_old_search_path||''',''false'')';

RAISE NOTICE 'Done';
RETURN;

EXCEPTION
    WHEN OTHERS THEN
        EXECUTE 'SELECT set_config(''search_path'',''@extschema@,'||v_dblink_schema||''',''false'')';
        -- Only cleanup remote objects if replication doesn't exist at all for source table
        EXECUTE 'SELECT count(*) FROM @extschema@.refresh_config_logdel WHERE source_table = '||quote_literal(p_src_table) INTO v_exists;
        IF dblink_get_connections() @> '{mimeo_logdel}' THEN
            IF v_exists = 0 THEN
                PERFORM dblink_exec('mimeo_logdel', 'DROP TABLE IF EXISTS @extschema@.'||v_src_table_name||'_q');
                PERFORM dblink_exec('mimeo_logdel', 'DROP TRIGGER IF EXISTS '||v_src_table_name||'_mimeo_trig ON '||p_src_table);
                PERFORM dblink_exec('mimeo_logdel', 'DROP FUNCTION IF EXISTS @extschema@.'||v_src_table_name||'_mimeo_queue()');
            END IF;
            PERFORM dblink_disconnect('mimeo_logdel');
        END IF;
        IF v_exists = 0 AND dblink_get_connections() @> '{mimeo_logdel}' THEN
            EXECUTE 'SELECT set_config(''search_path'','''||v_old_search_path||''',''false'')';
            RAISE EXCEPTION 'logdel_maker() failure. Cleaned up source table mimeo objects (queue table, function & trigger) if they existed. SQLERRM: %', SQLERRM;
        ELSE
            EXECUTE 'SELECT set_config(''search_path'','''||v_old_search_path||''',''false'')';
            RAISE EXCEPTION 'logdel_maker() failure. Unable to clean up source database objects (trigger/queue table) if they were made. SQLERRM: % ', SQLERRM;
        END IF;
END
$$;


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
    , p_debug boolean DEFAULT false) 
RETURNS void
    LANGUAGE plpgsql
    AS $$
DECLARE

v_data_source               text;
v_insert_refresh_config     text;
v_jobmon                    boolean;

BEGIN

SELECT data_source INTO v_data_source FROM @extschema@.dblink_mapping WHERE data_source_id = p_dblink_id; 
IF NOT FOUND THEN
	RAISE EXCEPTION 'ERROR: Database link ID does not exist in @extschema@.dblink_mapping: %', p_dblink_id; 
END IF;  

IF p_dest_table IS NULL THEN
    p_dest_table := p_src_table;
END IF;

IF position('.' in p_dest_table) = 0 AND position('.' in p_src_table) = 0 THEN
    RAISE EXCEPTION 'Source (and destination) table must be schema qualified';
END IF;

-- Determine if pg_jobmon is installed to set config table option below
SELECT 
    CASE 
        WHEN count(nspname) > 0 THEN true
        ELSE false
    END AS jobmon_schema
INTO v_jobmon 
FROM pg_namespace n, pg_extension e WHERE e.extname = 'pg_jobmon' AND e.extnamespace = n.oid;

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

RAISE NOTICE 'attempting first snapshot';
EXECUTE 'SELECT @extschema@.refresh_snap('||quote_literal(p_dest_table)||', p_index := '||p_index||', p_pulldata := '||p_pulldata||', p_debug := '||p_debug||')'; 

RAISE NOTICE 'attempting second snapshot';
EXECUTE 'SELECT @extschema@.refresh_snap('||quote_literal(p_dest_table)||', p_index := '||p_index||', p_pulldata := '||p_pulldata||', p_debug := '||p_debug||')';

RAISE NOTICE 'Done';

RETURN;
END
$$;


/*
 *  Plain table refresh maker function. 
 */
CREATE OR REPLACE FUNCTION table_maker(
    p_src_table text
    , p_dblink_id int
    , p_dest_table text DEFAULT NULL
    , p_index boolean DEFAULT true
    , p_filter text[] DEFAULT NULL
    , p_condition text DEFAULT NULL
    , p_sequences text[] DEFAULT NULL
    , p_pulldata boolean DEFAULT true
    , p_debug boolean DEFAULT false) 
RETURNS void
    LANGUAGE plpgsql
    AS $$
DECLARE

v_data_source               text;
v_dest_schema_name          text;
v_dest_table_name           text;
v_dst_active                boolean;
v_insert_refresh_config     text;
v_jobmon                    boolean;
v_max_timestamp             timestamptz;
v_seq                       text;
v_seq_max                   bigint;
v_table_exists              boolean;

BEGIN

SELECT data_source INTO v_data_source FROM @extschema@.dblink_mapping WHERE data_source_id = p_dblink_id; 
IF NOT FOUND THEN
	RAISE EXCEPTION 'ERROR: database link ID is incorrect %', p_dblink_id; 
END IF;  

IF p_dest_table IS NULL THEN
    p_dest_table := p_src_table;
END IF;

IF position('.' in p_dest_table) > 0 AND position('.' in p_src_table) > 0 THEN
    v_dest_schema_name := split_part(p_dest_table, '.', 1); 
    v_dest_table_name := split_part(p_dest_table, '.', 2);
ELSE
    RAISE EXCEPTION 'Source (and destination) table must be schema qualified';
END IF;

-- Determine if pg_jobmon is installed to set config table option below
SELECT 
    CASE 
        WHEN count(nspname) > 0 THEN true
        ELSE false
    END AS jobmon_schema
INTO v_jobmon 
FROM pg_namespace n, pg_extension e WHERE e.extname = 'pg_jobmon' AND e.extnamespace = n.oid;

v_insert_refresh_config := 'INSERT INTO @extschema@.refresh_config_table(
        source_table
        , dest_table
        , dblink
        , last_run
        , filter
        , condition
        , sequences 
        , jobmon)
    VALUES('
    ||quote_literal(p_src_table)
    ||', '||quote_literal(p_dest_table)
    ||', '|| p_dblink_id
    ||', '||quote_literal(CURRENT_TIMESTAMP)
    ||', '||COALESCE(quote_literal(p_filter), 'NULL')
    ||', '||COALESCE(quote_literal(p_condition), 'NULL')
    ||', '||COALESCE(quote_literal(p_sequences), 'NULL')
    ||', '||v_jobmon||')';
PERFORM @extschema@.gdb(p_debug, 'v_insert_refresh_config: '||v_insert_refresh_config);
EXECUTE v_insert_refresh_config;

SELECT p_table_exists FROM @extschema@.manage_dest_table(p_dest_table, NULL, p_debug) INTO v_table_exists;

IF p_pulldata AND v_table_exists = false THEN
    RAISE NOTICE 'Pulling all data from source...';
    EXECUTE 'SELECT @extschema@.refresh_table('||quote_literal(p_dest_table)||', p_debug := '||p_debug||')';
END IF;

IF p_index AND v_table_exists = false THEN
    PERFORM @extschema@.create_index(p_dest_table, NULL, p_debug);
END IF;

IF v_table_exists THEN
    RAISE NOTICE 'Destination table % already exists. No data or indexes were pulled from source', p_dest_table;
END IF;

RAISE NOTICE 'Done';
END
$$;


/*
 *  Updater maker function.
 */ 
CREATE OR REPLACE FUNCTION updater_maker(
    p_src_table text
    , p_control_field text
    , p_dblink_id int
    , p_boundary interval DEFAULT '00:10:00'
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

v_data_source               text;
v_dblink_schema             text;
v_dest_schema_name          text;
v_dest_table_name           text;
v_dst_active                boolean;
v_field                     text;
v_insert_refresh_config     text;
v_jobmon                    boolean;
v_key_type                  text;
v_link_exists               boolean;
v_max_timestamp             timestamptz;
v_old_search_path           text;
v_pk_name                   text[] := p_pk_name;
v_pk_type                   text[] := p_pk_type;
v_remote_key_sql            text;
v_table_exists              boolean;
v_update_refresh_config     text;

BEGIN

SELECT nspname INTO v_dblink_schema FROM pg_namespace n, pg_extension e WHERE e.extname = 'dblink' AND e.extnamespace = n.oid;
SELECT current_setting('search_path') INTO v_old_search_path;
EXECUTE 'SELECT set_config(''search_path'',''@extschema@,'||v_dblink_schema||',public'',''false'')';

IF (p_pk_name IS NULL AND p_pk_type IS NOT NULL) OR (p_pk_name IS NOT NULL AND p_pk_type IS NULL) THEN
    RAISE EXCEPTION 'Cannot manually set primary/unique key field(s) without defining type(s) or vice versa';
END IF;

SELECT data_source INTO v_data_source FROM @extschema@.dblink_mapping WHERE data_source_id = p_dblink_id; 
IF NOT FOUND THEN
	RAISE EXCEPTION 'ERROR: database link ID is incorrect %', p_dblink_id; 
END IF;  

IF p_dest_table IS NULL THEN
    p_dest_table := p_src_table;
END IF;

IF position('.' in p_dest_table) > 0 AND position('.' in p_src_table) > 0 THEN
    v_dest_schema_name := split_part(p_dest_table, '.', 1); 
    v_dest_table_name := split_part(p_dest_table, '.', 2);
ELSE
    RAISE EXCEPTION 'Source (and destination) table must be schema qualified';
END IF;

PERFORM dblink_connect('mimeo_updater', @extschema@.auth(p_dblink_id));

-- Automatically get source primary/unique key if none given
IF p_pk_name IS NULL AND p_pk_type IS NULL THEN
    SELECT v_key_type, indkey_names, indkey_types INTO v_key_type, v_pk_name, v_pk_type FROM fetch_replication_key(p_src_table, 'mimeo_updater');
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

v_dst_active := @extschema@.dst_utc_check();

-- Determine if pg_jobmon is installed to set config table option below
SELECT 
    CASE 
        WHEN count(nspname) > 0 THEN true
        ELSE false
    END AS jobmon_schema
INTO v_jobmon 
FROM pg_namespace n, pg_extension e WHERE e.extname = 'pg_jobmon' AND e.extnamespace = n.oid;

v_insert_refresh_config := 'INSERT INTO @extschema@.refresh_config_updater(
        source_table
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
        ||', '||quote_literal(p_dest_table)
        ||', '|| p_dblink_id
        ||', '||quote_literal(p_control_field)
        ||', '||quote_literal(p_boundary)
        ||', '||quote_literal(v_pk_name)
        ||', '||quote_literal(v_pk_type)
        ||', ''0001-01-01''::date ,'
              ||quote_literal(CURRENT_TIMESTAMP)
        ||', '||v_dst_active
        ||', '||COALESCE(quote_literal(p_filter), 'NULL')
        ||', '||COALESCE(quote_literal(p_condition), 'NULL')
        ||', '||v_jobmon||')';
PERFORM gdb(p_debug, 'v_insert_refresh_config: '||v_insert_refresh_config);
EXECUTE v_insert_refresh_config;

SELECT p_table_exists FROM @extschema@.manage_dest_table(p_dest_table, NULL, p_debug) INTO v_table_exists;

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
        EXECUTE 'ALTER TABLE '||p_dest_table||' ADD PRIMARY KEY('||array_to_string(v_pk_name, ',')||')';
    ELSE
        EXECUTE 'CREATE UNIQUE INDEX ON '||p_dest_table||' ('||array_to_string(v_pk_name, ',')||')';
    END IF;
END IF;

IF v_table_exists THEN
    RAISE NOTICE 'Destination table % already exists. No data or indexes was pulled from source', p_dest_table;
END IF;

PERFORM dblink_disconnect('mimeo_updater');

RAISE NOTICE 'Getting the maximum destination timestamp...';
EXECUTE 'SELECT max('||p_control_field||') FROM '||p_dest_table||';' INTO v_max_timestamp;
EXECUTE 'UPDATE @extschema@.refresh_config_updater SET last_value = '||quote_literal(COALESCE(v_max_timestamp, CURRENT_TIMESTAMP))||' WHERE dest_table = '||quote_literal(p_dest_table);

EXECUTE 'SELECT set_config(''search_path'','''||v_old_search_path||''',''false'')';

RAISE NOTICE 'Done';

RETURN;

EXCEPTION
    WHEN QUERY_CANCELED OR OTHERS THEN
        EXECUTE 'SELECT '||v_dblink_schema||'.dblink_get_connections() @> ARRAY['||quote_literal(v_dblink_name)||']' INTO v_link_exists;
        IF v_link_exists THEN
            EXECUTE 'SELECT '||v_dblink_schema||'.dblink_disconnect('||quote_literal(v_dblink_name)||')';
        END IF;
        EXECUTE 'SELECT set_config(''search_path'','''||v_old_search_path||''',''false'')';
        RAISE EXCEPTION '%', SQLERRM;   
END  
$$;


/*
 *  DML destroyer function. 
 */
CREATE FUNCTION dml_destroyer(p_dest_table text, p_keep_table boolean DEFAULT true) RETURNS void
    LANGUAGE plpgsql
    AS $$
DECLARE

v_dblink            int;
v_dblink_schema     text;
v_dest_table        text;
v_drop_function     text;
v_drop_q_table      text;
v_drop_trigger      text;
v_old_search_path   text;
v_src_table         text;
v_table_name        text;
    
BEGIN

SELECT nspname INTO v_dblink_schema FROM pg_namespace n, pg_extension e WHERE e.extname = 'dblink' AND e.extnamespace = n.oid;
SELECT current_setting('search_path') INTO v_old_search_path;
EXECUTE 'SELECT set_config(''search_path'',''@extschema@,'||v_dblink_schema||',public'',''false'')';

SELECT source_table, dest_table, dblink INTO v_src_table, v_dest_table, v_dblink
		FROM @extschema@.refresh_config_dml WHERE dest_table = p_dest_table;
IF NOT FOUND THEN
		RAISE NOTICE 'This table is not set up for dml replication: %', v_dest_table;
ELSE
    v_table_name := replace(v_src_table, '.', '_');

    v_drop_function := 'DROP FUNCTION IF EXISTS @extschema@.'||v_table_name||'_mimeo_queue()';
    v_drop_trigger := 'DROP TRIGGER IF EXISTS '||v_table_name||'_mimeo_trig ON '||v_src_table;
    v_drop_q_table := 'DROP TABLE IF EXISTS @extschema@.'||v_table_name||'_pgq';

    RAISE NOTICE 'Removing mimeo objects from source database if they exist (trigger, function, queue table)';
    PERFORM dblink_connect('mimeo_dml_destroy', @extschema@.auth(v_dblink));
    PERFORM dblink_exec('mimeo_dml_destroy', v_drop_trigger);
    PERFORM dblink_exec('mimeo_dml_destroy', v_drop_function);
    PERFORM dblink_exec('mimeo_dml_destroy', v_drop_q_table);
    PERFORM dblink_disconnect('mimeo_dml_destroy');

    IF p_keep_table THEN 
        RAISE NOTICE 'Destination table NOT destroyed: %', v_dest_table; 
    ELSE
        RAISE NOTICE 'Destination table destroyed: %', v_dest_table;
        EXECUTE 'DROP TABLE IF EXISTS ' || v_dest_table;
    END IF;

    RAISE NOTICE 'Removing config data';
    EXECUTE 'DELETE FROM @extschema@.refresh_config_dml WHERE dest_table = ' || quote_literal(v_dest_table);

    RAISE NOTICE 'Done';
END IF;

EXECUTE 'SELECT set_config(''search_path'','''||v_old_search_path||''',''false'')';

EXCEPTION
    WHEN OTHERS THEN
        EXECUTE 'SELECT set_config(''search_path'',''@extschema@,'||v_dblink_schema||''',''false'')';
        IF dblink_get_connections() @> '{mimeo_dml_destroy}' THEN
            PERFORM dblink_disconnect('mimeo_dml_destroy');
        END IF;
        EXECUTE 'SELECT set_config(''search_path'','''||v_old_search_path||''',''false'')';
        RAISE EXCEPTION '%', SQLERRM;    
END
$$;


/*
 *  Inserter destroyer function. 
 */
CREATE FUNCTION inserter_destroyer(p_dest_table text, p_keep_table boolean DEFAULT true) RETURNS void
    LANGUAGE plpgsql
    AS $$
DECLARE

v_dest_table        text;
    
BEGIN

SELECT dest_table INTO v_dest_table
    FROM @extschema@.refresh_config_inserter WHERE dest_table = p_dest_table;
IF NOT FOUND THEN
    RAISE NOTICE 'This table is not set up for inserter replication: %', p_dest_table;
ELSE
    IF p_keep_table THEN 
        RAISE NOTICE 'Destination table NOT destroyed: %', v_dest_table; 
    ELSE
        RAISE NOTICE 'Destination table destroyed: %', v_dest_table;
        EXECUTE 'DROP TABLE IF EXISTS ' || v_dest_table;
    END IF;

    EXECUTE 'DELETE FROM @extschema@.refresh_config_inserter WHERE dest_table = ' || quote_literal(v_dest_table);	
END IF;

END
$$;


/*
 *  Logdel destroyer function. 
 */
CREATE FUNCTION logdel_destroyer(p_dest_table text, p_keep_table boolean DEFAULT true) RETURNS void
    LANGUAGE plpgsql
    AS $$
    
DECLARE

v_dblink            int;
v_dblink_schema     text;
v_dest_table        text;
v_drop_function     text;
v_drop_q_table      text;
v_drop_trigger      text;
v_old_search_path   text;
v_src_table         text;
v_table_name        text;
    
BEGIN

SELECT nspname INTO v_dblink_schema FROM pg_namespace n, pg_extension e WHERE e.extname = 'dblink' AND e.extnamespace = n.oid;
SELECT current_setting('search_path') INTO v_old_search_path;
EXECUTE 'SELECT set_config(''search_path'',''@extschema@,'||v_dblink_schema||',public'',''false'')';

SELECT source_table, dest_table, dblink INTO v_src_table, v_dest_table, v_dblink
		FROM @extschema@.refresh_config_logdel WHERE dest_table = p_dest_table;
IF NOT FOUND THEN
		RAISE NOTICE 'This table is not set up for logdel replication: %', v_dest_table;
ELSE
    v_table_name := replace(v_src_table, '.', '_');

    v_drop_function := 'DROP FUNCTION IF EXISTS @extschema@.'||v_table_name||'_mimeo_queue()';
    v_drop_trigger := 'DROP TRIGGER IF EXISTS '||v_table_name||'_mimeo_trig ON '||v_src_table;
    v_drop_q_table := 'DROP TABLE IF EXISTS @extschema@.'||v_table_name||'_pgq';

    RAISE NOTICE 'Removing mimeo objects from source database (trigger, function, queue table)';
    PERFORM dblink_connect('mimeo_logdel_destroy', @extschema@.auth(v_dblink));
    PERFORM dblink_exec('mimeo_logdel_destroy', v_drop_trigger);
    PERFORM dblink_exec('mimeo_logdel_destroy', v_drop_function);
    PERFORM dblink_exec('mimeo_logdel_destroy', v_drop_q_table);
    PERFORM dblink_disconnect('mimeo_logdel_destroy');

    IF p_keep_table THEN 
        RAISE NOTICE 'Destination table NOT destroyed: %', v_dest_table; 
    ELSE
        RAISE NOTICE 'Destination table destroyed: %', v_dest_table;
        EXECUTE 'DROP TABLE IF EXISTS ' || v_dest_table;
    END IF;

    RAISE NOTICE 'Removing config data';
    EXECUTE 'DELETE FROM @extschema@.refresh_config_logdel WHERE dest_table = ' || quote_literal(v_dest_table);	

    RAISE NOTICE 'Done';
END IF;

EXECUTE 'SELECT set_config(''search_path'','''||v_old_search_path||''',''false'')';

EXCEPTION
    WHEN OTHERS THEN
        EXECUTE 'SELECT set_config(''search_path'',''@extschema@,'||v_dblink_schema||''',''false'')';
        IF dblink_get_connections() @> '{mimeo_logdel_destroy}' THEN
            PERFORM dblink_disconnect('mimeo_logdel_destroy');
        END IF;
        EXECUTE 'SELECT set_config(''search_path'','''||v_old_search_path||''',''false'')';
        RAISE EXCEPTION '%', SQLERRM;    
END
$$;


/*
 *  Snapshot destroyer function. 
 */
CREATE FUNCTION snapshot_destroyer(p_dest_table text, p_keep_table boolean DEFAULT true) RETURNS void
    LANGUAGE plpgsql
    AS $$
DECLARE

v_dest_table        text;
v_exists            int;
v_snap_suffix       text;
v_src_table         text;
v_table_name        text;
v_view_definition   text;

BEGIN

SELECT source_table, dest_table INTO v_src_table, v_dest_table
    FROM @extschema@.refresh_config_snap WHERE dest_table = p_dest_table;
IF NOT FOUND THEN
    RAISE EXCEPTION 'This table is not set up for snapshot replication: %', v_dest_table;
END IF;

-- Keep one of the snap tables as a real table with the original view name
IF p_keep_table THEN
    SELECT definition INTO v_view_definition FROM pg_views WHERE schemaname || '.' || viewname = v_dest_table;
    v_exists := strpos(v_view_definition, 'snap1');
    IF v_exists > 0 THEN
        v_snap_suffix := '_snap1';
    ELSE
        v_snap_suffix := '_snap2';
    END IF;
    
    IF position('.' in p_dest_table) > 0 THEN 
        v_table_name := substring(p_dest_table from position('.' in p_dest_table)+1);
    ELSE
        v_table_name := p_dest_table;
    END IF;

    EXECUTE 'DROP VIEW ' || v_dest_table;
    EXECUTE 'ALTER TABLE '||v_dest_table||v_snap_suffix||' RENAME TO '||v_table_name;
    
    RAISE NOTICE 'Destination table NOT destroyed: %. Changed from a view into a plain table', v_dest_table; 
ELSE
    EXECUTE 'DROP VIEW ' || v_dest_table;    
    RAISE NOTICE 'Destination table destroyed: %', v_dest_table;
END IF;

EXECUTE 'DROP TABLE IF EXISTS ' || v_dest_table || '_snap1';
EXECUTE 'DROP TABLE IF EXISTS ' || v_dest_table || '_snap2';

EXECUTE 'DELETE FROM @extschema@.refresh_config_snap WHERE dest_table = ' || quote_literal(v_dest_table);

END
$$;


/*
 *  Plain table destroyer function. 
 */
CREATE FUNCTION table_destroyer(p_dest_table text, p_keep_table boolean DEFAULT true) RETURNS void
    LANGUAGE plpgsql
    AS $$
DECLARE

v_dest_table        text;
    
BEGIN

SELECT dest_table INTO v_dest_table
    FROM @extschema@.refresh_config_table WHERE dest_table = p_dest_table;
IF NOT FOUND THEN
    RAISE NOTICE 'This table is not set up for plain table replication: %', p_dest_table;
ELSE
    IF p_keep_table THEN 
        RAISE NOTICE 'Destination table NOT destroyed: %', v_dest_table; 
    ELSE
        RAISE NOTICE 'Destination table destroyed: %', v_dest_table;
        EXECUTE 'DROP TABLE IF EXISTS ' || v_dest_table;
    END IF;

    EXECUTE 'DELETE FROM @extschema@.refresh_config_table WHERE dest_table = ' || quote_literal(v_dest_table);	
END IF;

END
$$;


/*
 *  Updater destroyer function. 
 */
CREATE FUNCTION updater_destroyer(p_dest_table text, p_keep_table boolean DEFAULT true) RETURNS void
    LANGUAGE plpgsql
    AS $$
DECLARE

v_dest_table        text;
    
BEGIN

SELECT dest_table INTO v_dest_table
    FROM @extschema@.refresh_config_updater WHERE dest_table = p_dest_table;
IF NOT FOUND THEN
    RAISE NOTICE 'This table is not set up for updater replication: %', v_dest_table;
ELSE
    IF p_keep_table THEN 
        RAISE NOTICE 'Destination table NOT destroyed: %', v_dest_table; 
    ELSE
        RAISE NOTICE 'Destination table destroyed: %', v_dest_table;
        EXECUTE 'DROP TABLE IF EXISTS ' || v_dest_table;
    END IF;

    EXECUTE 'DELETE FROM @extschema@.refresh_config_updater WHERE dest_table = ' || quote_literal(v_dest_table);
END IF;

END
$$;

-- Restore original privileges to objects that were dropped
SELECT @extschema@.replay_preserved_privs();
DROP FUNCTION @extschema@.replay_preserved_privs();
DROP TABLE mimeo_preserve_privs_temp;
