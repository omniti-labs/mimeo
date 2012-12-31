/*
 *  Refresh insert only table based on timestamp control field
 */
CREATE FUNCTION refresh_inserter(p_destination text, p_limit integer DEFAULT NULL, p_repull boolean DEFAULT false, p_repull_start text DEFAULT NULL, p_repull_end text DEFAULT NULL, p_debug boolean DEFAULT false) RETURNS void
    LANGUAGE plpgsql SECURITY DEFINER
    AS $$
DECLARE

v_adv_lock              boolean;
v_batch_limit_reached   boolean := false; 
v_boundary              timestamptz;
v_cols_n_types          text;
v_cols                  text;
v_condition             text;
v_control               text;
v_create_sql            text;
v_dblink_schema         text;
v_dblink                int;
v_delete_sql            text;
v_dest_table            text;
v_dst_active            boolean;
v_dst_check             boolean;
v_dst_start             int;
v_dst_end               int;
v_filter                text[]; 
v_full_refresh          boolean := false;
v_insert_sql            text;
v_job_id                int;
v_jobmon_schema         text;
v_job_name              text;
v_last_value_sql        text;
v_last_value            timestamptz;
v_limit                 int;
v_now                   timestamptz := now();
v_old_search_path       text;
v_remote_sql            text;
v_rowcount              bigint; 
v_source_table          text;
v_step_id               int;
v_tmp_table             text;

BEGIN

IF p_debug IS DISTINCT FROM true THEN
    PERFORM set_config( 'client_min_messages', 'warning', true );
END IF;

v_job_name := 'Refresh Inserter: '||p_destination;

SELECT nspname INTO v_dblink_schema FROM pg_namespace n, pg_extension e WHERE e.extname = 'dblink' AND e.extnamespace = n.oid;
SELECT nspname INTO v_jobmon_schema FROM pg_namespace n, pg_extension e WHERE e.extname = 'pg_jobmon' AND e.extnamespace = n.oid;

-- Set custom search path to allow easier calls to other functions, especially job logging
SELECT current_setting('search_path') INTO v_old_search_path;
EXECUTE 'SELECT set_config(''search_path'',''@extschema@,'||v_jobmon_schema||','||v_dblink_schema||',public'',''false'')';

v_job_id := add_job(v_job_name);
PERFORM gdb(p_debug,'Job ID: '||v_job_id::text);

-- Take advisory lock to prevent multiple calls to function overlapping
v_adv_lock := pg_try_advisory_lock(hashtext('refresh_inserter'), hashtext(v_job_name));
IF v_adv_lock = 'false' THEN
    v_step_id := add_step(v_job_id,'Obtaining advisory lock for job: '||v_job_name);
    PERFORM gdb(p_debug,'Obtaining advisory lock FAILED for job: '||v_job_name);
    PERFORM update_step(v_step_id, 'WARNING','Found concurrent job. Exiting gracefully');
    PERFORM fail_job(v_job_id, 2);
    EXECUTE 'SELECT set_config(''search_path'','''||v_old_search_path||''',''false'')';
    RETURN;
END IF;

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
FROM refresh_config_inserter WHERE dest_table = p_destination 
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
    , v_limit; 
IF NOT FOUND THEN
   RAISE EXCEPTION 'ERROR: no configuration found for %',v_job_name; 
END IF;  

-- Do not allow this function to run during DST time change if config option is true. Otherwise will miss data from source
IF v_dst_active THEN
    v_dst_check := @extschema@.dst_change(v_now);
    IF v_dst_check THEN 
        IF to_number(to_char(v_now, 'HH24MM'), '0000') > v_dst_start AND to_number(to_char(v_now, 'HH24MM'), '0000') < v_dst_end THEN
            v_step_id := add_step( v_job_id, 'DST Check');
            PERFORM update_step(v_step_id, 'OK', 'Job CANCELLED - Does not run during DST time change');
            PERFORM close_job(v_job_id);
            PERFORM gdb(p_debug, 'Cannot run during DST time change');
            UPDATE refresh_config_inserter SET last_run = CURRENT_TIMESTAMP WHERE dest_table = p_destination;  
            EXECUTE 'SELECT set_config(''search_path'','''||v_old_search_path||''',''false'')';
            PERFORM pg_advisory_unlock(hashtext('refresh_inserter'), hashtext(v_job_name));
            RETURN;
        END IF;
    END IF;
END IF;

v_step_id := add_step(v_job_id,'Building SQL');

IF v_filter IS NULL THEN 
    SELECT array_to_string(array_agg(attname),','), array_to_string(array_agg(attname||' '||format_type(atttypid, atttypmod)::text),',') 
        INTO v_cols, v_cols_n_types
        FROM pg_attribute WHERE attrelid = p_destination::regclass AND attnum > 0 AND attisdropped is false;
ELSE
    SELECT array_to_string(array_agg(attname),','), array_to_string(array_agg(attname||' '||format_type(atttypid, atttypmod)::text),',') 
        INTO v_cols, v_cols_n_types
        FROM pg_attribute WHERE attrelid = p_destination::regclass AND ARRAY[attname::text] <@ v_filter AND attnum > 0 AND attisdropped is false;
END IF;

v_last_value_sql := 'SELECT max('||v_control||') FROM '||v_tmp_table;
v_limit = COALESCE(p_limit, v_limit, 10000);

-- Repull old data instead of normal new data pull
IF p_repull THEN
    -- Repull ALL data if no start and end values set
    IF p_repull_start IS NULL AND p_repull_end IS NULL THEN
        -- Actual truncate is done after pull to temp table to minimize lock on dest_table
        PERFORM update_step(v_step_id, 'OK','Request to repull ALL data from source. This could take a while...');
        v_full_refresh := true;
        v_remote_sql := 'SELECT '||v_cols||' FROM '||v_source_table;
        IF v_condition IS NOT NULL THEN
            v_remote_sql := v_remote_sql || ' ' || v_condition;
        END IF;
    ELSE
        PERFORM update_step(v_step_id, 'OK','Request to repull data from '||COALESCE(p_repull_start, '-infinity')||' to '||COALESCE(p_repull_end, 'infinity'));
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
        v_step_id := add_step(v_job_id, 'Deleting current, local data');
        PERFORM gdb(p_debug,'Deleting current, local data: '||v_delete_sql);
        EXECUTE v_delete_sql;
        PERFORM update_step(v_step_id, 'OK','Done');
        -- Set last_value equal to local, real table max instead of temp table (just in case)
        v_last_value_sql := 'SELECT max('||v_control||') FROM '||v_dest_table;
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
    v_remote_sql := v_remote_sql ||v_control||' > '||quote_literal(v_last_value)||' AND '||v_control||' < '||quote_literal(v_boundary)||' ORDER BY '||v_control||' ASC LIMIT '|| v_limit;

    PERFORM update_step(v_step_id, 'OK','Grabbing rows from '||v_last_value::text||' to '||v_boundary::text);
    PERFORM gdb(p_debug,'Grabbing rows from '||v_last_value::text||' to '||v_boundary::text);

END IF;

v_create_sql := 'CREATE TEMP TABLE '||v_tmp_table||' AS SELECT '||v_cols||' FROM dblink(auth('||v_dblink||'),'||quote_literal(v_remote_sql)||') t ('||v_cols_n_types||')';

v_insert_sql := 'INSERT INTO '||v_dest_table||'('||v_cols||') SELECT '||v_cols||' FROM '||v_tmp_table; 

-- create temp from remote
v_step_id := add_step(v_job_id,'Creating temp table ('||v_tmp_table||') from remote table');
    PERFORM gdb(p_debug,v_create_sql);
    EXECUTE v_create_sql; 
    GET DIAGNOSTICS v_rowcount = ROW_COUNT;
    -- Do nothing if no new rows
    IF v_rowcount < 1 THEN 
        PERFORM update_step(v_step_id, 'OK','No new rows found');
        EXECUTE 'DROP TABLE IF EXISTS ' || v_tmp_table;
        UPDATE refresh_config_inserter SET last_run = CURRENT_TIMESTAMP WHERE dest_table = p_destination;  
        PERFORM close_job(v_job_id);
        PERFORM gdb(p_debug, 'No new rows found');
        EXECUTE 'SELECT set_config(''search_path'','''||v_old_search_path||''',''false'')';
        PERFORM pg_advisory_unlock(hashtext('refresh_inserter'), hashtext(v_job_name));
        RETURN;
    -- Not recommended that the batch actually equal the limit set if possible. Handle all edge cases to keep data consistent
    ELSIF (v_rowcount = v_limit) AND (p_repull = false) THEN
        PERFORM update_step(v_step_id, 'WARNING','Row count fetched equal to limit set: '||v_limit||'. Recommend increasing batch limit if possible.');
        PERFORM gdb(p_debug, 'Row count fetched equal to limit set: '||v_limit||'. Recommend increasing batch limit if possible.'); 
        EXECUTE v_last_value_sql INTO v_last_value;
        v_step_id := add_step(v_job_id, 'Removing high boundary rows from this batch to avoid missing data');       
        EXECUTE 'DELETE FROM '||v_tmp_table||' WHERE '||v_control||' = '||quote_literal(v_last_value);
        GET DIAGNOSTICS v_rowcount = ROW_COUNT;
        PERFORM update_step(v_step_id, 'OK', 'Removed '||v_rowcount||' rows. Batch now contains '||v_limit - v_rowcount||' records');
        PERFORM gdb(p_debug, 'Removed '||v_rowcount||' rows from batch. Batch table now contains '||v_limit - v_rowcount||' records');
        v_batch_limit_reached = true;
        IF (v_limit - v_rowcount) < 1 THEN
            v_step_id := add_step(v_job_id, 'Reached inconsistent state');
            PERFORM update_step(v_step_id, 'CRITICAL', 'Batch contained max rows ('||v_limit||') and all contained the same timestamp value. Unable to guarentee rows will ever be replicated consistently. Increase row limit parameter to allow a consistent batch.');
            PERFORM gdb(p_debug, 'Batch contained max rows desired ('||v_limit||') and all contained the same timestamp value. Unable to guarentee rows will be replicated consistently. Increase row limit parameter to allow a consistent batch.');
            UPDATE refresh_config_inserter SET last_run = CURRENT_TIMESTAMP WHERE dest_table = p_destination;  
            PERFORM fail_job(v_job_id);
            EXECUTE 'DROP TABLE IF EXISTS ' || v_tmp_table;
            EXECUTE 'SELECT set_config(''search_path'','''||v_old_search_path||''',''false'')';
            PERFORM pg_advisory_unlock(hashtext('refresh_inserter'), hashtext(v_job_name));
            RETURN;
        END IF;
    ELSE
        PERFORM update_step(v_step_id, 'OK','Batch contains '||v_rowcount||' records');
        PERFORM gdb(p_debug, 'Batch contains '||v_rowcount||' records');
    END IF;

IF v_full_refresh THEN        
        EXECUTE 'TRUNCATE '||v_dest_table;
END IF;

-- insert
v_step_id := add_step(v_job_id,'Inserting new records into local table');
PERFORM gdb(p_debug,v_insert_sql);
EXECUTE v_insert_sql; 
GET DIAGNOSTICS v_rowcount = ROW_COUNT;
PERFORM update_step(v_step_id, 'OK','Inserted '||v_rowcount||' records');
PERFORM gdb(p_debug, v_rowcount || ' rows added to ' || v_dest_table);

-- Get new last_value
v_step_id := add_step(v_job_id, 'Getting local max control field value for next lower boundary');
PERFORM gdb(p_debug, v_last_value_sql);
EXECUTE v_last_value_sql INTO v_last_value;
PERFORM update_step(v_step_id, 'OK','Max value is: '||v_last_value);
PERFORM gdb(p_debug, 'Max value is: '||v_last_value);

-- update boundries
v_step_id := add_step(v_job_id,'Updating last_value in config');
UPDATE refresh_config_inserter set last_value = v_last_value, last_run = CURRENT_TIMESTAMP WHERE dest_table = p_destination;  
PERFORM update_step(v_step_id, 'OK','Done');

EXECUTE 'DROP TABLE IF EXISTS ' || v_tmp_table;

IF v_batch_limit_reached = false THEN
    PERFORM close_job(v_job_id);
ELSE
    -- Set final job status to level 2 (WARNING) to bring notice that the batch limit was reached and may need adjusting.
    -- Preventive warning to keep replication from falling behind.
    PERFORM fail_job(v_job_id, 2);
END IF;

-- Ensure old search path is reset for the current session
EXECUTE 'SELECT set_config(''search_path'','''||v_old_search_path||''',''false'')';

PERFORM pg_advisory_unlock(hashtext('refresh_inserter'), hashtext(v_job_name));

EXCEPTION
    WHEN QUERY_CANCELED THEN
        PERFORM pg_advisory_unlock(hashtext('refresh_inserter'), hashtext(v_job_name));
        RAISE EXCEPTION '%', SQLERRM;    
    WHEN OTHERS THEN
        EXECUTE 'SELECT set_config(''search_path'',''@extschema@,'||v_jobmon_schema||','||v_dblink_schema||''',''false'')';
        IF v_job_id IS NULL THEN
            v_job_id := add_job('Refresh Inserter: '||p_destination);
            v_step_id := add_step(v_job_id, 'EXCEPTION before job logging started');
        END IF;
        IF v_step_id IS NULL THEN
            v_step_id := add_step(v_job_id, 'EXCEPTION before first step logged');
        END IF;
        PERFORM update_step(v_step_id, 'BAD', 'ERROR: '||coalesce(SQLERRM,'unknown'));
        PERFORM fail_job(v_job_id);

        -- Ensure old search path is reset for the current session
        EXECUTE 'SELECT set_config(''search_path'','''||v_old_search_path||''',''false'')';

        PERFORM pg_advisory_unlock(hashtext('refresh_inserter'), hashtext(v_job_name));
        RAISE EXCEPTION '%', SQLERRM;    
END
$$;
