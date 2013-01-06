-- Reaching the exact batch limit to cause a pg_jobmon log warning for dml/logdel replication would almost never actually occur due to getting DISTINCT entries from the source queue table. This would make notification of replication possibly falling behind not occur. Changed the alert to happen if the actual batch reaches 75% of the batch limit.

/*
 *  Refresh based on DML (Insert, Update, Delete)
 */
CREATE OR REPLACE FUNCTION refresh_dml(p_destination text, p_limit int default NULL, p_repull boolean DEFAULT false, p_debug boolean DEFAULT false) RETURNS void
    LANGUAGE plpgsql SECURITY DEFINER
    AS $$
DECLARE

v_adv_lock              boolean;
v_batch_limit_reached   boolean := false;
v_cols_n_types          text;
v_cols                  text;
v_condition             text;
v_control               text;
v_create_f_sql          text;
v_create_q_sql          text;
v_dblink                int;
v_dblink_name           text;
v_dblink_schema         text;
v_delete_sql            text;
v_dest_table            text;
v_exec_status           text;
v_field                 text;
v_filter                text[];
v_insert_sql            text;
v_job_id                int;
v_jobmon_schema         text;
v_job_name              text;
v_limit                 int; 
v_old_search_path       text;
v_pk_counter            int;
v_pk_name_csv           text := '';
v_pk_name_type_csv      text := '';
v_pk_name               text[];
v_pk_type               text[];
v_pk_where              text;
v_remote_f_sql          text;
v_remote_q_sql          text;
v_rowcount              bigint := 0; 
v_source_table          text;
v_step_id               int;
v_tmp_table             text;
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
EXECUTE 'SELECT set_config(''search_path'',''@extschema@,'||v_jobmon_schema||','||v_dblink_schema||',public'',''false'')';

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
FROM refresh_config_dml 
WHERE dest_table = p_destination INTO 
    v_source_table
    , v_dest_table
    , v_tmp_table
    , v_dblink
    , v_control
    , v_pk_name
    , v_pk_type
    , v_filter
    , v_condition
    , v_limit; 
IF NOT FOUND THEN
   RAISE EXCEPTION 'ERROR: no configuration found for %',v_job_name; 
END IF;

v_job_id := add_job(v_job_name);
PERFORM gdb(p_debug,'Job ID: '||v_job_id::text);

-- Take advisory lock to prevent multiple calls to function overlapping
v_adv_lock := pg_try_advisory_lock(hashtext('refresh_dml'), hashtext(v_job_name));
IF v_adv_lock = 'false' THEN
    v_step_id := add_step(v_job_id,'Obtaining advisory lock for job: '||v_job_name);
    PERFORM gdb(p_debug,'Obtaining advisory lock FAILED for job: '||v_job_name);
    PERFORM update_step(v_step_id, 'WARNING','Found concurrent job. Exiting gracefully');
    PERFORM fail_job(v_job_id, 2);
    EXECUTE 'SELECT set_config(''search_path'','''||v_old_search_path||''',''false'')';
    RETURN;
END IF;

v_step_id := add_step(v_job_id,'Building SQL');

IF v_pk_name IS NULL OR v_pk_type IS NULL THEN
    RAISE EXCEPTION 'ERROR: primary key fields in refresh_config_dml must be defined';
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
            RAISE EXCEPTION 'ERROR: filter list did not contain all columns that compose primary/unique key for %',v_job_name; 
        END IF;
    END LOOP;
    SELECT array_to_string(array_agg(attname),','), array_to_string(array_agg(attname||' '||format_type(atttypid, atttypmod)::text),',') 
        INTO v_cols, v_cols_n_types
        FROM pg_attribute WHERE attrelid = p_destination::regclass AND ARRAY[attname::text] <@ v_filter AND attnum > 0 AND attisdropped is false;
END IF;    

v_limit = COALESCE(p_limit, v_limit, 10000);

v_pk_name_csv := array_to_string(v_pk_name, ',');
v_pk_counter := 1;
WHILE v_pk_counter <= array_length(v_pk_name,1) LOOP
    IF v_pk_counter > 1 THEN
        v_pk_name_type_csv := v_pk_name_type_csv || ', ';
    END IF;
    v_pk_name_type_csv := v_pk_name_type_csv ||v_pk_name[v_pk_counter]||' '||v_pk_type[v_pk_counter];
    v_pk_counter := v_pk_counter + 1;
END LOOP;

v_with_update := 'WITH a AS (SELECT '||v_pk_name_csv||' FROM '|| v_control ||' ORDER BY '||v_pk_name_csv||' LIMIT '|| v_limit ||') UPDATE '||v_control||' b SET processed = true FROM a WHERE a.'||v_pk_name[1]||' = b.'||v_pk_name[1];

v_pk_counter := 2;
IF array_length(v_pk_name, 1) > 1 THEN
    v_pk_where := '';
    WHILE v_pk_counter <= array_length(v_pk_name,1) LOOP
        v_pk_where := v_pk_where || ' AND a.'||v_pk_name[v_pk_counter]||' = b.'||v_pk_name[v_pk_counter];
        v_pk_counter := v_pk_counter + 1;
    END LOOP;
END IF;

IF v_pk_where IS NOT NULL THEN
    v_with_update := v_with_update || v_pk_where;
END IF;
PERFORM gdb(p_debug, v_with_update);

v_trigger_update := 'SELECT dblink_exec('||quote_literal(v_dblink_name)||','|| quote_literal(v_with_update)||')';

v_remote_q_sql := 'SELECT DISTINCT '||v_pk_name_csv||' FROM '||v_control||' WHERE processed = true';

IF p_repull THEN
    v_remote_f_sql := 'SELECT '||v_cols||' FROM '||v_source_table;
    IF v_condition IS NOT NULL THEN
        v_remote_f_sql := v_remote_f_sql || ' ' || v_condition;
    END IF;
    v_truncate_remote_q := 'SELECT dblink_exec('||quote_literal(v_dblink_name)||','||quote_literal('TRUNCATE TABLE '||v_control)||')';
ELSE
    v_remote_f_sql := 'SELECT '||v_cols||' FROM '||v_source_table||' JOIN ('||v_remote_q_sql||') x USING ('||v_pk_name_csv||')';
    IF v_condition IS NOT NULL THEN
        v_remote_f_sql := v_remote_f_sql || ' ' || v_condition;
    END IF;

    v_create_q_sql := 'CREATE TEMP TABLE '||v_tmp_table||'_queue AS SELECT '||v_pk_name_csv||'
        FROM dblink('||quote_literal(v_dblink_name)||','||quote_literal(v_remote_q_sql)||') t ('||v_pk_name_type_csv||')';

    v_delete_sql := 'DELETE FROM '||v_dest_table||' a USING '||v_tmp_table||'_queue b WHERE a.'||v_pk_name[1]||'= b.'||v_pk_name[1];
    IF array_length(v_pk_name, 1) > 1 THEN
        v_delete_sql := v_delete_sql || v_pk_where;
    END IF; 

END IF;

v_create_f_sql := 'CREATE TEMP TABLE '||v_tmp_table||'_full AS SELECT '||v_cols||' 
        FROM dblink('||quote_literal(v_dblink_name)||','||quote_literal(v_remote_f_sql)||') t ('||v_cols_n_types||')';

v_insert_sql := 'INSERT INTO '||v_dest_table||'('||v_cols||') SELECT '||v_cols||' FROM '||v_tmp_table||'_full'; 

v_trigger_delete := 'SELECT dblink_exec('||quote_literal(v_dblink_name)||','||quote_literal('DELETE FROM '||v_control||' WHERE processed = true')||')'; 

PERFORM update_step(v_step_id, 'OK','Done');

PERFORM dblink_connect(v_dblink_name, auth(v_dblink));

-- update remote entries
v_step_id := add_step(v_job_id,'Updating remote trigger table');
PERFORM gdb(p_debug,v_trigger_update);
EXECUTE v_trigger_update INTO v_exec_status;    
PERFORM update_step(v_step_id, 'OK','Result was '||v_exec_status);

-- create temp tables 
v_step_id := add_step(v_job_id,'Creating full temp table');
IF p_repull THEN
    -- Do truncate of remote queue table here before full data pull is actually started to ensure all new changes are recorded
    PERFORM update_step(v_step_id, 'OK','Request to repull ALL data from source. This could take a while...');
    PERFORM gdb(p_debug, 'Request to repull ALL data from source. This could take a while...');
    EXECUTE v_truncate_remote_q;
END IF;
-- Full table with all insert/update data    
PERFORM gdb(p_debug,v_create_f_sql);
EXECUTE v_create_f_sql;
GET DIAGNOSTICS v_rowcount = ROW_COUNT;
PERFORM gdb(p_debug,'Temp full table row count '||v_rowcount);
IF v_rowcount < 1 THEN 
    PERFORM update_step(v_step_id, 'OK','No new rows found');
ELSE 
    -- remove records from local table 
    IF p_repull THEN
        PERFORM update_step(v_step_id, 'OK','Number of rows to process: '||v_rowcount);
        v_step_id := add_step(v_job_id,'Truncating local table');
        PERFORM gdb(p_debug,'Truncating local table');
        EXECUTE 'TRUNCATE '||v_dest_table;
        PERFORM update_step(v_step_id, 'OK','Done');
    ELSE
        IF v_rowcount <= (v_limit * .75) THEN
            PERFORM update_step(v_step_id, 'OK','Number of rows to process: '||v_rowcount);
        ELSE
            PERFORM update_step(v_step_id, 'WARNING','Row count fetched greater than 75% of batch limit: '||v_limit||'. Recommend increasing batch limit if possible.');
            v_batch_limit_reached := true;
        END IF;
        EXECUTE v_create_q_sql;
        GET DIAGNOSTICS v_rowcount = ROW_COUNT;
        PERFORM gdb(p_debug,'Temp queue table row count '||v_rowcount::text);
        v_step_id := add_step(v_job_id,'Deleting records from local table');
        PERFORM gdb(p_debug,v_delete_sql);
        EXECUTE v_delete_sql; 
        GET DIAGNOSTICS v_rowcount = ROW_COUNT;
        PERFORM gdb(p_debug,'Rows removed from local table before applying changes: '||v_rowcount::text);
        PERFORM update_step(v_step_id, 'OK','Removed '||v_rowcount||' records');
    END IF;

    -- insert records to local table
    v_step_id := add_step(v_job_id,'Inserting new records into local table');
    PERFORM gdb(p_debug,v_insert_sql);
    EXECUTE v_insert_sql;
    GET DIAGNOSTICS v_rowcount = ROW_COUNT;
    PERFORM gdb(p_debug,'Rows inserted: '||v_rowcount::text);
    PERFORM update_step(v_step_id, 'OK','Inserted '||v_rowcount||' records');

    -- clean out rows from txn table
    v_step_id := add_step(v_job_id,'Cleaning out rows from txn table');
    PERFORM gdb(p_debug,v_trigger_delete);
    EXECUTE v_trigger_delete INTO v_exec_status;
    PERFORM update_step(v_step_id, 'OK','Result was '||v_exec_status);
END IF;

-- update activity status
v_step_id := add_step(v_job_id,'Updating last_run in config table');
UPDATE refresh_config_dml SET last_run = CURRENT_TIMESTAMP WHERE dest_table = p_destination; 
PERFORM update_step(v_step_id, 'OK','Last run was '||CURRENT_TIMESTAMP);

EXECUTE 'DROP TABLE IF EXISTS '||v_tmp_table||'_queue';
EXECUTE 'DROP TABLE IF EXISTS '||v_tmp_table||'_full';

PERFORM dblink_disconnect(v_dblink_name);

IF v_batch_limit_reached = false THEN
    PERFORM close_job(v_job_id);
ELSE
    -- Set final job status to level 2 (WARNING) to bring notice that the batch limit was reached and may need adjusting.
    -- Preventive warning to keep replication from falling behind.
    PERFORM fail_job(v_job_id, 2);
END IF;

-- Ensure old search path is reset for the current session
EXECUTE 'SELECT set_config(''search_path'','''||v_old_search_path||''',''false'')';

PERFORM pg_advisory_unlock(hashtext('refresh_dml'), hashtext(v_job_name));

EXCEPTION
    WHEN QUERY_CANCELED THEN
        EXECUTE 'SELECT set_config(''search_path'',''@extschema@,'||v_jobmon_schema||','||v_dblink_schema||''',''false'')';
        IF dblink_get_connections() @> ARRAY[v_dblink_name] THEN
            PERFORM dblink_disconnect(v_dblink_name);
        END IF;
        EXECUTE 'SELECT set_config(''search_path'','''||v_old_search_path||''',''false'')';
        PERFORM pg_advisory_unlock(hashtext('refresh_dml'), hashtext(v_job_name));
        RAISE EXCEPTION '%', SQLERRM;  
    WHEN OTHERS THEN
        EXECUTE 'SELECT set_config(''search_path'',''@extschema@,'||v_jobmon_schema||','||v_dblink_schema||''',''false'')';
        IF v_job_id IS NULL THEN
                v_job_id := add_job('Refresh DML: '||p_destination);
                v_step_id := add_step(v_job_id, 'EXCEPTION before job logging started');
        END IF;
        IF v_step_id IS NULL THEN
            v_step_id := add_step(v_job_id, 'EXCEPTION before first step logged');
        END IF;
        IF dblink_get_connections() @> ARRAY[v_dblink_name] THEN
            PERFORM dblink_disconnect(v_dblink_name);
        END IF;
        PERFORM update_step(v_step_id, 'CRITICAL', 'ERROR: '||coalesce(SQLERRM,'unknown'));
        PERFORM fail_job(v_job_id);

        EXECUTE 'SELECT set_config(''search_path'','''||v_old_search_path||''',''false'')';

        PERFORM pg_advisory_unlock(hashtext('refresh_dml'), hashtext(v_job_name));
        RAISE EXCEPTION '%', SQLERRM;
END
$$;


/*
 *  Refresh based on DML (Insert, Update, Delete), but logs all deletes on the destination table
 *  Destination table requires extra column: mimeo_source_deleted timestamptz
 */
CREATE OR REPLACE FUNCTION refresh_logdel(p_destination text, p_limit int default NULL, p_debug boolean DEFAULT false) RETURNS void
    LANGUAGE plpgsql SECURITY DEFINER
    AS $$
DECLARE

v_adv_lock              boolean;
v_batch_limit_reached   boolean := false;
v_cols_n_types          text;
v_cols                  text;
v_condition             text;
v_control               text;
v_create_d_sql          text;
v_create_f_sql          text;
v_create_q_sql          text;
v_dblink                int;
v_dblink_name           text;
v_dblink_schema         text;
v_delete_d_sql          text;
v_delete_f_sql          text;
v_dest_table            text;
v_exec_status           text;
v_field                 text;
v_filter                text[];
v_full_rowcount         bigint := 0;
v_insert_deleted_sql    text;
v_insert_sql            text;
v_job_id                int;
v_jobmon_schema         text;
v_job_name              text;
v_limit                 int; 
v_old_search_path       text;
v_pk_counter            int;
v_pk_name               text[];
v_pk_name_csv           text;
v_pk_name_type_csv      text := '';
v_pk_type               text[];
v_pk_where              text;
v_remote_d_sql          text;
v_remote_f_sql          text;
v_remote_q_sql          text;
v_rowcount              bigint := 0; 
v_source_table          text;
v_step_id               int;
v_tmp_table             text;
v_trigger_delete        text; 
v_trigger_update        text;
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
EXECUTE 'SELECT set_config(''search_path'',''@extschema@,'||v_jobmon_schema||','||v_dblink_schema||',public'',''false'')';

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
FROM refresh_config_logdel 
WHERE dest_table = p_destination INTO 
    v_source_table
    , v_dest_table
    , v_tmp_table
    , v_dblink
    , v_control
    , v_pk_name
    , v_pk_type
    , v_filter
    , v_condition
    , v_limit; 
IF NOT FOUND THEN
   RAISE EXCEPTION 'ERROR: no configuration found for %',v_job_name; 
END IF;

v_job_id := add_job(v_job_name);
PERFORM gdb(p_debug,'Job ID: '||v_job_id::text);

-- Take advisory lock to prevent multiple calls to function overlapping
v_adv_lock := pg_try_advisory_lock(hashtext('refresh_logdel'), hashtext(v_job_name));
IF v_adv_lock = 'false' THEN
    v_step_id := add_step(v_job_id,'Obtaining advisory lock for job: '||v_job_name);
    PERFORM gdb(p_debug,'Obtaining advisory lock FAILED for job: '||v_job_name);
    PERFORM update_step(v_step_id, 'WARNING','Found concurrent job. Exiting gracefully');
    PERFORM fail_job(v_job_id, 2);
    EXECUTE 'SELECT set_config(''search_path'','''||v_old_search_path||''',''false'')';
    RETURN;
END IF;

v_step_id := add_step(v_job_id,'Grabbing Boundries, Building SQL');

IF v_pk_name IS NULL OR v_pk_type IS NULL THEN
    RAISE EXCEPTION 'ERROR: primary key fields in refresh_config_logdel must be defined';
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
            RAISE EXCEPTION 'ERROR: filter list did not contain all columns that compose primary key for %',v_job_name; 
        END IF;
    END LOOP;
    SELECT array_to_string(array_agg(attname),','), array_to_string(array_agg(attname||' '||format_type(atttypid, atttypmod)::text),',') 
        INTO v_cols, v_cols_n_types
        FROM pg_attribute WHERE attrelid = p_destination::regclass AND ARRAY[attname::text] <@ v_filter AND attnum > 0 AND attisdropped is false AND attname != 'mimeo_source_deleted' ;
END IF;    

-- init sql statements 

v_limit = COALESCE(p_limit, v_limit, 10000);

v_pk_name_csv := array_to_string(v_pk_name,',');

v_with_update := 'WITH a AS (SELECT '||v_pk_name_csv||' FROM '|| v_control ||' ORDER BY '||v_pk_name_csv||' LIMIT '|| v_limit ||') UPDATE '||v_control||' b SET processed = true FROM a WHERE a.'||v_pk_name[1]||' = b.'||v_pk_name[1];

v_pk_counter := 2;
IF array_length(v_pk_name, 1) > 1 THEN
    v_pk_where := '';
    WHILE v_pk_counter <= array_length(v_pk_name,1) LOOP
        v_pk_where := v_pk_where || ' AND a.'||v_pk_name[v_pk_counter]||' = b.'||v_pk_name[v_pk_counter];
        v_pk_counter := v_pk_counter + 1;
    END LOOP;
END IF;

IF v_pk_where IS NOT NULL THEN
    v_with_update := v_with_update || v_pk_where;
END IF;
PERFORM gdb(p_debug, v_with_update);

v_pk_counter := 1;
WHILE v_pk_counter <= array_length(v_pk_name,1) LOOP
    IF v_pk_counter > 1 THEN
        v_pk_name_type_csv := v_pk_name_type_csv || ', ';
    END IF;
    v_pk_name_type_csv := v_pk_name_type_csv ||v_pk_name[v_pk_counter]||' '||v_pk_type[v_pk_counter];
    v_pk_counter := v_pk_counter + 1;
END LOOP;

v_trigger_update := 'SELECT dblink_exec('||quote_literal(v_dblink_name)||','|| quote_literal(v_with_update)||')';

v_remote_q_sql := 'SELECT DISTINCT '||v_pk_name_csv||' FROM '||v_control||' WHERE processed = true and mimeo_source_deleted IS NULL';
v_create_q_sql := 'CREATE TEMP TABLE '||v_tmp_table||'_queue AS SELECT '||v_pk_name_csv||'
        FROM dblink('||quote_literal(v_dblink_name)||','||quote_literal(v_remote_q_sql)||') t ('||v_pk_name_type_csv||')';

v_remote_f_sql := 'SELECT '||v_cols||' FROM '||v_source_table||' JOIN ('||v_remote_q_sql||') x USING ('||v_pk_name_csv||')';
IF v_condition IS NOT NULL THEN
    v_remote_f_sql := v_remote_f_sql || ' ' || v_condition;
END IF;
v_create_f_sql := 'CREATE TEMP TABLE '||v_tmp_table||'_full AS SELECT '||v_cols||' 
    FROM dblink('||quote_literal(v_dblink_name)||','||quote_literal(v_remote_f_sql)||') t ('||v_cols_n_types||')';

v_remote_d_sql = 'SELECT '||v_cols||', mimeo_source_deleted FROM '||v_control||' WHERE processed = true and mimeo_source_deleted IS NOT NULL';
v_create_d_sql = 'CREATE TEMP TABLE '||v_tmp_table||'_deleted AS SELECT '||v_cols||', mimeo_source_deleted
    FROM dblink('||quote_literal(v_dblink_name)||','||quote_literal(v_remote_d_sql)||') t ('||v_cols_n_types||', mimeo_source_deleted timestamptz)';

v_delete_f_sql := 'DELETE FROM '||v_dest_table||' a USING '||v_tmp_table||'_queue b WHERE a.'||v_pk_name[1]||'= b.'||v_pk_name[1];
IF array_length(v_pk_name, 1) > 1 THEN
    v_delete_f_sql := v_delete_f_sql || v_pk_where;
END IF; 

-- remove rows that were deleted on source to ensure most recently deleted data is logged 
v_delete_d_sql := 'DELETE FROM '||v_dest_table||' a USING '||v_tmp_table||'_deleted b WHERE a.'||v_pk_name[1]||'= b.'||v_pk_name[1];
IF array_length(v_pk_name, 1) > 1 THEN
    v_delete_d_sql := v_delete_d_sql || v_pk_where;
END IF; 

v_insert_sql := 'INSERT INTO '||v_dest_table||'('||v_cols||') SELECT '||v_cols||' FROM '||v_tmp_table||'_full';
v_insert_deleted_sql := 'INSERT INTO '||v_dest_table||'('||v_cols||', mimeo_source_deleted) SELECT '||v_cols||', mimeo_source_deleted FROM '||v_tmp_table||'_deleted'; 

v_trigger_delete := 'SELECT dblink_exec('||quote_literal(v_dblink_name)||','||quote_literal('DELETE FROM '||v_control||' WHERE processed = true')||')'; 

PERFORM update_step(v_step_id, 'OK','Remote table is '||v_source_table);

PERFORM dblink_connect(v_dblink_name, auth(v_dblink));

-- update remote entries
v_step_id := add_step(v_job_id,'Updating remote trigger table');
PERFORM gdb(p_debug,v_trigger_update);
EXECUTE v_trigger_update INTO v_exec_status;    
PERFORM update_step(v_step_id, 'OK','Result was '||v_exec_status);

-- create temp table for insertion (inserts/updates)
v_step_id := add_step(v_job_id,'Create temp table from remote full table');
PERFORM gdb(p_debug,v_create_f_sql);
EXECUTE v_create_f_sql;  
GET DIAGNOSTICS v_full_rowcount = ROW_COUNT;
PERFORM gdb(p_debug,'Number of Insert/Update rows to process: '||v_full_rowcount);
PERFORM update_step(v_step_id, 'OK','Number of Insert/Update rows to process: '||v_full_rowcount);

-- create temp table for insertion (deleted rows)
v_step_id := add_step(v_job_id,'Create temp table from remote delete table');
PERFORM gdb(p_debug,v_create_d_sql);
EXECUTE v_create_d_sql;  
GET DIAGNOSTICS v_rowcount = ROW_COUNT;
PERFORM gdb(p_debug,'Number of Deleted rows to process '||v_rowcount);
PERFORM update_step(v_step_id, 'OK','Number of Deleted rows to process '||v_rowcount);
-- Check is here instead of earlier in case there are only deletes
v_step_id := add_step(v_job_id, 'Processing fetched rows');
IF v_rowcount < 1 AND v_full_rowcount < 1 THEN 
    PERFORM update_step(v_step_id, 'OK','No new rows found');
ELSE
    IF (v_full_rowcount + v_rowcount) <= (v_limit * .75) THEN
        PERFORM update_step(v_step_id, 'OK','Total number of rows to processes: '||v_full_rowcount + v_rowcount);
    ELSE
        PERFORM update_step(v_step_id, 'WARNING','Row count fetched greater than 75% of batch limit: '||v_limit||'. Recommend increasing batch limit if possible.');
        v_batch_limit_reached := true;
    END IF;
    -- remove records from local table (inserts/updates)
    v_step_id := add_step(v_job_id,'Deleting insert/update records from local table');
    EXECUTE v_create_q_sql;
    GET DIAGNOSTICS v_rowcount = ROW_COUNT;
    PERFORM gdb(p_debug,'Temp queue table row count '||v_rowcount::text);
    PERFORM gdb(p_debug,v_delete_f_sql);
    EXECUTE v_delete_f_sql; 
    GET DIAGNOSTICS v_rowcount = ROW_COUNT;
    PERFORM gdb(p_debug,'Insert/Update rows removed from local table before applying changes: '||v_rowcount::text);
    PERFORM update_step(v_step_id, 'OK','Removed '||v_rowcount||' records');

    -- remove records from local table (deleted rows)
    v_step_id := add_step(v_job_id,'Deleting removed records from local table');
    PERFORM gdb(p_debug,v_delete_d_sql);
    EXECUTE v_delete_d_sql; 
    GET DIAGNOSTICS v_rowcount = ROW_COUNT;
    PERFORM gdb(p_debug,'Deleted Rows removed from local table before applying changes: '||v_rowcount::text);
    PERFORM update_step(v_step_id, 'OK','Removed '||v_rowcount||' records');

    -- insert records to local table (inserts/updates)
    v_step_id := add_step(v_job_id,'Inserting new/updated records into local table');
    PERFORM gdb(p_debug,v_insert_sql);
    EXECUTE v_insert_sql;
    GET DIAGNOSTICS v_rowcount = ROW_COUNT;
    PERFORM gdb(p_debug,'Rows inserted: '||v_rowcount::text);
    PERFORM update_step(v_step_id, 'OK','Inserted '||v_rowcount||' records');

    -- insert records to local table (deleted rows to be kept)
    v_step_id := add_step(v_job_id,'Inserting deleted records into local table');
    PERFORM gdb(p_debug,v_insert_deleted_sql);
    EXECUTE v_insert_deleted_sql;
    GET DIAGNOSTICS v_rowcount = ROW_COUNT;
    PERFORM gdb(p_debug,'Rows inserted: '||v_rowcount::text);
    PERFORM update_step(v_step_id, 'OK','Inserted '||v_rowcount||' records');

    -- clean out rows from txn table
    v_step_id := add_step(v_job_id,'Cleaning out rows from txn table');
    PERFORM gdb(p_debug,v_trigger_delete);
    EXECUTE v_trigger_delete INTO v_exec_status;
    PERFORM update_step(v_step_id, 'OK','Result was '||v_exec_status);
END IF;

-- update activity status
v_step_id := add_step(v_job_id,'Updating last_run in config table');
UPDATE refresh_config_logdel SET last_run = CURRENT_TIMESTAMP WHERE dest_table = p_destination; 
PERFORM update_step(v_step_id, 'OK','Last Value was '||current_timestamp);

PERFORM dblink_disconnect(v_dblink_name);

EXECUTE 'DROP TABLE IF EXISTS '||v_tmp_table||'_full';
EXECUTE 'DROP TABLE IF EXISTS '||v_tmp_table||'_deleted';

IF v_batch_limit_reached = false THEN
    PERFORM close_job(v_job_id);
ELSE
    -- Set final job status to level 2 (WARNING) to bring notice that the batch limit was reached and may need adjusting.
    -- Preventive warning to keep replication from falling behind.
    PERFORM fail_job(v_job_id, 2);
END IF;
-- Ensure old search path is reset for the current session
EXECUTE 'SELECT set_config(''search_path'','''||v_old_search_path||''',''false'')';

PERFORM pg_advisory_unlock(hashtext('refresh_logdel'), hashtext(v_job_name));

EXCEPTION
    WHEN QUERY_CANCELED THEN
        EXECUTE 'SELECT set_config(''search_path'',''@extschema@,'||v_jobmon_schema||','||v_dblink_schema||''',''false'')';
        IF dblink_get_connections() @> ARRAY[v_dblink_name] THEN
            PERFORM dblink_disconnect(v_dblink_name);
        END IF;
        EXECUTE 'SELECT set_config(''search_path'','''||v_old_search_path||''',''false'')';
        PERFORM pg_advisory_unlock(hashtext('refresh_logdel'), hashtext(v_job_name));
        RAISE EXCEPTION '%', SQLERRM;  
    WHEN OTHERS THEN
        EXECUTE 'SELECT set_config(''search_path'',''@extschema@,'||v_jobmon_schema||','||v_dblink_schema||''',''false'')';
        IF v_job_id IS NULL THEN
                v_job_id := add_job('Refresh Log Del: '||p_destination);
                v_step_id := add_step(v_job_id, 'EXCEPTION before job logging started');
        END IF;
        IF v_step_id IS NULL THEN
            v_step_id := add_step(v_job_id, 'EXCEPTION before first step logged');
        END IF;
        IF dblink_get_connections() @> ARRAY[v_dblink_name] THEN
            PERFORM dblink_disconnect(v_dblink_name);
        END IF;
        PERFORM update_step(v_step_id, 'BAD', 'ERROR: '||coalesce(SQLERRM,'unknown'));
        PERFORM fail_job(v_job_id);

        EXECUTE 'SELECT set_config(''search_path'','''||v_old_search_path||''',''false'')';

        PERFORM pg_advisory_unlock(hashtext('refresh_logdel'), hashtext(v_job_name));
        RAISE EXCEPTION '%', SQLERRM;
END
$$;
