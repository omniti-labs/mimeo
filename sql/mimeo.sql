-- ########## mimeo table definitions ##########
CREATE TABLE mviews (
    mv_name text NOT NULL,
    v_name text NOT NULL,
    last_refresh timestamp with time zone
);
CREATE UNIQUE INDEX mviews_pkey ON mviews USING btree (mv_name);


CREATE TABLE dblink_mapping (
    data_source_id integer NOT NULL,
    data_source text NOT NULL,
    user_name text NOT NULL,
    auth text,
    dbh_attr text
);

CREATE SEQUENCE dblink_mapping_data_source_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;
ALTER SEQUENCE dblink_mapping_data_source_id_seq OWNED BY dblink_mapping.data_source_id;
CREATE UNIQUE INDEX dblink_mapping_pkey ON dblink_mapping USING btree (data_source_id);


CREATE TABLE refresh_config (
    source_table text NOT NULL,
    dest_table text NOT NULL,
    dblink integer REFERENCES dblink_mapping(data_source_id) NOT NULL,
    control_field text,
    last_value timestamp with time zone,
    boundary interval,
    pk_field text,
    pk_type text,
    filter text[],
    condition text  
);
ALTER TABLE ONLY refresh_config
    ADD CONSTRAINT refresh_config_pkey PRIMARY KEY (dest_table);
    

-- ########## mimeo function definitions ##########
/*
 *  auth(integer) RETURNS text
 *
 */
CREATE FUNCTION auth(integer) RETURNS text
    LANGUAGE sql
    AS $$
    select data_source||' user='||user_name||' password='||auth from @extschema@.dblink_mapping where data_source_id = $1; 
$$;


CREATE FUNCTION gdb(in_debug boolean, in_notice text) RETURNS void
    LANGUAGE plpgsql
    AS $$
BEGIN
    IF in_debug THEN 
        RAISE NOTICE '%', in_notice;
    END IF;
END
$$;

/*
 *  refresh_snap(p_destination text, p_debug boolean) RETURNS void
 *
 */
CREATE OR REPLACE FUNCTION refresh_snap(p_destination text, p_debug boolean) RETURNS void
    LANGUAGE plpgsql
    AS $$
declare
v_job_name          text;
v_job_id            int;
v_step_id           int;
v_rowcount          bigint; 

v_source_table      text;
v_dest_table        text;
v_dblink            text;
v_dblink_schema     text;
v_jobmon_schema     text;
--v_dblink_sql        text;
v_cols              text;
v_cols_n_types      text;
v_rcols_array       text[];
v_lcols_array       text[];
v_r                 text;
v_l                 text;
v_match             boolean := 'f';
v_parts             record;
v_table_exists      int;
v_create_sql        text;

v_remote_sql        text;
v_insert_sql        text;
v_local_sql         text;

v_refresh_snap      text;
v_view_definition   text;
v_exists            int;
v_snap              text;

BEGIN

IF p_debug IS DISTINCT FROM true THEN
    PERFORM set_config( 'client_min_messages', 'notice', true );
END IF;

v_job_name := 'Refresh Snap: '||p_destination;
-- Take advisory lock to prevent multiple calls to snapshot the same table causing a deadlock
PERFORM pg_advisory_lock(hashtext('refresh_snap'), hashtext(v_job_name));

SELECT nspname INTO v_dblink_schema FROM pg_namespace n, pg_extension e WHERE e.extname = 'dblink' AND e.extnamespace = n.oid;
SELECT nspname INTO v_jobmon_schema FROM pg_namespace n, pg_extension e WHERE e.extname = 'pg_jobmon' AND e.extnamespace = n.oid;

EXECUTE 'SELECT '||v_jobmon_schema||'.add_job('||quote_literal(v_job_name)||')' INTO v_job_id;
PERFORM @extschema@.gdb(p_debug,'Job ID: '||v_job_id::text);

EXECUTE 'SELECT '||v_jobmon_schema||'.add_step('||v_job_id||',''Grabbing Mapping, Building SQL'')' INTO v_step_id;

SELECT source_table, dest_table, dblink INTO v_source_table, v_dest_table, v_dblink FROM @extschema@.refresh_config
WHERE dest_table = p_destination; 
IF NOT FOUND THEN
   RAISE EXCEPTION 'ERROR: This table is not set up for snapshot replication: %',v_job_name; 
END IF;  

-- checking for current view

SELECT INTO v_view_definition definition FROM pg_views where
      ((schemaname || '.') || viewname)=v_dest_table;

SELECT INTO v_exists strpos(v_view_definition, 'snap1');
  IF v_exists > 0 THEN
    v_snap := '_snap2';
    ELSE
    v_snap := '_snap1';
 END IF;


v_refresh_snap := v_dest_table||v_snap;

PERFORM @extschema@.gdb(p_debug,'v_refresh_snap: '||v_refresh_snap::text);

-- init sql statements 

v_remote_sql := 'SELECT array_to_string(array_agg(attname),'','') as cols, array_to_string(array_agg(attname||'' ''||atttypid::regtype::text),'','') as cols_n_types FROM pg_attribute WHERE attnum > 0 AND attisdropped is false AND attrelid = ' || quote_literal(v_source_table) || '::regclass';
v_remote_sql := 'SELECT cols, cols_n_types FROM '|| v_dblink_schema ||'.dblink(@extschema@.auth(' || v_dblink || '), ' || quote_literal(v_remote_sql) || ') t (cols text, cols_n_types text)';
perform @extschema@.gdb(p_debug,'v_remote_sql: '||v_remote_sql);
EXECUTE v_remote_sql INTO v_cols, v_cols_n_types;  
perform @extschema@.gdb(p_debug,'v_cols: '||v_cols);
perform @extschema@.gdb(p_debug,'v_cols_n_types: '||v_cols_n_types);

v_remote_sql := 'SELECT '||v_cols||' FROM '||v_source_table;
v_insert_sql := 'INSERT INTO ' || v_refresh_snap || ' SELECT '||v_cols||' FROM '|| v_dblink_schema ||'.dblink(@extschema@.auth('||v_dblink||'),'||quote_literal(v_remote_sql)||') t ('||v_cols_n_types||')';

EXECUTE 'SELECT '||v_jobmon_schema||'.update_step('||v_job_id||', '||v_step_id||', ''OK'',''Grabbing rows from source table'')';

EXECUTE 'SELECT '||v_jobmon_schema||'.add_step('||v_job_id||',''Truncate non-active snap table'')' INTO v_step_id;

-- Create snap table if it doesn't exist
SELECT string_to_array(v_refresh_snap, '.') AS oparts INTO v_parts;
SELECT INTO v_table_exists count(1) FROM pg_tables
    WHERE  schemaname = v_parts.oparts[1] AND
           tablename = v_parts.oparts[2];
IF v_table_exists = 0 THEN

    perform @extschema@.gdb(p_debug,'Snap table does not exist. Creating... ');
    
    v_create_sql := 'CREATE TABLE ' || v_refresh_snap || ' (' || v_cols_n_types || ')';
    perform @extschema@.gdb(p_debug,'v_create_sql: '||v_create_sql::text);
    EXECUTE v_create_sql;
ELSE 

/* Check local column definitions against remote and recreate table if different. allows automatic recreation of
        snap tables if columns change (add, drop type change)  */  
    v_local_sql := 'SELECT array_agg(attname||'' ''||atttypid::regtype::text) as cols_n_types FROM pg_attribute WHERE attnum > 0 AND attisdropped is false AND attrelid = ' || quote_literal(v_refresh_snap) || '::regclass'; 
        
    perform @extschema@.gdb(p_debug,'v_local_sql: '||v_local_sql::text);

    EXECUTE v_local_sql INTO v_lcols_array;
    SELECT string_to_array(v_cols_n_types, ',') AS cols INTO v_rcols_array;

    -- Check to see if there's a change in the column structure on the remote
    FOREACH v_r IN ARRAY v_rcols_array LOOP
        v_match := 'f';
        FOREACH v_l IN ARRAY v_lcols_array LOOP
            IF v_r = v_l THEN
                v_match := 't';
                EXIT;
            END IF;
        END LOOP;
    END LOOP;

    IF v_match = 'f' THEN
        EXECUTE 'DROP TABLE ' || v_refresh_snap;
        EXECUTE 'DROP VIEW ' || v_dest_table;
        v_create_sql := 'CREATE TABLE ' || v_refresh_snap || ' (' || v_cols_n_types || ')';
        perform @extschema@.gdb(p_debug,'v_create_sql: '||v_create_sql::text);
        EXECUTE v_create_sql;
        EXECUTE 'SELECT '||v_jobmon_schema||'.add_step('||v_job_id||',''Source table structure changed.'')' INTO v_step_id;
        EXECUTE 'SELECT '||v_jobmon_schema||'.update_step('||v_job_id||', '||v_step_id||', ''OK'',''Tables and view dropped and recreated. Please double-check snap table attributes (permissions, indexes, etc'')';
        PERFORM @extschema@.gdb(p_debug,'Source table structure changed. Tables and view dropped and recreated. Please double-check snap table attributes (permissions, indexes, etc)');
    END IF;
    -- truncate non-active snap table
    EXECUTE 'TRUNCATE TABLE ' || v_refresh_snap;

EXECUTE 'SELECT '||v_jobmon_schema||'.update_step('||v_job_id||', '||v_step_id||', ''OK'',''Done'')';
END IF;
-- populating snap table
EXECUTE 'SELECT '||v_jobmon_schema||'.add_step('||v_job_id||',''Inserting records into local table'')' INTO v_step_id;
    PERFORM @extschema@.gdb(p_debug,'Inserting rows... '||v_insert_sql);
    EXECUTE v_insert_sql; 
    GET DIAGNOSTICS v_rowcount = ROW_COUNT;
EXECUTE 'SELECT '||v_jobmon_schema||'.update_step('||v_job_id||', '||v_step_id||', ''OK'',''Inserted '||v_rowcount||' records'')';

IF v_rowcount IS NOT NULL THEN
     EXECUTE 'ANALYZE ' ||v_refresh_snap;

    set statement_timeout='30 min';
    
    -- swap view
    EXECUTE 'SELECT '||v_jobmon_schema||'.add_step('||v_job_id||',''Swap view :'|| v_dest_table||''')' INTO v_step_id;
         EXECUTE 'CREATE OR REPLACE VIEW '||v_dest_table||' AS SELECT * FROM '||v_refresh_snap;
    EXECUTE 'SELECT '||v_jobmon_schema||'.update_step('||v_job_id||', '||v_step_id||', ''OK'',''View Swapped'')';

    EXECUTE 'SELECT '||v_jobmon_schema||'.add_step('||v_job_id||',''Updating last value'')' INTO v_step_id;
    UPDATE @extschema@.refresh_config set last_value = now() WHERE dest_table = p_destination;  

    EXECUTE 'SELECT '||v_jobmon_schema||'.update_step('||v_job_id||', '||v_step_id||', ''OK'',''Done'')';

    EXECUTE 'SELECT '||v_jobmon_schema||'.close_job('||v_job_id||')';
ELSE
    RAISE EXCEPTION 'No rows found in source table';
END IF;

PERFORM pg_advisory_unlock(hashtext('refresh_snap'), hashtext(v_job_name));

EXCEPTION
    WHEN RAISE_EXCEPTION THEN
        EXECUTE 'SELECT '||v_jobmon_schema||'.update_step('||v_job_id||', '||v_step_id||', ''BAD'', ''ERROR: '''||coalesce(SQLERRM,'unknown')||''')';
        EXECUTE 'SELECT '||v_jobmon_schema||'.fail_job('||v_job_id||')';
        PERFORM pg_advisory_unlock(hashtext('refresh_snap'), hashtext(v_job_name));
        RAISE EXCEPTION '%', SQLERRM;
    WHEN others THEN
        EXECUTE 'SELECT '||v_jobmon_schema||'.update_step('||v_job_id||', '||v_step_id||', ''BAD'', ''ERROR: '''||coalesce(SQLERRM,'unknown')||''')';
        EXECUTE 'SELECT '||v_jobmon_schema||'.fail_job('||v_job_id||')';
        PERFORM pg_advisory_unlock(hashtext('refresh_snap'), hashtext(v_job_name));
        RAISE EXCEPTION '%', SQLERRM;
END
$$;


/*
 *  refresh_incremental(p_destination text, p_debug boolean) RETURNS void
 *
 */
CREATE FUNCTION refresh_incremental(p_destination text, p_debug boolean) RETURNS void
    LANGUAGE plpgsql
    AS $$
declare
v_job_name       text;
v_job_id         int;
v_step_id        int;
v_rowcount       bigint; 
v_dblink_schema     text;
v_jobmon_schema     text;

v_source_table   text;
v_dest_table     text;
v_tmp_table      text;
v_dblink         text;
v_control_field  text;
v_last_value     timestamptz;
v_boundary        timestamptz;
v_pk_field       text;
v_filter         text[]; 
v_cols           text;
v_cols_n_types   text;

v_remote_sql     text;
v_insert_sql     text;
v_create_sql     text;
v_delete_sql     text;

BEGIN

IF p_debug IS DISTINCT FROM true THEN
    PERFORM set_config( 'client_min_messages', 'warning', true );
END IF;

v_job_name := 'Refresh Incremental: '||p_destination;

PERFORM pg_advisory_lock(hashtext('refresh_incremental'), hashtext(v_job_name));

SELECT nspname INTO v_dblink_schema FROM pg_namespace n, pg_extension e WHERE e.extname = 'dblink' AND e.extnamespace = n.oid;
SELECT nspname INTO v_jobmon_schema FROM pg_namespace n, pg_extension e WHERE e.extname = 'pg_jobmon' AND e.extnamespace = n.oid;

EXECUTE 'SELECT '||v_jobmon_schema||'.add_job('||quote_literal(v_job_name)||')' INTO v_job_id;
PERFORM @extschema@.gdb(p_debug,'Job ID: '||v_job_id::text);

EXECUTE 'SELECT '||v_jobmon_schema||'.add_step('||v_job_id||',''Grabbing Boundries, Building SQL'')' INTO v_step_id;

SELECT source_table, dest_table, 'tmp_'||replace(dest_table,'.','_'), dblink, control_field, last_value, now() - boundary::interval, filter FROM @extschema@.refresh_config WHERE dest_table = p_destination INTO v_source_table, v_dest_table, v_tmp_table, v_dblink, v_control_field, v_last_value, v_boundary, v_filter; 
IF NOT FOUND THEN
   RAISE EXCEPTION 'ERROR: no mapping found for %',v_job_name; 
END IF;  

IF v_filter IS NULL THEN 
    SELECT array_to_string(array_agg(attname),','), array_to_string(array_agg(attname||' '||atttypid::regtype::text),',') FROM 
    pg_attribute WHERE attnum > 0 AND attisdropped is false AND attrelid = p_destination::regclass INTO v_cols, v_cols_n_types;
ELSE
    SELECT array_to_string(array_agg(attname),','), array_to_string(array_agg(attname||' '||atttypid::regtype::text),',') FROM 
        (SELECT unnest(filter) FROM @extschema@.refresh_config WHERE dest_table = p_destination) x 
         JOIN pg_attribute ON (unnest=attname::text AND attrelid=p_destination::regclass) INTO v_cols, v_cols_n_types;
END IF;    

-- init sql statements 

-- does >= and < to keep missing data from happening on rare edge case where a newly inserted row outside the transaction batch
-- has the exact same timestamp as the previous batch's max timestamp
-- Note that this means the destination table is always at least one row behind even when no new data is entered on the source.
v_remote_sql := 'SELECT '||v_cols||' FROM '||v_source_table||' WHERE '||v_control_field||' >= '||quote_literal(v_last_value)||' AND '||v_control_field||' < '||quote_literal(v_boundary);

v_create_sql := 'CREATE TEMP TABLE '||v_tmp_table||' AS SELECT '||v_cols||' FROM '|| v_dblink_schema ||'.dblink(@extschema@.auth('||v_dblink||'),'||quote_literal(v_remote_sql)||') t ('||v_cols_n_types||')';

v_insert_sql := 'INSERT INTO '||v_dest_table||'('||v_cols||') SELECT '||v_cols||' FROM '||v_tmp_table; 

EXECUTE 'SELECT '||v_jobmon_schema||'.update_step('||v_job_id||', '||v_step_id||', ''OK'',''Grabbing rows from '||v_last_value::text||' to '||v_boundary::text||''')';

-- create temp from remote
EXECUTE 'SELECT '||v_jobmon_schema||'.add_step('||v_job_id||',''Creating temp table ('||v_tmp_table||') from remote table'')' INTO v_step_id;
    PERFORM @extschema@.gdb(p_debug,v_create_sql);
    EXECUTE v_create_sql; 
    GET DIAGNOSTICS v_rowcount = ROW_COUNT;
    EXECUTE 'SELECT '||v_jobmon_schema||'.update_step('||v_job_id||', '||v_step_id||', ''OK'',''Table contains '||v_rowcount||' records'')';
    PERFORM @extschema@.gdb(p_debug, v_rowcount || ' rows added to temp table');

-- insert
EXECUTE 'SELECT '||v_jobmon_schema||'.add_step('||v_job_id||',''Inserting new records into local table'')' INTO v_step_id;
    PERFORM @extschema@.gdb(p_debug,v_insert_sql);
    EXECUTE v_insert_sql; 
    GET DIAGNOSTICS v_rowcount = ROW_COUNT;
    EXECUTE 'SELECT '||v_jobmon_schema||'.update_step('||v_job_id||', '||v_step_id||', ''OK'',''Inserted '||v_rowcount||' records'')';
    PERFORM @extschema@.gdb(p_debug, v_rowcount || ' rows added to ' || v_dest_table);

-- update boundries
EXECUTE 'SELECT '||v_jobmon_schema||'.add_step('||v_job_id||',''Updating boundary values'')' INTO v_step_id;
UPDATE @extschema@.refresh_config set last_value = v_boundary WHERE dest_table = p_destination;  

EXECUTE 'SELECT '||v_jobmon_schema||'.update_step('||v_job_id||', '||v_step_id||', ''OK'',''Done'')';

EXECUTE 'SELECT '||v_jobmon_schema||'.close_job('||v_job_id||')';

EXECUTE 'DROP TABLE IF EXISTS ' || v_tmp_table;

PERFORM pg_advisory_unlock(hashtext('refresh_incremental'), hashtext(v_job_name));

EXCEPTION
    WHEN others THEN
    EXECUTE 'SELECT '||v_jobmon_schema||'.update_step('||v_job_id||', '||v_step_id||', ''BAD'', ''ERROR: '''||coalesce(SQLERRM,'unknown')||''')';
    EXECUTE 'SELECT '||v_jobmon_schema||'.fail_job('||v_job_id||')';
    PERFORM pg_advisory_unlock(hashtext('refresh_incremental'), hashtext(v_job_name));
    RAISE EXCEPTION '%', SQLERRM;    
END
$$;


/*
 *  refresh_dml(p_destination text, p_debug boolean) RETURNS void
 *
 */
CREATE FUNCTION refresh_dml(p_destination text, p_debug boolean) RETURNS void
    LANGUAGE plpgsql
    AS $$
declare
v_job_name          text;
v_job_id            int;
v_step_id           int;
v_rowcount          bigint; 
v_dblink_schema     text;
v_jobmon_schema     text;

v_source_table      text;
v_dest_table        text;
v_tmp_table         text;
v_dblink            text;
v_control_field     text;
v_last_value_sql    text; 
v_boundry           timestamptz;
v_pk_field          text;
v_pk_type           text;
v_filter            text[];
v_cols              text;
v_cols_n_types      text;

v_trigger_update    text;
v_trigger_delete    text; 
v_exec_status       text;

v_remote_q_sql      text;
v_remote_f_sql      text;
v_insert_sql        text;
v_create_q_sql      text;
v_create_f_sql      text;
v_delete_sql        text;

BEGIN
    IF p_debug IS DISTINCT FROM true THEN
        PERFORM set_config( 'client_min_messages', 'warning', true );
    END IF;

v_job_name := 'Refresh DML: '||p_destination;

PERFORM pg_advisory_lock(hashtext('refresh_dml'), hashtext(v_job_name));

SELECT nspname INTO v_dblink_schema FROM pg_namespace n, pg_extension e WHERE e.extname = 'dblink' AND e.extnamespace = n.oid;
SELECT nspname INTO v_jobmon_schema FROM pg_namespace n, pg_extension e WHERE e.extname = 'pg_jobmon' AND e.extnamespace = n.oid;

EXECUTE 'SELECT '||v_jobmon_schema||'.add_job('||quote_literal(v_job_name)||')' INTO v_job_id;
PERFORM @extschema@.gdb(p_debug,'Job ID: '||v_job_id::text);

EXECUTE 'SELECT '||v_jobmon_schema||'.add_step('||v_job_id||',''Grabbing Boundries, Building SQL'')' INTO v_step_id;

SELECT source_table, dest_table, 'tmp_'||replace(dest_table,'.','_'), dblink, control_field, pk_field, pk_type, filter FROM @extschema@.refresh_config 
WHERE dest_table = p_destination INTO v_source_table, v_dest_table, v_tmp_table, v_dblink, v_control_field, v_pk_field, v_pk_type, v_filter; 
IF NOT FOUND THEN
   RAISE EXCEPTION 'ERROR: no mapping found for %',v_job_name; 
END IF;  

-- determine column list, column type list
IF v_filter IS NULL THEN 
    SELECT array_to_string(array_agg(attname),','), array_to_string(array_agg(attname||' '||atttypid::regtype::text),',') FROM 
        pg_attribute WHERE attnum > 0 AND attisdropped is false AND attrelid = p_destination::regclass INTO v_cols, v_cols_n_types;
ELSE
    IF v_pk_field = ANY(v_filter) THEN
        SELECT array_to_string(array_agg(attname),','), array_to_string(array_agg(attname||' '||atttypid::regtype::text),',') FROM 
            (SELECT unnest(filter) FROM @extschema@.refresh_config WHERE dest_table = p_destination) x 
             JOIN pg_attribute ON (unnest=attname::text AND attrelid=p_destination::regclass) INTO v_cols, v_cols_n_types;
    ELSE
        RAISE EXCEPTION 'ERROR: filter list did not contain primary key for %',v_job_name; 
    END IF;
END IF;    

-- init sql statements 

v_trigger_update := 'SELECT '|| v_dblink_schema ||'.dblink_exec(@extschema@.auth('||v_dblink||'),'||quote_literal('UPDATE '||v_control_field||' SET processed = true WHERE '||v_pk_field||' IN (SELECT '|| v_pk_field||' FROM '|| v_control_field ||' ORDER BY 1 LIMIT 100000)')||')';

v_trigger_delete := 'SELECT '|| v_dblink_schema ||'.dblink_exec(@extschema@.auth('||v_dblink||'),'||quote_literal('DELETE FROM '||v_control_field||' WHERE processed = true')||')'; 

v_remote_q_sql := 'SELECT DISTINCT '||v_pk_field||' FROM '||v_control_field||' WHERE processed = true';

v_remote_f_sql := 'SELECT '||v_cols||' FROM '||v_source_table||' JOIN ('||v_remote_q_sql||') x USING ('||v_pk_field||')';

v_create_q_sql := 'CREATE TEMP TABLE '||v_tmp_table||'_queue AS SELECT '||v_pk_field||' 
    FROM '|| v_dblink_schema ||'.dblink(@extschema@.auth('||v_dblink||'),'||quote_literal(v_remote_q_sql)||') t ('||v_pk_field||' '||v_pk_type||')';

v_create_f_sql := 'CREATE TEMP TABLE '||v_tmp_table||'_full AS SELECT '||v_cols||' 
    FROM '|| v_dblink_schema ||'.dblink(@extschema@.auth('||v_dblink||'),'||quote_literal(v_remote_f_sql)||') t ('||v_cols_n_types||')';

v_delete_sql := 'DELETE FROM '||v_dest_table||' USING '||v_tmp_table||'_queue t WHERE '||v_dest_table||'.'||v_pk_field||'=t.'||v_pk_field; 

v_insert_sql := 'INSERT INTO '||v_dest_table||'('||v_cols||') SELECT '||v_cols||' FROM '||v_tmp_table||'_full'; 

EXECUTE 'SELECT '||v_jobmon_schema||'.update_step('||v_job_id||', '||v_step_id||', ''OK'',''Remote table is '||v_source_table||''')';

-- update remote entries
EXECUTE 'SELECT '||v_jobmon_schema||'.add_step('||v_job_id||',''Updating remote trigger table'')' INTO v_step_id;
    perform @extschema@.gdb(p_debug,v_trigger_update);
    execute v_trigger_update into v_exec_status;    
EXECUTE 'SELECT '||v_jobmon_schema||'.update_step('||v_job_id||', '||v_step_id||', ''OK'',''Result was '||v_exec_status||''')';

-- create temp table that contains queue of primary key values that changed 
EXECUTE 'SELECT '||v_jobmon_schema||'.add_step('||v_job_id||',''Create temp table from remote _q table'')' INTO v_step_id;
    PERFORM @extschema@.gdb(p_debug,v_create_q_sql);
    execute v_create_q_sql;  
    GET DIAGNOSTICS v_rowcount = ROW_COUNT;
EXECUTE 'SELECT '||v_jobmon_schema||'.update_step('||v_job_id||', '||v_step_id||', ''OK'',''Table contains '||v_rowcount||' records'')';

-- create temp table for insertion 
EXECUTE 'SELECT '||v_jobmon_schema||'.add_step('||v_job_id||',''Create temp table from remote full table'')' INTO v_step_id;
    perform @extschema@.gdb(p_debug,v_create_f_sql);
    execute v_create_f_sql;  
    GET DIAGNOSTICS v_rowcount = ROW_COUNT;
    PERFORM @extschema@.gdb(p_debug,'Temp table row count '||v_rowcount::text);
EXECUTE 'SELECT '||v_jobmon_schema||'.update_step('||v_job_id||', '||v_step_id||', ''OK'',''Table contains '||v_rowcount||' records'')';

-- remove records from local table 
EXECUTE 'SELECT '||v_jobmon_schema||'.add_step('||v_job_id||',''Deleting records from local table'')' INTO v_step_id;
    perform @extschema@.gdb(p_debug,v_delete_sql);
    execute v_delete_sql; 
    GET DIAGNOSTICS v_rowcount = ROW_COUNT;
    PERFORM @extschema@.gdb(p_debug,'Rows removed from local table before applying changes: '||v_rowcount::text);
EXECUTE 'SELECT '||v_jobmon_schema||'.update_step('||v_job_id||', '||v_step_id||', ''OK'',''Removed '||v_rowcount||' records'')';

-- insert records to local table
EXECUTE 'SELECT '||v_jobmon_schema||'.add_step('||v_job_id||',''Inserting new records into local table'')' INTO v_step_id;
    perform @extschema@.gdb(p_debug,v_insert_sql);
    execute v_insert_sql;
    GET DIAGNOSTICS v_rowcount = ROW_COUNT;
    PERFORM @extschema@.gdb(p_debug,'Rows inserted: '||v_rowcount::text);
EXECUTE 'SELECT '||v_jobmon_schema||'.update_step('||v_job_id||', '||v_step_id||', ''OK'',''Inserted '||v_rowcount||' records'')';

-- clean out rows from txn table
EXECUTE 'SELECT '||v_jobmon_schema||'.add_step('||v_job_id||',''Cleaning out rows from txn table'')' INTO v_step_id;
    perform @extschema@.gdb(p_debug,v_trigger_delete);
    execute v_trigger_delete into v_exec_status;
EXECUTE 'SELECT '||v_jobmon_schema||'.update_step('||v_job_id||', '||v_step_id||', ''OK'',''Result was '||v_exec_status||''')';

-- update activity status
EXECUTE 'SELECT '||v_jobmon_schema||'.add_step('||v_job_id||',''Updating last_value in config table'')' INTO v_step_id;
    v_last_value_sql := 'UPDATE @extschema@.refresh_config SET last_value = '|| quote_literal(current_timestamp::timestamp) ||' WHERE dest_table = ' ||quote_literal(p_destination); 
    perform @extschema@.gdb(p_debug,v_last_value_sql);
    execute v_last_value_sql; 
EXECUTE 'SELECT '||v_jobmon_schema||'.update_step('||v_job_id||', '||v_step_id||', ''OK'',''Last Value was '||current_timestamp||''')';

EXECUTE 'SELECT '||v_jobmon_schema||'.close_job('||v_job_id||')';

EXECUTE 'DROP TABLE IF EXISTS '||v_tmp_table||'_queue';
EXECUTE 'DROP TABLE IF EXISTS '||v_tmp_table||'_full';

PERFORM pg_advisory_unlock(hashtext('refresh_dml'), hashtext(v_job_name));

EXCEPTION
    WHEN others THEN
        EXECUTE 'SELECT '||v_jobmon_schema||'.update_step('||v_job_id||', '||v_step_id||', ''BAD'', ''ERROR: '''||coalesce(SQLERRM,'unknown')||''')';
        EXECUTE 'SELECT '||v_jobmon_schema||'.fail_job('||v_job_id||')';
        PERFORM pg_advisory_unlock(hashtext('refresh_dml'), hashtext(v_job_name));
        RAISE EXCEPTION '%', SQLERRM;
END
$$;

