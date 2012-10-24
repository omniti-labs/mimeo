-- IMPORTANT NOTE: Before installation check permissions on the following functions that were dropped. They've got a new signature so will need to be granted the previous versions' permissions.
-- updater, dml, and logdel maker functions can now automatically obtain the primary key or unique index from the source table. Parameters to manually set the key columns are still part of the maker functions if needed, but are now optional.
-- Made source_table column in config table unique for dml and logdel replication. Cannot have multiple jobs with same source due to source queue tables.
-- For all but snap, make destroyer functions more intelligent so they won't accidentally destroy local tables that aren't set up with mimeo.
-- dml_maker() & logdel_maker() now clean up after themselves on the source database tables if a make run fails. They will remove the queue table, function & trigger if and only if configuration information for the source table given does not exist in their respective configuration table.
-- New p_pulldata option for all maker functions to allow not pulling data from source if desired. It is set to TRUE by default.
-- Documentation updates

ALTER TABLE @extschema@.refresh_config_dml ADD CONSTRAINT refresh_config_dml_source_table_unique UNIQUE (source_table);
ALTER TABLE @extschema@.refresh_config_logdel ADD CONSTRAINT refresh_config_logdel_source_table_unique UNIQUE (source_table);

DROP FUNCTION @extschema@.refresh_snap(text, bool);
DROP FUNCTION @extschema@.snapshot_maker(text, int, text);
DROP FUNCTION @extschema@.inserter_maker(text, text, int, interval, text);
DROP FUNCTION @extschema@.dml_maker(text, int, text[], text[], text);
DROP FUNCTION @extschema@.updater_maker(text,text,integer,text[],text[],interval,text);
DROP FUNCTION @extschema@.logdel_maker(text, int, text[], text[], text);

/*
 *  Snap refresh to repull all table data
 */
CREATE FUNCTION refresh_snap(p_destination text, p_debug boolean DEFAULT false, p_pulldata boolean DEFAULT true) RETURNS void
    LANGUAGE plpgsql SECURITY DEFINER
    AS $$
DECLARE

v_adv_lock          boolean; 
v_cols_n_types      text;
v_cols              text;
v_create_sql        text;
v_dblink_schema     text;
v_dblink            text;
v_dest_table        text;
v_exists            int;
v_insert_sql        text;
v_job_id            int;
v_jobmon_schema     text;
v_job_name          text;
v_lcols_array       text[];
v_local_sql         text;
v_l                 text;
v_match             boolean := 'f';
v_old_search_path   text;
v_parts             record;
v_post_script       text[];
v_rcols_array       text[];
v_refresh_snap      text;
v_remote_sql        text;
v_rowcount          bigint;
v_r                 text;
v_snap              text;
v_source_table      text;
v_step_id           int;
v_table_exists      int;
v_view_definition   text;

BEGIN

IF p_debug IS DISTINCT FROM true THEN
    PERFORM set_config( 'client_min_messages', 'notice', true );
END IF;

v_job_name := 'Refresh Snap: '||p_destination;

SELECT nspname INTO v_dblink_schema FROM pg_namespace n, pg_extension e WHERE e.extname = 'dblink' AND e.extnamespace = n.oid;
SELECT nspname INTO v_jobmon_schema FROM pg_namespace n, pg_extension e WHERE e.extname = 'pg_jobmon' AND e.extnamespace = n.oid;

-- Set custom search path to allow easier calls to other functions, especially job logging
SELECT current_setting('search_path') INTO v_old_search_path;
EXECUTE 'SELECT set_config(''search_path'',''@extschema@,'||v_jobmon_schema||','||v_dblink_schema||',public'',''false'')';

v_job_id := add_job(v_job_name);
PERFORM gdb(p_debug,'Job ID: '||v_job_id::text);

-- Take advisory lock to prevent multiple calls to function overlapping and causing possible deadlock
v_adv_lock := pg_try_advisory_lock(hashtext('refresh_snap'), hashtext(v_job_name));
IF v_adv_lock = 'false' THEN
    v_step_id := add_step(v_job_id,'Obtaining advisory lock for job: '||v_job_name);
    PERFORM gdb(p_debug,'Obtaining advisory lock FAILED for job: '||v_job_name);
    PERFORM update_step(v_step_id, 'OK','Found concurrent job. Exiting gracefully');
    PERFORM close_job(v_job_id);
    EXECUTE 'SELECT set_config(''search_path'','''||v_old_search_path||''',''false'')';
    RETURN;
END IF;

v_step_id := add_step(v_job_id,'Grabbing Mapping, Building SQL');

SELECT source_table, dest_table, dblink, post_script INTO v_source_table, v_dest_table, v_dblink, v_post_script FROM refresh_config_snap
WHERE dest_table = p_destination; 
IF NOT FOUND THEN
   RAISE EXCEPTION 'ERROR: This table is not set up for snapshot replication: %',v_job_name; 
END IF;  

-- checking for current view

SELECT definition INTO v_view_definition FROM pg_views where
      ((schemaname || '.') || viewname)=v_dest_table;

v_exists := strpos(v_view_definition, 'snap1');
  IF v_exists > 0 THEN
    v_snap := '_snap2';
    ELSE
    v_snap := '_snap1';
 END IF;


v_refresh_snap := v_dest_table||v_snap;

PERFORM gdb(p_debug,'v_refresh_snap: '||v_refresh_snap::text);

-- init sql statements 

v_remote_sql := 'SELECT array_to_string(array_agg(attname),'','') as cols, array_to_string(array_agg(attname||'' ''||atttypid::regtype::text),'','') as cols_n_types FROM pg_attribute WHERE attnum > 0 AND attisdropped is false AND attrelid = ' || quote_literal(v_source_table) || '::regclass';
v_remote_sql := 'SELECT cols, cols_n_types FROM dblink(auth(' || v_dblink || '), ' || quote_literal(v_remote_sql) || ') t (cols text, cols_n_types text)';
perform gdb(p_debug,'v_remote_sql: '||v_remote_sql);
EXECUTE v_remote_sql INTO v_cols, v_cols_n_types;  
perform gdb(p_debug,'v_cols: '||v_cols);
perform gdb(p_debug,'v_cols_n_types: '||v_cols_n_types);

v_remote_sql := 'SELECT '||v_cols||' FROM '||v_source_table;
-- Used by p_pull options in maker functions to be able to create a replication job but pull no data
IF p_pulldata = false THEN
    v_remote_sql := v_remote_sql || ' LIMIT 0';
END IF;

v_insert_sql := 'INSERT INTO ' || v_refresh_snap || ' SELECT '||v_cols||' FROM dblink(auth('||v_dblink||'),'||quote_literal(v_remote_sql)||') t ('||v_cols_n_types||')';

PERFORM update_step(v_step_id, 'OK','Done');

v_step_id := add_step(v_job_id,'Truncate non-active snap table');

-- Create snap table if it doesn't exist
SELECT string_to_array(v_refresh_snap, '.') AS oparts INTO v_parts;
SELECT INTO v_table_exists count(1) FROM pg_tables
    WHERE  schemaname = v_parts.oparts[1] AND
           tablename = v_parts.oparts[2];
IF v_table_exists = 0 THEN

    PERFORM gdb(p_debug,'Snap table does not exist. Creating... ');
    
    v_create_sql := 'CREATE TABLE ' || v_refresh_snap || ' (' || v_cols_n_types || ')';
    perform gdb(p_debug,'v_create_sql: '||v_create_sql::text);
    EXECUTE v_create_sql;
ELSE 

/* Check local column definitions against remote and recreate table if different. Allows automatic recreation of
        snap tables if columns change (add, drop type change)  */  
    v_local_sql := 'SELECT array_agg(attname||'' ''||atttypid::regtype::text) as cols_n_types FROM pg_attribute WHERE attnum > 0 AND attisdropped is false AND attrelid = ' || quote_literal(v_refresh_snap) || '::regclass'; 
        
    PERFORM gdb(p_debug,'v_local_sql: '||v_local_sql::text);

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
        PERFORM gdb(p_debug,'v_create_sql: '||v_create_sql::text);
        EXECUTE v_create_sql;
        v_step_id := add_step(v_job_id,'Source table structure changed.');
        PERFORM update_step(v_step_id, 'OK','Tables and view dropped and recreated. Please double-check snap table attributes (permissions, indexes, etc');
        PERFORM gdb(p_debug,'Source table structure changed. Tables and view dropped and recreated. Please double-check snap table attributes (permissions, indexes, etc)');

    END IF;
    -- truncate non-active snap table
    EXECUTE 'TRUNCATE TABLE ' || v_refresh_snap;

PERFORM update_step(v_step_id, 'OK','Done');
END IF;
-- populating snap table
v_step_id := add_step(v_job_id,'Inserting records into local table');
    PERFORM gdb(p_debug,'Inserting rows... '||v_insert_sql);
    EXECUTE v_insert_sql; 
    GET DIAGNOSTICS v_rowcount = ROW_COUNT;
PERFORM update_step(v_step_id, 'OK','Inserted '||v_rowcount||' records');

IF v_rowcount IS NOT NULL THEN
     EXECUTE 'ANALYZE ' ||v_refresh_snap;

    SET statement_timeout='30 min';
    
    -- swap view
    v_step_id := add_step(v_job_id,'Swap view to '||v_refresh_snap);
    PERFORM gdb(p_debug,'Swapping view to '||v_refresh_snap);
    EXECUTE 'CREATE OR REPLACE VIEW '||v_dest_table||' AS SELECT * FROM '||v_refresh_snap;
    PERFORM update_step(v_step_id, 'OK','View Swapped');

    v_step_id := add_step(v_job_id,'Updating last value');
    UPDATE refresh_config_snap set last_value = now() WHERE dest_table = p_destination;  

    PERFORM update_step(v_step_id, 'OK','Done');

    -- Runs special sql to fix indexes, permissions, etc on recreated objects
    IF v_match = 'f' AND v_post_script IS NOT NULL THEN
        v_step_id := add_step(v_job_id,'Applying post_script sql commands due to schema change');
        PERFORM @extschema@.post_script(v_dest_table);
        PERFORM update_step(v_step_id, 'OK','Done');
    END IF;

    PERFORM close_job(v_job_id);
ELSE
    RAISE EXCEPTION 'No rows found in source table';
END IF;

-- Ensure old search path is reset for the current session
EXECUTE 'SELECT set_config(''search_path'','''||v_old_search_path||''',''false'')';

PERFORM pg_advisory_unlock(hashtext('refresh_snap'), hashtext(v_job_name));

EXCEPTION

    WHEN QUERY_CANCELED THEN
        PERFORM pg_advisory_unlock(hashtext('refresh_inserter'), hashtext(v_job_name));
        RAISE EXCEPTION '%', SQLERRM;   
    WHEN OTHERS THEN
        EXECUTE 'SELECT set_config(''search_path'',''@extschema@,'||v_jobmon_schema||','||v_dblink_schema||''',''false'')';
        IF v_step_id IS NULL THEN
            v_step_id := jobmon.add_step(v_job_id, 'EXCEPTION before first step logged');
        END IF;
        PERFORM update_step(v_step_id, 'BAD', 'ERROR: '||coalesce(SQLERRM,'unknown'));
        PERFORM fail_job(v_job_id);

        -- Ensure old search path is reset for the current session
        EXECUTE 'SELECT set_config(''search_path'','''||v_old_search_path||''',''false'')';

        PERFORM pg_advisory_unlock(hashtext('refresh_snap'), hashtext(v_job_name));
        RAISE EXCEPTION '%', SQLERRM;
END
$$;


/*
 *  Updater maker function.
 */
CREATE FUNCTION updater_maker(p_src_table text, p_control_field text, p_dblink_id int, p_boundary interval DEFAULT '00:10:00', p_dest_table text DEFAULT NULL, p_pulldata boolean DEFAULT true, p_pk_field text[] DEFAULT NULL, p_pk_type text[] DEFAULT NULL) RETURNS void
    LANGUAGE plpgsql
    AS $$
DECLARE

v_alter_table               text;
v_data_source               text;
v_dblink_schema             text;
v_dest_check                text;
v_dest_schema_name          text;
v_dest_table_name           text;
v_dst_active                boolean;
v_exists                    int;
v_insert_refresh_config     text;
v_max_timestamp             timestamptz;
v_old_search_path           text;
v_pk_field                  text[] := p_pk_field;
v_pk_field_csv              text := '';
v_pk_type                   text[] := p_pk_field;
v_pk_type_csv               text := '';
v_remote_key_sql            text;
v_snap_suffix               text;
v_update_refresh_config     text;
v_view_definition           text;

BEGIN

SELECT nspname INTO v_dblink_schema FROM pg_namespace n, pg_extension e WHERE e.extname = 'dblink' AND e.extnamespace = n.oid;
SELECT current_setting('search_path') INTO v_old_search_path;
EXECUTE 'SELECT set_config(''search_path'',''@extschema@,'||v_dblink_schema||',public'',''false'')';

IF (p_pk_field IS NULL AND p_pk_type IS NOT NULL) OR (p_pk_field IS NOT NULL AND p_pk_type IS NULL) THEN
    RAISE EXCEPTION 'Cannot manually set primary/unique key field(s) without defining type(s) or vice versa';
END IF;

SELECT data_source INTO v_data_source FROM @extschema@.dblink_mapping WHERE data_source_id = p_dblink_id; 
IF NOT FOUND THEN
	RAISE EXCEPTION 'ERROR: database link ID is incorrect %', p_dblink_id; 
END IF;  

IF p_dest_table IS NULL THEN
    p_dest_table := p_src_table;
END IF;

IF position('.' in p_dest_table) > 0 THEN
    v_dest_schema_name := split_part(p_dest_table, '.', 1); 
    v_dest_table_name := split_part(p_dest_table, '.', 2);
END IF;

IF p_pk_field IS NULL AND p_pk_type IS NULL THEN
    PERFORM dblink_connect('mimeo_updater', @extschema@.auth(p_dblink_id));
    -- Either gets the primary key or it gets the first unique index in alphabetical order by index name
    v_remote_key_sql := 'SELECT
                    CASE
                        WHEN i.indisprimary IS true THEN ''primary''
                        WHEN i.indisunique IS true THEN ''unique''
                    END AS key_type,
                    array_agg( a.attname ) AS indkey_names,
                    array_agg( a.atttypid::regtype) AS indkey_types
                FROM
                    pg_index i
                    JOIN pg_attribute a ON i.indrelid = a.attrelid AND a.attnum = any( i.indkey )
                WHERE
                    i.indrelid = '||quote_literal(p_src_table)||'::regclass
                    AND ( i.indisprimary OR i.indisunique )
                GROUP BY 1
                HAVING bool_and( a.attnotnull )
                ORDER BY 1 LIMIT 1';
    EXECUTE 'SELECT indkey_names, indkey_types FROM dblink(''mimeo_updater'', '||quote_literal(v_remote_key_sql)||') t (key_type text, indkey_names text[], indkey_types text[])' 
        INTO v_pk_field, v_pk_type;
    PERFORM dblink_disconnect('mimeo_updater');
END IF;

IF v_pk_field IS NULL OR v_pk_type IS NULL THEN
    RAISE EXCEPTION 'Source table has no valid primary key or unique index';
END IF;

v_pk_field_csv := array_to_string(v_pk_field,',');
v_pk_type_csv := array_to_string(v_pk_type,',');

-- Only create destination table if it doesn't already exist
SELECT schemaname||'.'||tablename INTO v_dest_check FROM pg_tables WHERE schemaname = v_dest_schema_name AND tablename = v_dest_table_name;
IF v_dest_check IS NULL THEN
    v_insert_refresh_config := 'INSERT INTO @extschema@.refresh_config_snap(source_table, dest_table, dblink) VALUES('
        ||quote_literal(p_src_table)||', '||quote_literal(p_dest_table)||', '|| p_dblink_id||')';

    RAISE NOTICE 'Snapshotting source table to pull all current source data...';
    EXECUTE v_insert_refresh_config;	
    
    EXECUTE 'SELECT @extschema@.refresh_snap('||quote_literal(p_dest_table)||', p_pulldata := '||p_pulldata||')'; 
    PERFORM @extschema@.snapshot_destroyer(p_dest_table, 'ARCHIVE');

    v_alter_table := 'ALTER TABLE '||p_dest_table||' ADD PRIMARY KEY('||v_pk_field_csv||');';

    RAISE NOTICE 'Snapshot complete. Adding primary key constraint to table..';
    EXECUTE v_alter_table;
    RAISE NOTICE 'Added successfully';
ELSE
    RAISE NOTICE 'Destination table % already exists. No data was pulled from source', p_dest_table;
END IF;

RAISE NOTICE 'Getting the maximum destination timestamp...';
EXECUTE 'SELECT max('||p_control_field||') FROM '||p_dest_table||';' INTO v_max_timestamp;

v_dst_active := @extschema@.dst_utc_check();

v_insert_refresh_config := 'INSERT INTO @extschema@.refresh_config_updater(source_table, dest_table, dblink, control, boundary, pk_field, pk_type, last_value, dst_active) VALUES('
    ||quote_literal(p_src_table)||', '||quote_literal(p_dest_table)||', '|| p_dblink_id||', '||quote_literal(p_control_field)||', '''
    ||p_boundary||'''::interval, '||quote_literal(v_pk_field)||', '||quote_literal(v_pk_type)||', '||quote_literal(COALESCE(v_max_timestamp, CURRENT_TIMESTAMP))||', '||v_dst_active||')';

RAISE NOTICE 'Inserting data into config table';
EXECUTE v_insert_refresh_config;

-- Remove temp snap from config
EXECUTE 'DELETE FROM @extschema@.refresh_config_snap WHERE source_table = '||quote_literal(p_src_table)||' AND dest_table = '||quote_literal(p_dest_table);

EXECUTE 'SELECT set_config(''search_path'','''||v_old_search_path||''',''false'')';

RAISE NOTICE 'Done';

RETURN;

EXCEPTION
    WHEN OTHERS THEN
        EXECUTE 'SELECT set_config(''search_path'',''@extschema@,'||v_dblink_schema||''',''false'')';
        IF dblink_get_connections() @> '{mimeo_updater}' THEN
            PERFORM dblink_disconnect('mimeo_updater');
        END IF;
        EXECUTE 'SELECT set_config(''search_path'','''||v_old_search_path||''',''false'')';
        RAISE EXCEPTION '%', SQLERRM;  
END  
$$;   


/*
 *  DML maker function.
 */
CREATE FUNCTION dml_maker(p_src_table text, p_dblink_id int, p_dest_table text DEFAULT NULL, p_pulldata boolean DEFAULT true, p_pk_field text[] DEFAULT NULL, p_pk_type text[] DEFAULT NULL) RETURNS void
    LANGUAGE plpgsql
    AS $$
DECLARE

v_create_trig               text;
v_data_source               text;
v_dblink_schema             text;
v_dest_check                text;
v_dest_schema_name          text;
v_dest_table_name           text;
v_exists                    int := 0;
v_insert_refresh_config     text;
v_old_search_path           text;
v_pk_counter                int := 1;
v_pk_field                  text[] := p_pk_field;
v_pk_field_csv              text := '';
v_pk_type                   text[] := p_pk_field;
v_pk_type_csv               text := '';
v_remote_exists             int := 0;
v_remote_key_sql            text;
v_remote_q_index            text;
v_remote_q_table            text;
v_src_table_name            text;
v_trigger_func              text;


BEGIN

SELECT nspname INTO v_dblink_schema FROM pg_namespace n, pg_extension e WHERE e.extname = 'dblink' AND e.extnamespace = n.oid;
SELECT current_setting('search_path') INTO v_old_search_path;
EXECUTE 'SELECT set_config(''search_path'',''@extschema@,'||v_dblink_schema||',public'',''false'')';

IF (p_pk_field IS NULL AND p_pk_type IS NOT NULL) OR (p_pk_field IS NOT NULL AND p_pk_type IS NULL) THEN
    RAISE EXCEPTION 'Cannot manually set primary/unique key field(s) without defining type(s) or vice versa';
END IF;

SELECT data_source INTO v_data_source FROM @extschema@.dblink_mapping WHERE data_source_id = p_dblink_id; 
IF NOT FOUND THEN
	RAISE EXCEPTION 'ERROR: database link ID is incorrect %', p_dblink_id; 
END IF;

-- Set custom search path to allow easier calls to other functions
SELECT current_setting('search_path') INTO v_old_search_path;
EXECUTE 'SELECT set_config(''search_path'',''@extschema@,'||v_dblink_schema||',public'',''false'')';

IF p_dest_table IS NULL THEN
    p_dest_table := p_src_table;
END IF;

EXECUTE 'SELECT count(*) FROM @extschema@.refresh_config_dml WHERE dest_table = '||quote_literal(p_dest_table)||' AND source_table = '||quote_literal(p_src_table) INTO v_exists;
IF v_exists > 0 THEN
    RAISE unique_violation;
END IF;

IF position('.' in p_src_table) > 0 THEN
    v_src_table_name := split_part(p_src_table, '.', 2);
END IF;

IF position('.' in p_dest_table) > 0 THEN
    v_dest_schema_name := split_part(p_dest_table, '.', 1); 
    v_dest_table_name := split_part(p_dest_table, '.', 2);
END IF;

PERFORM dblink_connect('mimeo_dml', @extschema@.auth(p_dblink_id));

IF p_pk_field IS NULL AND p_pk_type IS NULL THEN
    -- Either gets the primary key or it gets the first unique index in alphabetical order by index name
    v_remote_key_sql := 'SELECT
                    CASE
                        WHEN i.indisprimary IS true THEN ''primary''
                        WHEN i.indisunique IS true THEN ''unique''
                    END AS key_type,
                    array_agg( a.attname ) AS indkey_names,
                    array_agg( a.atttypid::regtype) AS indkey_types
                FROM
                    pg_index i
                    JOIN pg_attribute a ON i.indrelid = a.attrelid AND a.attnum = any( i.indkey )
                WHERE
                    i.indrelid = '||quote_literal(p_src_table)||'::regclass
                    AND ( i.indisprimary OR i.indisunique )
                GROUP BY 1
                HAVING bool_and( a.attnotnull )
                ORDER BY 1 LIMIT 1';
    EXECUTE 'SELECT indkey_names, indkey_types FROM dblink(''mimeo_dml'', '||quote_literal(v_remote_key_sql)||') t (key_type text, indkey_names text[], indkey_types text[])' 
        INTO v_pk_field, v_pk_type;
END IF;

IF v_pk_field IS NULL OR v_pk_type IS NULL THEN
    RAISE EXCEPTION 'Source table has no valid primary key or unique index';
END IF;

v_pk_field_csv := array_to_string(v_pk_field, ',');
WHILE v_pk_counter <= array_length(v_pk_field,1) LOOP
    IF v_pk_counter > 1 THEN
        v_pk_type_csv := v_pk_type_csv || ', ';
    END IF;
    v_pk_type_csv := v_pk_type_csv ||v_pk_field[v_pk_counter]||' '||v_pk_type[v_pk_counter];
    v_pk_counter := v_pk_counter + 1;
END LOOP;


v_remote_q_table := 'CREATE TABLE @extschema@.'||v_src_table_name||'_pgq (';

v_remote_q_table := v_remote_q_table || v_pk_type_csv || ', processed boolean)';

v_remote_q_index := 'CREATE INDEX '||v_src_table_name||'_pgq_'||replace(v_pk_field_csv,',','_')||'_idx ON @extschema@.'||v_src_table_name||'_pgq ('||v_pk_field_csv||')';

v_pk_counter := 1;
v_trigger_func := 'CREATE FUNCTION @extschema@.'||v_src_table_name||'_mimeo_queue() RETURNS trigger LANGUAGE plpgsql AS $_$ DECLARE ';
    WHILE v_pk_counter <= array_length(v_pk_field,1) LOOP
        v_trigger_func := v_trigger_func||'v_'||v_pk_field[v_pk_counter]||' '||v_pk_type[v_pk_counter]||'; ';
        v_pk_counter := v_pk_counter + 1;
    END LOOP;
    v_pk_counter := 1;
    v_trigger_func := v_trigger_func || ' BEGIN IF TG_OP = ''INSERT'' THEN ';
    WHILE v_pk_counter <= array_length(v_pk_field,1) LOOP
        v_trigger_func := v_trigger_func||' v_'||v_pk_field[v_pk_counter]||' := NEW.'||v_pk_field[v_pk_counter]||'; ';
        v_pk_counter := v_pk_counter + 1;
    END LOOP;
    v_pk_counter := 1;
    v_trigger_func := v_trigger_func || ' ELSE ';
    WHILE v_pk_counter <= array_length(v_pk_field,1) LOOP
        v_trigger_func := v_trigger_func||' v_'||v_pk_field[v_pk_counter]||' := OLD.'||v_pk_field[v_pk_counter]||'; ';
        v_pk_counter := v_pk_counter + 1;
    END LOOP;
    v_pk_counter := 1;
    v_trigger_func := v_trigger_func || ' END IF; INSERT INTO @extschema@.'||v_src_table_name||'_pgq ('||v_pk_field_csv||') ';
    v_trigger_func := v_trigger_func || ' VALUES (';
    WHILE v_pk_counter <= array_length(v_pk_field,1) LOOP
        IF v_pk_counter > 1 THEN
            v_trigger_func := v_trigger_func || ', ';
        END IF;
        v_trigger_func := v_trigger_func||'v_'||v_pk_field[v_pk_counter];
        v_pk_counter := v_pk_counter + 1;
    END LOOP;
    v_trigger_func := v_trigger_func || '); RETURN NULL; END $_$;';

v_create_trig := 'CREATE TRIGGER '||v_src_table_name||'_mimeo_trig AFTER INSERT OR UPDATE OR DELETE ON '||p_src_table||
    ' FOR EACH ROW EXECUTE PROCEDURE @extschema@.'||v_src_table_name||'_mimeo_queue()';

RAISE NOTICE 'Creating objects on source database (function, trigger & queue table)...';

PERFORM dblink_exec('mimeo_dml', v_remote_q_table);
PERFORM dblink_exec('mimeo_dml', v_remote_q_index);
PERFORM dblink_exec('mimeo_dml', v_trigger_func);
PERFORM dblink_exec('mimeo_dml', v_create_trig);

PERFORM dblink_disconnect('mimeo_dml');

-- Only create destination table if it doesn't already exist
SELECT schemaname||'.'||tablename INTO v_dest_check FROM pg_tables WHERE schemaname = v_dest_schema_name AND tablename = v_dest_table_name;
IF v_dest_check IS NULL THEN
    RAISE NOTICE 'Snapshotting source table to pull all current source data...';
    -- Snapshot the table after triggers have been created to ensure all new data after setup is replicated
    v_insert_refresh_config := 'INSERT INTO @extschema@.refresh_config_snap(source_table, dest_table, dblink) VALUES('
        ||quote_literal(p_src_table)||', '||quote_literal(p_dest_table)||', '|| p_dblink_id||')';
    EXECUTE v_insert_refresh_config;

    EXECUTE 'SELECT @extschema@.refresh_snap('||quote_literal(p_dest_table)||', p_pulldata := '||p_pulldata||')';
    PERFORM @extschema@.snapshot_destroyer(p_dest_table, 'ARCHIVE');
ELSE
    RAISE NOTICE 'Destination table % already exists. No data was pulled from source', p_dest_table;
END IF;

v_insert_refresh_config := 'INSERT INTO @extschema@.refresh_config_dml(source_table, dest_table, dblink, control, pk_field, pk_type, last_value) VALUES('
    ||quote_literal(p_src_table)||', '||quote_literal(p_dest_table)||', '|| p_dblink_id||', '||quote_literal('@extschema@.'||v_src_table_name||'_pgq')||', '
    ||quote_literal(v_pk_field)||', '||quote_literal(v_pk_type)||', '||quote_literal(clock_timestamp())||')';
RAISE NOTICE 'Inserting data into config table';
EXECUTE v_insert_refresh_config;

EXECUTE 'SELECT set_config(''search_path'','''||v_old_search_path||''',''false'')';

RAISE NOTICE 'Done';

EXCEPTION
    WHEN OTHERS THEN
        EXECUTE 'SELECT set_config(''search_path'',''@extschema@,'||v_dblink_schema||''',''false'')';
        -- Only cleanup remote objects if replication doesn't exist at all for source table
        EXECUTE 'SELECT count(*) FROM @extschema@.refresh_config_dml WHERE source_table = '||quote_literal(p_src_table) INTO v_exists;
        IF v_exists = 0 THEN
            IF (dblink_get_connections() @> '{mimeo_dml}') = false THEN
                PERFORM dblink_connect('mimeo_dml', @extschema@.auth(p_dblink_id));
            END IF;
            PERFORM dblink_exec('mimeo_dml', 'DROP TABLE IF EXISTS @extschema@.'||v_src_table_name||'_pgq');
            PERFORM dblink_exec('mimeo_dml', 'DROP TRIGGER IF EXISTS '||v_src_table_name||'_mimeo_trig ON '||p_src_table);
            PERFORM dblink_exec('mimeo_dml', 'DROP FUNCTION IF EXISTS @extschema@.'||v_src_table_name||'_mimeo_queue()');
        END IF;
        IF dblink_get_connections() @> '{mimeo_dml}' THEN
            PERFORM dblink_disconnect('mimeo_dml');
        END IF;
        EXECUTE 'SELECT set_config(''search_path'','''||v_old_search_path||''',''false'')';
        IF v_exists = 0 THEN
            RAISE EXCEPTION 'dml_maker() failure. No mimeo configuration found for source %. Cleaned up source table mimeo objects (queue table, function & trigger) if they existed.  SQLERRM: %', p_src_table, SQLERRM;
        ELSE
            RAISE EXCEPTION 'dml_maker() failure. Check to see if dml configuration for % already exists. SQLERRM: % ', p_src_table, SQLERRM;
        END IF;
END
$$;


/*
 *  Logdel maker function.
 */
CREATE FUNCTION logdel_maker(p_src_table text, p_dblink_id int, p_dest_table text DEFAULT NULL, p_pulldata boolean DEFAULT true, p_pk_field text[] DEFAULT NULL, p_pk_type text[] DEFAULT NULL) RETURNS void
    LANGUAGE plpgsql
    AS $$
DECLARE

v_col_exists                int;
v_cols                      text[];
v_cols_csv                  text;
v_cols_n_types              text[];
v_cols_n_types_csv          text;
v_create_trig               text;
v_data_source               text;
v_dblink_schema             text;
v_dest_check                text;
v_dest_schema_name          text;
v_dest_table_name           text;
v_exists                    int := 0;
v_insert_refresh_config     text;
v_old_search_path           text;
v_counter                   int := 1;
v_pk_field                  text[] := p_pk_field;
v_pk_field_csv              text := '';
v_pk_type                   text[] := p_pk_field;
v_pk_type_csv               text := '';
v_remote_key_sql            text;
v_remote_sql                text;
v_remote_q_index            text;
v_remote_q_table            text;
v_src_table_name            text;
v_trigger_func              text;
v_types                     text[];

v_sql                   text;

BEGIN

SELECT nspname INTO v_dblink_schema FROM pg_namespace n, pg_extension e WHERE e.extname = 'dblink' AND e.extnamespace = n.oid;
SELECT current_setting('search_path') INTO v_old_search_path;
EXECUTE 'SELECT set_config(''search_path'',''@extschema@,'||v_dblink_schema||',public'',''false'')';

IF (p_pk_field IS NULL AND p_pk_type IS NOT NULL) OR (p_pk_field IS NOT NULL AND p_pk_type IS NULL) THEN
    RAISE EXCEPTION 'Cannot manually set primary/unique key field(s) without defining type(s) or vice versa';
END IF;

SELECT data_source INTO v_data_source FROM @extschema@.dblink_mapping WHERE data_source_id = p_dblink_id; 
IF NOT FOUND THEN
	RAISE EXCEPTION 'ERROR: database link ID is incorrect %', p_dblink_id; 
END IF;

IF p_dest_table IS NULL THEN
    p_dest_table := p_src_table;
END IF;

EXECUTE 'SELECT count(*) FROM @extschema@.refresh_config_logdel WHERE dest_table = '||quote_literal(p_dest_table)||' AND source_table = '||quote_literal(p_src_table) INTO v_exists;
IF v_exists > 0 THEN
    RAISE unique_violation;
END IF;

IF position('.' in p_src_table) > 0 THEN
    v_src_table_name := split_part(p_src_table, '.', 2);
END IF;

IF position('.' in p_dest_table) > 0 THEN
    v_dest_schema_name := split_part(p_dest_table, '.', 1); 
    v_dest_table_name := split_part(p_dest_table, '.', 2);
END IF;

PERFORM dblink_connect('mimeo_logdel', @extschema@.auth(p_dblink_id));

v_remote_sql := 'SELECT array_agg(attname) as cols, array_agg(atttypid::regtype::text) as types, array_agg(attname||'' ''||atttypid::regtype::text) as cols_n_types FROM pg_attribute WHERE attnum > 0 AND attisdropped is false AND attrelid = ' || quote_literal(p_src_table) || '::regclass';
v_remote_sql := 'SELECT cols, types, cols_n_types FROM dblink(''mimeo_logdel'', ' || quote_literal(v_remote_sql) || ') t (cols text[], types text[], cols_n_types text[])';
EXECUTE v_remote_sql INTO v_cols, v_types, v_cols_n_types;

v_cols_csv := array_to_string(v_cols, ',');
v_cols_n_types_csv := array_to_string(v_cols_n_types, ',');

v_remote_q_table := 'CREATE TABLE @extschema@.'||v_src_table_name||'_pgq ('||v_cols_n_types_csv||', mimeo_source_deleted timestamptz, processed boolean)';

IF p_pk_field IS NULL AND p_pk_type IS NULL THEN
    -- Either gets the primary key or it gets the first unique index in alphabetical order by index name
    v_remote_key_sql := 'SELECT
                    CASE
                        WHEN i.indisprimary IS true THEN ''primary''
                        WHEN i.indisunique IS true THEN ''unique''
                    END AS key_type,
                    array_agg( a.attname ) AS indkey_names,
                    array_agg( a.atttypid::regtype) AS indkey_types
                FROM
                    pg_index i
                    JOIN pg_attribute a ON i.indrelid = a.attrelid AND a.attnum = any( i.indkey )
                WHERE
                    i.indrelid = '||quote_literal(p_src_table)||'::regclass
                    AND ( i.indisprimary OR i.indisunique )
                GROUP BY 1
                HAVING bool_and( a.attnotnull )
                ORDER BY 1 LIMIT 1';
    EXECUTE 'SELECT indkey_names, indkey_types FROM dblink(''mimeo_logdel'', '||quote_literal(v_remote_key_sql)||') t (key_type text, indkey_names text[], indkey_types text[])' 
        INTO v_pk_field, v_pk_type;
END IF;

IF v_pk_field IS NULL OR v_pk_type IS NULL THEN
    RAISE EXCEPTION 'Source table has no valid primary key or unique index';
END IF;

v_pk_field_csv := array_to_string(v_pk_field, ',');
WHILE v_counter <= array_length(v_pk_field,1) LOOP
    IF v_counter > 1 THEN
        v_pk_type_csv := v_pk_type_csv || ', ';
    END IF;
    v_pk_type_csv := v_pk_type_csv ||v_pk_field[v_counter]||' '||v_pk_type[v_counter];
    v_counter := v_counter + 1;
END LOOP;

v_remote_q_index := 'CREATE INDEX '||v_src_table_name||'_pgq_'||replace(v_pk_field_csv,',','_')||'_idx ON @extschema@.'||v_src_table_name||'_pgq ('||v_pk_field_csv||')';

v_counter := 1;
v_trigger_func := 'CREATE FUNCTION @extschema@.'||v_src_table_name||'_mimeo_queue() RETURNS trigger LANGUAGE plpgsql AS $_$ DECLARE ';
    WHILE v_counter <= array_length(v_cols,1) LOOP
        v_trigger_func := v_trigger_func||'v_'||v_cols[v_counter]||' '||v_types[v_counter]||'; ';
        v_counter := v_counter + 1;
    END LOOP;
    v_trigger_func := v_trigger_func || 'v_del_time timestamptz; ';
    v_counter := 1;
    v_trigger_func := v_trigger_func || ' BEGIN 
        IF TG_OP = ''INSERT'' THEN ';
    WHILE v_counter <= array_length(v_pk_field,1) LOOP
        v_trigger_func := v_trigger_func||' v_'||v_pk_field[v_counter]||' := NEW.'||v_pk_field[v_counter]||'; ';
        v_counter := v_counter + 1;
    END LOOP;
    v_counter := 1;
    v_trigger_func := v_trigger_func || ' ELSIF TG_OP = ''UPDATE'' THEN  ';
    WHILE v_counter <= array_length(v_pk_field,1) LOOP
        v_trigger_func := v_trigger_func||' v_'||v_pk_field[v_counter]||' := OLD.'||v_pk_field[v_counter]||'; ';
        v_counter := v_counter + 1;
    END LOOP;
    v_counter := 1;
    v_trigger_func := v_trigger_func || ' ELSIF TG_OP = ''DELETE'' THEN  ';
    WHILE v_counter <= array_length(v_cols,1) LOOP
        v_trigger_func := v_trigger_func||' v_'||v_cols[v_counter]||' := OLD.'||v_cols[v_counter]||'; ';
        v_counter := v_counter + 1;
    END LOOP;
    v_trigger_func := v_trigger_func || 'v_del_time := clock_timestamp(); ';
    v_counter := 1;
    v_trigger_func := v_trigger_func || ' END IF; INSERT INTO @extschema@.'||v_src_table_name||'_pgq ('||v_cols_csv||', mimeo_source_deleted) ';
    v_trigger_func := v_trigger_func || ' VALUES (';
    WHILE v_counter <= array_length(v_cols,1) LOOP
        IF v_counter > 1 THEN
            v_trigger_func := v_trigger_func || ', ';
        END IF;
        v_trigger_func := v_trigger_func||'v_'||v_cols[v_counter];
        v_counter := v_counter + 1;
    END LOOP;
    v_trigger_func := v_trigger_func ||', v_del_time); RETURN NULL; END $_$;';

v_create_trig := 'CREATE TRIGGER '||v_src_table_name||'_mimeo_trig AFTER INSERT OR UPDATE OR DELETE ON '||p_src_table||
    ' FOR EACH ROW EXECUTE PROCEDURE @extschema@.'||v_src_table_name||'_mimeo_queue()';

RAISE NOTICE 'Creating objects on source database (function, trigger & queue table)...';
PERFORM dblink_exec('mimeo_logdel', v_remote_q_table);
PERFORM dblink_exec('mimeo_logdel', v_remote_q_index);
PERFORM dblink_exec('mimeo_logdel', v_trigger_func);
PERFORM dblink_exec('mimeo_logdel', v_create_trig);

PERFORM dblink_disconnect('mimeo_logdel');

-- Only create destination table if it doesn't already exist
SELECT schemaname||'.'||tablename INTO v_dest_check FROM pg_tables WHERE schemaname = v_dest_schema_name AND tablename = v_dest_table_name;
IF v_dest_check IS NULL THEN
    RAISE NOTICE 'Snapshotting source table to pull all current source data...';
    -- Snapshot the table after triggers have been created to ensure all new data after setup is replicated
    v_insert_refresh_config := 'INSERT INTO @extschema@.refresh_config_snap(source_table, dest_table, dblink) VALUES('
        ||quote_literal(p_src_table)||', '||quote_literal(p_dest_table)||', '|| p_dblink_id||')';

    EXECUTE v_insert_refresh_config;

    EXECUTE 'SELECT @extschema@.refresh_snap('||quote_literal(p_dest_table)||', p_pulldata := '||p_pulldata||')';
    PERFORM @extschema@.snapshot_destroyer(p_dest_table, 'ARCHIVE');
ELSE
    RAISE NOTICE 'Destination table % already exists. No data was pulled from source', p_dest_table;
END IF;

SELECT count(*) INTO v_col_exists FROM pg_attribute 
    WHERE attrelid = p_dest_table::regclass AND attname = 'mimeo_source_deleted' AND attisdropped = false;
IF v_col_exists < 1 THEN
    EXECUTE 'ALTER TABLE '||p_dest_table||' ADD COLUMN mimeo_source_deleted timestamptz';
ELSE
    RAISE WARNING 'Special column (mimeo_source_deleted) already exists on destination table (%)', p_dest_table;
END IF;

v_insert_refresh_config := 'INSERT INTO @extschema@.refresh_config_logdel(source_table, dest_table, dblink, control, pk_field, pk_type, last_value) VALUES('
    ||quote_literal(p_src_table)||', '||quote_literal(p_dest_table)||', '|| p_dblink_id||', '||quote_literal('@extschema@.'||v_src_table_name||'_pgq')||', '
    ||quote_literal(v_pk_field)||', '||quote_literal(v_pk_type)||', '||quote_literal(clock_timestamp())||')';
RAISE NOTICE 'Inserting data into config table';

EXECUTE v_insert_refresh_config;

EXECUTE 'SELECT set_config(''search_path'','''||v_old_search_path||''',''false'')';

RAISE NOTICE 'Done';

EXCEPTION
    WHEN OTHERS THEN
        EXECUTE 'SELECT set_config(''search_path'',''@extschema@,'||v_dblink_schema||''',''false'')';
        -- Only cleanup remote objects if replication doesn't exist at all for source table
        EXECUTE 'SELECT count(*) FROM @extschema@.refresh_config_logdel WHERE source_table = '||quote_literal(p_src_table) INTO v_exists;
        IF v_exists = 0 THEN
            IF (dblink_get_connections() @> '{mimeo_logdel}') = false THEN
                PERFORM dblink_connect('mimeo_logdel', @extschema@.auth(p_dblink_id));
            END IF;
            PERFORM dblink_exec('mimeo_logdel', 'DROP TABLE IF EXISTS @extschema@.'||v_src_table_name||'_pgq');
            PERFORM dblink_exec('mimeo_logdel', 'DROP TRIGGER IF EXISTS '||v_src_table_name||'_mimeo_trig ON '||p_src_table);
            PERFORM dblink_exec('mimeo_logdel', 'DROP FUNCTION IF EXISTS @extschema@.'||v_src_table_name||'_mimeo_queue()');
        END IF;
        IF dblink_get_connections() @> '{mimeo_logdel}' THEN
            PERFORM dblink_disconnect('mimeo_logdel');
        END IF;
        EXECUTE 'SELECT set_config(''search_path'','''||v_old_search_path||''',''false'')';
        IF v_exists = 0 THEN
            RAISE EXCEPTION 'logdel_maker() failure. No mimeo configuration found for source %. Cleaned up source table mimeo objects (queue table, function & trigger) if they existed.  SQLERRM: %', p_src_table, SQLERRM;
        ELSE
            RAISE EXCEPTION 'logdel_maker() failure. Check to see if logdel configuration for % already exists. SQLERRM: % ', p_src_table, SQLERRM;
        END IF;
END
$$;


/*
 *  DML destroyer function. Pass ARCHIVE to keep table intact.
 */
CREATE OR REPLACE FUNCTION dml_destroyer(p_dest_table text, p_archive_option text) RETURNS void
    LANGUAGE plpgsql
    AS $$
    
DECLARE

v_conns             text[];
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
    IF position('.' in v_src_table) > 0 THEN 
        v_table_name := substring(v_src_table from position('.' in v_src_table)+1);
    END IF;

    v_drop_function := 'DROP FUNCTION IF EXISTS @extschema@.'||v_table_name||'_mimeo_queue()';
    v_drop_trigger := 'DROP TRIGGER IF EXISTS '||v_table_name||'_mimeo_trig ON '||v_src_table;
    v_drop_q_table := 'DROP TABLE IF EXISTS @extschema@.'||v_table_name||'_pgq';

    RAISE NOTICE 'Removing mimeo objects from source database if they exist (trigger, function, queue table)';
    PERFORM dblink_connect('mimeo_dml_destroy', @extschema@.auth(v_dblink));
    PERFORM dblink_exec('mimeo_dml_destroy', v_drop_trigger);
    PERFORM dblink_exec('mimeo_dml_destroy', v_drop_function);
    PERFORM dblink_exec('mimeo_dml_destroy', v_drop_q_table);
    PERFORM dblink_disconnect('mimeo_dml_destroy');

    IF p_archive_option != 'ARCHIVE' THEN 
        RAISE NOTICE 'Destination table destroyed: %', v_dest_table;
        EXECUTE 'DROP TABLE IF EXISTS ' || v_dest_table;
    ELSE
        RAISE NOTICE 'Archive option set. Destination table NOT destroyed: %', v_dest_table; 
    END IF;

    RAISE NOTICE 'Removing config data';
    EXECUTE 'DELETE FROM @extschema@.refresh_config_dml WHERE dest_table = ' || quote_literal(v_dest_table);

    RAISE NOTICE 'Done';
END IF;

EXECUTE 'SELECT set_config(''search_path'','''||v_old_search_path||''',''false'')';

EXCEPTION
    WHEN OTHERS THEN
        EXECUTE 'SELECT set_config(''search_path'',''@extschema@,'||v_dblink_schema||''',''false'')';
        v_conns := dblink_get_connections();
        IF dblink_get_connections() @> '{mimeo_dml_destroy}' THEN
            PERFORM dblink_disconnect('mimeo_dml_destroy');
        END IF;
        EXECUTE 'SELECT set_config(''search_path'','''||v_old_search_path||''',''false'')';
        RAISE EXCEPTION '%', SQLERRM;    
END
$$;


/*
 *  Inserter destroyer function. Pass archive to keep table intact.
 */
CREATE OR REPLACE FUNCTION inserter_destroyer(p_dest_table text, p_archive_option text) RETURNS void
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
    -- Keep destination table
    IF p_archive_option != 'ARCHIVE' THEN 
        EXECUTE 'DROP TABLE IF EXISTS ' || v_dest_table;
        RAISE NOTICE 'Destination table destroyed: %', v_dest_table;
    ELSE
        RAISE NOTICE 'Archive option set. Destination table NOT destroyed: %', v_dest_table; 
    END IF;

    EXECUTE 'DELETE FROM @extschema@.refresh_config_inserter WHERE dest_table = ' || quote_literal(v_dest_table);	
END IF;

END
$$;


/*
 *  Updater destroyer function. Pass archive to keep table intact.
 */
CREATE OR REPLACE FUNCTION updater_destroyer(p_dest_table text, p_archive_option text) RETURNS void
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
    -- Keep destination table
    IF p_archive_option != 'ARCHIVE' THEN 
        EXECUTE 'DROP TABLE IF EXISTS ' || v_dest_table;
        RAISE NOTICE 'Destination table destroyed: %', v_dest_table;
    ELSE 
        RAISE NOTICE 'Archive option set. Destination table NOT destroyed: %', v_dest_table; 
    END IF;

    EXECUTE 'DELETE FROM @extschema@.refresh_config_updater WHERE dest_table = ' || quote_literal(v_dest_table);
END IF;

END
$$;


/*
 *  Logdel destroyer function. Pass ARCHIVE to keep table intact.
 */
CREATE OR REPLACE FUNCTION logdel_destroyer(p_dest_table text, p_archive_option text) RETURNS void
    LANGUAGE plpgsql
    AS $$
    
DECLARE

v_conns             text[];
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
    IF position('.' in v_src_table) > 0 THEN 
        v_table_name := substring(v_src_table from position('.' in v_src_table)+1);
    END IF;

    v_drop_function := 'DROP FUNCTION IF EXISTS @extschema@.'||v_table_name||'_mimeo_queue()';
    v_drop_trigger := 'DROP TRIGGER IF EXISTS '||v_table_name||'_mimeo_trig ON '||v_src_table;
    v_drop_q_table := 'DROP TABLE IF EXISTS @extschema@.'||v_table_name||'_pgq';

    RAISE NOTICE 'Removing mimeo objects from source database (trigger, function, queue table)';
    PERFORM dblink_connect('mimeo_logdel_destroy', @extschema@.auth(v_dblink));
    PERFORM dblink_exec('mimeo_logdel_destroy', v_drop_trigger);
    PERFORM dblink_exec('mimeo_logdel_destroy', v_drop_function);
    PERFORM dblink_exec('mimeo_logdel_destroy', v_drop_q_table);
    PERFORM dblink_disconnect('mimeo_logdel_destroy');

    IF p_archive_option != 'ARCHIVE' THEN 
        RAISE NOTICE 'Destination table destroyed: %', v_dest_table;
        EXECUTE 'DROP TABLE IF EXISTS ' || v_dest_table;
    ELSE
        RAISE NOTICE 'Archive option set. Destination table NOT destroyed: %', v_dest_table; 
    END IF;

    RAISE NOTICE 'Removing config data';
    EXECUTE 'DELETE FROM @extschema@.refresh_config_logdel WHERE dest_table = ' || quote_literal(v_dest_table);	

    RAISE NOTICE 'Done';
END IF;

EXECUTE 'SELECT set_config(''search_path'','''||v_old_search_path||''',''false'')';

EXCEPTION
    WHEN OTHERS THEN
        EXECUTE 'SELECT set_config(''search_path'',''@extschema@,'||v_dblink_schema||''',''false'')';
        v_conns := dblink_get_connections();
        IF dblink_get_connections() @> '{mimeo_logdel_destroy}' THEN
            PERFORM dblink_disconnect('mimeo_logdel_destroy');
        END IF;
        EXECUTE 'SELECT set_config(''search_path'','''||v_old_search_path||''',''false'')';
        RAISE EXCEPTION '%', SQLERRM;    

END
$$;


/*
 *  Inserter maker function. 
 */
CREATE FUNCTION inserter_maker(p_src_table text, p_control_field text, p_dblink_id int, p_boundary interval DEFAULT '00:10:00', p_dest_table text DEFAULT NULL, p_pulldata boolean DEFAULT true) RETURNS void
    LANGUAGE plpgsql
    AS $$
DECLARE

v_data_source               text;
v_dest_check                text;
v_dest_schema_name          text;
v_dest_table_name           text;
v_dst_active                boolean;
v_exists                    int;
v_insert_refresh_config     text;
v_max_timestamp             timestamptz;
v_snap_suffix               text;
v_view_definition           text;

BEGIN

SELECT data_source INTO v_data_source FROM @extschema@.dblink_mapping WHERE data_source_id = p_dblink_id; 
IF NOT FOUND THEN
	RAISE EXCEPTION 'ERROR: database link ID is incorrect %', p_dblink_id; 
END IF;  

IF p_dest_table IS NULL THEN
    p_dest_table := p_src_table;
END IF;

IF position('.' in p_dest_table) > 0 THEN
    v_dest_schema_name := split_part(p_dest_table, '.', 1); 
    v_dest_table_name := split_part(p_dest_table, '.', 2);
END IF;

-- Only create destination table if it doesn't already exist
SELECT schemaname||'.'||tablename INTO v_dest_check FROM pg_tables WHERE schemaname = v_dest_schema_name AND tablename = v_dest_table_name;
IF v_dest_check IS NULL THEN
    v_insert_refresh_config := 'INSERT INTO @extschema@.refresh_config_snap(source_table, dest_table, dblink) VALUES('
        ||quote_literal(p_src_table)||', '||quote_literal(p_dest_table)||', '|| p_dblink_id||')';

    RAISE NOTICE 'Snapshotting source table to pull all current source data...';
    EXECUTE v_insert_refresh_config;	

    EXECUTE 'SELECT @extschema@.refresh_snap('||quote_literal(p_dest_table)||', p_pulldata := '||p_pulldata||')';
    PERFORM @extschema@.snapshot_destroyer(p_dest_table, 'ARCHIVE');
	
    RAISE NOTICE 'Snapshot complete.';
ELSE
    RAISE NOTICE 'Destination table % already exists. No data was pulled from source', p_dest_table;
END IF;

RAISE NOTICE 'Getting the maximum destination timestamp...';
EXECUTE 'SELECT max('||p_control_field||') FROM '||p_dest_table||';' INTO v_max_timestamp;

v_dst_active := @extschema@.dst_utc_check();

v_insert_refresh_config := 'INSERT INTO @extschema@.refresh_config_inserter(source_table, dest_table, dblink, control, boundary, last_value, dst_active) VALUES('
    ||quote_literal(p_src_table)||', '||quote_literal(p_dest_table)||', '|| p_dblink_id||', '
    ||quote_literal(p_control_field)||', '||quote_literal(p_boundary)||', '||quote_literal(COALESCE(v_max_timestamp, CURRENT_TIMESTAMP))||', '||v_dst_active||');';

RAISE NOTICE 'Inserting data into config table';
EXECUTE v_insert_refresh_config;

-- Remove temp snap from config
EXECUTE 'DELETE FROM @extschema@.refresh_config_snap WHERE source_table = '||quote_literal(p_src_table)||' AND dest_table = '||quote_literal(p_dest_table);

RAISE NOTICE 'Done';

RETURN;

END
$$;


/*
 *  Snapshot maker function. Optional custom destination table name.
 */
CREATE FUNCTION snapshot_maker(p_src_table text, p_dblink_id int, p_dest_table text DEFAULT NULL, p_pulldata boolean DEFAULT true) RETURNS void
    LANGUAGE plpgsql
    AS $$
DECLARE

v_data_source               text;
v_insert_refresh_config     text;

v_sql text;

BEGIN

SELECT data_source INTO v_data_source FROM @extschema@.dblink_mapping WHERE data_source_id = p_dblink_id; 
IF NOT FOUND THEN
	RAISE EXCEPTION 'ERROR: Database link ID does not exist in @extschema@.dblink_mapping: %', p_dblink_id; 
END IF;  

IF p_dest_table IS NULL THEN
    p_dest_table := p_src_table;
END IF;

v_insert_refresh_config := 'INSERT INTO @extschema@.refresh_config_snap(source_table, dest_table, dblink) VALUES('
    ||quote_literal(p_src_table)||', '||quote_literal(p_dest_table)||', '|| p_dblink_id||');';


RAISE NOTICE 'Inserting record in @extschema@.refresh_config';
EXECUTE v_insert_refresh_config;	
RAISE NOTICE 'Insert successful';	

RAISE NOTICE 'attempting first snapshot';
EXECUTE 'SELECT @extschema@.refresh_snap('||quote_literal(p_dest_table)||', p_pulldata := '||p_pulldata||')'; 

RAISE NOTICE 'attempting second snapshot';
EXECUTE 'SELECT @extschema@.refresh_snap('||quote_literal(p_dest_table)||', p_pulldata := '||p_pulldata||')';

RAISE NOTICE 'all done';

RETURN;

END
$$;
