-- Added new updater_maker and updater_destroyer functions. Also added support for composite keys in refresh_updater function.

/*
 *  Updater maker function. Accepts custom destination name.
 */
CREATE FUNCTION updater_maker(p_src_table text, p_dest_table text, p_control_field text, p_dblink_id int, p_pk_field text[], p_pk_type text[], p_boundary text DEFAULT '''10mins''::interval') RETURNS void
    LANGUAGE plpgsql
    AS $_$
declare
v_insert_refresh_config          text;
v_update_refresh_config          text;
v_max_timestamp			 timestamptz;
v_data_source			 text;
v_exists            		 int;
v_snap_suffix       		 text;
v_view_definition   		 text;
v_pk_field_csv			 text;
v_pk_type_csv			 text;
v_primary_key			 text;
v_alter_table			 text;

BEGIN
	SELECT data_source INTO v_data_source FROM @extschema@.dblink_mapping WHERE data_source_id = p_dblink_id; 
	IF NOT FOUND THEN
   		RAISE EXCEPTION 'ERROR: database link ID is incorrect %', p_dblink_id; 
	END IF;  

	v_pk_field_csv := ''''||array_to_string(p_pk_field,''',''')||'''';
	v_pk_type_csv := ''''||array_to_string(p_pk_type,''',''')||'''';
	v_primary_key := array_to_string(p_pk_field,',');

	v_insert_refresh_config := 'INSERT INTO @extschema@.refresh_config(source_table, dest_table, type, dblink, control, boundary, pk_field, pk_type) VALUES('||quote_literal(p_src_table)||', '||quote_literal(p_dest_table)||',''snap'', '|| p_dblink_id||', '||quote_literal(p_control_field)||', '||p_boundary||', ARRAY['||v_pk_field_csv||'], ARRAY['||v_pk_type_csv||']);';

	RAISE NOTICE 'Inserting record in @extschema@.refresh_config';
	EXECUTE v_insert_refresh_config;	
	RAISE NOTICE 'Insert successful';	

	RAISE NOTICE 'attempting snapshot';
	PERFORM @extschema@.refresh_snap(p_dest_table, FALSE);

	RAISE NOTICE 'attempting to destroy snapshot';

	SELECT definition INTO v_view_definition FROM pg_views WHERE schemaname || '.' || viewname = p_dest_table;
    	v_exists := strpos(v_view_definition, 'snap1');
    	IF v_exists > 0 THEN
        	v_snap_suffix := 'snap1';
    	END IF;
    
    	EXECUTE 'DROP VIEW ' || p_dest_table;
    	EXECUTE 'CREATE TABLE ' || p_dest_table || ' AS SELECT * FROM ' || p_dest_table || '_' || v_snap_suffix;
	EXECUTE 'DROP TABLE ' || p_dest_table || '_snap1';

	RAISE NOTICE 'Destroyed successfully';

	v_alter_table := 'ALTER TABLE '||p_dest_table||' ADD PRIMARY KEY('||v_primary_key||');';

	RAISE NOTICE 'Adding primary key constraint to table';
	EXECUTE v_alter_table;
	RAISE NOTICE 'Constraint added successfully';

	RAISE NOTICE 'Taking the maximum timestamp';
	EXECUTE 'SELECT max('||p_control_field||') FROM '||p_dest_table||';' INTO v_max_timestamp;
	RAISE NOTICE 'The select statement ran successfully.';

	v_update_refresh_config := 'UPDATE @extschema@.refresh_config SET (type, last_value) = (''inserter'', '||quote_literal(v_max_timestamp)||') WHERE dest_table = '||quote_literal(p_src_table)||';';

	RAISE NOTICE 'Updating config table with highest timestamp value';
	EXECUTE v_update_refresh_config;
	RAISE NOTICE 'Update successful';
	
	RAISE NOTICE 'All Done';

	RETURN;
END
$_$;

/*
 *  Updater maker function. Assumes source and destination are the same tablename.
 */
CREATE FUNCTION updater_maker(p_src_table text, p_control_field text, p_dblink_id int, p_pk_field text[], p_pk_type text[], p_boundary text DEFAULT '''10mins''::interval') RETURNS void
    LANGUAGE plpgsql
    AS $_$
declare
v_insert_refresh_config          text;
v_update_refresh_config          text;
v_max_timestamp			 timestamptz;
v_data_source			 text;
v_exists            		 int;
v_snap_suffix       		 text;
v_view_definition   		 text;
v_pk_field_csv			 text;
v_pk_type_csv			 text;
v_primary_key			 text;
v_alter_table			 text;

BEGIN
	SELECT data_source INTO v_data_source FROM @extschema@.dblink_mapping WHERE data_source_id = p_dblink_id; 
	IF NOT FOUND THEN
   		RAISE EXCEPTION 'ERROR: database link ID is incorrect %', p_dblink_id; 
	END IF;  

	v_pk_field_csv := ''''||array_to_string(p_pk_field,''',''')||'''';
	v_pk_type_csv := ''''||array_to_string(p_pk_type,''',''')||'''';
	v_primary_key := array_to_string(p_pk_field,',');

	v_insert_refresh_config := 'INSERT INTO @extschema@.refresh_config(source_table, dest_table, type, dblink, control, boundary, pk_field, pk_type) VALUES('||quote_literal(p_src_table)||', '||quote_literal(p_src_table)||',''snap'', '|| p_dblink_id||', '||quote_literal(p_control_field)||', '||p_boundary||', ARRAY['||v_pk_field_csv||'], ARRAY['||v_pk_type_csv||']);';

	RAISE NOTICE 'Inserting record in @extschema@.refresh_config';
	EXECUTE v_insert_refresh_config;	
	RAISE NOTICE 'Insert successful';	

	RAISE NOTICE 'attempting snapshot';
	PERFORM @extschema@.refresh_snap(p_src_table, FALSE);

	RAISE NOTICE 'attempting to destroy snapshot';

	SELECT definition INTO v_view_definition FROM pg_views WHERE schemaname || '.' || viewname = p_src_table;
    	v_exists := strpos(v_view_definition, 'snap1');
    	IF v_exists > 0 THEN
        	v_snap_suffix := 'snap1';
    	END IF;
    
    	EXECUTE 'DROP VIEW ' || p_src_table;
    	EXECUTE 'CREATE TABLE ' || p_src_table || ' AS SELECT * FROM ' || p_src_table || '_' || v_snap_suffix;
	EXECUTE 'DROP TABLE ' || p_src_table || '_snap1';

	RAISE NOTICE 'Destroyed successfully';

	v_alter_table := 'ALTER TABLE '||p_src_table||' ADD PRIMARY KEY('||v_primary_key||');';

	RAISE NOTICE 'Adding primary key constraint to table';
	EXECUTE v_alter_table;
	RAISE NOTICE 'Constraint added successfully';

	RAISE NOTICE 'Taking the maximum timestamp';
	EXECUTE 'SELECT max('||p_control_field||') FROM '||p_src_table||';' INTO v_max_timestamp;
	RAISE NOTICE 'The select statement ran successfully.';

	v_update_refresh_config := 'UPDATE @extschema@.refresh_config SET (type, last_value) = (''inserter'', '||quote_literal(v_max_timestamp)||') WHERE dest_table = '||quote_literal(p_src_table)||';';

	RAISE NOTICE 'Updating config table with highest timestamp value';
	EXECUTE v_update_refresh_config;
	RAISE NOTICE 'Update successful';
	
	RAISE NOTICE 'All Done';

	RETURN;
END
$_$;

/*
 *  Updater destroyer function. Pass archive to keep table intact.
 */
CREATE FUNCTION updater_destroyer(p_dest_table text, p_archive_option text) RETURNS void
    LANGUAGE plpgsql
    AS $_$
    
DECLARE
    v_dest_table        text;
    v_src_table         text;
    
BEGIN

SELECT source_table, dest_table INTO v_src_table, v_dest_table
    FROM @extschema@.refresh_config WHERE dest_table = p_dest_table;
IF NOT FOUND THEN
    RAISE EXCEPTION 'This table is not set up for updater replication: %', v_dest_table;
END IF;

-- Deletes entry in config and keeps the replicated table intact.
IF p_archive_option = 'ARCHIVE' THEN 

    EXECUTE 'DELETE FROM @extschema@.refresh_config WHERE dest_table = ' || quote_literal(v_dest_table);

ELSE

EXECUTE 'DROP TABLE ' || v_dest_table;

EXECUTE 'DELETE FROM @extschema@.refresh_config WHERE dest_table = ' || quote_literal(v_dest_table);

END IF;

END
$_$;

/*
 *  Refresh insert/update only table based on timestamp control field
 */
CREATE FUNCTION refresh_updater(p_destination text, p_debug boolean, integer DEFAULT 100000) RETURNS void
    LANGUAGE plpgsql SECURITY DEFINER
    AS $_$
declare
v_job_name          text;
v_job_id            int;
v_step_id           int;
v_rowcount          bigint; 
v_dblink_schema     text;
v_jobmon_schema     text;
v_old_search_path   text;
v_adv_lock          boolean;

v_source_table      text;
v_dest_table        text;
v_tmp_table         text;
v_dblink            text;
v_control           text;
v_last_value_sql    text; 
v_last_value        timestamptz; 
v_boundry           timestamptz;
v_remote_boundry      timestamptz;
v_pk_field          text[];
v_pk_type           text[];
v_pk_counter        int := 2;
v_pk_field_csv      text;
v_with_update       text;
v_field             text;
v_filter            text[];
v_cols              text;
v_cols_n_types      text;
v_pk_where          text;

v_trigger_update    text;
v_trigger_delete    text; 
v_exec_status       text;

v_remote_sql      text;
v_remote_f_sql      text;
v_insert_sql        text;
v_create_sql      text;
v_create_f_sql      text;
v_delete_sql        text;
v_boundry_sql       text;
v_remote_boundry_sql        text;

BEGIN

IF p_debug IS DISTINCT FROM true THEN
    PERFORM set_config( 'client_min_messages', 'warning', true );
END IF;

v_job_name := 'Refresh Updater: '||p_destination;

SELECT nspname INTO v_dblink_schema FROM pg_namespace n, pg_extension e WHERE e.extname = 'dblink' AND e.extnamespace = n.oid;
SELECT nspname INTO v_jobmon_schema FROM pg_namespace n, pg_extension e WHERE e.extname = 'pg_jobmon' AND e.extnamespace = n.oid;

-- Set custom search path to allow easier calls to other functions, especially job logging
SELECT current_setting('search_path') INTO v_old_search_path;
EXECUTE 'SELECT set_config(''search_path'',''@extschema@,'||v_jobmon_schema||','||v_dblink_schema||''',''false'')';


v_job_id := add_job(v_job_name);
PERFORM gdb(p_debug,'Job ID: '||v_job_id::text);

-- Take advisory lock to prevent multiple calls to function overlapping
v_adv_lock := pg_try_advisory_lock(hashtext('refresh_updater'), hashtext(v_job_name));
IF v_adv_lock = 'false' THEN
    v_step_id := add_step(v_job_id,'Obtaining advisory lock for job: '||v_job_name);
    PERFORM gdb(p_debug,'Obtaining advisory lock FAILED for job: '||v_job_name);
    PERFORM update_step(v_step_id, 'OK','Found concurrent job. Exiting gracefully');
    PERFORM close_job(v_job_id);
    RETURN;
END IF;

-- grab boundry
v_step_id := add_step(v_job_id,'Grabbing Boundries, Building SQL');

SELECT source_table, dest_table, 'tmp_'||replace(dest_table,'.','_'), dblink, control, last_value, now() - boundary::interval, pk_field, pk_type, filter FROM refresh_config
WHERE dest_table = p_destination INTO v_source_table, v_dest_table, v_tmp_table, v_dblink, v_control, v_last_value, v_boundry, v_pk_field, v_pk_type, v_filter;
IF NOT FOUND THEN
   RAISE EXCEPTION 'ERROR: no mapping found for %',v_job_name;
END IF;

-- determine column list, column type list
IF v_filter IS NULL THEN 
    SELECT array_to_string(array_agg(attname),','), array_to_string(array_agg(attname||' '||atttypid::regtype::text),',') FROM 
        pg_attribute WHERE attnum > 0 AND attisdropped is false AND attrelid = p_destination::regclass INTO v_cols, v_cols_n_types;
ELSE
    -- ensure all primary key columns are included in any column filters
    FOREACH v_field IN ARRAY v_pk_field LOOP
        IF v_field = ANY(v_filter) THEN
            CONTINUE;
        ELSE
            RAISE EXCEPTION 'ERROR: filter list did not contain all columns that compose primary key for %',v_job_name; 
        END IF;
    END LOOP;
    SELECT array_to_string(array_agg(attname),','), array_to_string(array_agg(attname||' '||atttypid::regtype::text),',') FROM 
        (SELECT unnest(filter) FROM refresh_config WHERE dest_table = p_destination) x 
         JOIN pg_attribute ON (unnest=attname::text AND attrelid=p_destination::regclass) INTO v_cols, v_cols_n_types;
END IF;    

PERFORM update_step(v_step_id, 'OK','Initial boundary from '||v_last_value::text||' to '||v_boundry::text);

-- Find boundary that will limit to ~ 50k rows 

v_remote_boundry_sql := 'SELECT max(' || v_control || ') as i FROM (SELECT * FROM '||v_source_table||' WHERE '||v_control||' > '||quote_literal(v_last_value)||' AND '||v_control||' <= '||quote_literal(v_boundry) || ' ORDER BY '||v_control||' ASC LIMIT '|| $3 ||' ) as x';

v_boundry_sql := 'SELECT i FROM dblink(auth('||v_dblink||'),'||quote_literal(v_remote_boundry_sql)||') t (i timestamptz)';

SELECT add_step(v_job_id,'Getting real boundary') INTO v_step_id;
    perform gdb(p_debug,v_boundry_sql);
    execute v_boundry_sql INTO v_remote_boundry;

PERFORM update_step(v_step_id, 'OK','Real boundary: ' || coalesce( v_remote_boundry, v_boundry ) || ' ' || ( v_boundry - coalesce( v_remote_boundry, v_boundry ) ) );

    v_boundry := coalesce( v_remote_boundry, v_boundry );

-- init sql statements 

v_remote_sql := 'SELECT '||v_cols||' FROM '||v_source_table||' WHERE '||v_control||' > '||quote_literal(v_last_value)||' AND '||v_control||' <= '||quote_literal(v_boundry);

v_create_sql := 'CREATE TEMP TABLE '||v_tmp_table||' AS SELECT '||v_cols||' FROM dblink(auth('||v_dblink||'),'||quote_literal(v_remote_sql)||') t ('||v_cols_n_types||')';

v_delete_sql := 'DELETE FROM '||v_dest_table||' USING '||v_tmp_table||' t WHERE '||v_dest_table||'.'||v_pk_field[1]||'=t.'||v_pk_field[1]; 

IF array_length(v_pk_field, 1) > 1 THEN
    v_pk_where := '';
    WHILE v_pk_counter <= array_length(v_pk_field,1) LOOP
        v_pk_where := v_pk_where || ' AND '||v_dest_table||'.'||v_pk_field[v_pk_counter]||' = t.'||v_pk_field[v_pk_counter];
        v_pk_counter := v_pk_counter + 1;
    END LOOP;
END IF;

IF v_pk_where IS NOT NULL THEN
    v_delete_sql := v_delete_sql || v_pk_where;
END IF; 

v_insert_sql := 'INSERT INTO '||v_dest_table||'('||v_cols||') SELECT '||v_cols||' FROM '||v_tmp_table; 

-- create temp from remote
SELECT add_step(v_job_id,'Creating temp table ('||v_tmp_table||') from remote table') INTO v_step_id;
    perform gdb(p_debug,v_create_sql);
    execute v_create_sql;     
    GET DIAGNOSTICS v_rowcount = ROW_COUNT;
PERFORM update_step(v_step_id, 'OK','Table contains '||v_rowcount||' records');

-- delete (update)
SELECT add_step(v_job_id,'Updating records in local table') INTO v_step_id;
    perform gdb(p_debug,v_delete_sql);
    execute v_delete_sql; 
    GET DIAGNOSTICS v_rowcount = ROW_COUNT;
PERFORM update_step(v_step_id, 'OK','Updated '||v_rowcount||' records');

-- insert
SELECT add_step(v_job_id,'Inserting new records into local table') INTO v_step_id;
    perform gdb(p_debug,v_insert_sql);
    execute v_insert_sql; 
    GET DIAGNOSTICS v_rowcount = ROW_COUNT;
PERFORM update_step(v_step_id, 'OK','Inserted '||v_rowcount||' records');

-- update activity status
v_step_id := add_step(v_job_id,'Updating last_value in config table');
    v_last_value_sql := 'UPDATE refresh_config SET last_value = '|| quote_literal(v_boundry) ||' WHERE dest_table = ' ||quote_literal(p_destination); 
    PERFORM gdb(p_debug,v_last_value_sql);
    EXECUTE v_last_value_sql; 
PERFORM update_step(v_step_id, 'OK','Last Value was '||quote_literal(v_boundry));

EXECUTE 'DROP TABLE IF EXISTS '||v_tmp_table;

PERFORM close_job(v_job_id);

-- Ensure old search path is reset for the current session
EXECUTE 'SELECT set_config(''search_path'','''||v_old_search_path||''',''false'')';

PERFORM pg_advisory_unlock(hashtext('refresh_updater'), hashtext(v_job_name));

EXCEPTION
    WHEN others THEN
        -- Exception block resets path, so have to reset it again
        EXECUTE 'SELECT set_config(''search_path'',''@extschema@,'||v_jobmon_schema||','||v_dblink_schema||''',''false'')';
        PERFORM update_step(v_step_id, 'BAD', 'ERROR: '||coalesce(SQLERRM,'unknown'));
        PERFORM fail_job(v_job_id);

        -- Ensure old search path is reset for the current session
       EXECUTE 'SELECT set_config(''search_path'','''||v_old_search_path||''',''false'')';

        PERFORM pg_advisory_unlock(hashtext('refresh_updater'), hashtext(v_job_name));
        RAISE EXCEPTION '%', SQLERRM;
END
$_$;

