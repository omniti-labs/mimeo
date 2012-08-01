-- Fix inserter/updater timestamp based refresh to be able to handle DST for servers not running in GMT/UTC
-- IMPORTANT NOTE: All jobs made before this update will default to the dst_active config option being true.
--      BE SURE TO CHECK YOUR CONFIGURATION SO IT IS SET ACCORDINGLY! I set it to true to ensure data isn't missed by accident for existing jobs.
--      But this will cause replication to stop during DST time changes. Please plan accordinly.
--      Any new jobs created using the inserter/updater maker functions will set the dst_active option based on the result of the dst_utc_check() function.

ALTER TABLE @extschema@.refresh_config_inserter ADD COLUMN dst_active boolean NOT NULL DEFAULT true;
ALTER TABLE @extschema@.refresh_config_inserter ADD COLUMN dst_start int NOT NULL DEFAULT 30;
ALTER TABLE @extschema@.refresh_config_inserter ADD COLUMN dst_end int NOT NULL DEFAULT 230;

ALTER TABLE @extschema@.refresh_config_updater ADD COLUMN dst_active boolean NOT NULL DEFAULT true;
ALTER TABLE @extschema@.refresh_config_updater ADD COLUMN dst_start int NOT NULL DEFAULT 30;
ALTER TABLE @extschema@.refresh_config_updater ADD COLUMN dst_end int NOT NULL DEFAULT 230;

CREATE FUNCTION dst_change(date timestamp with time zone) RETURNS boolean
    LANGUAGE sql
    AS $$ 
    SELECT to_char( date_trunc('day', $1) , 'TZ' ) <> to_char( date_trunc( 'day', $1 ) + '1 day'::interval, 'TZ' ); 
$$;

CREATE FUNCTION dst_utc_check() RETURNS boolean
    LANGUAGE sql
    AS $$
    SELECT to_char( date_trunc('day', now()) , 'TZ' ) <> 'UTC' AND to_char( date_trunc('day', now()) , 'TZ' ) <> 'GMT';
$$;


/*
 *  Refresh insert only table based on timestamp control field
 */
CREATE OR REPLACE FUNCTION refresh_inserter(p_destination text, p_debug boolean, integer DEFAULT 100000) RETURNS void
    LANGUAGE plpgsql SECURITY DEFINER
    AS $$
DECLARE

v_adv_lock          boolean; 
v_boundary          timestamptz;
v_cols_n_types      text;
v_cols              text;
v_control           text;
v_create_sql        text;
v_dblink_schema     text;
v_dblink            text;
v_dest_table        text;
v_dst_active        boolean;
v_dst_check         boolean;
v_dst_start         int;
v_dst_end           int;
v_filter            text[]; 
v_insert_sql        text;
v_job_id            int;
v_jobmon_schema     text;
v_job_name          text;
v_last_value_sql    text;
v_last_value        timestamptz;
v_now               timestamptz := now();
v_old_search_path   text;
v_remote_sql        text;
v_rowcount          bigint; 
v_source_table      text;
v_step_id           int;
v_tmp_table         text;

BEGIN

IF p_debug IS DISTINCT FROM true THEN
    PERFORM set_config( 'client_min_messages', 'warning', true );
END IF;

v_job_name := 'Refresh Inserter: '||p_destination;

SELECT nspname INTO v_dblink_schema FROM pg_namespace n, pg_extension e WHERE e.extname = 'dblink' AND e.extnamespace = n.oid;
SELECT nspname INTO v_jobmon_schema FROM pg_namespace n, pg_extension e WHERE e.extname = 'pg_jobmon' AND e.extnamespace = n.oid;

-- Set custom search path to allow easier calls to other functions, especially job logging
SELECT current_setting('search_path') INTO v_old_search_path;
EXECUTE 'SELECT set_config(''search_path'',''@extschema@,'||v_jobmon_schema||','||v_dblink_schema||''',''false'')';

v_job_id := add_job(v_job_name);
PERFORM gdb(p_debug,'Job ID: '||v_job_id::text);

-- Take advisory lock to prevent multiple calls to function overlapping
v_adv_lock := pg_try_advisory_lock(hashtext('refresh_inserter'), hashtext(v_job_name));
IF v_adv_lock = 'false' THEN
    v_step_id := add_step(v_job_id,'Obtaining advisory lock for job: '||v_job_name);
    PERFORM gdb(p_debug,'Obtaining advisory lock FAILED for job: '||v_job_name);
    PERFORM update_step(v_step_id, 'OK','Found concurrent job. Exiting gracefully');
    PERFORM close_job(v_job_id);
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
    , dst_active
    , dst_start
    , dst_end 
FROM refresh_config_inserter WHERE dest_table = p_destination 
INTO v_source_table, v_dest_table, v_tmp_table, v_dblink, v_control
    , v_last_value, v_boundary, v_filter, v_dst_active, v_dst_start, v_dst_end; 
IF NOT FOUND THEN
   RAISE EXCEPTION 'ERROR: no mapping found for %',v_job_name; 
END IF;  

-- Do not allow this function to run during DST time change if config option is true. Otherwise will miss data from source
IF v_dst_active THEN
    v_dst_check := @extschema@.dst_change(v_now);
    IF v_dst_check THEN 
        IF to_number(to_char(v_now, 'HH24MM'), '0000') > v_dst_start AND to_number(to_char(v_now, 'HH24MM'), '0000') < v_dst_end THEN
            v_step_id := jobmon.add_step( v_job_id, 'DST Check');
            PERFORM jobmon.update_step(v_step_id, 'OK', 'Job CANCELLED - Does not run during DST time change');
            PERFORM jobmon.close_job(v_job_id);
            PERFORM gdb(p_debug, 'Cannot run during DST time change');
            EXECUTE 'SELECT set_config(''search_path'','''||v_old_search_path||''',''false'')';
            PERFORM pg_advisory_unlock(hashtext('refresh_inserter'), hashtext(v_job_name));
            RETURN;
        END IF;
    END IF;
END IF;

v_step_id := add_step(v_job_id,'Building SQL');

IF v_filter IS NULL THEN 
    SELECT array_to_string(array_agg(attname),','), array_to_string(array_agg(attname||' '||atttypid::regtype::text),',') FROM 
    pg_attribute WHERE attnum > 0 AND attisdropped is false AND attrelid = p_destination::regclass INTO v_cols, v_cols_n_types;
ELSE
    SELECT array_to_string(array_agg(attname),','), array_to_string(array_agg(attname||' '||atttypid::regtype::text),',') FROM 
        (SELECT unnest(filter) FROM @extschema@.refresh_config_inserter WHERE dest_table = p_destination) x 
         JOIN pg_attribute ON (unnest=attname::text AND attrelid=p_destination::regclass) INTO v_cols, v_cols_n_types;
END IF;    

-- init sql statements 

-- does < for upper boundary to keep missing data from happening on rare edge case where a newly inserted row outside the transaction batch
-- has the exact same timestamp as the previous batch's max timestamp
-- Note that this means the destination table may always be at least one row behind even when no new data is entered on the source.
v_remote_sql := 'SELECT '||v_cols||' FROM '||v_source_table||' WHERE '||v_control||' > '||quote_literal(v_last_value)||' AND '||v_control||' < '||quote_literal(v_boundary)||' ORDER BY '||v_control||' ASC LIMIT '|| $3;

v_create_sql := 'CREATE TEMP TABLE '||v_tmp_table||' AS SELECT '||v_cols||' FROM dblink(auth('||v_dblink||'),'||quote_literal(v_remote_sql)||') t ('||v_cols_n_types||')';

v_insert_sql := 'INSERT INTO '||v_dest_table||'('||v_cols||') SELECT '||v_cols||' FROM '||v_tmp_table; 

v_last_value_sql := 'SELECT max('||v_control||') FROM '||v_tmp_table;

PERFORM update_step(v_step_id, 'OK','Grabbing rows from '||v_last_value::text||' to '||v_boundary::text);

-- create temp from remote
v_step_id := add_step(v_job_id,'Creating temp table ('||v_tmp_table||') from remote table');
    PERFORM gdb(p_debug,v_create_sql);
    EXECUTE v_create_sql; 
    GET DIAGNOSTICS v_rowcount = ROW_COUNT;
    -- Do nothing if no new rows
    IF v_rowcount < 1 THEN 
        PERFORM update_step(v_step_id, 'OK','No new rows found');
        EXECUTE 'DROP TABLE IF EXISTS ' || v_tmp_table;
        PERFORM close_job(v_job_id);
        PERFORM gdb(p_debug, 'No new rows found');
        -- Ensure old search path is reset for the current session
        EXECUTE 'SELECT set_config(''search_path'','''||v_old_search_path||''',''false'')';
        PERFORM pg_advisory_unlock(hashtext('refresh_inserter'), hashtext(v_job_name));
        RETURN;
    END IF;
    PERFORM update_step(v_step_id, 'OK','Table contains '||v_rowcount||' records');
    PERFORM gdb(p_debug, v_rowcount || ' rows added to temp table');

v_step_id := add_step(v_job_id, 'Getting max control field value');
    PERFORM gdb(p_debug, v_last_value_sql);
    EXECUTE v_last_value_sql INTO v_last_value;
    PERFORM update_step(v_step_id, 'OK','Max value is: '||v_last_value);
    PERFORM gdb(p_debug, 'Max value is: '||v_last_value);

-- insert
v_step_id := add_step(v_job_id,'Inserting new records into local table');
    PERFORM gdb(p_debug,v_insert_sql);
    EXECUTE v_insert_sql; 
    GET DIAGNOSTICS v_rowcount = ROW_COUNT;
    PERFORM update_step(v_step_id, 'OK','Inserted '||v_rowcount||' records');
    PERFORM gdb(p_debug, v_rowcount || ' rows added to ' || v_dest_table);

-- update boundries
v_step_id := add_step(v_job_id,'Updating boundary values');
UPDATE refresh_config_inserter set last_value = v_last_value WHERE dest_table = p_destination;  
PERFORM update_step(v_step_id, 'OK','Done');

EXECUTE 'DROP TABLE IF EXISTS ' || v_tmp_table;

PERFORM close_job(v_job_id);

-- Ensure old search path is reset for the current session
EXECUTE 'SELECT set_config(''search_path'','''||v_old_search_path||''',''false'')';

PERFORM pg_advisory_unlock(hashtext('refresh_inserter'), hashtext(v_job_name));

EXCEPTION
    WHEN OTHERS THEN
        EXECUTE 'SELECT set_config(''search_path'',''@extschema@,'||v_jobmon_schema||','||v_dblink_schema||''',''false'')';
        IF v_step_id IS NULL THEN
            v_step_id := jobmon.add_step(v_job_id, 'EXCEPTION before first step logged');
        END IF;
        PERFORM update_step(v_step_id, 'BAD', 'ERROR: '||coalesce(SQLERRM,'unknown'));
        PERFORM fail_job(v_job_id);

        -- Ensure old search path is reset for the current session
        EXECUTE 'SELECT set_config(''search_path'','''||v_old_search_path||''',''false'')';

        PERFORM pg_advisory_unlock(hashtext('refresh_inserter'), hashtext(v_job_name));
        RAISE EXCEPTION '%', SQLERRM;    
END
$$;


/*
 *  Refresh insert/update only table based on timestamp control field
 */
CREATE OR REPLACE FUNCTION refresh_updater(p_destination text, p_debug boolean, integer DEFAULT 100000) RETURNS void
    LANGUAGE plpgsql SECURITY DEFINER
    AS $$
DECLARE

v_adv_lock              boolean;
v_boundry_sql           text;
v_boundry               timestamptz;
v_cols_n_types          text;
v_cols                  text;
v_control               text;
v_create_sql            text;
v_dblink_schema         text;
v_dblink                text;
v_delete_sql            text;
v_dest_table            text;
v_dst_active            boolean;
v_dst_check             boolean;
v_dst_start             int;
v_dst_end               int;
v_field                 text;
v_filter                text[];
v_insert_sql            text;
v_job_id                int;
v_jobmon_schema         text;
v_job_name              text;
v_last_value_sql        text; 
v_last_value            timestamptz;
v_now                   timestamptz := now(); 
v_old_search_path       text;
v_pk_counter            int := 2;
v_pk_field              text[];
v_pk_type               text[];
v_pk_where              text;
v_remote_boundry_sql    text;
v_remote_boundry        timestamptz;
v_remote_sql            text;
v_rowcount              bigint; 
v_source_table          text;
v_step_id               int;
v_tmp_table             text;

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
SELECT source_table
    , dest_table
    , 'tmp_'||replace(dest_table,'.','_')
    , dblink, control
    , last_value
    , now() - boundary::interval
    , pk_field
    , pk_type
    , filter
    , dst_active
    , dst_start
    , dst_end  
FROM refresh_config_updater
WHERE dest_table = p_destination INTO v_source_table, v_dest_table, v_tmp_table, v_dblink, v_control, 
    v_last_value, v_boundry, v_pk_field, v_pk_type, v_filter, v_dst_active, v_dst_start, v_dst_end;
IF NOT FOUND THEN
   RAISE EXCEPTION 'ERROR: no mapping found for %',v_job_name;
END IF;

-- Do not allow this function to run during DST time change if config option is true. Otherwise will miss data from source
IF v_dst_active THEN
    v_dst_check := @extschema@.dst_change(v_now);
    IF v_dst_check THEN 
        IF to_number(to_char(v_now, 'HH24MM'), '0000') > v_dst_start AND to_number(to_char(v_now, 'HH24MM'), '0000') < v_dst_end THEN
            v_step_id := jobmon.add_step( v_job_id, 'DST Check');
            PERFORM jobmon.update_step(v_step_id, 'OK', 'Job CANCELLED - Does not run during DST time change');
            PERFORM jobmon.close_job(v_job_id);
            PERFORM gdb(p_debug, 'Cannot run during DST time change');
            EXECUTE 'SELECT set_config(''search_path'','''||v_old_search_path||''',''false'')';
            PERFORM pg_advisory_unlock(hashtext('refresh_updater'), hashtext(v_job_name));
            RETURN;
        END IF;
    END IF;
END IF;

v_step_id := add_step(v_job_id,'Building SQL');

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
        (SELECT unnest(filter) FROM refresh_config_updater WHERE dest_table = p_destination) x 
         JOIN pg_attribute ON (unnest=attname::text AND attrelid=p_destination::regclass) INTO v_cols, v_cols_n_types;
END IF;    

PERFORM update_step(v_step_id, 'OK','Initial boundary from '||v_last_value::text||' to '||v_boundry::text);

-- Find boundary that will limit to optional limit argument rows 

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
v_step_id := add_step(v_job_id,'Creating temp table ('||v_tmp_table||') from remote table');
    perform gdb(p_debug,v_create_sql);
    execute v_create_sql;     
    GET DIAGNOSTICS v_rowcount = ROW_COUNT;
    IF v_rowcount < 1 THEN 
        PERFORM update_step(v_step_id, 'OK','No new rows found');
        EXECUTE 'DROP TABLE IF EXISTS ' || v_tmp_table;
        PERFORM close_job(v_job_id);
        PERFORM gdb(p_debug, 'No new rows found');
        -- Ensure old search path is reset for the current session
        EXECUTE 'SELECT set_config(''search_path'','''||v_old_search_path||''',''false'')';
        PERFORM pg_advisory_unlock(hashtext('refresh_updater'), hashtext(v_job_name));
        RETURN;
    END IF;
PERFORM update_step(v_step_id, 'OK','Table contains '||v_rowcount||' records');

-- delete (update)
v_step_id := add_step(v_job_id,'Updating records in local table');
    perform gdb(p_debug,v_delete_sql);
    execute v_delete_sql; 
    GET DIAGNOSTICS v_rowcount = ROW_COUNT;
PERFORM update_step(v_step_id, 'OK','Updated '||v_rowcount||' records');

-- insert
v_step_id := add_step(v_job_id,'Inserting new records into local table');
    perform gdb(p_debug,v_insert_sql);
    execute v_insert_sql; 
    GET DIAGNOSTICS v_rowcount = ROW_COUNT;
PERFORM update_step(v_step_id, 'OK','Inserted '||v_rowcount||' records');

-- update activity status
v_step_id := add_step(v_job_id,'Updating last_value in config table');
    v_last_value_sql := 'UPDATE refresh_config_updater SET last_value = '|| quote_literal(v_boundry) ||' WHERE dest_table = ' ||quote_literal(p_destination); 
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
        IF v_step_id IS NULL THEN
            v_step_id := jobmon.add_step(v_job_id, 'EXCEPTION before first step logged');
        END IF;
        PERFORM update_step(v_step_id, 'BAD', 'ERROR: '||coalesce(SQLERRM,'unknown'));
        PERFORM fail_job(v_job_id);

        -- Ensure old search path is reset for the current session
       EXECUTE 'SELECT set_config(''search_path'','''||v_old_search_path||''',''false'')';

        PERFORM pg_advisory_unlock(hashtext('refresh_updater'), hashtext(v_job_name));
        RAISE EXCEPTION '%', SQLERRM;
END
$$;


/*
 *  Inserter maker function. Assumes source and destination are the same tablename.
 */
CREATE OR REPLACE FUNCTION inserter_maker(p_src_table text, p_control_field text, p_dblink_id int, p_boundary interval DEFAULT '00:10:00') RETURNS void
    LANGUAGE plpgsql
    AS $$
DECLARE

v_data_source               text;
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

-- Temp snap config
v_insert_refresh_config := 'INSERT INTO @extschema@.refresh_config_snap(source_table, dest_table, dblink) VALUES('
    ||quote_literal(p_src_table)||', '||quote_literal(p_src_table)||', '|| p_dblink_id||')';

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

RAISE NOTICE 'Taking the maximum timestamp';
EXECUTE 'SELECT max('||p_control_field||') FROM '||p_src_table||';' INTO v_max_timestamp;
RAISE NOTICE 'The select statement ran successfully.';

v_dst_active := @extschema@.dst_utc_check();

v_insert_refresh_config := 'INSERT INTO @extschema@.refresh_config_inserter(source_table, dest_table, dblink, control, boundary, last_value, dst_active) VALUES('
    ||quote_literal(p_src_table)||', '||quote_literal(p_src_table)||', '|| p_dblink_id||', '
    ||quote_literal(p_control_field)||', '''||p_boundary||'''::interval,'''||v_max_timestamp||'''::timestamptz, '||v_dst_active||');';

RAISE NOTICE 'Updating config table with highest timestamp value';
EXECUTE v_insert_refresh_config;
RAISE NOTICE 'Update successful';

-- Remove temp snap from config
EXECUTE 'DELETE FROM @extschema@.refresh_config_snap WHERE source_table = '||quote_literal(p_src_table)||' AND dest_table = '||quote_literal(p_src_table);

RAISE NOTICE 'all done';

RETURN;

END
$$;


/*
 *  Inserter maker function. Accepts custom destination name.
 */
CREATE OR REPLACE FUNCTION inserter_maker(p_src_table text, p_dest_table text, p_control_field text, p_dblink_id int, p_boundary interval DEFAULT '00:10:00') RETURNS void
    LANGUAGE plpgsql
    AS $$
DECLARE

v_data_source               text;
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

-- Temp snap config
v_insert_refresh_config := 'INSERT INTO @extschema@.refresh_config_snap(source_table, dest_table, dblink) VALUES('
    ||quote_literal(p_src_table)||', '||quote_literal(p_dest_table)||', '|| p_dblink_id||')';

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

RAISE NOTICE 'Taking the maximum timestamp';
EXECUTE 'SELECT max('||p_control_field||') FROM '||p_dest_table||';' INTO v_max_timestamp;
RAISE NOTICE 'The select statement ran successfully.';

v_dst_active := @extschema@.dst_utc_check();

v_insert_refresh_config := 'INSERT INTO @extschema@.refresh_config_inserter(source_table, dest_table, dblink, control, boundary, last_value, dst_active) VALUES('
    ||quote_literal(p_src_table)||', '||quote_literal(p_dest_table)||', '|| p_dblink_id||', '
    ||quote_literal(p_control_field)||', '''||p_boundary||'''::interval, '''||v_max_timestamp||'''::timestamptz, '||v_dst_active||');';

RAISE NOTICE 'Updating config table with highest timestamp value';
EXECUTE v_insert_refresh_config;
RAISE NOTICE 'Update successful';

-- Remove temp snap from config
EXECUTE 'DELETE FROM @extschema@.refresh_config_snap WHERE source_table = '||quote_literal(p_src_table)||' AND dest_table = '||quote_literal(p_dest_table);

RAISE NOTICE 'all done';

RETURN;

END
$$;


/*
 *  Updater maker function. Assumes source and destination are the same tablename.
 */
CREATE OR REPLACE FUNCTION updater_maker(p_src_table text, p_control_field text, p_dblink_id int, p_pk_field text[], p_pk_type text[], p_boundary interval DEFAULT '00:10:00') RETURNS void
    LANGUAGE plpgsql
    AS $$
DECLARE

v_alter_table               text;
v_data_source               text;
v_dst_active                boolean;
v_exists                    int;
v_insert_refresh_config     text;
v_max_timestamp             timestamptz;
v_pk_field_csv              text;
v_pk_type_csv               text;
v_primary_key               text;
v_snap_suffix               text;
v_update_refresh_config     text;
v_view_definition           text;

BEGIN

SELECT data_source INTO v_data_source FROM @extschema@.dblink_mapping WHERE data_source_id = p_dblink_id; 
IF NOT FOUND THEN
	RAISE EXCEPTION 'ERROR: database link ID is incorrect %', p_dblink_id; 
END IF;  

v_pk_field_csv := ''''||array_to_string(p_pk_field,''',''')||'''';
v_pk_type_csv := ''''||array_to_string(p_pk_type,''',''')||'''';
v_primary_key := array_to_string(p_pk_field,',');

v_insert_refresh_config := 'INSERT INTO @extschema@.refresh_config_snap(source_table, dest_table, dblink) VALUES('
    ||quote_literal(p_src_table)||', '||quote_literal(p_src_table)||', '|| p_dblink_id||');';

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

v_dst_active := @extschema@.dst_utc_check();

v_insert_refresh_config := 'INSERT INTO @extschema@.refresh_config_updater(source_table, dest_table, dblink, control, boundary, pk_field, pk_type, last_value, dst_active) VALUES('||quote_literal(p_src_table)||', '||quote_literal(p_src_table)||', '|| p_dblink_id||', '||quote_literal(p_control_field)||', '''||p_boundary||'''::interval, ARRAY['||v_pk_field_csv||'], ARRAY['||v_pk_type_csv||'], '''||v_max_timestamp||'''::timestamptz, '||v_dst_active||')';

RAISE NOTICE 'Updating config table with highest timestamp value';
EXECUTE v_insert_refresh_config;
RAISE NOTICE 'Update successful';

-- Remove temp snap from config
EXECUTE 'DELETE FROM @extschema@.refresh_config_snap WHERE source_table = '||quote_literal(p_src_table)||' AND dest_table = '||quote_literal(p_src_table);

RAISE NOTICE 'All Done';

RETURN;

END
$$;


/*
 *  Updater maker function. Accepts custom destination name.
 */
CREATE OR REPLACE FUNCTION updater_maker(p_src_table text, p_dest_table text, p_control_field text, p_dblink_id int, p_pk_field text[], p_pk_type text[], p_boundary interval DEFAULT '00:10:00') RETURNS void
    LANGUAGE plpgsql
    AS $$
DECLARE

v_alter_table               text;
v_data_source               text;
v_dst_active                boolean;
v_exists                    int;
v_insert_refresh_config     text;
v_max_timestamp             timestamptz;
v_pk_field_csv              text;
v_pk_type_csv               text;
v_primary_key               text;
v_snap_suffix               text;
v_update_refresh_config     text;
v_view_definition           text;

BEGIN

SELECT data_source INTO v_data_source FROM @extschema@.dblink_mapping WHERE data_source_id = p_dblink_id; 
IF NOT FOUND THEN
	RAISE EXCEPTION 'ERROR: database link ID is incorrect %', p_dblink_id; 
END IF;  

v_pk_field_csv := ''''||array_to_string(p_pk_field,''',''')||'''';
v_pk_type_csv := ''''||array_to_string(p_pk_type,''',''')||'''';
v_primary_key := array_to_string(p_pk_field,',');

v_insert_refresh_config := 'INSERT INTO @extschema@.refresh_config_snap(source_table, dest_table, dblink) VALUES('
    ||quote_literal(p_src_table)||', '||quote_literal(p_dest_table)||', '|| p_dblink_id||')';

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

v_dst_active := @extschema@.dst_utc_check();

v_insert_refresh_config := 'INSERT INTO @extschema@.refresh_config_updater(source_table, dest_table, dblink, control, boundary, pk_field, pk_type, last_value, dst_active) VALUES('
    ||quote_literal(p_src_table)||', '||quote_literal(p_dest_table)||', '|| p_dblink_id||', '||quote_literal(p_control_field)||', '''
    ||p_boundary||'''::interval, ARRAY['||v_pk_field_csv||'], ARRAY['||v_pk_type_csv||'], '''||v_max_timestamp||'''::timestamptz, '||v_dst_active||')';

RAISE NOTICE 'Updating config table with highest timestamp value';
EXECUTE v_insert_refresh_config;
RAISE NOTICE 'Update successful';

-- Remove temp snap from config
EXECUTE 'DELETE FROM @extschema@.refresh_config_snap WHERE source_table = '||quote_literal(p_src_table)||' AND dest_table = '||quote_literal(p_dest_table);

RAISE NOTICE 'All Done';

RETURN;

END
$$;

