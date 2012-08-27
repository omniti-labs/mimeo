-- Restructured SQL source files in /sql folder. Run 'make' to create the single file needed for extension installation or just cat all the files in /sql/tables and sql/functions together in the properly formatted filename.
-- IMPORTANT NOTE: All maker functions have been dropped and recreated. Please check permissions before and after update!
-- Created dml_maker, logdel_maker, dml_destroyer, logdel_destroyer functions. Will require a schema on the source database that mimeo replication user owns. Assumed to be the same schema as where the extension is installed on the destination. Will also require giving the mimeo replication user trigger privileges on the source table.
-- Fixed refresh_dml to actually delete rows that were deleted on the source
-- Removed temporary table creation in snapshot_destroyer if ARCHIVE was set. Now renames the current snap table to the old view name. This allows any permissions, indexes, etc to be kept.
-- Changed table drop statements in snapshot_destroyer to be more friendly with other parts of extension (DROP IF EXISTS)
-- Simplify maker functions to only have one version and more efficiently create the local table using the now better snapshot_destroyer. Custom destination table name is an optional argument. Default is NULL and maker will create destination table with same schema and tablename as the source unless this parameter is set.
-- Update auth() function to support passwordless authentication string.

CREATE OR REPLACE FUNCTION auth(integer) RETURNS text
    LANGUAGE plpgsql
    AS $$
DECLARE

    v_auth          text;
    v_data_source   text;
    v_pwd           text;
    v_username      text;
    
BEGIN
    
SELECT data_source, username, pwd INTO v_data_source, v_username, v_pwd FROM @extschema@.dblink_mapping WHERE data_source_id = $1;

IF v_pwd IS NOT NULL THEN
    v_auth := v_data_source||' user='||v_username||' password='||v_pwd;
ELSE
    v_auth := v_data_source||' user='||v_username;
END IF;

RETURN v_auth;

END
$$;


DROP FUNCTION IF EXISTS @extschema@.snapshot_maker(text,int);
DROP FUNCTION IF EXISTS @extschema@.snapshot_maker(text,text,int);
/*
 *  Snapshot maker function. Optional custom destination table name.
 */
CREATE OR REPLACE FUNCTION snapshot_maker(p_src_table text, p_dblink_id int, p_dest_table text DEFAULT NULL) RETURNS void
    LANGUAGE plpgsql
    AS $$
DECLARE

v_data_source               text;
v_insert_refresh_config     text;

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
PERFORM @extschema@.refresh_snap(p_dest_table);

RAISE NOTICE 'attempting second snapshot';
PERFORM @extschema@.refresh_snap(p_dest_table);

RAISE NOTICE 'all done';

RETURN;

END
$$;


/*
 *  Snapshot destroyer function. Pass ARCHIVE to keep permanent copy of snap view.
 */
CREATE OR REPLACE FUNCTION snapshot_destroyer(p_dest_table text, p_archive_option text) RETURNS void
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
IF p_archive_option = 'ARCHIVE' THEN

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
    
ELSE

    EXECUTE 'DROP VIEW ' || v_dest_table;    

END IF;

EXECUTE 'DROP TABLE IF EXISTS ' || v_dest_table || '_snap1';
EXECUTE 'DROP TABLE IF EXISTS ' || v_dest_table || '_snap2';

EXECUTE 'DELETE FROM @extschema@.refresh_config_snap WHERE dest_table = ' || quote_literal(v_dest_table);

END
$$;


DROP FUNCTION IF EXISTS @extschema@.inserter_maker(text,text,int,interval);
DROP FUNCTION IF EXISTS @extschema@.inserter_maker(text,text,text,int,interval);
/*
 *  Inserter maker function. Optional custom destination table name.
 */
CREATE OR REPLACE FUNCTION inserter_maker(p_src_table text, p_control_field text, p_dblink_id int, p_boundary interval DEFAULT '00:10:00', p_dest_table text DEFAULT NULL) RETURNS void
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

IF p_dest_table IS NULL THEN
    p_dest_table := p_src_table;
END IF;

-- Temp snap config
v_insert_refresh_config := 'INSERT INTO @extschema@.refresh_config_snap(source_table, dest_table, dblink) VALUES('
    ||quote_literal(p_src_table)||', '||quote_literal(p_dest_table)||', '|| p_dblink_id||')';

RAISE NOTICE 'Snapshotting source table to pull all current source data...';
EXECUTE v_insert_refresh_config;	

PERFORM @extschema@.refresh_snap(p_dest_table);

PERFORM @extschema@.snapshot_destroyer(p_dest_table, 'ARCHIVE');
	
RAISE NOTICE 'Snapshot complete. Getting the maximum destination timestamp...';
EXECUTE 'SELECT max('||p_control_field||') FROM '||p_dest_table||';' INTO v_max_timestamp;

v_dst_active := @extschema@.dst_utc_check();

v_insert_refresh_config := 'INSERT INTO @extschema@.refresh_config_inserter(source_table, dest_table, dblink, control, boundary, last_value, dst_active) VALUES('
    ||quote_literal(p_src_table)||', '||quote_literal(p_dest_table)||', '|| p_dblink_id||', '
    ||quote_literal(p_control_field)||', '''||p_boundary||'''::interval, '''||v_max_timestamp||'''::timestamptz, '||v_dst_active||');';

RAISE NOTICE 'Inserting data into config table';
EXECUTE v_insert_refresh_config;

-- Remove temp snap from config
EXECUTE 'DELETE FROM @extschema@.refresh_config_snap WHERE source_table = '||quote_literal(p_src_table)||' AND dest_table = '||quote_literal(p_dest_table);

RAISE NOTICE 'Done';

RETURN;

END
$$;


DROP FUNCTION IF EXISTS @extschema@.updater_maker(text,text,int,text[],text[],interval);
DROP FUNCTION IF EXISTS @extschema@.updater_maker(text,text,text,int,text[],text[],interval);
/*
 *  Updater maker function. Optional custom destination table name.
 */
CREATE OR REPLACE FUNCTION updater_maker(p_src_table text, p_control_field text, p_dblink_id int, p_pk_field text[], p_pk_type text[], p_boundary interval DEFAULT '00:10:00', p_dest_table text DEFAULT NULL) RETURNS void
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
v_snap_suffix               text;
v_update_refresh_config     text;
v_view_definition           text;

BEGIN

SELECT data_source INTO v_data_source FROM @extschema@.dblink_mapping WHERE data_source_id = p_dblink_id; 
IF NOT FOUND THEN
	RAISE EXCEPTION 'ERROR: database link ID is incorrect %', p_dblink_id; 
END IF;  

IF p_dest_table IS NULL THEN
    p_dest_table := p_src_table;
END IF;

v_pk_field_csv := array_to_string(p_pk_field,',');
v_pk_type_csv := array_to_string(p_pk_type,',');

v_insert_refresh_config := 'INSERT INTO @extschema@.refresh_config_snap(source_table, dest_table, dblink) VALUES('
    ||quote_literal(p_src_table)||', '||quote_literal(p_dest_table)||', '|| p_dblink_id||')';

RAISE NOTICE 'Snapshotting source table to pull all current source data...';
EXECUTE v_insert_refresh_config;	

PERFORM @extschema@.refresh_snap(p_dest_table);
PERFORM @extschema@.snapshot_destroyer(p_dest_table, 'ARCHIVE');

v_alter_table := 'ALTER TABLE '||p_dest_table||' ADD PRIMARY KEY('||v_pk_field_csv||');';

RAISE NOTICE 'Snapshot complete. Adding primary key constraint to table..';
EXECUTE v_alter_table;
RAISE NOTICE 'Added successfully';

RAISE NOTICE 'Getting the maximum destination timestamp...';
EXECUTE 'SELECT max('||p_control_field||') FROM '||p_dest_table||';' INTO v_max_timestamp;

v_dst_active := @extschema@.dst_utc_check();

v_insert_refresh_config := 'INSERT INTO @extschema@.refresh_config_updater(source_table, dest_table, dblink, control, boundary, pk_field, pk_type, last_value, dst_active) VALUES('
    ||quote_literal(p_src_table)||', '||quote_literal(p_dest_table)||', '|| p_dblink_id||', '||quote_literal(p_control_field)||', '''
    ||p_boundary||'''::interval, '||quote_literal(p_pk_field)||', '||quote_literal(p_pk_type)||', '||quote_literal(v_max_timestamp)||', '||v_dst_active||')';

RAISE NOTICE 'Inserting data into config table';
EXECUTE v_insert_refresh_config;

-- Remove temp snap from config
EXECUTE 'DELETE FROM @extschema@.refresh_config_snap WHERE source_table = '||quote_literal(p_src_table)||' AND dest_table = '||quote_literal(p_dest_table);

RAISE NOTICE 'Done';

RETURN;

END
$$;


/*
 *  DML maker function. Optional custom destination table name.
 */
CREATE FUNCTION dml_maker(p_src_table text, p_dblink_id int, p_pk_field text[], p_pk_type text[], p_dest_table text DEFAULT NULL) RETURNS void
    LANGUAGE plpgsql
    AS $$
DECLARE

v_conns                     text[];
v_create_trig               text;
v_dblink_schema             text;
v_insert_refresh_config     text;
v_old_search_path           text;
v_pk_counter                int := 1;
v_pk_field_csv              text := '';
v_pk_field_type_csv         text := '';
v_table_name                text;
v_remote_q_index            text;
v_remote_q_table            text;
v_trigger_func              text;


BEGIN

SELECT nspname INTO v_dblink_schema FROM pg_namespace n, pg_extension e WHERE e.extname = 'dblink' AND e.extnamespace = n.oid;

-- Set custom search path to allow easier calls to other functions
SELECT current_setting('search_path') INTO v_old_search_path;
EXECUTE 'SELECT set_config(''search_path'',''@extschema@,'||v_dblink_schema||''',''false'')';

IF p_dest_table IS NULL THEN
    p_dest_table := p_src_table;
END IF;

-- Split off schema name if it exists
IF position('.' in p_src_table) > 0 THEN 
    v_table_name := substring(p_src_table from position('.' in p_src_table)+1);
END IF;

v_remote_q_table := 'CREATE TABLE @extschema@.'||v_table_name||'_pgq (';

v_pk_field_csv := array_to_string(p_pk_field, ',');
WHILE v_pk_counter <= array_length(p_pk_field,1) LOOP
    IF v_pk_counter > 1 THEN
        v_pk_field_type_csv := v_pk_field_type_csv || ', ';
    END IF;
    v_pk_field_type_csv := v_pk_field_type_csv ||p_pk_field[v_pk_counter]||' '||p_pk_type[v_pk_counter];
    v_pk_counter := v_pk_counter + 1;
END LOOP;

v_remote_q_table := v_remote_q_table || v_pk_field_type_csv || ', processed boolean)';

v_remote_q_index := 'CREATE INDEX ON @extschema@.'||v_table_name||'_pgq ('||v_pk_field_csv||')';

v_pk_counter := 1;
v_trigger_func := 'CREATE FUNCTION @extschema@.'||v_table_name||'_mimeo_queue() RETURNS trigger LANGUAGE plpgsql AS $_$ DECLARE ';
    WHILE v_pk_counter <= array_length(p_pk_field,1) LOOP
        v_trigger_func := v_trigger_func||'v_'||p_pk_field[v_pk_counter]||' '||p_pk_type[v_pk_counter]||'; ';
        v_pk_counter := v_pk_counter + 1;
    END LOOP;
    v_pk_counter := 1;
    v_trigger_func := v_trigger_func || ' BEGIN IF TG_OP = ''INSERT'' THEN ';
    WHILE v_pk_counter <= array_length(p_pk_field,1) LOOP
        v_trigger_func := v_trigger_func||' v_'||p_pk_field[v_pk_counter]||' := NEW.'||p_pk_field[v_pk_counter]||'; ';
        v_pk_counter := v_pk_counter + 1;
    END LOOP;
    v_pk_counter := 1;
    v_trigger_func := v_trigger_func || ' ELSE ';
    WHILE v_pk_counter <= array_length(p_pk_field,1) LOOP
        v_trigger_func := v_trigger_func||' v_'||p_pk_field[v_pk_counter]||' := OLD.'||p_pk_field[v_pk_counter]||'; ';
        v_pk_counter := v_pk_counter + 1;
    END LOOP;
    v_pk_counter := 1;
    v_trigger_func := v_trigger_func || ' END IF; INSERT INTO @extschema@.'||v_table_name||'_pgq ('||v_pk_field_csv||') ';
    v_trigger_func := v_trigger_func || ' VALUES (';
    WHILE v_pk_counter <= array_length(p_pk_field,1) LOOP
        IF v_pk_counter > 1 THEN
            v_trigger_func := v_trigger_func || ', ';
        END IF;
        v_trigger_func := v_trigger_func||'v_'||p_pk_field[v_pk_counter];
        v_pk_counter := v_pk_counter + 1;
    END LOOP;
    v_trigger_func := v_trigger_func || '); RETURN NULL; END $_$;';

v_create_trig := 'CREATE TRIGGER '||v_table_name||'_mimeo_trig AFTER INSERT OR UPDATE OR DELETE ON '||p_src_table||
    ' FOR EACH ROW EXECUTE PROCEDURE @extschema@.'||v_table_name||'_mimeo_queue()';

PERFORM dblink_connect('mimeo_dml', @extschema@.auth(p_dblink_id));

RAISE NOTICE 'Creating objects on source database (function, trigger & queue table)...';
PERFORM dblink_exec('mimeo_dml', v_remote_q_table);
PERFORM dblink_exec('mimeo_dml', v_remote_q_index);
PERFORM dblink_exec('mimeo_dml', v_trigger_func);
PERFORM dblink_exec('mimeo_dml', v_create_trig);

PERFORM dblink_disconnect('mimeo_dml');

RAISE NOTICE 'Snapshotting source table to pull all current source data...';
-- Snapshot the table after triggers have been created to ensure all new data after setup is replicated
v_insert_refresh_config := 'INSERT INTO @extschema@.refresh_config_snap(source_table, dest_table, dblink) VALUES('
    ||quote_literal(p_src_table)||', '||quote_literal(p_dest_table)||', '|| p_dblink_id||')';
EXECUTE v_insert_refresh_config;

PERFORM @extschema@.refresh_snap(p_dest_table);
PERFORM @extschema@.snapshot_destroyer(p_dest_table, 'ARCHIVE');


v_insert_refresh_config := 'INSERT INTO @extschema@.refresh_config_dml(source_table, dest_table, dblink, control, pk_field, pk_type, last_value) VALUES('
    ||quote_literal(p_src_table)||', '||quote_literal(p_dest_table)||', '|| p_dblink_id||', '||quote_literal('@extschema@.'||v_table_name||'_pgq')||', '
    ||quote_literal(p_pk_field)||', '||quote_literal(p_pk_type)||', '||quote_literal(clock_timestamp())||')';
RAISE NOTICE 'Inserting data into config table';
EXECUTE v_insert_refresh_config;

EXECUTE 'SELECT set_config(''search_path'','''||v_old_search_path||''',''false'')';

RAISE NOTICE 'Done';

EXCEPTION
    WHEN OTHERS THEN
        EXECUTE 'SELECT set_config(''search_path'',''@extschema@,'||v_dblink_schema||''',''false'')';
        v_conns := dblink_get_connections();
        IF dblink_get_connections() @> '{mimeo_dml}' THEN
            PERFORM dblink_disconnect('mimeo_dml');
        END IF;
        EXECUTE 'SELECT set_config(''search_path'','''||v_old_search_path||''',''false'')';
        RAISE EXCEPTION '%', SQLERRM;    
END
$$;


/*
 *  DML destroyer function. Pass ARCHIVE to keep table intact.
 */
CREATE FUNCTION dml_destroyer(p_dest_table text, p_archive_option text) RETURNS void
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
EXECUTE 'SELECT set_config(''search_path'',''@extschema@,'||v_dblink_schema||''',''false'')';

SELECT source_table, dest_table, dblink INTO v_src_table, v_dest_table, v_dblink
		FROM @extschema@.refresh_config_dml WHERE dest_table = p_dest_table;
IF NOT FOUND THEN
		RAISE EXCEPTION 'This table is not set up for dml replication: %', v_dest_table;
END IF;

-- Split off schema name if it exists
IF position('.' in v_src_table) > 0 THEN 
    v_table_name := substring(v_src_table from position('.' in v_src_table)+1);
END IF;

v_drop_function := 'DROP FUNCTION @extschema@.'||v_table_name||'_mimeo_queue()';
v_drop_trigger := 'DROP TRIGGER '||v_table_name||'_mimeo_trig ON '||v_src_table;
v_drop_q_table := 'DROP TABLE @extschema@.'||v_table_name||'_pgq';

RAISE NOTICE 'Removing mimeo objects from source database (trigger, function, queue table)';
PERFORM dblink_connect('mimeo_dml_destroy', @extschema@.auth(v_dblink));
PERFORM dblink_exec('mimeo_dml_destroy', v_drop_trigger);
PERFORM dblink_exec('mimeo_dml_destroy', v_drop_function);
PERFORM dblink_exec('mimeo_dml_destroy', v_drop_q_table);

IF p_archive_option != 'ARCHIVE' THEN 
    EXECUTE 'DROP TABLE ' || v_dest_table;
END IF;

RAISE NOTICE 'Removing config data';
EXECUTE 'DELETE FROM @extschema@.refresh_config_dml WHERE dest_table = ' || quote_literal(v_dest_table);	

PERFORM dblink_disconnect('mimeo_dml_destroy');

RAISE NOTICE 'Done';

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
 *  Refresh based on DML (Insert, Update, Delete)
 */
CREATE OR REPLACE FUNCTION refresh_dml(p_destination text, p_limit int default NULL, p_repull boolean DEFAULT false, p_debug boolean DEFAULT false) RETURNS void
    LANGUAGE plpgsql SECURITY DEFINER
    AS $$
DECLARE

v_adv_lock          boolean;
v_cols_n_types      text;
v_cols              text;
v_control           text;
v_create_f_sql      text;
v_create_q_sql      text;
v_dblink_schema     text;
v_dblink            text;
v_delete_sql        text;
v_dest_table        text;
v_exec_status       text;
v_field             text;
v_filter            text[];
v_insert_sql        text;
v_job_id            int;
v_jobmon_schema     text;
v_job_name          text;
v_last_value_sql    text;
v_limit             int; 
v_old_search_path   text;
v_pk_counter        int;
v_pk_field_csv      text := '';
v_pk_field_type_csv text := '';
v_pk_field          text[];
v_pk_type           text[];
v_pk_where          text;
v_remote_f_sql      text;
v_remote_q_sql      text;
v_rowcount          bigint; 
v_source_table      text;
v_step_id           int;
v_tmp_table         text;
v_trigger_delete    text; 
v_trigger_update    text;
v_with_update       text;

BEGIN

IF p_debug IS DISTINCT FROM true THEN
    PERFORM set_config( 'client_min_messages', 'warning', true );
END IF;

v_job_name := 'Refresh DML: '||p_destination;

SELECT nspname INTO v_dblink_schema FROM pg_namespace n, pg_extension e WHERE e.extname = 'dblink' AND e.extnamespace = n.oid;
SELECT nspname INTO v_jobmon_schema FROM pg_namespace n, pg_extension e WHERE e.extname = 'pg_jobmon' AND e.extnamespace = n.oid;

-- Set custom search path to allow easier calls to other functions, especially job logging
SELECT current_setting('search_path') INTO v_old_search_path;
EXECUTE 'SELECT set_config(''search_path'',''@extschema@,'||v_jobmon_schema||','||v_dblink_schema||''',''false'')';

SELECT source_table
    , dest_table
    , 'tmp_'||replace(dest_table,'.','_')
    , dblink
    , control
    , pk_field
    , pk_type
    , filter
    , batch_limit 
FROM refresh_config_dml 
WHERE dest_table = p_destination INTO v_source_table, v_dest_table, v_tmp_table, 
    v_dblink, v_control, v_pk_field, v_pk_type, v_filter, v_limit; 
IF NOT FOUND THEN
   RAISE EXCEPTION 'ERROR: no mapping found for %',v_job_name; 
END IF;

v_job_id := add_job(v_job_name);
PERFORM gdb(p_debug,'Job ID: '||v_job_id::text);

-- Take advisory lock to prevent multiple calls to function overlapping
v_adv_lock := pg_try_advisory_lock(hashtext('refresh_dml'), hashtext(v_job_name));
IF v_adv_lock = 'false' THEN
    v_step_id := add_step(v_job_id,'Obtaining advisory lock for job: '||v_job_name);
    PERFORM gdb(p_debug,'Obtaining advisory lock FAILED for job: '||v_job_name);
    PERFORM update_step(v_step_id, 'OK','Found concurrent job. Exiting gracefully');
    PERFORM close_job(v_job_id);
    EXECUTE 'SELECT set_config(''search_path'','''||v_old_search_path||''',''false'')';
    RETURN;
END IF;

v_step_id := add_step(v_job_id,'Building SQL');

IF v_pk_field IS NULL OR v_pk_type IS NULL THEN
    RAISE EXCEPTION 'ERROR: primary key fields in refresh_config_dml must be defined';
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
        (SELECT unnest(filter) FROM refresh_config_dml WHERE dest_table = p_destination) x 
         JOIN pg_attribute ON (unnest=attname::text AND attrelid=p_destination::regclass) INTO v_cols, v_cols_n_types;
END IF;    

-- init sql statements 

v_limit = COALESCE(p_limit, v_limit, 10000);

v_pk_field_csv := array_to_string(v_pk_field, ',');
v_pk_counter := 1;
WHILE v_pk_counter <= array_length(v_pk_field,1) LOOP
    IF v_pk_counter > 1 THEN
        v_pk_field_type_csv := v_pk_field_type_csv || ', ';
    END IF;
    v_pk_field_type_csv := v_pk_field_type_csv ||v_pk_field[v_pk_counter]||' '||v_pk_type[v_pk_counter];
    v_pk_counter := v_pk_counter + 1;
END LOOP;

v_with_update := 'WITH a AS (SELECT '||v_pk_field_csv||' FROM '|| v_control ||' ORDER BY 1 LIMIT '|| v_limit ||') UPDATE '||v_control||' b SET processed = true FROM a WHERE a.'||v_pk_field[1]||' = b.'||v_pk_field[1];

v_pk_counter := 2;
IF array_length(v_pk_field, 1) > 1 THEN
    v_pk_where := '';
    WHILE v_pk_counter <= array_length(v_pk_field,1) LOOP
        v_pk_where := v_pk_where || ' AND a.'||v_pk_field[v_pk_counter]||' = b.'||v_pk_field[v_pk_counter];
        v_pk_counter := v_pk_counter + 1;
    END LOOP;
END IF;

IF v_pk_where IS NOT NULL THEN
    v_with_update := v_with_update || v_pk_where;
END IF;
PERFORM gdb(p_debug, v_with_update);

v_trigger_update := 'SELECT dblink_exec(auth('||v_dblink||'),'|| quote_literal(v_with_update)||')';

v_remote_q_sql := 'SELECT DISTINCT '||v_pk_field_csv||' FROM '||v_control||' WHERE processed = true';

IF p_repull THEN
    v_remote_f_sql := 'SELECT '||v_cols||' FROM '||v_source_table;
        -- Actual truncate is done after pull to temp table to minimize lock on dest_table
    PERFORM update_step(v_step_id, 'OK','Request to repull ALL data from source. This could take a while...');
    PERFORM gdb(p_debug, 'Request to repull ALL data from source. This could take a while...');

ELSE 
    v_remote_f_sql := 'SELECT '||v_cols||' FROM '||v_source_table||' JOIN ('||v_remote_q_sql||') x USING ('||v_pk_field_csv||')';
    
    v_create_q_sql := 'CREATE TEMP TABLE '||v_tmp_table||'_queue AS SELECT '||v_pk_field_csv||'
        FROM dblink(auth('||v_dblink||'),'||quote_literal(v_remote_q_sql)||') t ('||v_pk_field_type_csv||')';

    v_delete_sql := 'DELETE FROM '||v_dest_table||' a USING '||v_tmp_table||'_queue b WHERE a.'||v_pk_field[1]||'= b.'||v_pk_field[1];
    IF array_length(v_pk_field, 1) > 1 THEN
        v_delete_sql := v_delete_sql || v_pk_where;
    END IF; 

    PERFORM update_step(v_step_id, 'OK','Done');

END IF;

v_create_f_sql := 'CREATE TEMP TABLE '||v_tmp_table||'_full AS SELECT '||v_cols||' 
        FROM dblink(auth('||v_dblink||'),'||quote_literal(v_remote_f_sql)||') t ('||v_cols_n_types||')';

v_insert_sql := 'INSERT INTO '||v_dest_table||'('||v_cols||') SELECT '||v_cols||' FROM '||v_tmp_table||'_full'; 

v_trigger_delete := 'SELECT dblink_exec(auth('||v_dblink||'),'||quote_literal('DELETE FROM '||v_control||' WHERE processed = true')||')'; 



-- update remote entries
v_step_id := add_step(v_job_id,'Updating remote trigger table');
    PERFORM gdb(p_debug,v_trigger_update);
    EXECUTE v_trigger_update INTO v_exec_status;    
PERFORM update_step(v_step_id, 'OK','Result was '||v_exec_status);

-- create temp tables 
v_step_id := add_step(v_job_id,'Creating temp tables');
    -- Full table with all insert/update data    
    PERFORM gdb(p_debug,v_create_f_sql);
    EXECUTE v_create_f_sql;
    -- Queue table with all rows to process (inserts, updates & deletes)
    PERFORM gdb(p_debug,v_create_q_sql);
    EXECUTE v_create_q_sql;
    GET DIAGNOSTICS v_rowcount = ROW_COUNT;
    PERFORM gdb(p_debug,'Temp queue table row count '||v_rowcount::text);
    IF v_rowcount < 1 THEN 
        PERFORM update_step(v_step_id, 'OK','No new rows found');
        EXECUTE 'DROP TABLE IF EXISTS '||v_tmp_table||'_full';
        PERFORM close_job(v_job_id);
        -- Ensure old search path is reset for the current session
        EXECUTE 'SELECT set_config(''search_path'','''||v_old_search_path||''',''false'')';
        PERFORM pg_advisory_unlock(hashtext('refresh_dml'), hashtext(v_job_name));
        RETURN;
    END IF;
PERFORM update_step(v_step_id, 'OK','Number of rows to process: '||v_rowcount);

-- remove records from local table 
IF p_repull THEN
    v_step_id := add_step(v_job_id,'Truncating local table');
    PERFORM gdb(p_debug,'Truncating local table');
    EXECUTE 'TRUNCATE '||v_dest_table;
    PERFORM update_step(v_step_id, 'OK','Done');
ELSE
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

-- update activity status
v_step_id := add_step(v_job_id,'Updating last_value in config table');
    v_last_value_sql := 'UPDATE refresh_config_dml SET last_value = '|| quote_literal(current_timestamp::timestamp) ||' WHERE dest_table = ' ||quote_literal(p_destination); 
    PERFORM gdb(p_debug,v_last_value_sql);
    EXECUTE v_last_value_sql; 
PERFORM update_step(v_step_id, 'OK','Last Value was '||current_timestamp);

PERFORM close_job(v_job_id);

EXECUTE 'DROP TABLE IF EXISTS '||v_tmp_table||'_queue';
EXECUTE 'DROP TABLE IF EXISTS '||v_tmp_table||'_full';

-- Ensure old search path is reset for the current session
EXECUTE 'SELECT set_config(''search_path'','''||v_old_search_path||''',''false'')';

PERFORM pg_advisory_unlock(hashtext('refresh_dml'), hashtext(v_job_name));

EXCEPTION
    WHEN QUERY_CANCELED THEN
        PERFORM pg_advisory_unlock(hashtext('refresh_inserter'), hashtext(v_job_name));
        RAISE EXCEPTION '%', SQLERRM;  
    WHEN OTHERS THEN
        -- Exception block resets path, so have to reset it again
        EXECUTE 'SELECT set_config(''search_path'',''@extschema@,'||v_jobmon_schema||','||v_dblink_schema||''',''false'')';
        IF v_step_id IS NULL THEN
            v_step_id := add_step(v_job_id, 'EXCEPTION before first step logged');
        END IF;
        PERFORM update_step(v_step_id, 'BAD', 'ERROR: '||coalesce(SQLERRM,'unknown'));
        PERFORM fail_job(v_job_id);

        -- Ensure old search path is reset for the current session
       EXECUTE 'SELECT set_config(''search_path'','''||v_old_search_path||''',''false'')';

        PERFORM pg_advisory_unlock(hashtext('refresh_dml'), hashtext(v_job_name));
        RAISE EXCEPTION '%', SQLERRM;
END
$$;


/*
 *  DML maker function. Optional custom destination table name.
 */
CREATE FUNCTION logdel_maker(p_src_table text, p_dblink_id int, p_pk_field text[], p_pk_type text[], p_dest_table text DEFAULT NULL) RETURNS void
    LANGUAGE plpgsql
    AS $$
DECLARE

v_cols                      text[];
v_cols_csv                  text;
v_cols_n_types              text[];
v_cols_n_types_csv          text;
v_conns                     text[];
v_create_trig               text;
v_dblink_schema             text;
v_insert_refresh_config     text;
v_old_search_path           text;
v_counter                int := 1;
v_pk_field_csv              text := '';
v_pk_field_type_csv         text := '';
v_remote_sql                text;
v_remote_q_index            text;
v_remote_q_table            text;
v_table_name                text;
v_trigger_func              text;
v_types                     text[];


BEGIN

SELECT nspname INTO v_dblink_schema FROM pg_namespace n, pg_extension e WHERE e.extname = 'dblink' AND e.extnamespace = n.oid;

-- Set custom search path to allow easier calls to other functions
SELECT current_setting('search_path') INTO v_old_search_path;
EXECUTE 'SELECT set_config(''search_path'',''@extschema@,'||v_dblink_schema||''',''false'')';

IF p_dest_table IS NULL THEN
    p_dest_table := p_src_table;
END IF;

-- Split off schema name if it exists
IF position('.' in p_src_table) > 0 THEN 
    v_table_name := substring(p_src_table from position('.' in p_src_table)+1);
END IF;

PERFORM dblink_connect('mimeo_logdel', @extschema@.auth(p_dblink_id));

v_remote_sql := 'SELECT array_agg(attname) as cols, array_agg(atttypid::regtype::text) as types, array_agg(attname||'' ''||atttypid::regtype::text) as cols_n_types FROM pg_attribute WHERE attnum > 0 AND attisdropped is false AND attrelid = ' || quote_literal(p_src_table) || '::regclass';
v_remote_sql := 'SELECT cols, types, cols_n_types FROM dblink(''mimeo_logdel'', ' || quote_literal(v_remote_sql) || ') t (cols text[], types text[], cols_n_types text[])';
EXECUTE v_remote_sql INTO v_cols, v_types, v_cols_n_types;

v_cols_csv := array_to_string(v_cols, ',');
v_cols_n_types_csv := array_to_string(v_cols_n_types, ',');

v_remote_q_table := 'CREATE TABLE @extschema@.'||v_table_name||'_pgq ('||v_cols_n_types_csv||', mimeo_source_deleted timestamptz, processed boolean)';

v_pk_field_csv := array_to_string(p_pk_field, ',');
WHILE v_counter <= array_length(p_pk_field,1) LOOP
    IF v_counter > 1 THEN
        v_pk_field_type_csv := v_pk_field_type_csv || ', ';
    END IF;
    v_pk_field_type_csv := v_pk_field_type_csv ||p_pk_field[v_counter]||' '||p_pk_type[v_counter];
    v_counter := v_counter + 1;
END LOOP;

v_remote_q_index := 'CREATE INDEX ON @extschema@.'||v_table_name||'_pgq ('||v_pk_field_csv||')';

v_counter := 1;
v_trigger_func := 'CREATE FUNCTION @extschema@.'||v_table_name||'_mimeo_queue() RETURNS trigger LANGUAGE plpgsql AS $_$ DECLARE ';
    WHILE v_counter <= array_length(v_cols,1) LOOP
        v_trigger_func := v_trigger_func||'v_'||v_cols[v_counter]||' '||v_types[v_counter]||'; ';
        v_counter := v_counter + 1;
    END LOOP;
    v_trigger_func := v_trigger_func || 'v_del_time timestamptz; ';
    v_counter := 1;
    v_trigger_func := v_trigger_func || ' BEGIN IF TG_OP = ''INSERT'' THEN ';
    WHILE v_counter <= array_length(p_pk_field,1) LOOP
        v_trigger_func := v_trigger_func||' v_'||p_pk_field[v_counter]||' := NEW.'||p_pk_field[v_counter]||'; ';
        v_counter := v_counter + 1;
    END LOOP;
    v_counter := 1;
    v_trigger_func := v_trigger_func || ' ELSIF TG_OP = ''UPDATE'' THEN  ';
    WHILE v_counter <= array_length(p_pk_field,1) LOOP
        v_trigger_func := v_trigger_func||' v_'||p_pk_field[v_counter]||' := OLD.'||p_pk_field[v_counter]||'; ';
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
    v_trigger_func := v_trigger_func || ' END IF; INSERT INTO @extschema@.'||v_table_name||'_pgq ('||v_cols_csv||', mimeo_source_deleted) ';
    v_trigger_func := v_trigger_func || ' VALUES (';
    WHILE v_counter <= array_length(v_cols,1) LOOP
        IF v_counter > 1 THEN
            v_trigger_func := v_trigger_func || ', ';
        END IF;
        v_trigger_func := v_trigger_func||'v_'||v_cols[v_counter];
        v_counter := v_counter + 1;
    END LOOP;
    v_trigger_func := v_trigger_func ||', v_del_time); RETURN NULL; END $_$;';

v_create_trig := 'CREATE TRIGGER '||v_table_name||'_mimeo_trig AFTER INSERT OR UPDATE OR DELETE ON '||p_src_table||
    ' FOR EACH ROW EXECUTE PROCEDURE @extschema@.'||v_table_name||'_mimeo_queue()';

RAISE NOTICE 'Creating objects on source database (function, trigger & queue table)...';
PERFORM dblink_exec('mimeo_logdel', v_remote_q_table);
PERFORM dblink_exec('mimeo_logdel', v_remote_q_index);
PERFORM dblink_exec('mimeo_logdel', v_trigger_func);
PERFORM dblink_exec('mimeo_logdel', v_create_trig);

PERFORM dblink_disconnect('mimeo_logdel');

RAISE NOTICE 'Snapshotting source table to pull all current source data...';
-- Snapshot the table after triggers have been created to ensure all new data after setup is replicated
v_insert_refresh_config := 'INSERT INTO @extschema@.refresh_config_snap(source_table, dest_table, dblink) VALUES('
    ||quote_literal(p_src_table)||', '||quote_literal(p_dest_table)||', '|| p_dblink_id||')';

EXECUTE v_insert_refresh_config;

PERFORM @extschema@.refresh_snap(p_dest_table);
PERFORM @extschema@.snapshot_destroyer(p_dest_table, 'ARCHIVE');

EXECUTE 'ALTER TABLE '||p_dest_table||' ADD COLUMN mimeo_source_deleted timestamptz';

v_insert_refresh_config := 'INSERT INTO @extschema@.refresh_config_logdel(source_table, dest_table, dblink, control, pk_field, pk_type, last_value) VALUES('
    ||quote_literal(p_src_table)||', '||quote_literal(p_dest_table)||', '|| p_dblink_id||', '||quote_literal('@extschema@.'||v_table_name||'_pgq')||', '
    ||quote_literal(p_pk_field)||', '||quote_literal(p_pk_type)||', '||quote_literal(clock_timestamp())||')';
RAISE NOTICE 'Inserting data into config table';

EXECUTE v_insert_refresh_config;

EXECUTE 'SELECT set_config(''search_path'','''||v_old_search_path||''',''false'')';

RAISE NOTICE 'Done';

EXCEPTION
    WHEN OTHERS THEN
        EXECUTE 'SELECT set_config(''search_path'',''@extschema@,'||v_dblink_schema||''',''false'')';
        v_conns := dblink_get_connections();
        IF dblink_get_connections() @> '{mimeo_logdel}' THEN
            PERFORM dblink_disconnect('mimeo_logdel');
        END IF;
        EXECUTE 'SELECT set_config(''search_path'','''||v_old_search_path||''',''false'')';
        RAISE EXCEPTION '%', SQLERRM;    
END
$$;


/*
 *  Logdel destroyer function. Pass ARCHIVE to keep table intact.
 */
CREATE FUNCTION logdel_destroyer(p_dest_table text, p_archive_option text) RETURNS void
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
EXECUTE 'SELECT set_config(''search_path'',''@extschema@,'||v_dblink_schema||''',''false'')';

SELECT source_table, dest_table, dblink INTO v_src_table, v_dest_table, v_dblink
		FROM @extschema@.refresh_config_logdel WHERE dest_table = p_dest_table;
IF NOT FOUND THEN
		RAISE EXCEPTION 'This table is not set up for logdel replication: %', v_dest_table;
END IF;

-- Split off schema name if it exists
IF position('.' in v_src_table) > 0 THEN 
    v_table_name := substring(v_src_table from position('.' in v_src_table)+1);
END IF;

v_drop_function := 'DROP FUNCTION @extschema@.'||v_table_name||'_mimeo_queue()';
v_drop_trigger := 'DROP TRIGGER '||v_table_name||'_mimeo_trig ON '||v_src_table;
v_drop_q_table := 'DROP TABLE @extschema@.'||v_table_name||'_pgq';

RAISE NOTICE 'Removing mimeo objects from source database (trigger, function, queue table)';
PERFORM dblink_connect('mimeo_logdel_destroy', @extschema@.auth(v_dblink));
PERFORM dblink_exec('mimeo_logdel_destroy', v_drop_trigger);
PERFORM dblink_exec('mimeo_logdel_destroy', v_drop_function);
PERFORM dblink_exec('mimeo_logdel_destroy', v_drop_q_table);

IF p_archive_option != 'ARCHIVE' THEN 
    EXECUTE 'DROP TABLE ' || v_dest_table;
END IF;

RAISE NOTICE 'Removing config data';
EXECUTE 'DELETE FROM @extschema@.refresh_config_logdel WHERE dest_table = ' || quote_literal(v_dest_table);	

PERFORM dblink_disconnect('mimeo_logdel_destroy');

RAISE NOTICE 'Done';

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
 *  Refresh based on DML (Insert, Update, Delete), but logs all deletes on the destination table
 *  Destination table requires extra column: mimeo_source_deleted timestamptz
 */
CREATE OR REPLACE FUNCTION refresh_logdel(p_destination text, p_limit int default NULL, p_debug boolean DEFAULT false) RETURNS void
    LANGUAGE plpgsql SECURITY DEFINER
    AS $$
DECLARE

v_adv_lock              boolean;
v_cols_n_types          text;
v_cols                  text;
v_control               text;
v_create_d_sql          text;
v_create_f_sql          text;
v_dblink_schema         text;
v_dblink                text;
v_delete_d_sql          text;
v_delete_f_sql          text;
v_dest_table            text;
v_exec_status           text;
v_field                 text;
v_filter                text[];
v_insert_deleted_sql    text;
v_insert_sql            text;
v_job_id                int;
v_jobmon_schema         text;
v_job_name              text;
v_last_value_sql        text;
v_limit                 int; 
v_old_search_path       text;
v_pk_counter            int := 2;
v_pk_field_csv          text;
v_pk_field              text[];
v_pk_type               text[];
v_pk_where              text;
v_remote_d_sql          text;
v_remote_f_sql          text;
v_remote_q_sql          text;
v_full_rowcount         bigint;
v_rowcount              bigint; 
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

SELECT nspname INTO v_dblink_schema FROM pg_namespace n, pg_extension e WHERE e.extname = 'dblink' AND e.extnamespace = n.oid;
SELECT nspname INTO v_jobmon_schema FROM pg_namespace n, pg_extension e WHERE e.extname = 'pg_jobmon' AND e.extnamespace = n.oid;

-- Set custom search path to allow easier calls to other functions, especially job logging
SELECT current_setting('search_path') INTO v_old_search_path;
EXECUTE 'SELECT set_config(''search_path'',''@extschema@,'||v_jobmon_schema||','||v_dblink_schema||''',''false'')';

SELECT source_table
    , dest_table
    , 'tmp_'||replace(dest_table,'.','_')
    , dblink
    , control
    , pk_field
    , pk_type
    , filter
    , batch_limit 
FROM refresh_config_logdel 
WHERE dest_table = p_destination INTO v_source_table, v_dest_table, v_tmp_table, 
    v_dblink, v_control, v_pk_field, v_pk_type, v_filter, v_limit; 
IF NOT FOUND THEN
   RAISE EXCEPTION 'ERROR: no mapping found for %',v_job_name; 
END IF;

v_job_id := add_job(v_job_name);
PERFORM gdb(p_debug,'Job ID: '||v_job_id::text);

-- Take advisory lock to prevent multiple calls to function overlapping
v_adv_lock := pg_try_advisory_lock(hashtext('refresh_logdel'), hashtext(v_job_name));
IF v_adv_lock = 'false' THEN
    v_step_id := add_step(v_job_id,'Obtaining advisory lock for job: '||v_job_name);
    PERFORM gdb(p_debug,'Obtaining advisory lock FAILED for job: '||v_job_name);
    PERFORM update_step(v_step_id, 'OK','Found concurrent job. Exiting gracefully');
    PERFORM close_job(v_job_id);
    EXECUTE 'SELECT set_config(''search_path'','''||v_old_search_path||''',''false'')';
    RETURN;
END IF;

v_step_id := add_step(v_job_id,'Grabbing Boundries, Building SQL');

IF v_pk_field IS NULL OR v_pk_type IS NULL THEN
    RAISE EXCEPTION 'ERROR: primary key fields in refresh_config_logdel must be defined';
END IF;

-- determine column list, column type list
IF v_filter IS NULL THEN 
    SELECT array_to_string(array_agg(attname),','), array_to_string(array_agg(attname||' '||atttypid::regtype::text),',') FROM 
        pg_attribute WHERE attnum > 0 AND attisdropped is false AND attrelid = p_destination::regclass AND attname != 'mimeo_source_deleted' INTO v_cols, v_cols_n_types;
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
        (SELECT unnest(filter) FROM refresh_config_logdel WHERE dest_table = p_destination) x 
         JOIN pg_attribute ON (unnest=attname::text AND attrelid=p_destination::regclass) WHERE attname != 'mimeo_source_deleted' INTO v_cols, v_cols_n_types;
END IF;    

-- init sql statements 

v_limit = COALESCE(p_limit, v_limit, 10000);

v_pk_field_csv := array_to_string(v_pk_field,',');
v_with_update := 'WITH a AS (SELECT '||v_pk_field_csv||' FROM '|| v_control ||' ORDER BY 1 LIMIT '|| v_limit ||') UPDATE '||v_control||' b SET processed = true FROM a WHERE a.'||v_pk_field[1]||' = b.'||v_pk_field[1];

IF array_length(v_pk_field, 1) > 1 THEN
    v_pk_where := '';
    WHILE v_pk_counter <= array_length(v_pk_field,1) LOOP
        v_pk_where := v_pk_where || ' AND a.'||v_pk_field[v_pk_counter]||' = b.'||v_pk_field[v_pk_counter];
        v_pk_counter := v_pk_counter + 1;
    END LOOP;
END IF;

IF v_pk_where IS NOT NULL THEN
    v_with_update := v_with_update || v_pk_where;
END IF;
PERFORM gdb(p_debug, v_with_update);

v_trigger_update := 'SELECT dblink_exec(auth('||v_dblink||'),'|| quote_literal(v_with_update)||')';
RAISE NOTICE 'v_trigger_update: %', v_trigger_update;

v_remote_q_sql := 'SELECT DISTINCT '||v_pk_field_csv||' FROM '||v_control||' WHERE processed = true and mimeo_source_deleted IS NULL';
RAISE NOTICE 'v_remote_q_sql: %', v_remote_q_sql;

v_remote_f_sql := 'SELECT '||v_cols||' FROM '||v_source_table||' JOIN ('||v_remote_q_sql||') x USING ('||v_pk_field_csv||')';
RAISE NOTICE 'v_remote_f_sql: %', v_remote_f_sql;
v_create_f_sql := 'CREATE TEMP TABLE '||v_tmp_table||'_full AS SELECT '||v_cols||' 
    FROM dblink(auth('||v_dblink||'),'||quote_literal(v_remote_f_sql)||') t ('||v_cols_n_types||')';
RAISE NOTICE 'v_remote_f_sql: %', v_remote_f_sql;

v_remote_d_sql = 'SELECT '||v_cols||', mimeo_source_deleted FROM '||v_control||' WHERE processed = true and mimeo_source_deleted IS NOT NULL';
v_create_d_sql = 'CREATE TEMP TABLE '||v_tmp_table||'_deleted AS SELECT '||v_cols||', mimeo_source_deleted
    FROM dblink(auth('||v_dblink||'),'||quote_literal(v_remote_d_sql)||') t ('||v_cols_n_types||', mimeo_source_deleted timestamptz)';

v_delete_f_sql := 'DELETE FROM '||v_dest_table||' a USING '||v_tmp_table||'_full b WHERE a.'||v_pk_field[1]||'= b.'||v_pk_field[1];
IF array_length(v_pk_field, 1) > 1 THEN
    v_delete_f_sql := v_delete_f_sql || v_pk_where;
END IF; 

-- remove rows that were deleted on source to ensure most recently deleted data is logged 
v_delete_d_sql := 'DELETE FROM '||v_dest_table||' a USING '||v_tmp_table||'_deleted b WHERE a.'||v_pk_field[1]||'= b.'||v_pk_field[1];
IF array_length(v_pk_field, 1) > 1 THEN
    v_delete_d_sql := v_delete_d_sql || v_pk_where;
END IF; 

v_insert_sql := 'INSERT INTO '||v_dest_table||'('||v_cols||') SELECT '||v_cols||' FROM '||v_tmp_table||'_full';
v_insert_deleted_sql := 'INSERT INTO '||v_dest_table||'('||v_cols||', mimeo_source_deleted) SELECT '||v_cols||', mimeo_source_deleted FROM '||v_tmp_table||'_deleted'; 

v_trigger_delete := 'SELECT dblink_exec(auth('||v_dblink||'),'||quote_literal('DELETE FROM '||v_control||' WHERE processed = true')||')'; 

PERFORM update_step(v_step_id, 'OK','Remote table is '||v_source_table);

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
    PERFORM gdb(p_debug,'Insert/Update Temp table row count '||v_full_rowcount::text);
PERFORM update_step(v_step_id, 'OK','Table contains '||v_full_rowcount||' records');

-- create temp table for insertion (deleted rows)
v_step_id := add_step(v_job_id,'Create temp table from remote delete table');
    PERFORM gdb(p_debug,v_create_d_sql);
    EXECUTE v_create_d_sql;  
    GET DIAGNOSTICS v_rowcount = ROW_COUNT;
    PERFORM gdb(p_debug,'Delete Temp table row count '||v_rowcount::text);
    -- Check is here instead of earlier in case there are only deletes
    IF v_rowcount < 1 AND v_full_rowcount < 1 THEN 
        PERFORM update_step(v_step_id, 'OK','No new rows found');
        EXECUTE 'DROP TABLE IF EXISTS '||v_tmp_table||'_full';
        EXECUTE 'DROP TABLE IF EXISTS '||v_tmp_table||'_deleted';
        PERFORM close_job(v_job_id);
        -- Ensure old search path is reset for the current session
        EXECUTE 'SELECT set_config(''search_path'','''||v_old_search_path||''',''false'')';
        PERFORM pg_advisory_unlock(hashtext('refresh_logdel'), hashtext(v_job_name));
        RETURN;
    END IF;
PERFORM update_step(v_step_id, 'OK','Table contains '||v_rowcount||' records');

-- remove records from local table (inserts/updates)
v_step_id := add_step(v_job_id,'Deleting insert/update records from local table');
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

-- insert records to local table (deleted rows to be kepts)
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

-- update activity status
v_step_id := add_step(v_job_id,'Updating last_value in config table');
    v_last_value_sql := 'UPDATE refresh_config_logdel SET last_value = '|| quote_literal(current_timestamp::timestamp) ||' WHERE dest_table = ' ||quote_literal(p_destination); 
    PERFORM gdb(p_debug,v_last_value_sql);
    EXECUTE v_last_value_sql; 
PERFORM update_step(v_step_id, 'OK','Last Value was '||current_timestamp);

PERFORM close_job(v_job_id);

EXECUTE 'DROP TABLE IF EXISTS '||v_tmp_table||'_full';
EXECUTE 'DROP TABLE IF EXISTS '||v_tmp_table||'_deleted';

-- Ensure old search path is reset for the current session
EXECUTE 'SELECT set_config(''search_path'','''||v_old_search_path||''',''false'')';

PERFORM pg_advisory_unlock(hashtext('refresh_logdel'), hashtext(v_job_name));

EXCEPTION
    WHEN QUERY_CANCELED THEN
        PERFORM pg_advisory_unlock(hashtext('refresh_inserter'), hashtext(v_job_name));
        RAISE EXCEPTION '%', SQLERRM;  
    WHEN OTHERS THEN
        -- Exception block resets path, so have to reset it again
        EXECUTE 'SELECT set_config(''search_path'',''@extschema@,'||v_jobmon_schema||','||v_dblink_schema||''',''false'')';
        IF v_step_id IS NULL THEN
            v_step_id := jobmon.add_step(v_job_id, 'EXCEPTION before first step logged');
        END IF;
        PERFORM update_step(v_step_id, 'BAD', 'ERROR: '||coalesce(SQLERRM,'unknown'));
        PERFORM fail_job(v_job_id);

        -- Ensure old search path is reset for the current session
       EXECUTE 'SELECT set_config(''search_path'','''||v_old_search_path||''',''false'')';

        PERFORM pg_advisory_unlock(hashtext('refresh_logdel'), hashtext(v_job_name));
        RAISE EXCEPTION '%', SQLERRM;
END
$$;
