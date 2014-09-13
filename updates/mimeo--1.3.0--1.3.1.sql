-- Fixed being able to use a view as a source table. This broke in 1.3.0 due to new checks to make sure that the source table existed. 
    -- Note that DML-based replication will not work with views as a source (they didn't work before 1.3.0 either). 
    -- Also, it will only work with updater replication if an underlying primary key is manually given during the maker function call.
-- Reduced the number of new connections generated via dblink during refresh function calls.
-- Allow dml & logdel maker functions to be run in parallel. Previously would get duplicate dblink connection name errors.
-- Add pgTAP tests for view sources

CREATE TEMP TABLE mimeo_preserve_privs_temp (statement text);

INSERT INTO mimeo_preserve_privs_temp 
SELECT 'GRANT EXECUTE ON FUNCTION @extschema@.manage_dest_table (text, text, text, boolean) TO '||string_agg(grantee::text, ',')||';' 
FROM information_schema.routine_privileges
WHERE routine_schema = '@extschema@'
AND routine_name = 'manage_dest_table'; 

INSERT INTO mimeo_preserve_privs_temp 
SELECT 'GRANT EXECUTE ON FUNCTION @extschema@.create_index(text, text, text, text, boolean) TO '||string_agg(grantee::text, ',')||';' 
FROM information_schema.routine_privileges
WHERE routine_schema = '@extschema@'
AND routine_name = 'create_index'; 

DROP FUNCTION manage_dest_table (text, text, boolean);
DROP FUNCTION create_index(text, text, boolean);

/*
 * Manages creating destination table and/or returning data about the columns.
 * p_snap parameter is passed if snap table is being managed. Should be equal to either _snap1 or _snap2.
 */ 
CREATE FUNCTION manage_dest_table (
    p_destination text
    , p_snap text
    , p_dblink_name text DEFAULT NULL
    , p_debug boolean DEFAULT false
    , OUT p_table_exists boolean
    , OUT p_cols text[]
    , OUT p_cols_n_types text[]
    , OUT p_source_schema_name text
    , OUT p_source_table_name text) 
RETURNS record
    LANGUAGE plpgsql SECURITY DEFINER
    AS $$
DECLARE

v_add_logdel_col    text;
v_col_exists        int;
v_condition         text;
v_create_sql        text; 
v_dblink            int;
v_dblink_name       text;
v_dblink_schema     text; 
v_dest_schema_name  text;
v_dest_table        text;
v_dest_table_name   text;
v_filter            text[];
v_link_exists       boolean;
v_remote_sql        text; 
v_old_search_path   text;
v_source_table      text;
v_type              text;

BEGIN

-- Allow re-use of existing remote connection to avoid load of creating another
IF p_dblink_name IS NULL THEN
    v_dblink_name := @extschema@.check_name_length('manage_dest_table_dblink_'||p_destination);
ELSE
    v_dblink_name := p_dblink_name;
END IF;

SELECT nspname INTO v_dblink_schema FROM pg_namespace n, pg_extension e WHERE e.extname = 'dblink' AND e.extnamespace = n.oid;

SELECT current_setting('search_path') INTO v_old_search_path;
EXECUTE 'SELECT set_config(''search_path'',''@extschema@,'||v_dblink_schema||',public'',''false'')';

SELECT dest_table
    , type
    , dblink
    , filter
    , condition
INTO v_dest_table
    , v_type
    , v_dblink
    , v_filter
    , v_condition
FROM refresh_config
WHERE dest_table = p_destination; 
IF NOT FOUND THEN
   RAISE EXCEPTION 'ERROR: This table is not set up for replication: %', p_destination; 
END IF;

EXECUTE 'SELECT source_table FROM refresh_config_'||v_type||' WHERE dest_table = '||quote_literal(v_dest_table) INTO v_source_table;

IF p_snap IS NOT NULL AND p_snap NOT IN ('snap1', 'snap2') THEN
    RAISE EXCEPTION 'Invalid value for p_snap parameter given to manage_dest_table() function';
END IF;

IF position('.' in v_dest_table) > 0 THEN
    v_dest_schema_name := split_part(v_dest_table, '.', 1); 
    v_dest_table_name := split_part(v_dest_table, '.', 2);
ELSE
    RAISE EXCEPTION 'Destination table set in refresh_config table must be schema qualified. Error in manage_dest_table() call.';
END IF;

IF p_dblink_name IS NULL THEN
    PERFORM dblink_connect(v_dblink_name, @extschema@.auth(v_dblink));
END IF;

SELECT schemaname, tablename INTO p_source_schema_name, p_source_table_name 
    FROM dblink(v_dblink_name, format('
            SELECT schemaname, tablename
            FROM (
                SELECT schemaname, tablename 
                FROM pg_catalog.pg_tables 
                WHERE schemaname ||''.''|| tablename = %L
                UNION
                SELECT schemaname, viewname AS tablename
                FROM pg_catalog.pg_views
                WHERE schemaname || ''.'' || viewname = %L
            ) tables LIMIT 1'
        , v_source_table, v_source_table) )
    t (schemaname text, tablename text);

IF p_source_schema_name IS NULL OR p_source_table_name IS NULL THEN
    RAISE EXCEPTION 'Source table % not found for dblink id %', v_source_table, v_dblink;
END IF;

-- Always return source column info in case extra columns were added to destination. Source columns should not be changed before destination.
-- Double-quotes are added around column names to account for mixed case, special chars or reserved words
v_remote_sql := 'SELECT array_agg(''"''||attname||''"'') as cols
    , array_agg(''"''||attname||''"''||'' ''||format_type(atttypid, atttypmod)::text) as cols_n_types 
    FROM pg_attribute 
    WHERE attrelid = '''||quote_ident(p_source_schema_name)||'.'||quote_ident(p_source_table_name)||'''::regclass 
    AND attnum > 0 
    AND attisdropped is false';
PERFORM gdb(p_debug,'v_remote_sql: '||v_remote_sql);
IF v_filter IS NOT NULL THEN -- Apply column filters if used
    v_remote_sql := v_remote_sql || ' AND ARRAY[attname::text] <@ '||quote_literal(v_filter);
END IF;
v_remote_sql := 'SELECT cols, cols_n_types FROM dblink('||quote_literal(v_dblink_name)||', ' || quote_literal(v_remote_sql) || ') t (cols text[], cols_n_types text[])';
PERFORM gdb(p_debug,'v_remote_sql: '||v_remote_sql);
EXECUTE v_remote_sql INTO p_cols, p_cols_n_types;  
PERFORM gdb(p_debug,'p_cols: {'|| array_to_string(p_cols, ',') ||'}');
PERFORM gdb(p_debug,'p_cols_n_types: {'|| array_to_string(p_cols_n_types, ',') ||'}');

IF p_cols IS NULL OR p_cols_n_types IS NULL THEN
    RAISE EXCEPTION 'Retrieval of source column schema returned NULL. Possible causes are an invalid column filter list.';
END IF;

SELECT 
    CASE    
        WHEN count(1) > 0 THEN true
        ELSE false 
    END
INTO p_table_exists FROM pg_catalog.pg_tables WHERE schemaname ||'.'|| tablename = v_dest_table || COALESCE('_'||p_snap, '');
IF p_table_exists = false THEN
    v_create_sql := format('CREATE TABLE %I.%I', v_dest_schema_name, v_dest_table_name || COALESCE('_'||p_snap, ''));
    v_create_sql := v_create_sql || ' (' || array_to_string(p_cols_n_types, ',') || ')';
    perform gdb(p_debug,'v_create_sql: '||v_create_sql::text);
    EXECUTE v_create_sql;

    IF v_type = 'logdel' THEN
        SELECT count(*) INTO v_col_exists 
        FROM pg_catalog.pg_attribute a
        JOIN pg_catalog.pg_class c ON a.attrelid = c.oid
        JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid
        WHERE n.nspname = v_dest_schema_name
        AND c.relname = v_dest_table_name
        AND a.attname = 'mimeo_source_deleted' 
        AND a.attisdropped = false;
        IF v_col_exists < 1 THEN
            v_add_logdel_col := format('ALTER TABLE %I.%I ADD COLUMN mimeo_source_deleted timestamptz', v_dest_schema_name, v_dest_table_name || COALESCE('_'||p_snap, ''));
            PERFORM gdb(p_debug, 'v_add_logdel_col : ' || v_add_logdel_col);
            EXECUTE v_add_logdel_col;
        ELSE
            RAISE WARNING 'Special column (mimeo_source_deleted) already exists on destination table (%)', v_dest_table;
        END IF;
    END IF;

END IF;

-- Only close link if dblink name wasn't passed in as parameter
IF p_dblink_name IS NULL THEN
    PERFORM dblink_disconnect(v_dblink_name);
END IF;

EXECUTE 'SELECT set_config(''search_path'','''||v_old_search_path||''',''false'')';

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
 * Create index(es) on destination table
 */
CREATE FUNCTION create_index(p_destination text, p_source_schema_name text, p_source_table_name text, p_snap text DEFAULT NULL, p_debug boolean DEFAULT false) RETURNS void
    LANGUAGE plpgsql SECURITY DEFINER
    AS $$
DECLARE

v_conf              text;
v_dblink            int;
v_dblink_name       text;
v_dblink_schema     text;
v_dest_schema_name  text;
v_dest_table        text;
v_dest_table_name   text;
v_filter            text;
v_link_exists       boolean;
v_old_search_path   text;
v_repl_index        oid;
v_remote_index_sql  text;
v_row               record;
v_source_table      text;
v_statement         text;
v_type              text;

BEGIN

v_dblink_name := @extschema@.check_name_length('create_index_dblink_'||p_destination);
SELECT nspname INTO v_dblink_schema FROM pg_namespace n, pg_extension e WHERE e.extname = 'dblink' AND e.extnamespace = n.oid;
SELECT current_setting('search_path') INTO v_old_search_path;
EXECUTE 'SELECT set_config(''search_path'',''@extschema@,'||v_dblink_schema||''',''false'')';

SELECT dest_table
    , type
    , dblink
    , filter
INTO v_dest_table
    , v_type
    , v_dblink
    , v_filter
FROM refresh_config
WHERE dest_table = p_destination; 
IF NOT FOUND THEN
   RAISE EXCEPTION 'ERROR: This table is not set up for replication: %', p_destination; 
END IF;

EXECUTE 'SELECT source_table FROM refresh_config_'||v_type||' WHERE dest_table = '||quote_literal(v_dest_table) INTO v_source_table;

IF p_snap IS NOT NULL AND p_snap NOT IN ('snap1', 'snap2') THEN
    RAISE EXCEPTION 'Invalid value for p_snap parameter given to create_index() function';
END IF;

PERFORM dblink_connect(v_dblink_name, @extschema@.auth(v_dblink));
-- Reset search_path on remote connection to ensure schema is included in table name in index creation statement
-- set_config returns a record value, so can't just use dblink_exec
SELECT set_config INTO v_conf FROM dblink(v_dblink_name, 'SELECT set_config(''search_path'', '''', false)::text') t (set_config text);

SELECT schemaname, tablename INTO v_dest_schema_name, v_dest_table_name FROM pg_catalog.pg_tables WHERE schemaname||'.'||tablename = v_dest_table||COALESCE('_'||p_snap, '');

-- Gets primary key or unique index used by updater/dml/logdel replication (same function is called in their makers). 
-- Should only loop once, but just easier to keep code consistent with below method
FOR v_row IN SELECT indexrelid, key_type, indkey_names, statement FROM fetch_replication_key(p_source_schema_name, p_source_table_name, v_dblink_name, p_debug)
LOOP

    EXIT WHEN v_row.indexrelid IS NULL; -- function still returns a row full of nulls when nothing found

    IF v_row.key_type = 'primary' THEN
        v_statement := format('ALTER TABLE %I.%I ADD CONSTRAINT %I PRIMARY KEY ("'||array_to_string(v_row.indkey_names, '","')||'")'
            , v_dest_schema_name, v_dest_table_name, v_dest_table_name ||'_'||array_to_string(v_row.indkey_names, '_')||'_pk');
        PERFORM gdb(p_debug, 'primary key statement: '||v_statement);
    ELSIF v_row.key_type = 'unique' THEN
        v_statement := v_row.statement;
        -- Replace source table name with destination
        v_statement := regexp_replace(
            v_statement
            , ' ON "?'||p_source_schema_name||'"?."?'||p_source_table_name||'"?'
            , ' ON '||quote_ident(v_dest_schema_name)||'.'||quote_ident(v_dest_table_name));
        -- If source index name contains the table name, replace it with the destination table.
        v_statement := regexp_replace(v_statement, '(INDEX \w*)'||p_source_table_name||'(\w* ON)', '\1'||v_dest_table_name||'\2');
        -- If it's a snap table, prepend to ensure unique index name (may cause snap1/2 to be in index name twice, but too complicated to fix that.
        -- This is done separately from above replace because it must always be done even if the index name doesn't contain the source table
        IF p_snap IS NOT NULL THEN
            v_statement := regexp_replace(v_statement, 'UNIQUE INDEX ("?)', 'UNIQUE INDEX \1'||p_snap||'_');
        END IF;
        PERFORM gdb(p_debug, 'unique key statement: ' || v_statement);
    END IF;
    EXECUTE v_statement;
    v_repl_index = v_row.indexrelid;
END LOOP;

-- Get all indexes other than one obtained above. 
-- Cannot set these indexes when column filters are in use because there's no easy way to check columns in expression indexes.
IF v_filter IS NULL THEN
    v_remote_index_sql := 'select c.relname AS src_table, pg_get_indexdef(i.indexrelid) as statement
        FROM pg_catalog.pg_index i
        JOIN pg_catalog.pg_class c ON i.indrelid = c.oid 
        JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid
        WHERE n.nspname = '||quote_literal(p_source_schema_name)||'
        AND c.relname = '||quote_literal(p_source_table_name)||'
        AND i.indisprimary IS false
        AND i.indisvalid';
    IF v_repl_index IS NOT NULL THEN
        v_remote_index_sql := v_remote_index_sql ||' AND i.indexrelid <> '||v_repl_index;
    END IF;

    FOR v_row IN EXECUTE 'SELECT src_table, statement FROM dblink('||quote_literal(v_dblink_name)||', '||quote_literal(v_remote_index_sql)||') t (src_table text, statement text)' LOOP
        v_statement := v_row.statement;
        -- Replace source table name with destination
        v_statement := regexp_replace(
            v_statement
            , ' ON "?'||p_source_schema_name||'"?."?'||p_source_table_name||'"?'
            , ' ON '||quote_ident(v_dest_schema_name)||'.'||quote_ident(v_dest_table_name));
        -- If source index name contains the table name, replace it with the destination table.
        v_statement := regexp_replace(v_statement, '(INDEX \w*)'||p_source_table_name||'(\w* ON)', '\1'||v_dest_table_name||'\2');
        -- If it's a snap table, prepend to ensure unique index name (may cause snap1/2 to be in index name twice, but too complicated to fix that.
        -- This is done separately from above replace because it must always be done even if the index name doesn't contain the source table
        IF p_snap IS NOT NULL THEN
            v_statement := regexp_replace(v_statement, 'E INDEX ("?)', 'E INDEX \1'||p_snap||'_');
        END IF;
        PERFORM gdb(p_debug, 'normal index statement: ' || v_statement);
        EXECUTE v_statement;
    END LOOP;
END IF;

PERFORM dblink_disconnect(v_dblink_name);

EXECUTE 'SELECT set_config(''search_path'','''||v_old_search_path||''',''false'')';

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
 *  Refresh based on DML (Insert, Update, Delete)
 */
CREATE OR REPLACE FUNCTION refresh_dml(p_destination text, p_limit int default NULL, p_repull boolean DEFAULT false, p_jobmon boolean DEFAULT NULL, p_debug boolean DEFAULT false) RETURNS void
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
v_dest_schema_name      text;
v_dest_table            text;
v_dest_table_name       text;
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
v_q_schema_name     text;
v_q_table_name      text;
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
v_delete_remote_q     text;
v_with_update           text;

BEGIN

IF p_debug IS DISTINCT FROM true THEN
    PERFORM set_config( 'client_min_messages', 'warning', true );
END IF;

v_job_name := 'Refresh DML: '||p_destination;
v_dblink_name := @extschema@.check_name_length('mimeo_dml_refresh_'||p_destination);

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
FROM refresh_config_dml 
WHERE dest_table = p_destination; 
IF NOT FOUND THEN
   RAISE EXCEPTION 'No configuration found for %',v_job_name; 
END IF;

-- Allow override with parameter
v_jobmon := COALESCE(p_jobmon, v_jobmon);

SELECT schemaname, tablename 
INTO v_dest_schema_name, v_dest_table_name
FROM pg_catalog.pg_tables
WHERE schemaname||'.'||tablename = v_dest_table;

IF v_dest_table_name IS NULL THEN
    RAISE EXCEPTION 'Destination table is missing (%)', v_dest_table;
END IF;

-- Take advisory lock to prevent multiple calls to function overlapping
v_adv_lock := pg_try_advisory_xact_lock(hashtext('refresh_dml'), hashtext(v_job_name));
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

IF v_src_table_name IS NULL THEN
    RAISE EXCEPTION 'Source table missing (%)', v_source_table;
END IF;

IF p_limit IS NOT NULL THEN
    v_limit := p_limit;
END IF;

v_pk_name_csv := '"'||array_to_string(v_pk_name, '","')||'"';
v_pk_counter := 1;
WHILE v_pk_counter <= array_length(v_pk_name,1) LOOP
    IF v_pk_counter > 1 THEN
        v_pk_name_type_csv := v_pk_name_type_csv || ', ';
        v_pk_where := v_pk_where ||' AND ';
    END IF;
    v_pk_name_type_csv := v_pk_name_type_csv||'"'||v_pk_name[v_pk_counter]||'" '||v_pk_type[v_pk_counter];
    v_pk_where := v_pk_where || ' a."'||v_pk_name[v_pk_counter]||'" = b."'||v_pk_name[v_pk_counter]||'"';
    v_pk_counter := v_pk_counter + 1;
END LOOP;

IF v_jobmon THEN
    PERFORM update_step(v_step_id, 'OK','Done');
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
            SELECT '||v_pk_name_csv||' FROM %I.%I ORDER BY '||v_pk_name_csv||' LIMIT '||COALESCE(v_limit::text, 'ALL')||')
        UPDATE %I.%I b SET processed = true 
        FROM a 
        WHERE '||v_pk_where
    , v_q_schema_name, v_q_table_name, v_q_schema_name, v_q_table_name);
PERFORM gdb(p_debug, v_with_update);
v_trigger_update := 'SELECT dblink_exec('||quote_literal(v_dblink_name)||','|| quote_literal(v_with_update)||')';
PERFORM gdb(p_debug,v_trigger_update);
EXECUTE v_trigger_update INTO v_exec_status;    
IF v_jobmon THEN
    PERFORM update_step(v_step_id, 'OK','Result was '||v_exec_status);
END IF;

IF p_repull THEN
    IF v_jobmon THEN
        PERFORM update_step(v_step_id, 'OK','Request to repull ALL data from source. This could take a while...');
    END IF;
    PERFORM gdb(p_debug, 'Request to repull ALL data from source. This could take a while...');

    IF v_jobmon THEN
        v_step_id := add_step(v_job_id,'Truncating local table');
    END IF;
    PERFORM gdb(p_debug,'Truncating local table');
    EXECUTE format('TRUNCATE %I.%I', v_dest_schema_name, v_dest_table_name);
    IF v_jobmon THEN
        PERFORM update_step(v_step_id, 'OK','Done');
    END IF;
    -- Define cursor query
    v_remote_f_sql := format('SELECT '||v_cols||' FROM %I.%I', v_src_schema_name, v_src_table_name);
    IF v_condition IS NOT NULL THEN
        v_remote_f_sql := v_remote_f_sql || ' ' || v_condition;
    END IF;
ELSE
    EXECUTE 'CREATE TEMP TABLE refresh_dml_queue ('||v_pk_name_type_csv||', PRIMARY KEY ('||v_pk_name_csv||'))';
    -- Copy queue locally for use in removing updated/deleted rows
    v_remote_q_sql := format('SELECT DISTINCT '||v_pk_name_csv||' FROM %I.%I WHERE processed = true', v_q_schema_name, v_q_table_name);
    PERFORM dblink_open(v_dblink_name, 'mimeo_cursor', v_remote_q_sql);
    IF v_jobmon THEN
        v_step_id := add_step(v_job_id, 'Creating local queue temp table');
    END IF;
    v_rowcount := 0;
    LOOP
        v_fetch_sql := 'INSERT INTO refresh_dml_queue ('||v_pk_name_csv||') 
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
    EXECUTE 'CREATE INDEX ON refresh_dml_queue ('||v_pk_name_csv||')';
    ANALYZE refresh_dml_queue;
    IF v_jobmon THEN
        PERFORM update_step(v_step_id, 'OK','Number of rows inserted: '||v_total);
    END IF;
    PERFORM gdb(p_debug,'Temp queue table row count '||v_total::text);

    IF v_jobmon THEN
        v_step_id := add_step(v_job_id,'Deleting records from local table');
    END IF;
    v_delete_sql := format('DELETE FROM %I.%I a USING refresh_dml_queue b WHERE '||v_pk_where, v_dest_schema_name, v_dest_table_name);
    PERFORM gdb(p_debug,v_delete_sql);
    EXECUTE v_delete_sql; 
    GET DIAGNOSTICS v_rowcount = ROW_COUNT;
    PERFORM gdb(p_debug,'Rows removed from local table before applying changes: '||v_rowcount::text);
    IF v_jobmon THEN
        PERFORM update_step(v_step_id, 'OK','Removed '||v_rowcount||' records');
    END IF;
    -- Define cursor query
    v_remote_f_sql := format('SELECT '||v_cols||' FROM %I.%I JOIN ('||v_remote_q_sql||') x USING ('||v_pk_name_csv||')', v_src_schema_name, v_src_table_name);
    IF v_condition IS NOT NULL THEN
        v_remote_f_sql := v_remote_f_sql || ' ' || v_condition;
    END IF;
END IF;

-- insert records to local table. Have to do temp table in case destination table is partitioned (returns 0 when inserting to parent)
PERFORM dblink_open(v_dblink_name, 'mimeo_cursor', v_remote_f_sql);
IF v_jobmon THEN
    v_step_id := add_step(v_job_id, 'Inserting new records into local table');
END IF;
EXECUTE 'CREATE TEMP TABLE refresh_dml_full ('||v_cols_n_types||')';
v_rowcount := 0;
v_total := 0;
LOOP
    v_fetch_sql := 'INSERT INTO refresh_dml_full ('||v_cols||') 
        SELECT '||v_cols||' FROM dblink_fetch('||quote_literal(v_dblink_name)||', ''mimeo_cursor'', 50000) AS ('||v_cols_n_types||')';
    EXECUTE v_fetch_sql;
    GET DIAGNOSTICS v_rowcount = ROW_COUNT;
    v_total := v_total + coalesce(v_rowcount, 0);
    EXECUTE format('INSERT INTO %I.%I ('||v_cols||') SELECT '||v_cols||' FROM refresh_dml_full', v_dest_schema_name, v_dest_table_name);
    EXECUTE 'TRUNCATE refresh_dml_full';
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

-- clean out rows from remote queue table
IF v_jobmon THEN
    v_step_id := add_step(v_job_id,'Cleaning out rows from remote queue table');
END IF;
v_trigger_delete := format('SELECT dblink_exec(%L, ''DELETE FROM %I.%I WHERE processed = true'')', v_dblink_name, v_q_schema_name, v_q_table_name);
PERFORM gdb(p_debug,v_trigger_delete);
EXECUTE v_trigger_delete INTO v_exec_status;
IF v_jobmon THEN
    PERFORM update_step(v_step_id, 'OK','Result was '||v_exec_status);
END IF;
-- update activity status
IF v_jobmon THEN
    v_step_id := add_step(v_job_id,'Updating last_run in config table');
END IF;
UPDATE refresh_config_dml SET last_run = CURRENT_TIMESTAMP WHERE dest_table = v_dest_table; 
IF v_jobmon THEN
    PERFORM update_step(v_step_id, 'OK','Last run was '||CURRENT_TIMESTAMP);
END IF;
EXECUTE 'DROP TABLE IF EXISTS refresh_dml_full';
EXECUTE 'DROP TABLE IF EXISTS refresh_dml_queue';

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
                EXECUTE 'SELECT '||v_jobmon_schema||'.add_job(''Refresh DML: '||p_destination||''')' INTO v_job_id;
                EXECUTE 'SELECT '||v_jobmon_schema||'.add_step('||v_job_id||', ''EXCEPTION before job logging started'')' INTO v_step_id;
            END IF;
            IF v_step_id IS NULL THEN
                EXECUTE 'SELECT '||v_jobmon_schema||'.add_step('||v_job_id||', ''EXCEPTION before first step logged'')' INTO v_step_id;
            END IF;
            EXECUTE 'SELECT '||v_jobmon_schema||'.update_step('||v_step_id||', ''CRITICAL'', ''ERROR: '||coalesce(SQLERRM,'unknown')||''')';
            EXECUTE 'SELECT '||v_jobmon_schema||'.fail_job('||v_job_id||')';
        END IF;
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
v_dblink_name               text;
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
v_pk_name_csv               text;
v_pk_name_n_type            text[];
v_pk_type                   text[] := p_pk_type;
v_pk_value                  text := '';
v_remote_exists             int := 0;
v_remote_grants_sql         text;
v_remote_key_sql            text;
v_remote_q_index            text;
v_remote_q_table            text;
v_row                       record;
v_source_queue_counter      int := 0;
v_source_queue_exists       text;
v_source_queue_function     text;
v_source_queue_table        text;
v_source_queue_trigger      text;
v_src_schema_name            text;
v_src_table_name             text;
v_src_table_template        text;
v_table_exists              boolean;
v_trigger_func              text;

BEGIN

v_dblink_name := @extschema@.check_name_length('mimeo_dml_maker_'||p_src_table);

SELECT nspname INTO v_dblink_schema FROM pg_namespace n, pg_extension e WHERE e.extname = 'dblink' AND e.extnamespace = n.oid;
SELECT current_setting('search_path') INTO v_old_search_path;
EXECUTE 'SELECT set_config(''search_path'',''@extschema@,'||v_dblink_schema||',public'',''false'')';

IF (p_pk_name IS NULL AND p_pk_type IS NOT NULL) OR (p_pk_name IS NOT NULL AND p_pk_type IS NULL) THEN
    RAISE EXCEPTION 'Cannot manually set primary/unique key field(s) without defining type(s) or vice versa';
END IF;

SELECT data_source INTO v_data_source FROM @extschema@.dblink_mapping_mimeo WHERE data_source_id = p_dblink_id; 
IF NOT FOUND THEN
    RAISE EXCEPTION 'Database link ID is incorrect %', p_dblink_id; 
END IF;

IF p_dest_table IS NULL THEN
    p_dest_table := p_src_table;
END IF;

IF position('.' in p_dest_table) > 0 AND position('.' in p_src_table) > 0 THEN
    -- Do nothing. Schema & table variable names set below after table is created
ELSE
    RAISE EXCEPTION 'Source (and destination) table must be schema qualified';
END IF;

PERFORM dblink_connect(v_dblink_name, @extschema@.auth(p_dblink_id));

SELECT schemaname ||'_'|| tablename, schemaname, tablename
INTO v_src_table_template, v_src_schema_name, v_src_table_name
FROM dblink(v_dblink_name, format('
            SELECT schemaname, tablename
            FROM (
                SELECT schemaname, tablename 
                FROM pg_catalog.pg_tables 
                WHERE schemaname ||''.''|| tablename = %L
                UNION
                SELECT schemaname, viewname AS tablename
                FROM pg_catalog.pg_views
                WHERE schemaname || ''.'' || viewname = %L
            ) tables LIMIT 1'
        , p_src_table, p_src_table) )
    t (schemaname text, tablename text);

IF v_src_table_template IS NULL THEN
    RAISE EXCEPTION 'Source table given (%) does not exist in configured source database', p_src_table;
END IF;

v_source_queue_table :=  check_name_length(v_src_table_template, '_q');
v_source_queue_function := check_name_length(v_src_table_template, '_mimeo_queue');
v_source_queue_trigger := check_name_length(v_src_table_template, '_mimeo_trig');

-- Automatically get source primary/unique key if none given
IF p_pk_name IS NULL AND p_pk_type IS NULL THEN
    SELECT key_type, indkey_names, indkey_types INTO v_key_type, v_pk_name, v_pk_type FROM fetch_replication_key(v_src_schema_name, v_src_table_name, v_dblink_name, p_debug);
END IF;

v_pk_name_csv := '"'||array_to_string(v_pk_name, '","')||'"';
PERFORM gdb(p_debug, 'v_key_type: '||COALESCE(v_key_type, ''));
PERFORM gdb(p_debug, 'v_pk_name: '||COALESCE(v_pk_name_csv, ''));
PERFORM gdb(p_debug, 'v_pk_type: '||COALESCE(array_to_string(v_pk_type, ','), ''));

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

-- Do check for existing queue table(s) to support multiple destinations
SELECT tablename 
    INTO v_source_queue_exists 
FROM dblink(v_dblink_name
        , 'SELECT tablename 
        FROM pg_catalog.pg_tables 
        WHERE schemaname = ''@extschema@''
        AND tablename = '||quote_literal(v_source_queue_table)) t (tablename text);
WHILE v_source_queue_exists IS NOT NULL LOOP -- loop until a tablename that doesn't exist is found
    v_source_queue_counter := v_source_queue_counter + 1;
    IF v_source_queue_counter > 99 THEN
        RAISE EXCEPTION 'Limit of 99 queue tables for a single source table reached. No more destination tables possible (and HIGHLY discouraged)';
    END IF;
    v_source_queue_table := check_name_length(v_src_table_template, '_q'||to_char(v_source_queue_counter, 'FM00'));
    SELECT tablename 
        INTO v_source_queue_exists 
    FROM dblink(v_dblink_name
        , 'SELECT tablename 
        FROM pg_catalog.pg_tables 
        WHERE schemaname = ''@extschema@''
        AND tablename = '||quote_literal(v_source_queue_table)) t (tablename text);
    v_source_queue_function := check_name_length(v_src_table_template, '_mimeo_queue'||to_char(v_source_queue_counter, 'FM00'));
    v_source_queue_trigger := check_name_length(v_src_table_template, '_mimeo_trig'||to_char(v_source_queue_counter, 'FM00'));
END LOOP;

v_remote_q_table := format('CREATE TABLE %I.%I (', '@extschema@', v_source_queue_table);
    PERFORM gdb(p_debug, 'v_remote_q_table: '||v_remote_q_table);
WHILE v_pk_counter <= array_length(v_pk_name,1) LOOP
    v_remote_q_table := v_remote_q_table || format('%I', v_pk_name[v_pk_counter]) ||' '||v_pk_type[v_pk_counter];
    v_pk_counter := v_pk_counter + 1;
    IF v_pk_counter <= array_length(v_pk_name,1) THEN
        v_remote_q_table := v_remote_q_table || ', ';
    END IF;
END LOOP;
v_remote_q_table := v_remote_q_table || ', processed boolean)';
v_remote_q_index := format('CREATE INDEX ON %I.%I (', '@extschema@', v_source_queue_table) || v_pk_name_csv ||')';

v_pk_counter := 1;
v_trigger_func := format('CREATE FUNCTION %I.%I() RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER AS $_$ ', '@extschema@', v_source_queue_function);
    v_trigger_func := v_trigger_func || ' 
        BEGIN IF TG_OP = ''INSERT'' THEN ';
    v_pk_value := 'NEW."' || array_to_string(v_pk_name, '", NEW."') || '"';
    v_trigger_func := v_trigger_func || format(' 
            INSERT INTO %I.%I (', '@extschema@', v_source_queue_table) || v_pk_name_csv ||') VALUES ('||v_pk_value||'); ';
    v_trigger_func := v_trigger_func || ' 
        ELSIF TG_OP = ''UPDATE'' THEN ';
    -- UPDATE needs to insert the NEW values so reuse v_pk_value from INSERT operation
    v_trigger_func := v_trigger_func || format(' 
            INSERT INTO %I.%I (', '@extschema@', v_source_queue_table) || v_pk_name_csv ||') VALUES ('||v_pk_value||'); ';
    -- Only insert the old row if the new key doesn't match the old key. This handles edge case when only one column of a composite key is updated
    v_trigger_func := v_trigger_func || ' 
            IF ';
    FOREACH v_field IN ARRAY v_pk_name LOOP
        IF v_pk_counter > 1 THEN
            v_trigger_func := v_trigger_func || ' OR ';
        END IF;
        v_trigger_func := v_trigger_func || format(' NEW.%I != OLD.%I ', v_field, v_field);
        v_pk_counter := v_pk_counter + 1;
    END LOOP;
    v_trigger_func := v_trigger_func || ' THEN ';
    v_pk_value := 'OLD."' || array_to_string(v_pk_name, '", OLD."') || '"';
    v_trigger_func := v_trigger_func || format(' 
                INSERT INTO %I.%I (', '@extschema@', v_source_queue_table)|| v_pk_name_csv ||') VALUES ('||v_pk_value||'); ';
    v_trigger_func := v_trigger_func || ' 
            END IF;';
    v_trigger_func := v_trigger_func || ' 
        ELSIF TG_OP = ''DELETE'' THEN ';
    -- DELETE needs to insert the OLD values so reuse v_pk_value from UPDATE operation
    v_trigger_func := v_trigger_func || format(' 
            INSERT INTO %I.%I (', '@extschema@', v_source_queue_table)|| v_pk_name_csv ||') VALUES ('||v_pk_value||'); ';
v_trigger_func := v_trigger_func || ' 
        END IF; RETURN NULL; END $_$;';

v_create_trig := format('CREATE TRIGGER %I AFTER INSERT OR DELETE OR UPDATE', v_source_queue_trigger);
IF p_filter IS NOT NULL THEN
    v_create_trig := v_create_trig || ' OF "'||array_to_string(p_filter, '","')||'"';
END IF;
v_create_trig := v_create_trig || format(' ON %I.%I FOR EACH ROW EXECUTE PROCEDURE %I.%I()', v_src_schema_name, v_src_table_name, '@extschema@', v_source_queue_function);

RAISE NOTICE 'Creating objects on source database (function, trigger & queue table)...';

PERFORM gdb(p_debug, 'v_remote_q_table: '||v_remote_q_table);
PERFORM dblink_exec(v_dblink_name, v_remote_q_table);
PERFORM gdb(p_debug, 'v_remote_q_index: '||v_remote_q_index);
PERFORM dblink_exec(v_dblink_name, v_remote_q_index);
PERFORM gdb(p_debug, 'v_trigger_func: '||v_trigger_func);
PERFORM dblink_exec(v_dblink_name, v_trigger_func);
PERFORM gdb(p_debug, 'v_create_trig: '||v_create_trig);
PERFORM dblink_exec(v_dblink_name, v_create_trig);

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
        ||', '||quote_literal('@extschema@.'||v_source_queue_table)
        ||', '||quote_literal(v_pk_name)
        ||', '||quote_literal(v_pk_type)
        ||', '||quote_literal(CURRENT_TIMESTAMP)
        ||', '||COALESCE(quote_literal(p_filter), 'NULL')
        ||', '||COALESCE(quote_literal(p_condition), 'NULL')
        ||', '||v_jobmon||')';
PERFORM gdb(p_debug, 'v_insert_refresh_config: '||v_insert_refresh_config);
EXECUTE v_insert_refresh_config;

SELECT p_table_exists FROM manage_dest_table(p_dest_table, NULL, v_dblink_name , p_debug) INTO v_table_exists;

SELECT schemaname, tablename 
INTO v_dest_schema_name, v_dest_table_name 
FROM pg_catalog.pg_tables 
WHERE schemaname||'.'||tablename = p_dest_table;

IF p_pulldata AND v_table_exists = false THEN
    RAISE NOTICE 'Pulling data from source...';
    EXECUTE 'SELECT refresh_dml('||quote_literal(p_dest_table)||', p_repull := true, p_debug := '||p_debug||')';
END IF;

IF p_index AND v_table_exists = false THEN
    PERFORM create_index(p_dest_table, v_src_schema_name, v_src_table_name, NULL, p_debug);
ELSIF v_table_exists = false THEN
-- Ensure destination indexes that are needed for efficient replication are created even if p_index is set false
    PERFORM gdb(p_debug, 'Creating indexes needed for replication');
    IF v_key_type = 'primary' THEN
        EXECUTE format('ALTER TABLE %I.%I ADD PRIMARY KEY (', v_dest_schema_name, v_dest_table_name) || v_pk_name_csv ||')';
    ELSE
        EXECUTE format('CREATE UNIQUE INDEX ON %I.%I (', v_dest_schema_name, v_dest_table_name) || v_pk_name_csv ||')';
    END IF;
END IF;

IF v_table_exists THEN
    RAISE NOTICE 'Destination table % already exists. No data or indexes were pulled from source', p_src_table;
END IF;

RAISE NOTICE 'Analyzing destination table...';
EXECUTE format('ANALYZE %I.%I', v_dest_schema_name, v_dest_table_name);

PERFORM dblink_disconnect(v_dblink_name);

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
                PERFORM dblink_exec(v_dblink_name, format('DROP TRIGGER IF EXISTS %I ON %I.%I', v_source_queue_trigger, v_src_schema_name, v_src_table_name));
                PERFORM dblink_exec(v_dblink_name, format('DROP TABLE IF EXISTS %I.%I', '@extschema@', v_source_queue_table));
                PERFORM dblink_exec(v_dblink_name, format('DROP FUNCTION IF EXISTS %I.%I()', '@extschema@', v_source_queue_function));
            END IF;
            PERFORM dblink_disconnect(v_dblink_name);
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
 *  Refresh insert only table based on serial control field
 */
CREATE OR REPLACE FUNCTION refresh_inserter_serial(p_destination text, p_limit integer DEFAULT NULL, p_repull boolean DEFAULT false, p_repull_start bigint DEFAULT NULL, p_repull_end bigint DEFAULT NULL, p_jobmon boolean DEFAULT NULL, p_debug boolean DEFAULT false) RETURNS void
    LANGUAGE plpgsql SECURITY DEFINER
    AS $$
DECLARE

v_adv_lock              boolean;
v_batch_limit_reached   int := 0; 
v_boundary              int;
v_cols_n_types          text;
v_cols                  text;
v_condition             text;
v_control               text;
v_create_sql            text;
v_dblink                int;
v_dblink_name           text;
v_dblink_schema         text;
v_delete_sql            text;
v_dest_schema_name      text;
v_dest_table            text;
v_dest_table_name       text;
v_fetch_sql             text;
v_filter                text[]; 
v_full_refresh          boolean := false;
v_insert_sql            text;
v_job_id                int;
v_jobmon                boolean;
v_jobmon_schema         text;
v_job_name              text;
v_last_fetched          bigint;
v_last_value            bigint;
v_limit                 int;
v_link_exists           boolean;
v_old_search_path       text;
v_remote_sql            text;
v_rowcount              bigint := 0; 
v_source_table          text;
v_sql                   text;
v_src_schema_name       text;
v_src_table_name        text;
v_step_id               int;
v_total                 bigint := 0;

BEGIN

IF p_debug IS DISTINCT FROM true THEN
    PERFORM set_config( 'client_min_messages', 'warning', true );
END IF;

v_job_name := 'Refresh Inserter: '||p_destination;

SELECT nspname INTO v_dblink_schema FROM pg_namespace n, pg_extension e WHERE e.extname = 'dblink' AND e.extnamespace = n.oid;
SELECT nspname INTO v_jobmon_schema FROM pg_namespace n, pg_extension e WHERE e.extname = 'pg_jobmon' AND e.extnamespace = n.oid;
IF p_jobmon IS TRUE AND v_jobmon_schema IS NULL THEN
    RAISE EXCEPTION 'p_jobmon parameter set to TRUE, but unable to determine if pg_jobmon extension is installed';
END IF;

v_dblink_name := @extschema@.check_name_length('mimeo_inserter_refresh_'||p_destination);

-- Set custom search path to allow easier calls to other functions, especially job logging
SELECT current_setting('search_path') INTO v_old_search_path;
EXECUTE 'SELECT set_config(''search_path'',''@extschema@,'||COALESCE(v_jobmon_schema||',', '')||v_dblink_schema||',public'',''false'')';

SELECT source_table
    , dest_table
    , dblink
    , control
    , last_value
    , boundary
    , filter
    , condition
    , batch_limit 
    , jobmon
INTO v_source_table
    , v_dest_table
    , v_dblink
    , v_control
    , v_last_value
    , v_boundary
    , v_filter
    , v_condition
    , v_limit
    , v_jobmon
FROM refresh_config_inserter_serial
WHERE dest_table = p_destination; 
IF NOT FOUND THEN
   RAISE EXCEPTION 'No configuration found for %',v_job_name; 
END IF;  

-- Allow override with parameter
v_jobmon := COALESCE(p_jobmon, v_jobmon);

SELECT schemaname, tablename 
INTO v_dest_schema_name, v_dest_table_name
FROM pg_catalog.pg_tables
WHERE schemaname||'.'||tablename = v_dest_table;

IF v_dest_table_name IS NULL THEN
    RAISE EXCEPTION 'Destination table is missing (%)', v_dest_table;
END IF;

-- Take advisory lock to prevent multiple calls to function overlapping
v_adv_lock := pg_try_advisory_xact_lock(hashtext('refresh_inserter'), hashtext(v_job_name));
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

IF v_jobmon THEN
    v_step_id := add_step(v_job_id,'Building SQL');
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

IF v_src_table_name IS NULL THEN
    RAISE EXCEPTION 'Source table missing (%)', v_source_table;
END IF;

IF p_limit IS NOT NULL THEN
    v_limit := p_limit;
END IF;

-- Unlike incremental time, there's nothing like CURRENT_TIMESTAMP to base the boundary on. So use the current source max to determine it.
-- For some reason this doesn't like using an int with %L (v_boundary) when making up the format command using dblink
v_sql := format('SELECT boundary FROM dblink(%L, ''SELECT max(%I) - '||v_boundary||' AS boundary FROM %I.%I'') AS (boundary bigint)'
                , v_dblink_name 
                , v_control
                , v_src_schema_name
                , v_src_table_name);
PERFORM gdb(p_debug, v_sql);
EXECUTE v_sql INTO v_boundary;

IF p_repull THEN
    -- Repull ALL data if no start and end values set
    IF p_repull_start IS NULL AND p_repull_end IS NULL THEN
        IF v_jobmon THEN
            PERFORM update_step(v_step_id, 'OK','Request to repull ALL data from source. This could take a while...');
        END IF;
        EXECUTE format('TRUNCATE %I.%I', v_dest_schema_name, v_dest_table_name);
        -- Use upper boundary remote max to avoid edge case of multiple upper boundary values inserting during refresh
        v_remote_sql := format('SELECT '||v_cols||' FROM %I.%I', v_src_schema_name, v_src_table_name);
        IF v_condition IS NOT NULL THEN
            v_remote_sql := v_remote_sql || ' ' || v_condition || ' AND ';
        ELSE
            v_remote_sql := v_remote_sql || ' WHERE ';
        END IF;
        v_remote_sql := format(v_remote_sql || '%I < %L', v_control, v_boundary);
    ELSE
        IF v_jobmon THEN
            PERFORM update_step(v_step_id, 'OK','Request to repull data from '||COALESCE(p_repull_start, '0')||' to '||COALESCE(p_repull_end, v_boundary));
        END IF;
        PERFORM gdb(p_debug,'Request to repull data from '||COALESCE(p_repull_start, '0')||' to '||COALESCE(p_repull_end, v_boundary));
        v_remote_sql := format('SELECT '||v_cols||' FROM %I.%I', v_src_schema_name, v_src_table_name);
        IF v_condition IS NOT NULL THEN
            v_remote_sql := v_remote_sql || ' ' || v_condition || ' AND ';
        ELSE
            v_remote_sql := v_remote_sql || ' WHERE ';
        END IF;
        -- Use upper boundary remote max to avoid edge case of multiple upper boundary values inserting during refresh
        v_remote_sql := v_remote_sql || format('%I > %L AND %I < %L'
                                        , v_control
                                        , COALESCE(p_repull_start, 0)
                                        , v_control
                                        , COALESCE(p_repull_end, v_boundary));
        -- Delete the old local data. Use higher than bigint max upper boundary to ensure all old data is deleted
        v_delete_sql := format('DELETE FROM %I.%I WHERE %I > %L AND %L < %L'
                        , v_dest_schema_name
                        , v_dest_table_name
                        , v_control
                        , COALESCE(p_repull_start, 0)
                        , v_control
                        , COALESCE(p_repull_end, 9300000000000000000));
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
    v_remote_sql := format('SELECT '||v_cols||' FROM %I.%I', v_src_schema_name, v_src_table_name);
    IF v_condition IS NOT NULL THEN
        v_remote_sql := v_remote_sql || ' ' || v_condition || ' AND ';
    ELSE
        v_remote_sql := v_remote_sql || ' WHERE ';
    END IF;
    v_remote_sql := v_remote_sql || format('%I > %L AND %I < %L ORDER BY %I ASC LIMIT '||COALESCE(v_limit::text, 'ALL')
                                    , v_control
                                    , v_last_value
                                    , v_control
                                    , v_boundary
                                    , v_control);

    IF v_jobmon THEN
        PERFORM update_step(v_step_id, 'OK','Grabbing rows from '||v_last_value::text||' to '||v_boundary::text);
    END IF;
    PERFORM gdb(p_debug,'Grabbing rows from '||v_last_value::text||' to '||v_boundary::text);

END IF;

EXECUTE 'CREATE TEMP TABLE mimeo_refresh_inserter_temp ('||v_cols_n_types||')';
PERFORM gdb(p_debug, 'v_remote_sql: '||COALESCE(v_remote_sql, '<NULL>'));
PERFORM dblink_open(v_dblink_name, 'mimeo_cursor', v_remote_sql);
IF v_jobmon THEN
    v_step_id := add_step(v_job_id, 'Inserting new records into local table');
END IF;
v_rowcount := 0;
v_total := 0;
LOOP
    v_fetch_sql := 'INSERT INTO mimeo_refresh_inserter_temp ('||v_cols||') 
        SELECT '||v_cols||' FROM dblink_fetch('||quote_literal(v_dblink_name)||', ''mimeo_cursor'', 50000) AS ('||v_cols_n_types||')';
    EXECUTE v_fetch_sql;
    GET DIAGNOSTICS v_rowcount = ROW_COUNT;
    v_total := v_total + coalesce(v_rowcount, 0);
    EXECUTE format('SELECT max(%I) FROM mimeo_refresh_inserter_temp', v_control) INTO v_last_fetched;
    IF v_limit IS NULL THEN -- insert into the real table in batches if no limit to avoid excessively large temp tables
        EXECUTE format('INSERT INTO %I.%I ('||v_cols||') SELECT '||v_cols||' FROM mimeo_refresh_inserter_temp', v_dest_schema_name, v_dest_table_name);
        TRUNCATE mimeo_refresh_inserter_temp;
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
        EXECUTE format('SELECT max(%I) FROM mimeo_refresh_inserter_temp', v_control) INTO v_last_value;
        IF v_jobmon THEN
            v_step_id := add_step(v_job_id, 'Removing high boundary rows from this batch to avoid missing data');       
        END IF;
        EXECUTE format('DELETE FROM mimeo_refresh_inserter_temp WHERE %I = %L', v_control, v_last_value);
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
                PERFORM update_step(v_step_id, 'CRITICAL', 'Batch contained max rows ('||v_limit||') or greater and all contained the same serial value. Unable to guarentee rows will ever be replicated consistently. Increase row limit parameter to allow a consistent batch.');
            END IF;
            PERFORM gdb(p_debug, 'Batch contained max rows desired ('||v_limit||') or greaer and all contained the same serial value. Unable to guarentee rows will be replicated consistently. Increase row limit parameter to allow a consistent batch.');
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
        EXECUTE format('INSERT INTO %I.%I ('||v_cols||') SELECT '||v_cols||' FROM mimeo_refresh_inserter_temp', v_dest_schema_name, v_dest_table_name);
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
    EXECUTE format('SELECT max(%I) FROM %I.%I', v_control, v_dest_schema_name, v_dest_table_name) INTO v_last_value;
    UPDATE refresh_config_inserter_serial SET last_value = coalesce(v_last_value, 0), last_run = CURRENT_TIMESTAMP WHERE dest_table = p_destination;  
    IF v_jobmon THEN
        PERFORM update_step(v_step_id, 'OK','Lower boundary value is: '|| coalesce(v_last_value, 0));
        PERFORM gdb(p_debug, 'Lower boundary value is: '||coalesce(v_last_value, 0));
    END IF;
END IF;

DROP TABLE IF EXISTS mimeo_refresh_inserter_temp;

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
                EXECUTE 'SELECT '||v_jobmon_schema||'.add_job(''Refresh Inserter: '||p_destination||''')' INTO v_job_id;
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


/*
 *  Refresh insert only table based on timestamp control field
 */
CREATE OR REPLACE FUNCTION refresh_inserter_time(p_destination text, p_limit integer DEFAULT NULL, p_repull boolean DEFAULT false, p_repull_start timestamp DEFAULT NULL, p_repull_end timestamp DEFAULT NULL, p_jobmon boolean DEFAULT NULL, p_debug boolean DEFAULT false) RETURNS void
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
v_dest_schema_name      text;
v_dest_table            text;
v_dest_table_name       text;
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
v_old_search_path       text;
v_remote_sql            text;
v_rowcount              bigint := 0; 
v_source_table          text;
v_src_schema_name       text;
v_src_table_name        text;
v_step_id               int;
v_total                 bigint := 0;

BEGIN

IF p_debug IS DISTINCT FROM true THEN
    PERFORM set_config( 'client_min_messages', 'warning', true );
END IF;

v_job_name := 'Refresh Inserter: '||p_destination;

SELECT nspname INTO v_dblink_schema FROM pg_namespace n, pg_extension e WHERE e.extname = 'dblink' AND e.extnamespace = n.oid;
SELECT nspname INTO v_jobmon_schema FROM pg_namespace n, pg_extension e WHERE e.extname = 'pg_jobmon' AND e.extnamespace = n.oid;
IF p_jobmon IS TRUE AND v_jobmon_schema IS NULL THEN
    RAISE EXCEPTION 'p_jobmon parameter set to TRUE, but unable to determine if pg_jobmon extension is installed';
END IF;

v_dblink_name := @extschema@.check_name_length('mimeo_inserter_refresh_'||p_destination);

-- Set custom search path to allow easier calls to other functions, especially job logging
SELECT current_setting('search_path') INTO v_old_search_path;
EXECUTE 'SELECT set_config(''search_path'',''@extschema@,'||COALESCE(v_jobmon_schema||',', '')||v_dblink_schema||',public'',''false'')';

SELECT source_table
    , dest_table
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
FROM refresh_config_inserter_time
WHERE dest_table = p_destination; 
IF NOT FOUND THEN
   RAISE EXCEPTION 'No configuration found for %',v_job_name; 
END IF;  

-- Allow override with parameter
v_jobmon := COALESCE(p_jobmon, v_jobmon);

SELECT schemaname, tablename 
INTO v_dest_schema_name, v_dest_table_name
FROM pg_catalog.pg_tables
WHERE schemaname||'.'||tablename = v_dest_table;

-- Take advisory lock to prevent multiple calls to function overlapping
v_adv_lock := pg_try_advisory_xact_lock(hashtext('refresh_inserter'), hashtext(v_job_name));
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
    v_dst_check := @extschema@.dst_change(CURRENT_TIMESTAMP);
    IF v_dst_check THEN 
        IF to_number(to_char(CURRENT_TIMESTAMP, 'HH24MM'), '0000') > v_dst_start AND to_number(to_char(CURRENT_TIMESTAMP, 'HH24MM'), '0000') < v_dst_end THEN
            IF v_jobmon THEN
                v_step_id := add_step( v_job_id, 'DST Check');
                PERFORM update_step(v_step_id, 'OK', 'Job CANCELLED - Does not run during DST time change');
                PERFORM close_job(v_job_id);
            END IF;
            PERFORM gdb(p_debug, 'Cannot run during DST time change');
            UPDATE refresh_config_inserter SET last_run = CURRENT_TIMESTAMP WHERE dest_table = p_destination;  
            EXECUTE 'SELECT set_config(''search_path'','''||v_old_search_path||''',''false'')';
            RETURN;
        END IF;
    END IF;
END IF;

IF v_jobmon THEN
    v_step_id := add_step(v_job_id,'Building SQL');
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

IF v_src_table_name IS NULL THEN
    RAISE EXCEPTION 'Source table missing (%)', v_source_table;
END IF;

IF p_limit IS NOT NULL THEN
    v_limit := p_limit;
END IF;

IF p_repull THEN
    -- Repull ALL data if no start and end values set
    IF p_repull_start IS NULL AND p_repull_end IS NULL THEN
        IF v_jobmon THEN
            PERFORM update_step(v_step_id, 'OK','Request to repull ALL data from source. This could take a while...');
        END IF;
        EXECUTE format('TRUNCATE %I.%I', v_dest_schema_name, v_dest_table_name);
        v_remote_sql := format('SELECT '||v_cols||' FROM %I.%I', v_src_schema_name, v_src_table_name);
        -- Use upper boundary to avoid edge case of multiple upper boundary values inserting during refresh
        IF v_condition IS NOT NULL THEN
            v_remote_sql := v_remote_sql || ' ' || v_condition || ' AND ';
        ELSE
            v_remote_sql := v_remote_sql || ' WHERE ';
        END IF;
        v_remote_sql := v_remote_sql ||format('%I < %L', v_control, v_boundary);
    ELSE
        IF v_jobmon THEN
            PERFORM update_step(v_step_id, 'OK','Request to repull data from '||COALESCE(p_repull_start, '-infinity')||' to '||COALESCE(p_repull_end, v_boundary));
        END IF;
        PERFORM gdb(p_debug,'Request to repull data from '||COALESCE(p_repull_start, '-infinity')||' to '||COALESCE(p_repull_end, v_boundary));
        v_remote_sql := format('SELECT '||v_cols||' FROM %I.%I', v_src_schema_name, v_src_table_name);
        IF v_condition IS NOT NULL THEN
            v_remote_sql := v_remote_sql || ' ' || v_condition || ' AND ';
        ELSE
            v_remote_sql := v_remote_sql || ' WHERE ';
        END IF;
        -- Use upper boundary to avoid edge case of multiple upper boundary values inserting during refresh
        v_remote_sql := v_remote_sql || format('%I > %L AND %I < %L'
                                                , v_control
                                                , COALESCE(p_repull_start, '-infinity')
                                                , v_control
                                                , COALESCE(p_repull_end, v_boundary));
        -- Delete the old local data. Use higher than upper boundary to ensure all old data is deleted
        v_delete_sql := format('DELETE FROM %I.%I WHERE %I > %L AND %I < %L'
                                , v_dest_schema_name
                                , v_dest_table_name
                                , v_control
                                , COALESCE(p_repull_start, '-infinity')
                                , v_control
                                , COALESCE(p_repull_end, 'infinity'));
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
    v_remote_sql := format('SELECT '||v_cols||' FROM %I.%I', v_src_schema_name, v_src_table_name);
    IF v_condition IS NOT NULL THEN
        v_remote_sql := v_remote_sql || ' ' || v_condition || ' AND ';
    ELSE
        v_remote_sql := v_remote_sql || ' WHERE ';
    END IF;
    v_remote_sql := v_remote_sql || format('%I > %L AND %I < %L ORDER BY %I ASC LIMIT '||COALESCE(v_limit::text, 'ALL')
                                            , v_control
                                            , v_last_value
                                            , v_control
                                            , v_boundary
                                            , v_control);

    IF v_jobmon THEN
        PERFORM update_step(v_step_id, 'OK','Grabbing rows from '||v_last_value::text||' to '||v_boundary::text);
    END IF;
    PERFORM gdb(p_debug,'Grabbing rows from '||v_last_value::text||' to '||v_boundary::text);

END IF;

EXECUTE 'CREATE TEMP TABLE mimeo_refresh_inserter_temp ('||v_cols_n_types||')';
PERFORM gdb(p_debug,v_remote_sql);
PERFORM dblink_open(v_dblink_name, 'mimeo_cursor', v_remote_sql);
IF v_jobmon THEN
    v_step_id := add_step(v_job_id, 'Inserting new records into local table');
END IF;
v_rowcount := 0;
v_total := 0;
LOOP
    v_fetch_sql := 'INSERT INTO mimeo_refresh_inserter_temp ('||v_cols||') 
        SELECT '||v_cols||' FROM dblink_fetch('||quote_literal(v_dblink_name)||', ''mimeo_cursor'', 50000) AS ('||v_cols_n_types||')';
    EXECUTE v_fetch_sql;
    GET DIAGNOSTICS v_rowcount = ROW_COUNT;
    v_total := v_total + coalesce(v_rowcount, 0);
    EXECUTE 'SELECT max('||quote_ident(v_control)||') FROM mimeo_refresh_inserter_temp' INTO v_last_fetched;
    IF v_limit IS NULL THEN -- insert into the real table in batches if no limit to avoid excessively large temp tables
        EXECUTE format('INSERT INTO %I.%I ('||v_cols||') SELECT '||v_cols||' FROM mimeo_refresh_inserter_temp', v_dest_schema_name, v_dest_table_name);
        TRUNCATE mimeo_refresh_inserter_temp;
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
        EXECUTE 'SELECT max('||quote_ident(v_control)||') FROM mimeo_refresh_inserter_temp' INTO v_last_value;
        IF v_jobmon THEN
            v_step_id := add_step(v_job_id, 'Removing high boundary rows from this batch to avoid missing data');       
        END IF;
        EXECUTE format('DELETE FROM mimeo_refresh_inserter_temp WHERE %I = %L', v_control, v_last_value);
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
        EXECUTE format('INSERT INTO %I.%I ('||v_cols||') SELECT '||v_cols||' FROM mimeo_refresh_inserter_temp', v_dest_schema_name, v_dest_table_name);
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
    EXECUTE format('SELECT max(%I) FROM %I.%I', v_control, v_dest_schema_name, v_dest_table_name) INTO v_last_value;
    UPDATE refresh_config_inserter_time SET last_value = coalesce(v_last_value, CURRENT_TIMESTAMP), last_run = CURRENT_TIMESTAMP WHERE dest_table = p_destination;  
    IF v_jobmon THEN
        PERFORM update_step(v_step_id, 'OK','Lower boundary value is: '|| coalesce(v_last_value, CURRENT_TIMESTAMP));
        PERFORM gdb(p_debug, 'Lower boundary value is: '||coalesce(v_last_value, CURRENT_TIMESTAMP));
    END IF;
END IF;

DROP TABLE IF EXISTS mimeo_refresh_inserter_temp;

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
                EXECUTE 'SELECT '||v_jobmon_schema||'.add_job(''Refresh Inserter: '||p_destination||''')' INTO v_job_id;
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


/*
 *  Inserter maker function. 
 */
CREATE OR REPLACE FUNCTION inserter_maker(
    p_src_table text
    , p_type text
    , p_control_field text
    , p_dblink_id int
    , p_boundary text DEFAULT NULL
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

v_boundary_serial           int;
v_boundary_time             interval;
v_data_source               text;
v_dest_schema_name          text;
v_dest_table_name           text;
v_dst_active                boolean;
v_jobmon                    boolean;
v_insert_refresh_config     text;
v_max_id                    bigint;
v_max_timestamp             timestamptz;
v_sql                       text;
v_src_schema_name           text;
v_src_table_name            text;
v_table_exists              boolean;

BEGIN

SELECT data_source INTO v_data_source FROM @extschema@.dblink_mapping_mimeo WHERE data_source_id = p_dblink_id; 
IF NOT FOUND THEN
    RAISE EXCEPTION 'Database link ID is incorrect %', p_dblink_id; 
END IF;

IF (p_type <> 'time' AND p_type <> 'serial') OR p_type IS NULL THEN
    RAISE EXCEPTION 'Invalid inserter type: %. Must be either "time" or "serial"', p_type;
END IF;

IF p_dest_table IS NULL THEN
    p_dest_table := p_src_table;
END IF;

IF position('.' in p_dest_table) > 0 AND position('.' in p_src_table) > 0 THEN
    -- Do nothing. Schema & table variable names set below after table is created
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
FROM pg_catalog.pg_namespace n, pg_catalog.pg_extension e WHERE e.extname = 'pg_jobmon' AND e.extnamespace = n.oid;

IF p_type = 'time' THEN
    v_dst_active := @extschema@.dst_utc_check();
    IF p_boundary IS NULL THEN
        v_boundary_time = '10 minutes'::interval;
    ELSE
        v_boundary_time = p_boundary::interval;
    END IF;
    v_insert_refresh_config := 'INSERT INTO @extschema@.refresh_config_inserter_time (
            source_table
            , type
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
            ||', '||quote_literal('inserter_time')
            ||', '||quote_literal(p_dest_table)
            ||', '||quote_literal(p_dblink_id)
            ||', '||quote_literal(p_control_field)
            ||', '||quote_literal(v_boundary_time)
            ||', '||quote_literal('0001-01-01'::date)
            ||', '||quote_literal(CURRENT_TIMESTAMP)
            ||', '||v_dst_active
            ||', '||COALESCE(quote_literal(p_filter), 'NULL')
            ||', '||COALESCE(quote_literal(p_condition), 'NULL')
            ||', '||v_jobmon||')';
ELSIF p_type = 'serial' THEN
    IF p_boundary IS NULL THEN
        v_boundary_serial = 10;
    ELSE
        v_boundary_serial = p_boundary::int;
    END IF;
    v_insert_refresh_config := 'INSERT INTO @extschema@.refresh_config_inserter_serial (
            source_table
            , type
            , dest_table
            , dblink
            , control
            , boundary
            , last_value
            , last_run
            , filter
            , condition
            , jobmon ) 
        VALUES('
            ||quote_literal(p_src_table)
            ||', '||quote_literal('inserter_serial')
            ||', '||quote_literal(p_dest_table)
            ||', '||quote_literal(p_dblink_id)
            ||', '||quote_literal(p_control_field)
            ||', '||quote_literal(v_boundary_serial)
            ||', '||quote_literal(0)
            ||', '||quote_literal(CURRENT_TIMESTAMP)
            ||', '||COALESCE(quote_literal(p_filter), 'NULL')
            ||', '||COALESCE(quote_literal(p_condition), 'NULL')
            ||', '||v_jobmon||')';
ELSE
    RAISE EXCEPTION 'Invalid inserter type: %. Must be either "time" or "serial"', p_type;
END IF;

PERFORM @extschema@.gdb(p_debug, 'v_insert_refresh_config: '||v_insert_refresh_config);
EXECUTE v_insert_refresh_config;

SELECT p_table_exists, p_source_schema_name, p_source_table_name INTO v_table_exists, v_src_schema_name, v_src_table_name
FROM @extschema@.manage_dest_table(p_dest_table, NULL, NULL, p_debug);

SELECT schemaname, tablename 
INTO v_dest_schema_name, v_dest_table_name 
FROM pg_catalog.pg_tables 
WHERE schemaname||'.'||tablename = p_dest_table;

IF p_pulldata AND v_table_exists = false THEN
    RAISE NOTICE 'Pulling all data from source...';
    EXECUTE 'SELECT @extschema@.refresh_inserter('||quote_literal(p_dest_table)||', p_repull := true, p_debug := '||p_debug||')';
END IF;

IF p_index AND v_table_exists = false THEN
    PERFORM @extschema@.create_index(p_dest_table, v_src_schema_name, v_src_table_name, NULL, p_debug);
END IF;

IF v_table_exists THEN
    RAISE NOTICE 'Destination table % already exists. No data or indexes were pulled from source', p_dest_table;
END IF;

RAISE NOTICE 'Analyzing destination table...';
EXECUTE format('ANALYZE %I.%I', v_dest_schema_name, v_dest_table_name);

v_sql := format('SELECT max(%I) FROM %I.%I', p_control_field, v_dest_schema_name, v_dest_table_name);
PERFORM @extschema@.gdb(p_debug, v_sql);
IF p_type = 'time' THEN
    RAISE NOTICE 'Getting the maximum destination timestamp...';
    EXECUTE v_sql INTO v_max_timestamp;
    v_sql := format('UPDATE %I.refresh_config_inserter_time SET last_value = %L WHERE dest_table = %L'
                    , '@extschema@'
                    , COALESCE(v_max_timestamp, CURRENT_TIMESTAMP)
                    , p_dest_table);
    PERFORM @extschema@.gdb(p_debug, v_sql);
    EXECUTE v_sql;
ELSIF p_type = 'serial' THEN
    RAISE NOTICE 'Getting the maximum destination id...';
    EXECUTE v_sql INTO v_max_id;
    v_sql := format('UPDATE %I.refresh_config_inserter_serial SET last_value = %L WHERE dest_table = %L'
                    , '@extschema@'
                    , COALESCE(v_max_id, 0)
                    , p_dest_table);
    PERFORM @extschema@.gdb(p_debug, v_sql);
    EXECUTE v_sql;
END IF;

RAISE NOTICE 'Done';
END
$$;


/*
 *  Refresh based on DML (Insert, Update, Delete), but logs all deletes on the destination table
 *  Destination table requires extra column: mimeo_source_deleted timestamptz
 */
CREATE OR REPLACE FUNCTION refresh_logdel(p_destination text, p_limit int DEFAULT NULL, p_repull boolean DEFAULT false, p_jobmon boolean DEFAULT NULL, p_debug boolean DEFAULT false) RETURNS void
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
v_delete_remote_q       text;
v_dest_schema_name      text;
v_dest_table            text;
v_dest_table_name       text;
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
FROM refresh_config_logdel 
WHERE dest_table = p_destination; 
IF NOT FOUND THEN
   RAISE EXCEPTION 'No configuration found for %',v_job_name; 
END IF;

-- Allow override with parameter
v_jobmon := COALESCE(p_jobmon, v_jobmon);

SELECT schemaname, tablename 
INTO v_dest_schema_name, v_dest_table_name
FROM pg_catalog.pg_tables
WHERE schemaname||'.'||tablename = v_dest_table;

IF v_dest_table_name IS NULL THEN
    RAISE EXCEPTION 'Destination table is missing (%)', v_dest_table;
END IF;

-- Take advisory lock to prevent multiple calls to function overlapping
v_adv_lock := pg_try_advisory_xact_lock(hashtext('refresh_logdel'), hashtext(v_job_name));
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
            SELECT '||v_pk_name_csv||' FROM %I.%I ORDER BY '||v_pk_name_csv||' LIMIT '||COALESCE(v_limit::text, 'ALL')||')
        UPDATE %I.%I b SET processed = true 
        FROM a 
        WHERE '||v_pk_where
    , v_q_schema_name, v_q_table_name, v_q_schema_name, v_q_table_name);
v_trigger_update := 'SELECT dblink_exec('||quote_literal(v_dblink_name)||','|| quote_literal(v_with_update)||')';
PERFORM gdb(p_debug,v_trigger_update);
EXECUTE v_trigger_update INTO v_exec_status;    
IF v_jobmon THEN
    PERFORM update_step(v_step_id, 'OK','Result was '||v_exec_status);
END IF;

-- create temp table for recording deleted rows
EXECUTE 'CREATE TEMP TABLE refresh_logdel_deleted ('||v_cols_n_types||', mimeo_source_deleted timestamptz)';
v_remote_d_sql := format('SELECT '||v_cols||', mimeo_source_deleted FROM %I.%I WHERE processed = true and mimeo_source_deleted IS NOT NULL', v_q_schema_name, v_q_table_name);
PERFORM dblink_open(v_dblink_name, 'mimeo_cursor', v_remote_d_sql);
IF v_jobmon THEN
    v_step_id := add_step(v_job_id, 'Creating local queue temp table for deleted rows on source');
END IF;
v_rowcount := 0;
v_total := 0;
LOOP
    v_fetch_sql := 'INSERT INTO refresh_logdel_deleted ('||v_cols||', mimeo_source_deleted) 
        SELECT '||v_cols||', mimeo_source_deleted 
        FROM dblink_fetch('||quote_literal(v_dblink_name)||', ''mimeo_cursor'', 50000) AS ('||v_cols_n_types||', mimeo_source_deleted timestamptz)';
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
EXECUTE 'CREATE INDEX ON refresh_logdel_deleted ('||v_pk_name_csv||')';
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
    v_remote_f_sql := format('SELECT '||v_cols||' FROM %I.%I', v_src_schema_name, v_src_table_name);
    IF v_condition IS NOT NULL THEN
        v_remote_f_sql := v_remote_f_sql || ' ' || v_condition;
    END IF;
ELSE
    -- Do normal stuff here
    EXECUTE 'CREATE TEMP TABLE refresh_logdel_queue ('||v_pk_name_type_csv||')';
    v_remote_q_sql := format('SELECT DISTINCT '||v_pk_name_csv||' FROM %I.%I WHERE processed = true and mimeo_source_deleted IS NULL', v_q_schema_name, v_q_table_name);
    PERFORM dblink_open(v_dblink_name, 'mimeo_cursor', v_remote_q_sql);
    IF v_jobmon THEN
        v_step_id := add_step(v_job_id, 'Creating local queue temp table for inserts/updates');
    END IF;
    v_rowcount := 0;
    LOOP
        v_fetch_sql := 'INSERT INTO refresh_logdel_queue ('||v_pk_name_csv||') 
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
    EXECUTE 'CREATE INDEX ON refresh_logdel_queue ('||v_pk_name_csv||')';
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
    v_remote_f_sql := format('SELECT '||v_cols||' FROM %I.%I JOIN ('||v_remote_q_sql||') x USING ('||v_pk_name_csv||')', v_src_schema_name, v_src_table_name);
    IF v_condition IS NOT NULL THEN
        v_remote_f_sql := v_remote_f_sql || ' ' || v_condition;
    END IF;
END IF;

-- insert records to local table (inserts/updates). Have to do temp table in case destination table is partitioned (returns 0 when inserting to parent)
PERFORM dblink_open(v_dblink_name, 'mimeo_cursor', v_remote_f_sql);
IF v_jobmon THEN
    v_step_id := add_step(v_job_id, 'Inserting new/updated records into local table');
END IF;
EXECUTE 'CREATE TEMP TABLE refresh_logdel_full ('||v_cols_n_types||')'; 
v_rowcount := 0;
v_total := 0;
LOOP
    v_fetch_sql := 'INSERT INTO refresh_logdel_full ('||v_cols||') 
        SELECT '||v_cols||' FROM dblink_fetch('||quote_literal(v_dblink_name)||', ''mimeo_cursor'', 50000) AS ('||v_cols_n_types||')';
    EXECUTE v_fetch_sql;
    GET DIAGNOSTICS v_rowcount = ROW_COUNT;
    v_total := v_total + coalesce(v_rowcount, 0);
    EXECUTE format('INSERT INTO %I.%I ('||v_cols||') SELECT '||v_cols||' FROM refresh_logdel_full', v_dest_schema_name, v_dest_table_name);
    TRUNCATE refresh_logdel_full;
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
v_insert_deleted_sql := format('INSERT INTO %I.%I ('||v_cols||', mimeo_source_deleted) 
                                SELECT '||v_cols||', mimeo_source_deleted FROM refresh_logdel_deleted', v_dest_schema_name, v_dest_table_name); 
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
                EXECUTE 'SELECT '||v_jobmon_schema||'.add_job(''Refresh Log Del: '||p_destination||''')' INTO v_job_id;
                EXECUTE 'SELECT '||v_jobmon_schema||'.add_step('||v_job_id||', ''EXCEPTION before job logging started'')' INTO v_step_id;
            END IF;
            IF v_step_id IS NULL THEN
                EXECUTE 'SELECT '||v_jobmon_schema||'.add_step('||v_job_id||', ''EXCEPTION before first step logged'')' INTO v_step_id;
            END IF;
                  EXECUTE 'SELECT '||v_jobmon_schema||'.update_step('||v_step_id||', ''CRITICAL'', ''ERROR: '||coalesce(SQLERRM,'unknown')||''')';
            EXECUTE 'SELECT '||v_jobmon_schema||'.fail_job('||v_job_id||')';
        END IF;
        RAISE EXCEPTION '%', SQLERRM;
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
v_dblink_name               text;
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
v_pk_name_csv               text;
v_pk_type                   text[] := p_pk_type;
v_q_value                   text := '';
v_remote_grants_sql         text;
v_remote_key_sql            text;
v_remote_sql                text;
v_remote_q_index            text;
v_remote_q_table            text;
v_row                       record;
v_source_queue_counter      int := 0;
v_source_queue_exists       text;
v_source_queue_function     text;
v_source_queue_table        text;
v_source_queue_trigger      text;
v_src_schema_name           text;
v_src_table_name            text;
v_src_table_template        text;
v_table_exists              boolean;
v_trigger_func              text;
v_types                     text[];

BEGIN

v_dblink_name := @extschema@.check_name_length('mimeo_logdel_maker_'||p_src_table);

SELECT nspname INTO v_dblink_schema FROM pg_namespace n, pg_extension e WHERE e.extname = 'dblink' AND e.extnamespace = n.oid;
SELECT current_setting('search_path') INTO v_old_search_path;
EXECUTE 'SELECT set_config(''search_path'',''@extschema@,'||v_dblink_schema||',public'',''false'')';

IF (p_pk_name IS NULL AND p_pk_type IS NOT NULL) OR (p_pk_name IS NOT NULL AND p_pk_type IS NULL) THEN
    RAISE EXCEPTION 'Cannot manually set primary/unique key field(s) without defining type(s) or vice versa';
END IF;

SELECT data_source INTO v_data_source FROM @extschema@.dblink_mapping_mimeo WHERE data_source_id = p_dblink_id; 
IF NOT FOUND THEN
    RAISE EXCEPTION 'Database link ID is incorrect %', p_dblink_id; 
END IF;

IF p_dest_table IS NULL THEN
    p_dest_table := p_src_table;
END IF;

IF position('.' in p_dest_table) > 0 AND position('.' in p_src_table) > 0 THEN
    -- Do nothing. Schema & table variable names set below after table is created
ELSE
    RAISE EXCEPTION 'Source (and destination) table must be schema qualified';
END IF;

PERFORM dblink_connect(v_dblink_name, @extschema@.auth(p_dblink_id));

SELECT schemaname ||'_'|| tablename, schemaname, tablename
INTO v_src_table_template, v_src_schema_name, v_src_table_name
FROM dblink(v_dblink_name, format('
            SELECT schemaname, tablename
            FROM (
                SELECT schemaname, tablename 
                FROM pg_catalog.pg_tables 
                WHERE schemaname ||''.''|| tablename = %L
                UNION
                SELECT schemaname, viewname AS tablename
                FROM pg_catalog.pg_views
                WHERE schemaname || ''.'' || viewname = %L
            ) tables LIMIT 1'
        , p_src_table, p_src_table) )
    t (schemaname text, tablename text);

IF v_src_table_template IS NULL THEN
    RAISE EXCEPTION 'Source table given (%) does not exist in configured source database', p_src_table;
END IF;

v_source_queue_table :=  check_name_length(v_src_table_template, '_q');
v_source_queue_function := check_name_length(v_src_table_template, '_mimeo_queue');
v_source_queue_trigger := check_name_length(v_src_table_template, '_mimeo_trig');

-- Automatically get source primary/unique key if none given
IF p_pk_name IS NULL AND p_pk_type IS NULL THEN
    SELECT key_type, indkey_names, indkey_types INTO v_key_type, v_pk_name, v_pk_type FROM fetch_replication_key(v_src_schema_name, v_src_table_name, v_dblink_name, p_debug);
END IF;

v_pk_name_csv := '"'||array_to_string(v_pk_name, '","')||'"';
PERFORM gdb(p_debug, 'v_key_type: '||COALESCE(v_key_type, ''));
PERFORM gdb(p_debug, 'v_pk_name: '||COALESCE(v_pk_name_csv, ''));
PERFORM gdb(p_debug, 'v_pk_type: '||COALESCE(array_to_string(v_pk_type, ','), ''));

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

-- Do check for existing queue table(s) to support multiple destinations
SELECT tablename 
    INTO v_source_queue_exists 
FROM dblink(v_dblink_name
            , 'SELECT tablename 
            FROM pg_catalog.pg_tables 
            WHERE schemaname = ''@extschema@''
            AND tablename = '||quote_literal(v_source_queue_table)) t (tablename text);
WHILE v_source_queue_exists IS NOT NULL LOOP -- loop until a tablename that doesn't exist is found
    v_source_queue_counter := v_source_queue_counter + 1;
    IF v_source_queue_counter > 99 THEN
        RAISE EXCEPTION 'Limit of 99 queue tables for a single source table reached. No more destination tables possible (and HIGHLY discouraged)';
    END IF;
    v_source_queue_table := check_name_length(v_src_table_template, '_q'||to_char(v_source_queue_counter, 'FM00'));
    SELECT tablename 
        INTO v_source_queue_exists 
    FROM dblink(v_dblink_name
            , 'SELECT tablename 
            FROM pg_catalog.pg_tables 
            WHERE schemaname = ''@extschema@''
            AND tablename = '||quote_literal(v_source_queue_table)) t (tablename text);
    v_source_queue_function := check_name_length(v_src_table_template, '_mimeo_queue'||to_char(v_source_queue_counter, 'FM00'));
    v_source_queue_trigger := check_name_length(v_src_table_template, '_mimeo_trig'||to_char(v_source_queue_counter, 'FM00'));
END LOOP;

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
        ||', '||quote_literal('@extschema@.'||v_source_queue_table)
        ||', '||quote_literal(v_pk_name)
        ||', '||quote_literal(v_pk_type)
        ||', '||quote_literal(CURRENT_TIMESTAMP)
        ||', '||COALESCE(quote_literal(p_filter), 'NULL')
        ||', '||COALESCE(quote_literal(p_condition), 'NULL')
        ||', '||v_jobmon||')';
RAISE NOTICE 'Inserting data into config table';
PERFORM gdb(p_debug, 'v_insert_refresh_config: '||v_insert_refresh_config);
EXECUTE v_insert_refresh_config;

SELECT p_table_exists, p_cols, p_cols_n_types INTO v_table_exists, v_cols, v_cols_n_types FROM manage_dest_table(p_dest_table, NULL, NULL, p_debug) ;

SELECT schemaname, tablename 
INTO v_dest_schema_name, v_dest_table_name 
FROM pg_catalog.pg_tables 
WHERE schemaname||'.'||tablename = p_dest_table;

v_remote_q_table := format('CREATE TABLE %I.%I ('||array_to_string(v_cols_n_types, ',')||', mimeo_source_deleted timestamptz, processed boolean)', '@extschema@', v_source_queue_table);
-- Indexes on queue table created below so the variable can be reused

v_trigger_func := format('CREATE FUNCTION %I.%I() RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER AS $_$ DECLARE ', '@extschema@', v_source_queue_function);
    v_trigger_func := v_trigger_func || ' 
        v_del_time timestamptz := clock_timestamp(); ';
    v_trigger_func := v_trigger_func || ' 
        BEGIN IF TG_OP = ''INSERT'' THEN ';
    v_q_value := 'NEW."' || array_to_string(v_pk_name, '", NEW."') || '"';
    v_trigger_func := v_trigger_func || format(' 
            INSERT INTO %I.%I (', '@extschema@', v_source_queue_table) || v_pk_name_csv ||') VALUES ('||v_q_value||');';
    v_trigger_func := v_trigger_func || ' 
        ELSIF TG_OP = ''UPDATE'' THEN  ';
    -- UPDATE needs to insert the NEW values so reuse v_q_value from INSERT operation
    v_trigger_func := v_trigger_func || format(' 
            INSERT INTO %I.%I (', '@extschema@', v_source_queue_table) || v_pk_name_csv ||') VALUES ('||v_q_value||');';
    -- Only insert the old row if the new key doesn't match the old key. This handles edge case when only one column of a composite key is updated
    v_trigger_func := v_trigger_func || ' 
            IF ';
    FOREACH v_field IN ARRAY v_pk_name LOOP
        IF v_counter > 1 THEN
            v_trigger_func := v_trigger_func || ' OR ';
        END IF;
        v_trigger_func := v_trigger_func || ' NEW."'||v_field||'" != OLD."'||v_field||'" ';
        v_counter := v_counter + 1;
    END LOOP;
    v_trigger_func := v_trigger_func || ' THEN ';
    v_q_value := 'OLD."' || array_to_string(v_pk_name, '", OLD."') || '"';
    v_trigger_func := v_trigger_func || format(' 
                INSERT INTO %I.%I (', '@extschema@', v_source_queue_table) || v_pk_name_csv ||') VALUES ('||v_q_value||'); ';
    v_trigger_func := v_trigger_func || ' 
            END IF;';
    v_trigger_func := v_trigger_func || ' 
        ELSIF TG_OP = ''DELETE'' THEN  ';
    v_q_value := 'OLD.' || array_to_string(v_cols, ', OLD.');
    v_trigger_func := v_trigger_func || format(' 
            INSERT INTO %I.%I ', '@extschema@', v_source_queue_table) ||' ('||array_to_string(v_cols, ',')||', mimeo_source_deleted) VALUES ('||v_q_value||', v_del_time);';
v_trigger_func := v_trigger_func ||' 
        END IF; RETURN NULL; END $_$;';

v_create_trig := format('CREATE TRIGGER %I AFTER INSERT OR DELETE OR UPDATE', v_source_queue_trigger);
IF p_filter IS NOT NULL THEN
    v_create_trig := v_create_trig || ' OF "'||array_to_string(p_filter, '","')||'"';
END IF;
v_create_trig := v_create_trig || format(' ON %I.%I FOR EACH ROW EXECUTE PROCEDURE %I.%I()', v_src_schema_name, v_src_table_name, '@extschema@', v_source_queue_function);

RAISE NOTICE 'Creating objects on source database (function, trigger & queue table)...';

PERFORM gdb(p_debug, 'v_remote_q_table: '||v_remote_q_table); 
PERFORM dblink_exec(v_dblink_name, v_remote_q_table);
v_remote_q_index := format('CREATE INDEX ON %I.%I', '@extschema@', v_source_queue_table)||' ("'||array_to_string(v_pk_name, '","')||'")';
PERFORM gdb(p_debug, 'v_remote_q_index: '||v_remote_q_index);
PERFORM dblink_exec(v_dblink_name, v_remote_q_index);
v_remote_q_index := format('CREATE INDEX ON %I.%I', '@extschema@', v_source_queue_table)||' (processed, mimeo_source_deleted)';
PERFORM gdb(p_debug, 'v_remote_q_index: '||v_remote_q_index);
PERFORM dblink_exec(v_dblink_name, v_remote_q_index);
PERFORM gdb(p_debug, 'v_trigger_func: '||v_trigger_func);
PERFORM dblink_exec(v_dblink_name, v_trigger_func);
PERFORM gdb(p_debug, 'v_create_trig: '||v_create_trig); 
PERFORM dblink_exec(v_dblink_name, v_create_trig);

IF p_pulldata AND v_table_exists = false THEN
    RAISE NOTICE 'Pulling all data from source...';
    EXECUTE 'SELECT refresh_logdel('||quote_literal(p_dest_table)||', p_repull := true, p_debug := '||p_debug||')';
END IF;

IF p_index AND v_table_exists = false THEN
    PERFORM create_index(p_dest_table, v_src_schema_name, v_src_table_name, NULL, p_debug);
    -- Create index on special column for logdel
    EXECUTE format('CREATE INDEX %I ON %I.%I (mimeo_source_deleted)', check_name_length(v_dest_table_name, '_mimeo_source_deleted'), v_dest_schema_name, v_dest_table_name);
ELSIF v_table_exists = false THEN
    -- Ensure destination indexes that are needed for efficient replication are created even if p_index is set false
    RAISE NOTICE 'Adding primary/unique key to table...';
    IF v_key_type = 'primary' THEN
        EXECUTE format('ALTER TABLE %I.%I ADD PRIMARY KEY', v_dest_schema_name, v_dest_table_name) ||'("'||array_to_string(v_pk_name, '","')||'")';
    ELSE
        EXECUTE format('CREATE UNIQUE INDEX ON %I.%I', v_dest_schema_name, v_dest_table_name) ||'("'||array_to_string(v_pk_name, '","')||'")';
    END IF;
END IF;

IF v_table_exists THEN
    RAISE NOTICE 'Destination table % already exists. No data or indexes were pulled from source: %. Recommend making index on special column mimeo_source_deleted if it doesn''t have one', p_dest_table;
END IF;

RAISE NOTICE 'Analyzing destination table...';
EXECUTE format('ANALYZE %I.%I', v_dest_schema_name, v_dest_table_name);

PERFORM dblink_disconnect(v_dblink_name);

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
                PERFORM dblink_exec(v_dblink_name, format('DROP TRIGGER IF EXISTS %I ON %I.%I', v_source_queue_trigger, v_src_schema_name, v_src_table_name));
                PERFORM dblink_exec(v_dblink_name, format('DROP TABLE IF EXISTS %I.%I', '@extschema@', v_source_queue_table));
                PERFORM dblink_exec(v_dblink_name, format('DROP FUNCTION IF EXISTS %I.%I()', '@extschema@', v_source_queue_function));
            END IF;
            PERFORM dblink_disconnect(v_dblink_name);
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
 *  Snap refresh to repull all table data
 */
CREATE OR REPLACE FUNCTION refresh_snap(p_destination text, p_index boolean DEFAULT true, p_pulldata boolean DEFAULT true, p_jobmon boolean DEFAULT NULL, p_debug boolean DEFAULT false) RETURNS void
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
v_adv_lock := pg_try_advisory_xact_lock(hashtext('refresh_snap'), hashtext(v_job_name));
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


/*
 *  Plain table refresh function. 
 */
CREATE OR REPLACE FUNCTION refresh_table(p_destination text, p_truncate_cascade boolean DEFAULT NULL, p_jobmon boolean DEFAULT NULL, p_debug boolean DEFAULT false) RETURNS void
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
v_dest_schema_name      text;
v_dest_table            text;
v_dest_table_name       text;
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
v_seq_name              text;
v_seq_oid               oid;
v_seq_reset             text;
v_seq_schema            text;
v_src_schema_name       text;
v_src_table_name        text;
v_step_id               bigint;
v_total                 bigint := 0;
v_truncate_cascade      boolean;
v_truncate_sql          text;

BEGIN

SELECT nspname INTO v_dblink_schema FROM pg_namespace n, pg_extension e WHERE e.extname = 'dblink' AND e.extnamespace = n.oid;
SELECT nspname INTO v_jobmon_schema FROM pg_namespace n, pg_extension e WHERE e.extname = 'pg_jobmon' AND e.extnamespace = n.oid;
IF p_jobmon IS TRUE AND v_jobmon_schema IS NULL THEN
    RAISE EXCEPTION 'p_jobmon parameter set to TRUE, but unable to determine if pg_jobmon extension is installed';
END IF;

v_dblink_name := @extschema@.check_name_length('mimeo_table_refresh_'||p_destination);
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

v_adv_lock := pg_try_advisory_xact_lock(hashtext('refresh_table'), hashtext(p_destination));
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

SELECT schemaname, tablename 
INTO v_dest_schema_name, v_dest_table_name
FROM pg_catalog.pg_tables
WHERE schemaname||'.'||tablename = v_dest_table;

IF p_truncate_cascade IS NOT NULL THEN
    v_truncate_cascade := p_truncate_cascade;
END IF;

IF v_jobmon THEN
    v_step_id := add_step(v_job_id, 'Truncating destination table');
END IF;
v_truncate_sql := format('TRUNCATE TABLE %I.%I', v_dest_schema_name, v_dest_table_name);
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

SELECT array_to_string(p_cols, ',')
    , array_to_string(p_cols_n_types, ',') 
    , p_source_schema_name
    , p_source_table_name
INTO v_cols
    , v_cols_n_types 
    , v_src_schema_name
    , v_src_table_name
FROM manage_dest_table(v_dest_table, NULL, v_dblink_name, p_debug);

IF v_src_table_name IS NULL THEN
    RAISE EXCEPTION 'Source table missing (%)', v_source_table;
END IF;

IF v_jobmon THEN
    PERFORM update_step(v_step_id, 'OK', 'Done');
    v_step_id := add_step(v_job_id,'Inserting records into local table');
END IF;

v_remote_sql := format('SELECT '||v_cols||' FROM %I.%I', v_src_schema_name, v_src_table_name);
IF v_condition IS NOT NULL THEN
    v_remote_sql := v_remote_sql || ' ' || v_condition;
END IF;
PERFORM dblink_open(v_dblink_name, 'mimeo_cursor', v_remote_sql);
v_rowcount := 0;
LOOP
    v_fetch_sql := format('INSERT INTO %I.%I ('||v_cols||')', v_dest_schema_name, v_dest_table_name)|| 
        'SELECT '||v_cols||' FROM dblink_fetch('||quote_literal(v_dblink_name)||', ''mimeo_cursor'', 50000) AS ('||v_cols_n_types||')';
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
        SELECT n.nspname, c.relname, c.oid 
        INTO v_seq_schema, v_seq_name, v_seq_oid
        FROM pg_catalog.pg_class c
        JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid
        WHERE n.nspname ||'.'|| c.relname = v_seq;

        IF v_seq_oid IS NOT NULL THEN
            v_seq_max := sequence_max_value(v_seq_oid);
        END IF;

        IF v_seq_max IS NOT NULL THEN
            v_seq_reset := format('SELECT setval(''%I.%I'', %L)', v_seq_schema, v_seq_name, v_seq_max);
            PERFORM gdb(p_debug, 'v_reset_seq: '||v_seq_reset);
            EXECUTE v_seq_reset;
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
                EXECUTE 'SELECT '||v_jobmon_schema||'.add_job(''Refresh Table: '||p_destination||''')' INTO v_job_id;
                EXECUTE 'SELECT '||v_jobmon_schema||'.add_step('||v_job_id||', ''EXCEPTION before job logging started'')' INTO v_step_id;
            END IF;
            IF v_step_id IS NULL THEN
                EXECUTE 'SELECT '||v_jobmon_schema||'.add_step('||v_job_id||', ''EXCEPTION before first step logged'')' INTO v_step_id;
            END IF;
            EXECUTE 'SELECT '||v_jobmon_schema||'.update_step('||v_step_id||', ''CRITICAL'', ''ERROR: '||coalesce(SQLERRM,'unknown')||''')';
            EXECUTE 'SELECT '||v_jobmon_schema||'.fail_job('||v_job_id||')';
        END IF;
        RAISE EXCEPTION '%', SQLERRM;
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
v_src_schema_name           text;
v_src_table_name            text;
v_table_exists              boolean;

BEGIN

SELECT data_source INTO v_data_source FROM @extschema@.dblink_mapping_mimeo WHERE data_source_id = p_dblink_id; 
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

SELECT p_table_exists, p_source_schema_name, p_source_table_name INTO v_table_exists, v_src_schema_name, v_src_table_name 
FROM @extschema@.manage_dest_table(p_dest_table, NULL, NULL, p_debug);

IF p_pulldata AND v_table_exists = false THEN
    RAISE NOTICE 'Pulling all data from source...';
    EXECUTE 'SELECT @extschema@.refresh_table('||quote_literal(p_dest_table)||', p_debug := '||p_debug||')';
END IF;

IF p_index AND v_table_exists = false THEN
    PERFORM @extschema@.create_index(p_dest_table, v_src_schema_name, v_src_table_name, NULL, p_debug);
END IF;

IF v_table_exists THEN
    RAISE NOTICE 'Destination table % already exists. No data or indexes were pulled from source', p_dest_table;
END IF;

RAISE NOTICE 'Done';
END
$$;


/*
 *  Refresh insert/update only table based on timestamp control field
 */
CREATE OR REPLACE FUNCTION refresh_updater_serial(p_destination text, p_limit integer DEFAULT NULL, p_repull boolean DEFAULT false, p_repull_start bigint DEFAULT NULL, p_repull_end bigint DEFAULT NULL, p_jobmon boolean DEFAULT NULL, p_debug boolean DEFAULT false) RETURNS void
    LANGUAGE plpgsql SECURITY DEFINER
    AS $$
DECLARE

v_adv_lock              boolean;
v_batch_limit_reached   int := 0;
v_boundary_sql          text;
v_boundary              int;
v_cols_n_types          text;
v_cols                  text;
v_condition             text;
v_control               text;
v_create_sql            text;
v_dblink                int;
v_dblink_name           text;
v_dblink_schema         text;
v_delete_sql            text;
v_dest_schema_name      text;
v_dest_table            text;
v_dest_table_name       text;
v_fetch_sql             text;
v_field                 text;
v_filter                text[];
v_full_refresh          boolean := false;
v_insert_sql            text;
v_job_id                int;
v_jobmon                boolean;
v_jobmon_schema         text;
v_job_name              text;
v_last_fetched          bigint;
v_last_value            bigint;
v_limit                 int;
v_link_exists           boolean;
v_old_search_path       text;
v_pk_counter            int := 1;
v_pk_name               text[];
v_remote_boundry_sql    text;
v_remote_boundry        timestamptz;
v_remote_sql            text;
v_rowcount              bigint := 0; 
v_source_table          text;
v_sql                   text;
v_src_schema_name       text;
v_src_table_name        text;
v_step_id               int;
v_total                 bigint := 0;

BEGIN

IF p_debug IS DISTINCT FROM true THEN
    PERFORM set_config( 'client_min_messages', 'warning', true );
END IF;

v_job_name := 'Refresh Updater: '||p_destination;

SELECT nspname INTO v_dblink_schema FROM pg_namespace n, pg_extension e WHERE e.extname = 'dblink' AND e.extnamespace = n.oid;
SELECT nspname INTO v_jobmon_schema FROM pg_namespace n, pg_extension e WHERE e.extname = 'pg_jobmon' AND e.extnamespace = n.oid;
IF p_jobmon IS TRUE AND v_jobmon_schema IS NULL THEN
    RAISE EXCEPTION 'p_jobmon parameter set to TRUE, but unable to determine if pg_jobmon extension is installed';
END IF;

v_dblink_name := @extschema@.check_name_length('mimeo_updater_refresh_'||p_destination);

-- Set custom search path to allow easier calls to other functions, especially job logging
SELECT current_setting('search_path') INTO v_old_search_path;
EXECUTE 'SELECT set_config(''search_path'',''@extschema@,'||COALESCE(v_jobmon_schema||',', '')||v_dblink_schema||',public'',''false'')';

SELECT source_table
    , dest_table
    , dblink
    , control
    , last_value
    , boundary
    , pk_name
    , filter
    , condition
    , batch_limit  
    , jobmon
INTO v_source_table
    , v_dest_table
    , v_dblink
    , v_control
    , v_last_value
    , v_boundary
    , v_pk_name
    , v_filter
    , v_condition
    , v_limit
    , v_jobmon
FROM refresh_config_updater_serial
WHERE dest_table = p_destination;
IF NOT FOUND THEN
   RAISE EXCEPTION 'No configuration found for %',v_job_name;
END IF;

-- Allow override with parameter
v_jobmon := COALESCE(p_jobmon, v_jobmon);

SELECT schemaname, tablename 
INTO v_dest_schema_name, v_dest_table_name
FROM pg_catalog.pg_tables
WHERE schemaname||'.'||tablename = v_dest_table;

IF v_dest_table_name IS NULL THEN
    RAISE EXCEPTION 'Destination table is missing (%)', v_dest_table;
END IF;

-- Take advisory lock to prevent multiple calls to function overlapping
v_adv_lock := pg_try_advisory_xact_lock(hashtext('refresh_updater'), hashtext(v_job_name));
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

IF v_jobmon THEN
    v_step_id := add_step(v_job_id,'Building SQL');
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

IF v_src_table_name IS NULL THEN
    RAISE EXCEPTION 'Source table missing (%)', v_source_table;
END IF;

IF p_limit IS NOT NULL THEN
    v_limit := p_limit;
END IF;

-- Unlike incremental time, there's nothing like CURRENT_TIMESTAMP to base the boundary on. So use the current source max to determine it.
-- For some reason this doesn't like using an int with %L (v_boundary) when making up the format command using dblink
v_sql := format('SELECT boundary FROM dblink(%L, ''SELECT max(%I) - '||v_boundary||' AS boundary FROM %I.%I'') AS (boundary bigint)'
                , v_dblink_name
                , v_control
                , v_src_schema_name
                , v_src_table_name);
PERFORM gdb(p_debug, v_sql);
EXECUTE v_sql INTO v_boundary;

-- Repull old data instead of normal new data pull
IF p_repull THEN
    -- Repull ALL data if no start and end values set
    IF p_repull_start IS NULL AND p_repull_end IS NULL THEN
        IF v_jobmon THEN
            PERFORM update_step(v_step_id, 'OK','Request to repull ALL data from source. This could take a while...');
        END IF;
        EXECUTE format('TRUNCATE %I.%I', v_dest_schema_name, v_dest_table_name);
        -- Use upper boundary remote max to avoid edge case of multiple upper boundary values inserting during refresh
        v_remote_sql := format('SELECT '||v_cols||' FROM %I.%I', v_src_schema_name, v_src_table_name);
        IF v_condition IS NOT NULL THEN
            v_remote_sql := v_remote_sql || ' ' || v_condition || ' AND ';
        ELSE
            v_remote_sql := v_remote_sql || ' WHERE ';
        END IF;
        v_remote_sql := v_remote_sql ||format('%I < %L', v_control, v_boundary);
    ELSE
        IF v_jobmon THEN
            PERFORM update_step(v_step_id, 'OK','Request to repull data from '||COALESCE(p_repull_start, 0)||' to '||COALESCE(p_repull_end, v_boundary));
        END IF;
        PERFORM gdb(p_debug,'Request to repull data from '||COALESCE(p_repull_start, 0)||' to '||COALESCE(p_repull_end, v_boundary));
        v_remote_sql := format('SELECT '||v_cols||' FROM %I.%I', v_src_schema_name, v_src_table_name);
        IF v_condition IS NOT NULL THEN
            v_remote_sql := v_remote_sql || ' ' || v_condition || ' AND ';
        ELSE
            v_remote_sql := v_remote_sql || ' WHERE ';
        END IF;
        -- Use upper boundary remote max to avoid edge case of multiple upper boundary values inserting during refresh
        v_remote_sql := v_remote_sql || format('%I > %L AND %I > %L'
                                                , v_control
                                                , COALESCE(p_repull_start, 0)
                                                , v_control
                                                , COALESCE(p_repull_end, v_boundary));
        -- Delete the old local data. Use higher than bigint max upper boundary to ensure all old data is deleted
        v_delete_sql := format('DELETE FROM %I.%I WHERE %I > %L AND %I < %L'
                                , v_dest_schema_name
                                , v_dest_table_name
                                , v_control
                                , COALESCE(p_repull_start, 0)
                                , v_control
                                , COALESCE(p_repull_end, 9300000000000000000));
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
    v_remote_sql := format('SELECT '||v_cols||' FROM %I.%I', v_src_schema_name, v_src_table_name);
    IF v_condition IS NOT NULL THEN
        v_remote_sql := v_remote_sql || ' ' || v_condition || ' AND ';
    ELSE
        v_remote_sql := v_remote_sql || ' WHERE ';
    END IF;
    v_remote_sql := v_remote_sql || format('%I > %L AND %I < %L ORDER BY %I ASC LIMIT '||COALESCE(v_limit::text, 'ALL')
                                            , v_control
                                            , v_last_value
                                            , v_control
                                            , v_boundary
                                            , v_control);

    v_delete_sql := format('DELETE FROM %I.%I a USING mimeo_refresh_updater_temp t WHERE ', v_dest_schema_name, v_dest_table_name);

    WHILE v_pk_counter <= array_length(v_pk_name,1) LOOP
        IF v_pk_counter > 1 THEN
            v_delete_sql := v_delete_sql ||' AND ';
        END IF;
        v_delete_sql := v_delete_sql ||'a."'||v_pk_name[v_pk_counter]||'" = t."'||v_pk_name[v_pk_counter]||'"';
        v_pk_counter := v_pk_counter + 1;
    END LOOP;

    IF v_jobmon THEN
        PERFORM update_step(v_step_id, 'OK','Grabbing rows from '||v_last_value::text||' to '||v_boundary::text);
    END IF;
    PERFORM gdb(p_debug,'Grabbing rows from '||v_last_value::text||' to '||v_boundary::text);
END IF;

v_insert_sql := format('INSERT INTO %I.%I ('||v_cols||') SELECT '||v_cols||' FROM mimeo_refresh_updater_temp', v_dest_schema_name, v_dest_table_name);

PERFORM gdb(p_debug,v_remote_sql);
PERFORM dblink_open(v_dblink_name, 'mimeo_cursor', v_remote_sql);
IF v_jobmon THEN
    v_step_id := add_step(v_job_id, 'Inserting new/updated records into local table');
END IF;
v_rowcount := 0;

EXECUTE 'CREATE TEMP TABLE mimeo_refresh_updater_temp ('||v_cols_n_types||')'; 
LOOP
    v_fetch_sql := 'INSERT INTO mimeo_refresh_updater_temp ('||v_cols||') 
        SELECT '||v_cols||' FROM dblink_fetch('||quote_literal(v_dblink_name)||', ''mimeo_cursor'', 50000) AS ('||v_cols_n_types||')';
    EXECUTE v_fetch_sql;
    GET DIAGNOSTICS v_rowcount = ROW_COUNT;
    v_total := v_total + coalesce(v_rowcount, 0);
    EXECUTE format('SELECT max(%I) FROM mimeo_refresh_updater_temp', v_control) INTO v_last_fetched;
    IF v_limit IS NULL OR p_repull IS TRUE THEN -- insert into the real table in batches if no limit or a repull to avoid excessively large temp tables
        IF p_repull IS FALSE THEN   -- Delete any rows that exist in the current temp table batch. repull delete is done above.
            EXECUTE v_delete_sql;
        END IF;
        EXECUTE v_insert_sql;
        TRUNCATE mimeo_refresh_updater_temp;
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
ELSIF p_repull IS FALSE THEN  -- don't care about limits when doing a repull
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
        EXECUTE format('SELECT max(%I) FROM mimeo_refresh_updater_temp', v_control) INTO v_last_value;
        EXECUTE format('DELETE FROM mimeo_refresh_updater_temp WHERE %I = %L', v_control, v_last_value);
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
        EXECUTE 'CREATE INDEX ON mimeo_refresh_updater_temp ("'||array_to_string(v_pk_name, '","')||'")'; -- incase of large batch limit
        ANALYZE mimeo_refresh_updater_temp;
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
    EXECUTE format('SELECT max(%I) FROM %I.%I', v_control, v_dest_schema_name, v_dest_table_name) INTO v_last_value;
    UPDATE refresh_config_updater_serial SET last_value = coalesce(v_last_value, 0), last_run = CURRENT_TIMESTAMP WHERE dest_table = p_destination;
    IF v_jobmon THEN
        PERFORM update_step(v_step_id, 'OK','Lower boundary value is: '||coalesce(v_last_value, 0));
    END IF;
    PERFORM gdb(p_debug, 'Lower boundary value is: '||coalesce(v_last_value, 0));
END IF;

DROP TABLE IF EXISTS mimeo_refresh_updater_temp;

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
                EXECUTE 'SELECT '||v_jobmon_schema||'.add_job(''Refresh Updater: '||p_destination||''')' INTO v_job_id;
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


/*
 *  Refresh insert/update only table based on timestamp control field
 */
CREATE OR REPLACE FUNCTION refresh_updater_time(p_destination text, p_limit integer DEFAULT NULL, p_repull boolean DEFAULT false, p_repull_start timestamp DEFAULT NULL, p_repull_end timestamp DEFAULT NULL, p_jobmon boolean DEFAULT NULL, p_debug boolean DEFAULT false) RETURNS void
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
v_dest_schema_name      text;
v_dest_table            text;
v_dest_table_name       text;
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
v_old_search_path       text;
v_pk_counter            int := 1;
v_pk_name               text[];
v_remote_boundry_sql    text;
v_remote_boundry        timestamptz;
v_remote_sql            text;
v_rowcount              bigint := 0; 
v_source_table          text;
v_src_schema_name       text;
v_src_table_name        text;
v_step_id               int;
v_total                 bigint := 0;

BEGIN

IF p_debug IS DISTINCT FROM true THEN
    PERFORM set_config( 'client_min_messages', 'warning', true );
END IF;

v_job_name := 'Refresh Updater: '||p_destination;
v_dblink_name := @extschema@.check_name_length('mimeo_updater_refresh_'||p_destination);

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
FROM refresh_config_updater_time
WHERE dest_table = p_destination;
IF NOT FOUND THEN
   RAISE EXCEPTION 'No configuration found for %',v_job_name;
END IF;

-- Allow override with parameter
v_jobmon := COALESCE(p_jobmon, v_jobmon);

-- Take advisory lock to prevent multiple calls to function overlapping
v_adv_lock := pg_try_advisory_xact_lock(hashtext('refresh_updater'), hashtext(v_job_name));
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

SELECT schemaname, tablename 
INTO v_dest_schema_name, v_dest_table_name
FROM pg_catalog.pg_tables
WHERE schemaname||'.'||tablename = v_dest_table;

IF v_dest_table_name IS NULL THEN
    RAISE EXCEPTION 'Destination table is missing (%)', v_dest_table;
END IF;

-- Do not allow this function to run during DST time change if config option is true. Otherwise will miss data from source
IF v_dst_active THEN
    v_dst_check := @extschema@.dst_change(CURRENT_TIMESTAMP);
    IF v_dst_check THEN 
        IF to_number(to_char(CURRENT_TIMESTAMP, 'HH24MM'), '0000') > v_dst_start AND to_number(to_char(CURRENT_TIMESTAMP, 'HH24MM'), '0000') < v_dst_end THEN
            IF v_jobmon THEN
                v_step_id := add_step( v_job_id, 'DST Check');
                PERFORM update_step(v_step_id, 'OK', 'Job CANCELLED - Does not run during DST time change');
                PERFORM close_job(v_job_id);
            END IF;
            UPDATE refresh_config_updater SET last_run = CURRENT_TIMESTAMP WHERE dest_table = p_destination;  
            PERFORM gdb(p_debug, 'Cannot run during DST time change');
            EXECUTE 'SELECT set_config(''search_path'','''||v_old_search_path||''',''false'')';
            RETURN;
        END IF;
    END IF;
END IF;

IF v_jobmon THEN
    v_step_id := add_step(v_job_id,'Building SQL');
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

IF v_src_table_name IS NULL THEN
    RAISE EXCEPTION 'Source table missing (%)', v_source_table;
END IF;

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
        EXECUTE format('TRUNCATE %I.%I', v_dest_schema_name, v_dest_table_name);
        -- Use upper boundary to avoid edge case of multiple upper boundary values inserting during refresh
        v_remote_sql := format('SELECT '||v_cols||' FROM %I.%I', v_src_schema_name, v_src_table_name);
        IF v_condition IS NOT NULL THEN
            v_remote_sql := v_remote_sql || ' ' || v_condition || ' AND ';
        ELSE
            v_remote_sql := v_remote_sql || ' WHERE ';
        END IF;
        v_remote_sql := v_remote_sql ||format('%I < %L', v_control, v_boundary);
    ELSE
        IF v_jobmon THEN
            PERFORM update_step(v_step_id, 'OK','Request to repull data from '||COALESCE(p_repull_start, '-infinity')||' to '||COALESCE(p_repull_end, v_boundary));
        END IF;
        PERFORM gdb(p_debug,'Request to repull data from '||COALESCE(p_repull_start, '-infinity')||' to '||COALESCE(p_repull_end, v_boundary));
        v_remote_sql := format('SELECT '||v_cols||' FROM %I.%I', v_src_schema_name, v_src_table_name);
        IF v_condition IS NOT NULL THEN
            v_remote_sql := v_remote_sql || ' ' || v_condition || ' AND ';
        ELSE
            v_remote_sql := v_remote_sql || ' WHERE ';
        END IF;
        -- Use upper boundary to avoid edge case of multiple upper boundary values inserting during refresh
        v_remote_sql := v_remote_sql || format('%I > %L AND %I < %L'
                                                , v_control
                                                , COALESCE(p_repull_start, '-infinity')
                                                , v_control
                                                , COALESCE(p_repull_end, v_boundary));
        -- Delete the old local data. Use higher than upper boundary to ensure all old data is deleted
        v_delete_sql := format('DELETE FROM %I.%I WHERE %I > %L AND %I < %L'
                                , v_dest_schema_name
                                , v_dest_table_name
                                , v_control
                                , COALESCE(p_repull_start, '-infinity')
                                , v_control
                                , COALESCE(p_repull_end, 'infinity'));
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
    v_remote_sql := format('SELECT '||v_cols||' FROM %I.%I', v_src_schema_name, v_src_table_name);
    IF v_condition IS NOT NULL THEN
        v_remote_sql := v_remote_sql || ' ' || v_condition || ' AND ';
    ELSE
        v_remote_sql := v_remote_sql || ' WHERE ';
    END IF;
    v_remote_sql := v_remote_sql || format('%I > %L AND %I < %L ORDER BY %I ASC LIMIT '||COALESCE(v_limit::text, 'ALL')
                                    , v_control
                                    , v_last_value
                                    , v_control
                                    , v_boundary
                                    , v_control);

    v_delete_sql := format('DELETE FROM %I.%I a USING mimeo_refresh_updater_temp t WHERE ', v_dest_schema_name, v_dest_table_name);

    WHILE v_pk_counter <= array_length(v_pk_name,1) LOOP
        IF v_pk_counter > 1 THEN
            v_delete_sql := v_delete_sql ||' AND ';
        END IF;
            v_delete_sql := v_delete_sql ||'a."'||v_pk_name[v_pk_counter]||'" = t."'||v_pk_name[v_pk_counter]||'"';
        v_pk_counter := v_pk_counter + 1;
    END LOOP;

    IF v_jobmon THEN
        PERFORM update_step(v_step_id, 'OK','Grabbing rows from '||v_last_value::text||' to '||v_boundary::text);
    END IF;
    PERFORM gdb(p_debug,'Grabbing rows from '||v_last_value::text||' to '||v_boundary::text);
END IF;

v_insert_sql := format('INSERT INTO %I.%I ('||v_cols||') SELECT '||v_cols||' FROM mimeo_refresh_updater_temp', v_dest_schema_name, v_dest_table_name); 

PERFORM gdb(p_debug,v_remote_sql);
PERFORM dblink_open(v_dblink_name, 'mimeo_cursor', v_remote_sql);
IF v_jobmon THEN
    v_step_id := add_step(v_job_id, 'Inserting new/updated records into local table');
END IF;
v_rowcount := 0;

EXECUTE 'CREATE TEMP TABLE mimeo_refresh_updater_temp ('||v_cols_n_types||')'; 
LOOP
    v_fetch_sql := 'INSERT INTO mimeo_refresh_updater_temp ('||v_cols||') 
        SELECT '||v_cols||' FROM dblink_fetch('||quote_literal(v_dblink_name)||', ''mimeo_cursor'', 50000) AS ('||v_cols_n_types||')';
    EXECUTE v_fetch_sql;
    GET DIAGNOSTICS v_rowcount = ROW_COUNT;
    v_total := v_total + coalesce(v_rowcount, 0);
    EXECUTE format('SELECT max(%I) FROM mimeo_refresh_updater_temp', v_control) INTO v_last_fetched;
    IF v_limit IS NULL OR p_repull IS TRUE THEN -- insert into the real table in batches if no limit or repull to avoid excessively large temp tables
        IF p_repull IS FALSE THEN   -- Delete any rows that exist in the current temp table batch. repull delete is done above.
            EXECUTE v_delete_sql;
        END IF;
        EXECUTE v_insert_sql;
        TRUNCATE mimeo_refresh_updater_temp;
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
ELSIF p_repull IS FALSE THEN -- don't care about limits when doing a repull
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
        EXECUTE format('SELECT max(%I) FROM mimeo_refresh_updater_temp', v_control) INTO v_last_value;
        EXECUTE format('DELETE FROM mimeo_refresh_updater_temp WHERE %I = %L', v_control, v_last_value);
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
        EXECUTE 'CREATE INDEX ON mimeo_refresh_updater_temp ("'||array_to_string(v_pk_name, '","')||'")'; -- incase of large batch limit
        ANALYZE mimeo_refresh_updater_temp;
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
    EXECUTE format('SELECT max(%I) FROM %I.%I', v_control, v_dest_schema_name, v_dest_table_name) INTO v_last_value;
    UPDATE refresh_config_updater_time set last_value = coalesce(v_last_value, CURRENT_TIMESTAMP), last_run = CURRENT_TIMESTAMP WHERE dest_table = p_destination;  
    IF v_jobmon THEN
        PERFORM update_step(v_step_id, 'OK','Lower boundary value is: '||coalesce(v_last_value, CURRENT_TIMESTAMP));
    END IF;
    PERFORM gdb(p_debug, 'Lower boundary value is: '||coalesce(v_last_value, CURRENT_TIMESTAMP));
END IF;

DROP TABLE IF EXISTS mimeo_refresh_updater_temp;

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
                EXECUTE 'SELECT '||v_jobmon_schema||'.add_job(''Refresh Updater: '||p_destination||''')' INTO v_job_id;
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


/*
 *  Updater maker function.
 */
CREATE OR REPLACE FUNCTION updater_maker(
    p_src_table text
    , p_type text
    , p_control_field text
    , p_dblink_id int
    , p_boundary text DEFAULT NULL
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

v_boundary_serial           int;
v_boundary_time             interval;
v_data_source               text;
v_dblink_name               text;
v_dblink_schema             text;
v_dest_schema_name          text;
v_dest_table_name           text;
v_dst_active                boolean;
v_field                     text;
v_insert_refresh_config     text;
v_jobmon                    boolean;
v_key_type                  text;
v_link_exists               boolean;
v_max_id                    bigint;
v_max_timestamp             timestamptz;
v_old_search_path           text;
v_pk_name                   text[] := p_pk_name;
v_pk_type                   text[] := p_pk_type;
v_remote_key_sql            text;
v_src_schema_name           text;
v_src_table_name            text;
v_table_exists              boolean;
v_update_refresh_config     text;

BEGIN

v_dblink_name := @extschema@.check_name_length('mimeo_updater_maker_'||p_src_table);

SELECT nspname INTO v_dblink_schema FROM pg_namespace n, pg_extension e WHERE e.extname = 'dblink' AND e.extnamespace = n.oid;
SELECT current_setting('search_path') INTO v_old_search_path;
EXECUTE 'SELECT set_config(''search_path'',''@extschema@,'||v_dblink_schema||',public'',''false'')';

IF (p_pk_name IS NULL AND p_pk_type IS NOT NULL) OR (p_pk_name IS NOT NULL AND p_pk_type IS NULL) THEN
    RAISE EXCEPTION 'Cannot manually set primary/unique key field(s) without defining type(s) or vice versa';
END IF;

SELECT data_source INTO v_data_source FROM @extschema@.dblink_mapping_mimeo WHERE data_source_id = p_dblink_id; 
IF NOT FOUND THEN
    RAISE EXCEPTION 'ERROR: database link ID is incorrect %', p_dblink_id; 
END IF;

IF p_dest_table IS NULL THEN
    p_dest_table := p_src_table;
END IF;

IF position('.' in p_dest_table) > 0 AND position('.' in p_src_table) > 0 THEN
    -- Do nothing. Schema & table variable names set below after table is created
ELSE
    RAISE EXCEPTION 'Source (and destination) table must be schema qualified';
END IF;

PERFORM dblink_connect(v_dblink_name, @extschema@.auth(p_dblink_id));

SELECT schemaname, tablename INTO v_src_schema_name, v_src_table_name 
FROM dblink(v_dblink_name, format('
            SELECT schemaname, tablename
            FROM (
                SELECT schemaname, tablename 
                FROM pg_catalog.pg_tables 
                WHERE schemaname ||''.''|| tablename = %L
                UNION
                SELECT schemaname, viewname AS tablename
                FROM pg_catalog.pg_views
                WHERE schemaname || ''.'' || viewname = %L
            ) tables LIMIT 1'
        , p_src_table, p_src_table) )
    t (schemaname text, tablename text);


IF v_src_table_name IS NULL THEN
    RAISE EXCEPTION 'Source table missing (%)', p_src_table;
END IF;

-- Automatically get source primary/unique key if none given
IF p_pk_name IS NULL AND p_pk_type IS NULL THEN
    SELECT v_key_type, indkey_names, indkey_types INTO v_key_type, v_pk_name, v_pk_type FROM fetch_replication_key(v_src_schema_name, v_src_table_name, v_dblink_name);
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


-- Determine if pg_jobmon is installed to set config table option below
SELECT 
    CASE 
        WHEN count(nspname) > 0 THEN true
        ELSE false
    END AS jobmon_schema
INTO v_jobmon 
FROM pg_namespace n, pg_extension e WHERE e.extname = 'pg_jobmon' AND e.extnamespace = n.oid;

IF p_type = 'time' THEN
    v_dst_active := @extschema@.dst_utc_check();
    IF p_boundary IS NULL THEN
        v_boundary_time = '10 minutes'::interval;
    ELSE
        v_boundary_time = p_boundary::interval;
    END IF;
    v_insert_refresh_config := 'INSERT INTO @extschema@.refresh_config_updater_time(
            source_table
            , type
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
            ||', '||quote_literal('updater_time')
            ||', '||quote_literal(p_dest_table)
            ||', '||quote_literal(p_dblink_id)
            ||', '||quote_literal(p_control_field)
            ||', '||quote_literal(v_boundary_time)
            ||', '||quote_literal(v_pk_name)
            ||', '||quote_literal(v_pk_type)
            ||', '||quote_literal('0001-01-01'::date)
            ||', '||quote_literal(CURRENT_TIMESTAMP)
            ||', '||v_dst_active
            ||', '||COALESCE(quote_literal(p_filter), 'NULL')
            ||', '||COALESCE(quote_literal(p_condition), 'NULL')
            ||', '||v_jobmon||')';
    PERFORM gdb(p_debug, 'v_insert_refresh_config: '||v_insert_refresh_config);
    EXECUTE v_insert_refresh_config;
ELSIF p_type = 'serial' THEN
    IF p_boundary IS NULL THEN
        v_boundary_serial = 10;
    ELSE
        v_boundary_serial = p_boundary::int;
    END IF;
    v_insert_refresh_config := 'INSERT INTO @extschema@.refresh_config_updater_serial(
            source_table
            , type
            , dest_table
            , dblink
            , control
            , boundary
            , pk_name
            , pk_type
            , last_value
            , last_run
            , filter
            , condition
            , jobmon) 
        VALUES('
            ||quote_literal(p_src_table)
            ||', '||quote_literal('updater_serial')
            ||', '||quote_literal(p_dest_table)
            ||', '||quote_literal(p_dblink_id)
            ||', '||quote_literal(p_control_field)
            ||', '||quote_literal(v_boundary_serial)
            ||', '||quote_literal(v_pk_name)
            ||', '||quote_literal(v_pk_type)
            ||', '||quote_literal(0)
            ||', '||quote_literal(CURRENT_TIMESTAMP)
            ||', '||COALESCE(quote_literal(p_filter), 'NULL')
            ||', '||COALESCE(quote_literal(p_condition), 'NULL')
            ||', '||v_jobmon||')';
    PERFORM gdb(p_debug, 'v_insert_refresh_config: '||v_insert_refresh_config);
    EXECUTE v_insert_refresh_config;
END IF;

SELECT p_table_exists FROM @extschema@.manage_dest_table(p_dest_table, NULL, NULL,  p_debug) INTO v_table_exists;

SELECT schemaname, tablename 
INTO v_dest_schema_name, v_dest_table_name
FROM pg_catalog.pg_tables
WHERE schemaname||'.'||tablename = p_dest_table;

IF p_pulldata AND v_table_exists = false THEN
    RAISE NOTICE 'Pulling all data from source...';
    EXECUTE 'SELECT @extschema@.refresh_updater('||quote_literal(p_dest_table)||', p_repull := true, p_debug := '||p_debug||')';
END IF;

IF p_index AND v_table_exists = false THEN
    PERFORM create_index(p_dest_table, v_src_schema_name, v_src_table_name, NULL, p_debug);
ELSIF v_table_exists = false THEN
-- Ensure destination indexes that are needed for efficient replication are created even if p_index is set false
    PERFORM gdb(p_debug, 'Creating indexes needed for replication');
    IF v_key_type = 'primary' THEN
        EXECUTE format('ALTER TABLE %I.%I', v_dest_schema_name, v_dest_table_name)||' ADD PRIMARY KEY('||array_to_string(v_pk_name, ',')||')';
    ELSE
        EXECUTE format('CREATE UNIQUE INDEX ON %I.%I', v_dest_schema_name, v_dest_table_name)||' ('||array_to_string(v_pk_name, ',')||')';
    END IF;
END IF;

IF v_table_exists THEN
    RAISE NOTICE 'Destination table % already exists. No data or indexes was pulled from source', p_dest_table;
END IF;

RAISE NOTICE 'Analyzing destination table...';
EXECUTE format('ANALYZE %I.%I', v_dest_schema_name, v_dest_table_name);

PERFORM dblink_disconnect(v_dblink_name);

IF p_type = 'time' THEN
    RAISE NOTICE 'Getting the maximum destination timestamp...';
    EXECUTE format('SELECT max(%I) FROM %I.%I', p_control_field, v_dest_schema_name, v_dest_table_name) INTO v_max_timestamp;
    EXECUTE format('UPDATE @extschema@.refresh_config_updater_time SET last_value = %L WHERE dest_table = %L', COALESCE(v_max_timestamp, CURRENT_TIMESTAMP), p_dest_table);
ELSIF p_type = 'serial' THEN
    RAISE NOTICE 'Getting the maximum destination id...';
    EXECUTE format('SELECT max(%I) FROM %I.%I', p_control_field, v_dest_schema_name, v_dest_table_name) INTO v_max_id;
    EXECUTE format('UPDATE @extschema@.refresh_config_updater_serial SET last_value = %L WHERE dest_table = %L', COALESCE(v_max_id, 0), p_dest_table);
END IF;

EXECUTE 'SELECT set_config(''search_path'','''||v_old_search_path||''',''false'')';

RAISE NOTICE 'Done';

RETURN;

EXCEPTION
    WHEN QUERY_CANCELED OR OTHERS THEN
        EXECUTE 'SELECT '||v_dblink_schema||'.dblink_get_connections() @> ARRAY['||quote_literal(v_dblink_name)||']' INTO v_link_exists;
        IF v_link_exists THEN
            EXECUTE 'SELECT '||v_dblink_schema||'.dblink_disconnect('||quote_literal(v_dblink_name)||')';
        END IF;
        RAISE EXCEPTION '%', SQLERRM;
END
$$;


/*
 * Check tables on data sources to see if columns have been added or types changed.
 * Returns a record value so that a WHERE condition can be used to ignore tables that aren't desired.
 * If p_data_source_id value is not given, all configured sources are checked.
 */
CREATE OR REPLACE FUNCTION check_source_columns(p_data_source_id int DEFAULT NULL
    , OUT dest_schemaname text
    , OUT dest_tablename text
    , OUT src_schemaname text
    , OUT src_tablename text
    , OUT missing_column_name text
    , OUT missing_column_type text
    , OUT data_source int) RETURNS SETOF record
    LANGUAGE plpgsql
    AS $$
DECLARE

v_dblink_schema         text;
v_exists                int;
v_row_dblink            record;
v_row_table             record;
v_row_col               record;
v_sql                   text;
v_src_schema_name       text;
v_src_table_name        text;

BEGIN

IF p_data_source_id IS NOT NULL THEN
    SELECT count(*) INTO v_exists FROM @extschema@.dblink_mapping_mimeo WHERE data_source_id = p_data_source_id;
    IF v_exists < 1 THEN
        RAISE EXCEPTION 'Given data_source_id (%) does not exist in @extschema@.dblink_mapping_mimeo config table', p_data_source_id;
    END IF;
END IF;

SELECT nspname INTO v_dblink_schema FROM pg_namespace n, pg_extension e WHERE e.extname = 'dblink' AND e.extnamespace = n.oid;

FOR v_row_dblink IN SELECT data_source_id FROM @extschema@.dblink_mapping_mimeo
LOOP
    -- Allow a parameter to choose which data sources are checked. If parameter is NULL, check them all.
    IF p_data_source_id IS NOT NULL THEN
        IF p_data_source_id <> v_row_dblink.data_source_id THEN
            CONTINUE;
        END IF;
    END IF;
    EXECUTE 'SELECT '||v_dblink_schema||'.dblink_connect(''mimeo_col_check'', @extschema@.auth('||v_row_dblink.data_source_id||'))';

    FOR v_row_table IN 
        ( SELECT source_table, dest_table, filter, type FROM @extschema@.refresh_config_snap WHERE dblink = v_row_dblink.data_source_id
        UNION
        SELECT source_table, dest_table, filter, type FROM @extschema@.refresh_config_inserter WHERE dblink = v_row_dblink.data_source_id
        UNION
        SELECT source_table, dest_table, filter, type FROM @extschema@.refresh_config_updater WHERE dblink = v_row_dblink.data_source_id
        UNION
        SELECT source_table, dest_table, filter, type FROM @extschema@.refresh_config_dml WHERE dblink = v_row_dblink.data_source_id
        UNION
        SELECT source_table, dest_table, filter, type FROM @extschema@.refresh_config_logdel WHERE dblink = v_row_dblink.data_source_id
        UNION
        SELECT source_table, dest_table, filter, type FROM @extschema@.refresh_config_table WHERE dblink = v_row_dblink.data_source_id )
        ORDER BY 1,2
    LOOP
        v_sql := format('SELECT schemaname, tablename
            FROM (
                SELECT schemaname, tablename 
                FROM pg_catalog.pg_tables 
                WHERE schemaname ||''.''|| tablename = %L
                UNION
                SELECT schemaname, viewname AS tablename
                FROM pg_catalog.pg_views
                WHERE schemaname || ''.'' || viewname = %L
            ) tables LIMIT 1'
        , v_row_table.source_table, v_row_table.source_table);

        EXECUTE format('SELECT schemaname, tablename
                    FROM %I.dblink(%L, %L)
                    AS (schemaname text, tablename text)'
                    , v_dblink_schema, 'mimeo_col_check', v_sql)
                INTO v_src_schema_name, v_src_table_name;

        v_sql := format('SELECT a.attname::text, a.atttypid, a.atttypmod, n.nspname AS schemaname, c.relname AS tablename
                    FROM pg_catalog.pg_attribute a
                    JOIN pg_catalog.pg_class c ON c.oid = a.attrelid
                    JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid
                    WHERE n.nspname = %L
                    AND c.relname = %L
                    AND a.attnum > 0
                    AND attisdropped = false
                    ORDER BY 1,2'
                , v_src_schema_name, v_src_table_name);

        FOR v_row_col IN EXECUTE 
            format('SELECT attname, atttypid, atttypmod, schemaname, tablename 
            FROM %I.dblink(%L, %L) 
            AS (attname text, atttypid oid, atttypmod int, schemaname text, tablename text)'
            , v_dblink_schema, 'mimeo_col_check', v_sql)
        LOOP
            IF v_row_col.attname <> ANY (v_row_table.filter) THEN
                CONTINUE;
            END IF;

            IF v_row_table.type = 'snap' THEN
                SELECT schemaname, viewname INTO dest_schemaname, dest_tablename FROM pg_catalog.pg_views WHERE schemaname||'.'||viewname = v_row_table.dest_table;
            ELSE
                SELECT schemaname, tablename INTO dest_schemaname, dest_tablename FROM pg_catalog.pg_tables WHERE schemaname||'.'||tablename = v_row_table.dest_table;
            END IF;

            SELECT count(*) INTO v_exists
            FROM pg_catalog.pg_attribute a
            JOIN pg_catalog.pg_class c ON a.attrelid = c.oid
            JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid
            WHERE n.nspname = dest_schemaname
            AND c.relname = dest_tablename
            AND attnum > 0
            AND attisdropped = false
            AND attname = v_row_col.attname
            AND atttypid = v_row_col.atttypid
            AND atttypmod = v_row_col.atttypmod;

            -- if column doesn't exist, means it's missing on destination.
            IF v_exists < 1 THEN
                src_schemaname := v_row_col.schemaname;
                src_tablename := v_row_col.tablename;
                missing_column_name := v_row_col.attname;
                missing_column_type := format_type(v_row_col.atttypid, v_row_col.atttypmod)::text;
                data_source := v_row_dblink.data_source_id;
                RETURN NEXT;
            ELSE
                -- Reset output variables used above
                dest_schemaname = NULL;
                dest_tablename = NULL;
            END IF;

        END LOOP; -- end v_row_col

    END LOOP; -- end v_row_table

    EXECUTE 'SELECT '||v_dblink_schema||'.dblink_disconnect(''mimeo_col_check'')';

END LOOP; -- end v_row_dblink

END
$$;


-- Restore dropped object privileges
DO $$
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

DROP TABLE mimeo_preserve_privs_temp;
