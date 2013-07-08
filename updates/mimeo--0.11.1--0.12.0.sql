-- Now supports creating GIN, GiST, and other expression indexes on the destination table as they exist on the source table. Previously, all indexes were recreated as b-tree (if at all) no matter what kind of index they were on the source.
-- Above change led to major rewriting of the underlying code for the maker functions. Previously they all used the refresh_snap() function as the basis for doing initial, full data pull of the source. This lead to indexes being named a bit oddly (they all had "snap1" in the name). This is no longer the case and destination table index names should match closely to what they were on the source. This is only applied to newly created replication sets and no existing indexes on the destination will be changed.
-- Logdel refresh function now has a "repull" option to do a complete refresh of data from the source. Note that it will NOT delete the rows on the destination that were previously deleted from the source (only deletes rows on the destination where mimeo_source_deleted column is null). Since a TRUNCATE cannot be done as is done with the other replication repulls, it is highly recommended to do a manual VACUUM of destination table after this is done, possibly even a VACUUM FULL to reclaim disk space.
-- To help the above repull process be more efficient, an index is now created on the "mimeo_source_deleted" destination column of all newly created logdel replication tables. Existing replication tables will not have this index added. Recommend adding one if you need to do this repull method on old setups.
-- DML & Logdel replication source queue tables now have indexes created on the "processed" and "processed, mimeo_source_deleted" columns respectively. This should help replication be more efficient for higher traffic tables that create larger queues. Existing replication sets will not have their source queue table modified to add this index. Recommend going back and manually adding it if you notice performance problems.
-- Exception messages if dml/logdel maker functions fail are clearer about what has happened reguarding objects created on the source.
-- Added p_debug option to maker functions. Also made debugging a little cleaner and have it provide more information in many cases.
-- Bug Fix: WHERE condition of logdel replication wasn't working properly. 


-- Preserve dropped function privileges. Re-applied at the end of this update.
CREATE TEMP TABLE mimeo_preserve_privs_temp (statement text);

INSERT INTO mimeo_preserve_privs_temp 
SELECT 'GRANT EXECUTE ON FUNCTION @extschema@.snapshot_maker(text, int, text, boolean, text[], text, boolean, boolean) TO '||array_to_string(array_agg(grantee::text), ',')||';' 
FROM information_schema.routine_privileges
WHERE routine_schema = '@extschema@'
AND routine_name = 'snapshot_maker'; 

INSERT INTO mimeo_preserve_privs_temp 
SELECT 'GRANT EXECUTE ON FUNCTION @extschema@.dml_maker(text, int, text, boolean, text[], text, boolean, text[], text[], boolean) TO '||array_to_string(array_agg(grantee::text), ',')||';' 
FROM information_schema.routine_privileges
WHERE routine_schema = '@extschema@'
AND routine_name = 'dml_maker'; 

INSERT INTO mimeo_preserve_privs_temp 
SELECT 'GRANT EXECUTE ON FUNCTION @extschema@.logdel_maker(text, int, text, boolean, text[], text, boolean, text[], text[], boolean) TO '||array_to_string(array_agg(grantee::text), ',')||';' 
FROM information_schema.routine_privileges
WHERE routine_schema = '@extschema@'
AND routine_name = 'logdel_maker'; 

INSERT INTO mimeo_preserve_privs_temp 
SELECT 'GRANT EXECUTE ON FUNCTION @extschema@.inserter_maker(text, text, int, interval, text, boolean, text[], text, boolean, boolean) TO '||array_to_string(array_agg(grantee::text), ',')||';' 
FROM information_schema.routine_privileges
WHERE routine_schema = '@extschema@'
AND routine_name = 'inserter_maker'; 

INSERT INTO mimeo_preserve_privs_temp 
SELECT 'GRANT EXECUTE ON FUNCTION @extschema@.updater_maker(text, text, int, interval, text, boolean, text[], text, boolean, text[], text[], boolean) TO '||array_to_string(array_agg(grantee::text), ',')||';' 
FROM information_schema.routine_privileges
WHERE routine_schema = '@extschema@'
AND routine_name = 'updater_maker'; 

INSERT INTO mimeo_preserve_privs_temp 
SELECT 'GRANT EXECUTE ON FUNCTION @extschema@.table_maker(text, int, text, boolean, text[], text, text[], boolean, boolean) TO '||array_to_string(array_agg(grantee::text), ',')||';' 
FROM information_schema.routine_privileges
WHERE routine_schema = '@extschema@'
AND routine_name = 'table_maker'; 

INSERT INTO mimeo_preserve_privs_temp 
SELECT 'GRANT EXECUTE ON FUNCTION @extschema@.refresh_logdel(text, int, boolean, boolean) TO '||array_to_string(array_agg(grantee::text), ',')||';' 
FROM information_schema.routine_privileges
WHERE routine_schema = '@extschema@'
AND routine_name = 'refresh_logdel'; 

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

DROP FUNCTION @extschema@.snapshot_maker(text, int, text, boolean, text[], text, boolean);
DROP FUNCTION @extschema@.dml_maker(text, int, text, boolean, text[], text, boolean, text[], text[]); 
DROP FUNCTION @extschema@.logdel_maker(text, int, text, boolean, text[], text, boolean, text[], text[]);
DROP FUNCTION @extschema@.inserter_maker(text, text, int, interval,text, boolean,text[], text, boolean); 
DROP FUNCTION @extschema@.updater_maker(text, text, int, interval, text, boolean, text[], text, boolean, text[], text[]); 
DROP FUNCTION @extschema@.table_maker(text, int, text, boolean, text[], text, text[], boolean);
DROP FUNCTION @extschema@.refresh_logdel(text, int, boolean);

/*
 * Manages creating destination table and/or returning data about the columns.
 * v_snap parameter is passed if snap table is being managed. Should be equal to either _snap1 or _snap2.
 */ 
CREATE FUNCTION manage_dest_table (p_destination text, p_snap text, p_debug boolean DEFAULT false, OUT p_table_exists boolean, OUT p_cols text[], OUT p_cols_n_types text[]) RETURNS record
    LANGUAGE plpgsql SECURITY DEFINER
    AS $$
DECLARE

v_col_exists        int;
v_condition         text;
v_create_sql        text; 
v_dblink            int;
v_dblink_name       text;
v_dblink_schema     text; 
v_dest_table        text;
v_filter            text[];
v_link_exists       boolean;
v_remote_sql        text; 
v_old_search_path   text;
v_source_table      text;
v_type              text;

BEGIN

SELECT nspname INTO v_dblink_schema FROM pg_namespace n, pg_extension e WHERE e.extname = 'dblink' AND e.extnamespace = n.oid;
v_dblink_name := 'manage_dest_table_dblink_'||p_destination;

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

PERFORM dblink_connect(v_dblink_name, @extschema@.auth(v_dblink));

-- Always return source column info in case extra columns were added to destination. Source columns should not be changed before destination.
v_remote_sql := 'SELECT array_agg(attname) as cols, array_agg(attname||'' ''||format_type(atttypid, atttypmod)::text) as cols_n_types FROM pg_attribute WHERE attrelid = '||quote_literal(v_source_table)||'::regclass AND attnum > 0 AND attisdropped is false';
IF v_filter IS NOT NULL THEN -- Apply column filters if used
    v_remote_sql := v_remote_sql || ' AND ARRAY[attname::text] <@ '||quote_literal(v_filter);
END IF;
v_remote_sql := 'SELECT cols, cols_n_types FROM dblink('||quote_literal(v_dblink_name)||', ' || quote_literal(v_remote_sql) || ') t (cols text[], cols_n_types text[])';
PERFORM gdb(p_debug,'v_remote_sql: '||v_remote_sql);
EXECUTE v_remote_sql INTO p_cols, p_cols_n_types;  
PERFORM gdb(p_debug,'p_cols: {'|| array_to_string(p_cols, ',') ||'}');
PERFORM gdb(p_debug,'p_cols_n_types: {'|| array_to_string(p_cols_n_types, ',') ||'}');

SELECT 
    CASE    
        WHEN count(1) > 0 THEN true
        ELSE false 
    END
INTO p_table_exists FROM pg_tables WHERE schemaname ||'.'|| tablename = v_dest_table || COALESCE('_'||p_snap, '');
IF p_table_exists = false THEN
    v_create_sql := 'CREATE TABLE ' || v_dest_table || COALESCE('_'||p_snap, '') || ' (' || array_to_string(p_cols_n_types, ',') || ')';
    perform gdb(p_debug,'v_create_sql: '||v_create_sql::text);
    EXECUTE v_create_sql;
END IF;

IF v_type = 'logdel' THEN
    SELECT count(*) INTO v_col_exists FROM pg_attribute 
        WHERE attrelid = v_dest_table::regclass AND attname = 'mimeo_source_deleted' AND attisdropped = false;
    IF v_col_exists < 1 THEN
        EXECUTE 'ALTER TABLE '||v_dest_table||' ADD COLUMN mimeo_source_deleted timestamptz';
    ELSE
        RAISE WARNING 'Special column (mimeo_source_deleted) already exists on destination table (%)', v_dest_table;
    END IF;
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
 * Create index(es) on destination table
 */
CREATE FUNCTION create_index(p_destination text, p_snap text DEFAULT NULL, p_debug boolean DEFAULT false) RETURNS void
    LANGUAGE plpgsql SECURITY DEFINER
    AS $$
DECLARE

v_dblink            int;
v_dblink_name       text;
v_dblink_schema     text;
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

SELECT nspname INTO v_dblink_schema FROM pg_namespace n, pg_extension e WHERE e.extname = 'dblink' AND e.extnamespace = n.oid;
v_dblink_name := 'create_index_dblink_'||p_destination;
SELECT current_setting('search_path') INTO v_old_search_path;
EXECUTE 'SELECT set_config(''search_path'',''@extschema@,'||v_dblink_schema||',public'',''false'')';

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

v_dest_table := v_dest_table;
v_dest_table_name := split_part(v_dest_table, '.', 2);

-- Gets primary key or unique index used by updater/dml/logdel replication. 
-- Ordering by statement should produce the same order as maker function since ALTER comes before CREATE.
-- Maker function already checks that columns needed by this index aren't filtered out.
v_remote_index_sql := 'SELECT indexrelid AS repl_index,
    relname AS src_table,
    CASE WHEN indisprimary THEN
        ''ALTER TABLE '||v_dest_table || COALESCE('_'||p_snap, '')||' ADD CONSTRAINT '||
            COALESCE(p_snap||'_', '')|| v_dest_table_name ||'_''||array_to_string(indkey_names, ''_'')||''_pk 
            PRIMARY KEY (''||array_to_string(indkey_names, '','')||'')''
        WHEN indisunique THEN
        pg_get_indexdef(indexrelid)
    END AS statement
    FROM ( 
        SELECT i.indexrelid, i.indisprimary, i.indisunique, c.relname, (
            SELECT array_agg( a.attname ORDER by x.r ) 
            FROM pg_catalog.pg_attribute a
            JOIN ( SELECT k, row_number() over () as r 
                    FROM unnest(i.indkey) k ) as x 
            ON a.attnum = x.k AND a.attrelid = i.indrelid
            WHERE a.attnotnull
        ) AS indkey_names
        FROM pg_catalog.pg_index i
        JOIN pg_catalog.pg_class c ON i.indrelid = c.oid 
        WHERE i.indrelid = '||quote_literal(v_source_table)||'::regclass
        AND (i.indisprimary OR i.indisunique)
        AND i.indisvalid
    ) AS x 
    ORDER BY statement
    LIMIT 1';

PERFORM gdb(p_debug, 'v_remote_index_sql: '||v_remote_index_sql);
FOR v_row IN EXECUTE 'SELECT repl_index, src_table, statement FROM dblink('||quote_literal(v_dblink_name)||', '||quote_literal(v_remote_index_sql)||') t (repl_index oid, src_table text, statement text)' LOOP
    v_statement := v_row.statement;
    -- Replace source table name with destination
    v_statement := replace(v_statement, ' ON '||v_source_table, ' ON '||v_dest_table || COALESCE('_'||p_snap, ''));
    -- If source index name contains the table name, replace it with the destination table. Not perfect, but good enough for now.
    v_statement := replace(v_statement, v_row.src_table, v_dest_table_name);
    -- If it's a snap table, prepend to ensure unique index name. 
    -- This is done separately from above replace because it must always be done even if the index name doesn't contain the source table
    IF p_snap IS NOT NULL THEN
        v_statement := replace(v_statement, 'UNIQUE INDEX ' , 'UNIQUE INDEX '||p_snap||'_');
    END IF;
    PERFORM gdb(p_debug, 'statement: ' || v_statement);
    v_repl_index := v_row.repl_index;
    EXECUTE v_statement;        
END LOOP;

-- Get all indexes other than one obtained above. 
-- Cannot set these indexes when column filters are in use because there's no easy way to check columns in expression indexes.
IF v_filter IS NULL THEN
    v_remote_index_sql := 'select c.relname AS src_table, pg_get_indexdef(i.indexrelid) as statement
        FROM pg_catalog.pg_index i
        JOIN pg_catalog.pg_class c ON i.indrelid = c.oid 
        WHERE i.indrelid = '||quote_literal(v_source_table)||'::regclass
        AND i.indisprimary IS false
        AND i.indisvalid';
    IF v_repl_index IS NOT NULL THEN
        v_remote_index_sql := v_remote_index_sql ||' AND i.indexrelid <> '||v_repl_index;
    END IF;

    FOR v_row IN EXECUTE 'SELECT src_table, statement FROM dblink('||quote_literal(v_dblink_name)||', '||quote_literal(v_remote_index_sql)||') t (src_table text, statement text)' LOOP
        v_statement := v_row.statement;
        -- Replace source table name with destination
        v_statement := replace(v_statement, ' ON '||v_source_table, ' ON '||v_dest_table || COALESCE('_'||p_snap, ''));
        -- If source index name contains the table name, replace it with the destination table. Not perfect, but good enough for now.
        v_statement := replace(v_statement, v_row.src_table, v_dest_table_name);
        -- If it's a snap table, prepend to ensure unique index name. 
        -- This is done separately from above replace because it must always be done even if the index name doesn't contain the source table
        IF p_snap IS NOT NULL THEN
            v_statement := replace(v_statement, 'E INDEX ' , 'E INDEX '||p_snap||'_');
        END IF;
        PERFORM gdb(p_debug, 'statement: ' || v_statement);
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
 *  Snap refresh to repull all table data
 */
CREATE OR REPLACE FUNCTION refresh_snap(p_destination text, p_index boolean DEFAULT true, p_debug boolean DEFAULT false, p_pulldata boolean DEFAULT true) RETURNS void
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
v_jobmon_schema     text;
v_job_name          text;
v_lcols_array       text[];
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
EXECUTE 'SELECT set_config(''search_path'',''@extschema@,'||v_jobmon_schema||','||v_dblink_schema||',public'',''false'')';

v_job_id := add_job(v_job_name);
PERFORM gdb(p_debug,'Job ID: '||v_job_id::text);

-- Take advisory lock to prevent multiple calls to function overlapping and causing possible deadlock
v_adv_lock := pg_try_advisory_lock(hashtext('refresh_snap'), hashtext(v_job_name));
IF v_adv_lock = 'false' THEN
    v_step_id := add_step(v_job_id,'Obtaining advisory lock for job: '||v_job_name);
    PERFORM gdb(p_debug,'Obtaining advisory lock FAILED for job: '||v_job_name);
    PERFORM update_step(v_step_id, 'WARNING','Found concurrent job. Exiting gracefully');
    PERFORM fail_job(v_job_id, 2);
    EXECUTE 'SELECT set_config(''search_path'','''||v_old_search_path||''',''false'')';
    RETURN;
END IF;

v_step_id := add_step(v_job_id,'Grabbing Mapping, Building SQL');

SELECT source_table
    , dest_table
    , dblink
    , filter
    , condition
    , n_tup_ins
    , n_tup_upd
    , n_tup_del
    , post_script 
INTO v_source_table
    , v_dest_table
    , v_dblink
    , v_filter
    , v_condition
    , v_tup_ins
    , v_tup_upd
    , v_tup_del
    , v_post_script 
FROM refresh_config_snap
WHERE dest_table = p_destination; 
IF NOT FOUND THEN
   RAISE EXCEPTION 'ERROR: This table is not set up for snapshot replication: %',v_job_name; 
END IF;  

-- checking for current view
SELECT definition INTO v_view_definition FROM pg_views where
      ((schemaname || '.') || viewname)=v_dest_table;

PERFORM dblink_connect(v_dblink_name, @extschema@.auth(v_dblink));

PERFORM update_step(v_step_id, 'OK','Done');
v_step_id := add_step(v_job_id,'Truncate non-active snap table');

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

        v_step_id := add_step(v_job_id,'Source table structure changed.');
        PERFORM update_step(v_step_id, 'OK','Tables and view dropped and recreated. Please double-check snap table attributes (permissions, indexes, etc');
        PERFORM gdb(p_debug,'Source table structure changed. Tables and view dropped and recreated. Please double-check snap table attributes (permissions, indexes, etc)');
    END IF;
    -- truncate non-active snap table
    EXECUTE 'TRUNCATE TABLE ' || v_refresh_snap;

    PERFORM update_step(v_step_id, 'OK','Done');
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
        PERFORM update_step(v_step_id, 'OK', 'Remote table has not had any writes. Skipping data pull');
        UPDATE refresh_config_snap SET last_run = CURRENT_TIMESTAMP WHERE dest_table = p_destination;  
        PERFORM dblink_disconnect(v_dblink_name);
        PERFORM close_job(v_job_id);
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

v_step_id := add_step(v_job_id,'Inserting records into local table');
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
    PERFORM update_step(v_step_id, 'PENDING', 'Fetching rows in batches: '||v_total||' done so far.');
END LOOP;
PERFORM dblink_close(v_dblink_name, 'mimeo_cursor');

PERFORM update_step(v_step_id, 'OK','Inserted '||v_total||' rows');

-- Create indexes if new table was created
IF (v_table_exists = false OR v_match = 'f') AND p_index = true THEN
    PERFORM gdb(p_debug, 'Creating indexes');
    PERFORM create_index(v_dest_table, v_snap, p_debug);
END IF;

EXECUTE 'ANALYZE ' ||v_refresh_snap;

-- swap view
v_step_id := add_step(v_job_id,'Swap view to '||v_refresh_snap);
PERFORM gdb(p_debug,'Swapping view to '||v_refresh_snap);
EXECUTE 'CREATE OR REPLACE VIEW '||v_dest_table||' AS SELECT * FROM '||v_refresh_snap;
PERFORM update_step(v_step_id, 'OK','View Swapped');

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
        v_step_id := add_step(v_job_id,'Applying post_script sql commands due to schema change');
        PERFORM @extschema@.post_script(v_dest_table);
        PERFORM update_step(v_step_id, 'OK','Done');
    END IF;
END IF;

SELECT 
    CASE    
        WHEN count(1) > 0 THEN true
        ELSE false 
    END
INTO v_table_exists FROM pg_tables WHERE schemaname ||'.'|| tablename = v_old_snap_table;
IF v_table_exists THEN
    v_step_id := add_step(v_job_id,'Truncating old snap table');
    EXECUTE 'TRUNCATE TABLE '||v_old_snap_table;
    PERFORM update_step(v_step_id, 'OK','Done');
END IF;

v_step_id := add_step(v_job_id,'Updating last_run & tuple change values');
UPDATE refresh_config_snap SET 
    last_run = CURRENT_TIMESTAMP 
    , n_tup_ins = v_tup_ins_new   
    , n_tup_upd = v_tup_upd_new
    , n_tup_del = v_tup_del_new
WHERE dest_table = p_destination;  
PERFORM update_step(v_step_id, 'OK','Done');

PERFORM dblink_disconnect(v_dblink_name);

PERFORM close_job(v_job_id);

-- Ensure old search path is reset for the current session
EXECUTE 'SELECT set_config(''search_path'','''||v_old_search_path||''',''false'')';

PERFORM pg_advisory_unlock(hashtext('refresh_snap'), hashtext(v_job_name));

EXCEPTION
    WHEN QUERY_CANCELED THEN
        EXECUTE 'SELECT set_config(''search_path'',''@extschema@,'||v_jobmon_schema||','||v_dblink_schema||''',''false'')';
        IF dblink_get_connections() @> ARRAY[v_dblink_name] THEN
            PERFORM dblink_disconnect(v_dblink_name);
        END IF;
        EXECUTE 'SELECT set_config(''search_path'','''||v_old_search_path||''',''false'')';
        PERFORM pg_advisory_unlock(hashtext('refresh_snap'), hashtext(v_job_name));
        RAISE EXCEPTION '%', SQLERRM;   
    WHEN OTHERS THEN
        EXECUTE 'SELECT set_config(''search_path'',''@extschema@,'||v_jobmon_schema||','||v_dblink_schema||''',''false'')';
        IF v_job_id IS NULL THEN
            v_job_id := add_job('Refresh Snap: '||p_destination);
            v_step_id := add_step(v_job_id, 'EXCEPTION before job logging started');
        END IF;
        IF v_step_id IS NULL THEN
            v_step_id := add_step(v_job_id, 'EXCEPTION before first step logged');
        END IF;
        PERFORM update_step(v_step_id, 'CRITICAL', 'ERROR: '||coalesce(SQLERRM,'unknown'));
        PERFORM fail_job(v_job_id);
        IF dblink_get_connections() @> ARRAY[v_dblink_name] THEN
            PERFORM dblink_disconnect(v_dblink_name);
        END IF;
        EXECUTE 'SELECT set_config(''search_path'','''||v_old_search_path||''',''false'')';
        PERFORM pg_advisory_unlock(hashtext('refresh_snap'), hashtext(v_job_name));
        RAISE EXCEPTION '%', SQLERRM;
END
$$;


/*
 *  Refresh based on DML (Insert, Update, Delete), but logs all deletes on the destination table
 *  Destination table requires extra column: mimeo_source_deleted timestamptz
 */
CREATE FUNCTION refresh_logdel(p_destination text, p_limit int DEFAULT NULL, p_repull boolean DEFAULT false, p_debug boolean DEFAULT false) RETURNS void
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
v_job_name              text;
v_limit                 int; 
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

v_step_id := add_step(v_job_id,'Sanity check primary/unique key values');

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

PERFORM update_step(v_step_id, 'OK','Done');

PERFORM dblink_connect(v_dblink_name, auth(v_dblink));

-- update remote entries
v_step_id := add_step(v_job_id,'Updating remote trigger table');
v_with_update := 'WITH a AS (SELECT '||v_pk_name_csv||' FROM '|| v_control ||' ORDER BY '||v_pk_name_csv||' LIMIT '|| COALESCE(v_limit::text, 'ALL') ||') UPDATE '||v_control||' b SET processed = true FROM a WHERE '|| v_pk_where;
v_trigger_update := 'SELECT dblink_exec('||quote_literal(v_dblink_name)||','|| quote_literal(v_with_update)||')';
PERFORM gdb(p_debug,v_trigger_update);
EXECUTE v_trigger_update INTO v_exec_status;    
PERFORM update_step(v_step_id, 'OK','Result was '||v_exec_status);

-- create temp table for recording deleted rows
EXECUTE 'CREATE TEMP TABLE '||v_tmp_table||'_deleted ('||v_cols_n_types||', mimeo_source_deleted timestamptz)';
v_remote_d_sql := 'SELECT '||v_cols||', mimeo_source_deleted FROM '||v_control||' WHERE processed = true and mimeo_source_deleted IS NOT NULL';
PERFORM dblink_open(v_dblink_name, 'mimeo_cursor', v_remote_d_sql);
v_step_id := add_step(v_job_id, 'Creating local queue temp table for deleted rows on source');
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
    PERFORM update_step(v_step_id, 'PENDING', 'Fetching rows in batches: '||v_total||' done so far.');
END LOOP;
PERFORM dblink_close(v_dblink_name, 'mimeo_cursor');
EXECUTE 'CREATE INDEX ON '||v_tmp_table||'_deleted ('||v_pk_name_csv||')';
EXECUTE 'ANALYZE '||v_tmp_table||'_deleted';
PERFORM update_step(v_step_id, 'OK','Number of rows inserted: '||v_total);
PERFORM gdb(p_debug,'Temp deleted queue table row count '||v_total::text);  

IF p_repull THEN
    -- Do delete instead of truncate like refresh_dml to avoid missing rows between the above deleted queue fetch and here.
    PERFORM update_step(v_step_id, 'OK','Request to repull ALL data from source. This could take a while...');
    PERFORM gdb(p_debug, 'Request to repull ALL data from source. This could take a while...');
    v_truncate_remote_q := 'SELECT dblink_exec('||quote_literal(v_dblink_name)||','||quote_literal('DELETE FROM '||v_control||' WHERE processed = true')||')';
    PERFORM gdb(p_debug, v_truncate_remote_q);
    EXECUTE v_truncate_remote_q;

    v_step_id := add_step(v_job_id,'Removing local, undeleted rows');
    PERFORM gdb(p_debug,'Removing local, undeleted rows');
    EXECUTE 'DELETE FROM '||v_dest_table||' WHERE mimeo_source_deleted IS NULL';
    PERFORM update_step(v_step_id, 'OK','Done');

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
    v_step_id := add_step(v_job_id, 'Creating local queue temp table for inserts/updates');
    v_rowcount := 0;
    LOOP
        v_fetch_sql := 'INSERT INTO '||v_tmp_table||'_queue ('||v_pk_name_csv||') 
            SELECT '||v_pk_name_csv||' FROM dblink_fetch('||quote_literal(v_dblink_name)||', ''mimeo_cursor'', 50000) AS ('||v_pk_name_type_csv||')';
        EXECUTE v_fetch_sql;
        GET DIAGNOSTICS v_rowcount = ROW_COUNT;
        EXIT WHEN v_rowcount = 0;
        v_total := v_total + coalesce(v_rowcount, 0);
        PERFORM gdb(p_debug,'Fetching rows in batches: '||v_total||' done so far.');
        PERFORM update_step(v_step_id, 'PENDING', 'Fetching rows in batches: '||v_total||' done so far.');
    END LOOP;
    PERFORM dblink_close(v_dblink_name, 'mimeo_cursor');
    EXECUTE 'CREATE INDEX ON '||v_tmp_table||'_queue ('||v_pk_name_csv||')';
    EXECUTE 'ANALYZE '||v_tmp_table||'_queue';
    PERFORM update_step(v_step_id, 'OK','Number of rows inserted: '||v_total);
    PERFORM gdb(p_debug,'Temp inserts/updates queue table row count '||v_total::text);

    -- remove records from local table (inserts/updates)
    v_step_id := add_step(v_job_id,'Removing insert/update records from local table');
    v_delete_f_sql := 'DELETE FROM '||v_dest_table||' a USING '||v_tmp_table||'_queue b WHERE '|| v_pk_where;
    PERFORM gdb(p_debug,v_delete_f_sql);
    EXECUTE v_delete_f_sql; 
    GET DIAGNOSTICS v_rowcount = ROW_COUNT;
    PERFORM gdb(p_debug,'Insert/Update rows removed from local table before applying changes: '||v_rowcount::text);
    PERFORM update_step(v_step_id, 'OK','Removed '||v_rowcount||' records');

    -- remove records from local table (deleted rows)
    v_step_id := add_step(v_job_id,'Removing deleted records from local table');
    v_delete_d_sql := 'DELETE FROM '||v_dest_table||' a USING '||v_tmp_table||'_deleted b WHERE '|| v_pk_where;
    PERFORM gdb(p_debug,v_delete_d_sql);
    EXECUTE v_delete_d_sql; 
    GET DIAGNOSTICS v_rowcount = ROW_COUNT;
    PERFORM gdb(p_debug,'Deleted rows removed from local table before applying changes: '||v_rowcount::text);
    PERFORM update_step(v_step_id, 'OK','Removed '||v_rowcount||' records');

    -- Remote full query for normal replication 
    v_remote_f_sql := 'SELECT '||v_cols||' FROM '||v_source_table||' JOIN ('||v_remote_q_sql||') x USING ('||v_pk_name_csv||')';
    IF v_condition IS NOT NULL THEN
        v_remote_f_sql := v_remote_f_sql || ' ' || v_condition;
    END IF;
END IF;

-- insert records to local table (inserts/updates). Have to do temp table in case destination table is partitioned (returns 0 when inserting to parent)
PERFORM dblink_open(v_dblink_name, 'mimeo_cursor', v_remote_f_sql);
v_step_id := add_step(v_job_id, 'Inserting new/updated records into local table');
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
    PERFORM update_step(v_step_id, 'PENDING', 'Fetching rows in batches: '||v_total||' done so far.');
END LOOP;
PERFORM dblink_close(v_dblink_name, 'mimeo_cursor');
PERFORM update_step(v_step_id, 'OK','New/updated rows inserted: '||v_total);

-- insert records to local table (deleted rows to be kept)
v_step_id := add_step(v_job_id,'Inserting deleted records into local table');
v_insert_deleted_sql := 'INSERT INTO '||v_dest_table||'('||v_cols||', mimeo_source_deleted) SELECT '||v_cols||', mimeo_source_deleted FROM '||v_tmp_table||'_deleted'; 
PERFORM gdb(p_debug,v_insert_deleted_sql);
EXECUTE v_insert_deleted_sql;
GET DIAGNOSTICS v_rowcount = ROW_COUNT;
PERFORM gdb(p_debug,'Deleted rows inserted: '||v_rowcount::text);
PERFORM update_step(v_step_id, 'OK','Inserted '||v_rowcount||' records');

IF (v_total + v_rowcount) > (v_limit * .75) THEN
    v_step_id := add_step(v_job_id, 'Row count warning');
    PERFORM update_step(v_step_id, 'WARNING','Row count fetched ('||v_total||') greater than 75% of batch limit ('||v_limit||'). Recommend increasing batch limit if possible.');
    v_batch_limit_reached := true;
END IF;

-- clean out rows from txn table
v_step_id := add_step(v_job_id,'Cleaning out rows from txn table');
v_trigger_delete := 'SELECT dblink_exec('||quote_literal(v_dblink_name)||','||quote_literal('DELETE FROM '||v_control||' WHERE processed = true')||')'; 
PERFORM gdb(p_debug,v_trigger_delete);
EXECUTE v_trigger_delete INTO v_exec_status;
PERFORM update_step(v_step_id, 'OK','Result was '||v_exec_status);

-- update activity status
v_step_id := add_step(v_job_id,'Updating last_run in config table');
UPDATE refresh_config_logdel SET last_run = CURRENT_TIMESTAMP WHERE dest_table = p_destination; 
PERFORM update_step(v_step_id, 'OK','Last Value was '||current_timestamp);

PERFORM dblink_disconnect(v_dblink_name);

EXECUTE 'DROP TABLE IF EXISTS '||v_tmp_table||'_full';
EXECUTE 'DROP TABLE IF EXISTS '||v_tmp_table||'_queue';
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
        PERFORM update_step(v_step_id, 'CRITICAL', 'ERROR: '||coalesce(SQLERRM,'unknown'));
        PERFORM fail_job(v_job_id);

        EXECUTE 'SELECT set_config(''search_path'','''||v_old_search_path||''',''false'')';

        PERFORM pg_advisory_unlock(hashtext('refresh_logdel'), hashtext(v_job_name));
        RAISE EXCEPTION '%', SQLERRM;
END
$$;


/*
 *  Snapshot maker function.
 */
CREATE FUNCTION snapshot_maker(
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

v_insert_refresh_config := 'INSERT INTO @extschema@.refresh_config_snap(source_table, dest_table, dblink, filter, condition) VALUES('||quote_literal(p_src_table)||', '||quote_literal(p_dest_table)||','||p_dblink_id||','||COALESCE(quote_literal(p_filter), 'NULL')||','||COALESCE(quote_literal(p_condition), 'NULL')||')';

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
 *  DML maker function.
 */
CREATE FUNCTION dml_maker(
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

IF p_pk_name IS NULL AND p_pk_type IS NULL THEN
    -- Either gets the primary key or it gets the first unique index in alphabetical order by index name. 
    v_remote_key_sql := 'SELECT 
        CASE
            WHEN i.indisprimary IS true THEN ''primary''
            WHEN i.indisunique IS true THEN ''unique''
        END AS key_type,
        ( SELECT array_agg( a.attname ORDER by x.r ) 
            FROM pg_attribute a 
            JOIN ( SELECT k, row_number() over () as r 
                    FROM unnest(i.indkey) k ) as x 
            ON a.attnum = x.k AND a.attrelid = i.indrelid
            WHERE a.attnotnull
        ) AS indkey_names,
        ( SELECT array_agg( a.atttypid::regtype::text ORDER by x.r ) 
            FROM pg_attribute a 
            JOIN ( SELECT k, row_number() over () as r 
                    FROM unnest(i.indkey) k ) as x 
            ON a.attnum = x.k AND a.attrelid = i.indrelid
            WHERE a.attnotnull 
        ) AS indkey_types
        FROM pg_index i
        WHERE i.indrelid = '||quote_literal(p_src_table)||'::regclass
            AND (i.indisprimary OR i.indisunique)
        ORDER BY key_type LIMIT 1';

    EXECUTE 'SELECT key_type, indkey_names, indkey_types FROM dblink(''mimeo_dml'', '||quote_literal(v_remote_key_sql)||') t (key_type text, indkey_names text[], indkey_types text[])' 
        INTO v_key_type, v_pk_name, v_pk_type;
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

v_insert_refresh_config := 'INSERT INTO @extschema@.refresh_config_dml(source_table, dest_table, dblink, control, pk_name, pk_type, last_run, filter, condition) VALUES('
    ||quote_literal(p_src_table)||', '||quote_literal(p_dest_table)||', '|| p_dblink_id||', '||quote_literal('@extschema@.'||v_src_table_name||'_q')||', '
    ||quote_literal(v_pk_name)||', '||quote_literal(v_pk_type)||', '||quote_literal(CURRENT_TIMESTAMP)||','||COALESCE(quote_literal(p_filter), 'NULL')||','
    ||COALESCE(quote_literal(p_condition), 'NULL')||')';
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
 *  Logdel maker function.
 */
CREATE FUNCTION logdel_maker(
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

IF p_pk_name IS NULL AND p_pk_type IS NULL THEN
    -- Either gets the primary key or it gets the first unique index in alphabetical order by index name
    v_remote_key_sql := 'SELECT 
        CASE
            WHEN i.indisprimary IS true THEN ''primary''
            WHEN i.indisunique IS true THEN ''unique''
        END AS key_type,
        ( SELECT array_agg( a.attname ORDER by x.r ) 
            FROM pg_attribute a 
            JOIN ( SELECT k, row_number() over () as r 
                    FROM unnest(i.indkey) k ) as x 
            ON a.attnum = x.k AND a.attrelid = i.indrelid
            WHERE a.attnotnull
        ) AS indkey_names,
        ( SELECT array_agg( a.atttypid::regtype::text ORDER by x.r ) 
            FROM pg_attribute a 
            JOIN ( SELECT k, row_number() over () as r 
                    FROM unnest(i.indkey) k ) as x 
            ON a.attnum = x.k AND a.attrelid = i.indrelid
            WHERE a.attnotnull 
        ) AS indkey_types
        FROM pg_index i
        WHERE i.indrelid = '||quote_literal(p_src_table)||'::regclass
            AND (i.indisprimary OR i.indisunique)
        ORDER BY key_type LIMIT 1';

    EXECUTE 'SELECT key_type, indkey_names, indkey_types FROM dblink(''mimeo_logdel'', '||quote_literal(v_remote_key_sql)||') t (key_type text, indkey_names text[], indkey_types text[])' 
        INTO v_key_type, v_pk_name, v_pk_type;
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

v_insert_refresh_config := 'INSERT INTO @extschema@.refresh_config_logdel(source_table, dest_table, dblink, control, pk_name, pk_type, last_run, filter, condition) VALUES('
    ||quote_literal(p_src_table)||', '||quote_literal(p_dest_table)||', '|| p_dblink_id||', '||quote_literal('@extschema@.'||v_src_table_name||'_q')||', '
    ||quote_literal(v_pk_name)||', '||quote_literal(v_pk_type)||', '||quote_literal(CURRENT_TIMESTAMP)||','||COALESCE(quote_literal(p_filter), 'NULL')||','
    ||COALESCE(quote_literal(p_condition), 'NULL')||')';
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
 *  Inserter maker function. 
 */
CREATE FUNCTION inserter_maker(
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

v_dst_active := @extschema@.dst_utc_check();

v_insert_refresh_config := 'INSERT INTO @extschema@.refresh_config_inserter(source_table, dest_table, dblink, control, boundary, last_value, last_run, dst_active, filter, condition) VALUES('
    ||quote_literal(p_src_table)||','||quote_literal(p_dest_table)||','|| p_dblink_id||','||quote_literal(p_control_field)||','
    ||quote_literal(p_boundary)||',''0001-01-01''::date,'||quote_literal(CURRENT_TIMESTAMP)||','||v_dst_active||','
    ||COALESCE(quote_literal(p_filter), 'NULL')||','||COALESCE(quote_literal(p_condition), 'NULL')||');';

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
 *  Updater maker function.
 */ 
CREATE FUNCTION updater_maker(
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

IF p_pk_name IS NULL AND p_pk_type IS NULL THEN
    -- Either gets the primary key or it gets the first unique index in alphabetical order by index name
    v_remote_key_sql := 'SELECT 
        CASE
            WHEN i.indisprimary IS true THEN ''primary''
            WHEN i.indisunique IS true THEN ''unique''
        END AS key_type,
        ( SELECT array_agg( a.attname ORDER by x.r ) 
            FROM pg_attribute a 
            JOIN ( SELECT k, row_number() over () as r 
                    FROM unnest(i.indkey) k ) as x 
            ON a.attnum = x.k AND a.attrelid = i.indrelid
            WHERE a.attnotnull
        ) AS indkey_names,
        ( SELECT array_agg( a.atttypid::regtype::text ORDER by x.r ) 
            FROM pg_attribute a 
            JOIN ( SELECT k, row_number() over () as r 
                    FROM unnest(i.indkey) k ) as x 
            ON a.attnum = x.k AND a.attrelid = i.indrelid
            WHERE a.attnotnull 
        ) AS indkey_types
        FROM pg_index i
        WHERE i.indrelid = '||quote_literal(p_src_table)||'::regclass
            AND (i.indisprimary OR i.indisunique)
        ORDER BY key_type LIMIT 1';

    EXECUTE 'SELECT key_type, indkey_names, indkey_types FROM dblink(''mimeo_updater'', '||quote_literal(v_remote_key_sql)||') t (key_type text, indkey_names text[], indkey_types text[])' 
        INTO v_key_type, v_pk_name, v_pk_type;
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

v_insert_refresh_config := 'INSERT INTO @extschema@.refresh_config_updater(source_table, dest_table, dblink, control, boundary, pk_name, pk_type, last_value, last_run, dst_active, filter, condition) VALUES('
    ||quote_literal(p_src_table)||', '||quote_literal(p_dest_table)||', '|| p_dblink_id||', '||quote_literal(p_control_field)||', '''
    ||p_boundary||'''::interval, '||quote_literal(v_pk_name)||', '||quote_literal(v_pk_type)||', ''0001-01-01''::date,'||quote_literal(CURRENT_TIMESTAMP)||','
    ||v_dst_active||','||COALESCE(quote_literal(p_filter), 'NULL')||','||COALESCE(quote_literal(p_condition), 'NULL')||')';
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
 *  Plain table refresh maker function. 
 */
CREATE FUNCTION table_maker(
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

v_insert_refresh_config := 'INSERT INTO @extschema@.refresh_config_table(source_table, dest_table, dblink, last_run, filter, condition, sequences) VALUES('
    ||quote_literal(p_src_table)||','||quote_literal(p_dest_table)||','|| p_dblink_id||','
    ||quote_literal(CURRENT_TIMESTAMP)||','||COALESCE(quote_literal(p_filter), 'NULL')||','||COALESCE(quote_literal(p_condition), 'NULL')||','||COALESCE(quote_literal(p_sequences), 'NULL')||');';
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

-- Restore original privileges to objects that were dropped
SELECT @extschema@.replay_preserved_privs();
DROP FUNCTION @extschema@.replay_preserved_privs();
DROP TABLE mimeo_preserve_privs_temp;
