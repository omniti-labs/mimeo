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

v_dblink_name := @extschema@.check_name_length('manage_dest_table_dblink_'||p_destination);

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


