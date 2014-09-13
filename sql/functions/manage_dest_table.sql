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


