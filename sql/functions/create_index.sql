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
SELECT nspname INTO v_dblink_schema FROM pg_catalog.pg_namespace n, pg_catalog.pg_extension e WHERE e.extname = 'dblink' AND e.extnamespace = n.oid;
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
        SELECT nspname INTO v_dblink_schema FROM pg_catalog.pg_namespace n, pg_catalog.pg_extension e WHERE e.extname = 'dblink' AND e.extnamespace = n.oid;
        EXECUTE format('SELECT %I.dblink_get_connections() @> ARRAY[%L]', v_dblink_schema, v_dblink_name) INTO v_link_exists;
        IF v_link_exists THEN
            EXECUTE format('SELECT %I.dblink_disconnect(%L)', v_dblink_schema, v_dblink_name);
        END IF;
        RAISE EXCEPTION '%', SQLERRM; 
END
$$;

