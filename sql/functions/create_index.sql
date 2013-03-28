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

