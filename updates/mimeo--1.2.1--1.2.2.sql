-- New function "check_missing_source_tables()" can show tables that exist on the configured data sources that are not configured for replication.
    -- Provides monitoring capability for situations where all tables on source should be replicated.
    -- Optional parameter to check one specific data source. Otherwise, all sources listed in dblink_mapping_mimeo table are checked.
    -- Returns a record value so WHERE conditions can be used to ignore tables that aren't desired.
-- New function "check_source_columns()" can show columns that exist on source tables that do not exist on the destination
    -- Provides monitoring capability for data source tables to see if columns have been added or types changed.
    -- Does not check if destination has columns that source does not (therefore does not check if columns were dropped on source but not on destination).
    -- Accounts for when the "filter" configuration option is used to only grab specific columns.
    -- Optional parameter to check one specific data source. Otherwise, all sources listed in dblink_mapping_mimeo table are checked.
    -- Returns a record value so WHERE conditions can be used to ignore tables and/or columns that don't matter for your situation.
-- Fix bug during index creation when dblink is not installed in a schema called "dblink" (Github Issue #6).
-- Added note to documentation about how to add/remove columns with DML replication and avoid errors.
-- Added pg_tap tests for new monitoring functions & snapshot column change replication.

/*
 * Check data sources to see what tables exist there that are not set up for mimeo replication
 * Provides monitoring capability for situations where all tables on source should be replicated.
 * Returns a record value so that a WHERE condition can be used to ignore tables that aren't desired.
 * If p_data_source_id value is not given, all configured sources are checked.
 */
CREATE FUNCTION check_missing_source_tables(p_data_source_id int DEFAULT NULL, OUT schemaname text, OUT tablename text, OUT data_source int) RETURNS SETOF record
    LANGUAGE plpgsql SECURITY DEFINER
    AS $$
DECLARE

v_dblink_schema     text;
v_exists            int;
v_row_dblink        record;
v_row_missing       record;

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
    EXECUTE 'SELECT '||v_dblink_schema||'.dblink_connect(''mimeo_missing'', @extschema@.auth('||v_row_dblink.data_source_id||'))';

    CREATE TEMP TABLE current_source_tables_tmp (schemaname text, tablename text);
    EXECUTE 'INSERT INTO current_source_tables_tmp SELECT schemaname, tablename FROM '||v_dblink_schema||'.dblink(''mimeo_missing'', 
            ''SELECT schemaname, tablename AS tablename FROM pg_catalog.pg_tables WHERE schemaname NOT IN (''''pg_catalog'''', ''''information_schema'''', ''''@extschema@'''')'') t (schemaname text, tablename text)';

    CREATE TEMP TABLE current_dest_tables_tmp AS
    SELECT source_table FROM @extschema@.refresh_config_snap WHERE dblink = v_row_dblink.data_source_id
    UNION
    SELECT source_table FROM @extschema@.refresh_config_inserter WHERE dblink = v_row_dblink.data_source_id
    UNION
    SELECT source_table FROM @extschema@.refresh_config_updater WHERE dblink = v_row_dblink.data_source_id
    UNION
    SELECT source_table FROM @extschema@.refresh_config_dml WHERE dblink = v_row_dblink.data_source_id
    UNION
    SELECT source_table FROM @extschema@.refresh_config_logdel WHERE dblink = v_row_dblink.data_source_id
    UNION
    SELECT source_table FROM @extschema@.refresh_config_table WHERE dblink = v_row_dblink.data_source_id;

    FOR v_row_missing IN 
        SELECT s.schemaname, s.tablename 
        FROM current_source_tables_tmp s
        LEFT OUTER JOIN current_dest_tables_tmp d ON s.schemaname||'.'||s.tablename = d.source_table
        WHERE d.source_table IS NULL
        ORDER BY s.schemaname, s.tablename
    LOOP
        schemaname := v_row_missing.schemaname;
        tablename := v_row_missing.tablename;
        data_source = v_row_dblink.data_source_id;
        RETURN NEXT;
    END LOOP;

    EXECUTE 'SELECT '||v_dblink_schema||'.dblink_disconnect(''mimeo_missing'')';

    DROP TABLE IF EXISTS current_source_tables_tmp;
    DROP TABLE IF EXISTS current_dest_tables_tmp;

END LOOP;

END
$$;


/*
 * Check tables on data sources to see if columns have been added or types changed.
 * Returns a record value so that a WHERE condition can be used to ignore tables that aren't desired.
 * If p_data_source_id value is not given, all configured sources are checked.
 */
CREATE FUNCTION check_source_columns(p_data_source_id int DEFAULT NULL
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
        FOR v_row_col IN
            EXECUTE 'SELECT attname, atttypid, atttypmod, schemaname, tablename FROM '||v_dblink_schema||'.dblink(''mimeo_col_check'', 
                ''SELECT a.attname::text, a.atttypid, a.atttypmod, n.nspname AS schemaname, c.relname AS tablename
                FROM pg_attribute a
                JOIN pg_class c ON c.oid = a.attrelid
                JOIN pg_namespace n ON c.relnamespace = n.oid
                WHERE a.attrelid = '''''||v_row_table.source_table||'''''::regclass 
                AND a.attnum > 0
                AND attisdropped = false
                ORDER BY 1,2'') t (attname text, atttypid oid, atttypmod int, schemaname text, tablename text)'
        LOOP
            IF v_row_col.attname <> ANY (v_row_table.filter) THEN
                CONTINUE;
            END IF;
            SELECT count(*) INTO v_exists
            FROM pg_attribute
            WHERE attrelid = v_row_table.dest_table::regclass
            AND attnum > 0
            AND attisdropped = false
            AND attname = v_row_col.attname
            AND atttypid = v_row_col.atttypid
            AND atttypmod = v_row_col.atttypmod;

            -- if column doesn't exist, means it's missing on destination.
            IF v_exists < 1 THEN
                IF v_row_table.type = 'snap' THEN
                    SELECT schemaname, viewname INTO dest_schemaname, dest_tablename FROM pg_catalog.pg_views WHERE schemaname||'.'||viewname = v_row_table.dest_table;
                ELSE
                    SELECT schemaname, tablename INTO dest_schemaname, dest_tablename FROM pg_catalog.pg_tables WHERE schemaname||'.'||tablename = v_row_table.dest_table;
                END IF;
                src_schemaname := v_row_col.schemaname;
                src_tablename := v_row_col.tablename;
                missing_column_name := v_row_col.attname;
                missing_column_type := format_type(v_row_col.atttypid, v_row_col.atttypmod)::text;
                data_source := v_row_dblink.data_source_id;
                RETURN NEXT;
            END IF;

        END LOOP; -- end v_row_col

    END LOOP; -- end v_row_table

    EXECUTE 'SELECT '||v_dblink_schema||'.dblink_disconnect(''mimeo_col_check'')';

END LOOP; -- end v_row_dblink

END
$$;


/*
 * Create index(es) on destination table
 */
CREATE OR REPLACE FUNCTION create_index(p_destination text, p_snap text DEFAULT NULL, p_debug boolean DEFAULT false) RETURNS void
    LANGUAGE plpgsql SECURITY DEFINER
    AS $$
DECLARE

v_conf              text;
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
v_src_table_name    text;
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

v_dest_table_name := split_part(v_dest_table, '.', 2);
SELECT tablename INTO v_src_table_name 
    FROM dblink(v_dblink_name, 'SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname ||''.''|| tablename = '||quote_literal(v_source_table)) t (tablename text);

-- Gets primary key or unique index used by updater/dml/logdel replication (same function is called in their makers). 
-- Should only loop once, but just easier to keep code consistent with below method
FOR v_row IN SELECT indexrelid, key_type, indkey_names, statement FROM fetch_replication_key(v_source_table, v_dblink_name)
LOOP

    EXIT WHEN v_row.indexrelid IS NULL; -- function still returns a row full of nulls when nothing found

    IF v_row.key_type = 'primary' THEN
        v_statement := 'ALTER TABLE '||v_dest_table || COALESCE('_'||p_snap, '')||' ADD CONSTRAINT '||
            COALESCE(p_snap||'_', '')|| v_dest_table_name ||'_'||array_to_string(v_row.indkey_names, '_')||'_pk 
            PRIMARY KEY ('||array_to_string(v_row.indkey_names, ',')||')';
    ELSIF v_row.key_type = 'unique' THEN
        v_statement := v_row.statement;
        -- Replace source table name with destination
        v_statement := replace(v_statement, ' ON '||v_source_table, ' ON '||v_dest_table || COALESCE('_'||p_snap, ''));
        -- If source index name contains the table name, replace it with the destination table.
        v_statement := regexp_replace(v_statement, '(INDEX \w*)'||v_src_table_name||'(\w* ON)', '\1'||v_dest_table_name||'\2');
        -- If it's a snap table, prepend to ensure unique index name. 
        -- This is done separately from above replace because it must always be done even if the index name doesn't contain the source table
        IF p_snap IS NOT NULL THEN
            v_statement := replace(v_statement, 'UNIQUE INDEX ' , 'UNIQUE INDEX '||p_snap||'_');
        END IF;
    END IF;
    PERFORM gdb(p_debug, 'statement: ' || v_statement);
    EXECUTE v_statement;
    v_repl_index = v_row.indexrelid;
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
        -- If source index name contains the table name, replace it with the destination table.
        v_statement := regexp_replace(v_statement, '(INDEX \w*)'||v_src_table_name||'(\w* ON)', '\1'||v_dest_table_name||'\2');
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


