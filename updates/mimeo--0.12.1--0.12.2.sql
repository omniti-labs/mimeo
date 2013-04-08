-- Separated process that determines source primary/unique key to use for replication into its own function. This was done to make some future work easier and simplify code.
-- Maker functions that require primary/unique key (updater/dml/logdel) redone to use this new function.

/*
 *  Fetches either the primary key or a valid, not-null unique index. Primary key is always preferred over unique key. 
 */
CREATE FUNCTION fetch_replication_key(p_src_table text, p_dblink_name text, OUT indkey_names text[], OUT indkey_types text[], OUT key_type text, OUT indexrelid oid, OUT statement text) RETURNS record
    LANGUAGE plpgsql
    AS $$
DECLARE

v_dblink_schema         text;
v_remote_sql            text;

BEGIN

IF p_src_table IS NULL OR p_dblink_name IS NULL THEN
    RAISE EXCEPTION 'p_src_table and p_dblink_name parameters cannot be null in fetch_replication_key() call';
END IF;

SELECT nspname INTO v_dblink_schema FROM pg_namespace n, pg_extension e WHERE e.extname = 'dblink' AND e.extnamespace = n.oid;

v_remote_sql := 'SELECT indexrelid,
    pg_get_indexdef(indexrelid) AS statement,
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
        AND i.indisvalid
    ORDER BY key_type LIMIT 1';

EXECUTE 'SELECT indexrelid, statement, key_type, indkey_names, indkey_types FROM '||v_dblink_schema||'.dblink('||quote_literal(p_dblink_name)||','||quote_literal(v_remote_sql)||') t (indexrelid oid, statement text, key_type text, indkey_names text[], indkey_types text[])' INTO indexrelid, statement, key_type, indkey_names, indkey_types;

END
$$;


/*
 * Create index(es) on destination table
 */
CREATE OR REPLACE FUNCTION create_index(p_destination text, p_snap text DEFAULT NULL, p_debug boolean DEFAULT false) RETURNS void
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
v_src_table_name    text;
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
v_src_table_name := split_part(v_source_table, '.', 2);

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
        -- If source index name contains the table name, replace it with the destination table. Not perfect, but good enough for now.
        v_statement := replace(v_statement, v_src_table_name, v_dest_table_name);
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

-- Automatically get source primary/unique key if none given
IF p_pk_name IS NULL AND p_pk_type IS NULL THEN
    SELECT v_key_type, indkey_names, indkey_types INTO v_key_type, v_pk_name, v_pk_type FROM fetch_replication_key(p_src_table, 'mimeo_dml');
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

-- Automatically get source primary/unique key if none given
IF p_pk_name IS NULL AND p_pk_type IS NULL THEN
   SELECT v_key_type, indkey_names, indkey_types INTO v_key_type, v_pk_name, v_pk_type FROM fetch_replication_key(p_src_table, 'mimeo_logdel');
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
 *  Updater maker function.
 */ 
CREATE OR REPLACE FUNCTION updater_maker(
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

-- Automatically get source primary/unique key if none given
IF p_pk_name IS NULL AND p_pk_type IS NULL THEN
    SELECT v_key_type, indkey_names, indkey_types INTO v_key_type, v_pk_name, v_pk_type FROM fetch_replication_key(p_src_table, 'mimeo_updater');
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
