-- The update from 0.9.3 to 0.10.0 does not work for PostgreSQL versions less than 9.2 due to using GET STACKED DIAGNOSTIC. The direct upgrade script from 0.9.3 to 0.10.1 should provide a working by-pass.
-- Removed call to GET STACKED DIAGNOSTIC since it's only compatible with 9.2
-- Set permissions for queue table & trigger function on source. Will set properly for any roles that have permissions on the source table at the time the maker function is run.
-- Changed queue table for dml replication on source to have "_q" suffix instead of "_pgq". Does not change existing queue table names.
-- Also truncates the queue table name if it is longer than 61 characters to avoid max table length issues.
-- Added pgTAP tests for empty source table.


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
    , p_pk_type text[] DEFAULT NULL) 
RETURNS void
    LANGUAGE plpgsql
    AS $$
DECLARE

v_create_trig               text;
v_data_source               text;
v_dblink_schema             text;
v_dest_check                text;
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

RAISE NOTICE 'v_key_type: %', v_key_type;
RAISE NOTICE 'v_pk_name: %', v_pk_name;
RAISE NOTICE 'v_pk_type: %', v_pk_type;

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

RAISE NOTICE 'v_remote_q_table: %', v_remote_q_table;

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

PERFORM dblink_exec('mimeo_dml', v_remote_q_table);
PERFORM dblink_exec('mimeo_dml', v_remote_q_index);
PERFORM dblink_exec('mimeo_dml', v_trigger_func);
-- Grant any current role with write privileges on source table INSERT on the queue table before the trigger is actually created
v_remote_grants_sql := 'SELECT DISTINCT grantee FROM information_schema.table_privileges WHERE table_schema ||''.''|| table_name = '||quote_literal(p_dest_table)||' and privilege_type IN (''INSERT'',''UPDATE'',''DELETE'')';
FOR v_row IN SELECT grantee FROM dblink('mimeo_dml', v_remote_grants_sql) t (grantee text)
LOOP
    PERFORM dblink_exec('mimeo_dml', 'GRANT USAGE ON SCHEMA @extschema@ TO '||v_row.grantee);
    PERFORM dblink_exec('mimeo_dml', 'GRANT INSERT ON TABLE @extschema@.'||v_src_table_name||'_q TO '||v_row.grantee);
    PERFORM dblink_exec('mimeo_dml', 'GRANT EXECUTE ON FUNCTION @extschema@.'||v_src_table_name||'_mimeo_queue() TO '||v_row.grantee);
END LOOP;
PERFORM dblink_exec('mimeo_dml', v_create_trig);

-- Only create destination table if it doesn't already exist
SELECT schemaname||'.'||tablename INTO v_dest_check FROM pg_tables WHERE schemaname = v_dest_schema_name AND tablename = v_dest_table_name;
IF v_dest_check IS NULL THEN
    RAISE NOTICE 'Snapshotting source table to pull all current source data...';
    -- Snapshot the table after triggers have been created to ensure all new data after setup is replicated
    v_insert_refresh_config := 'INSERT INTO @extschema@.refresh_config_snap(source_table, dest_table, dblink, filter, condition) VALUES('
        ||quote_literal(p_src_table)||', '||quote_literal(p_dest_table)||', '|| p_dblink_id||','
        ||COALESCE(quote_literal(p_filter), 'NULL')||','||COALESCE(quote_literal(p_condition), 'NULL')||')';
    EXECUTE v_insert_refresh_config;

    EXECUTE 'SELECT @extschema@.refresh_snap('||quote_literal(p_dest_table)||', p_index := '||p_index||', p_pulldata := '||p_pulldata||')';
    PERFORM @extschema@.snapshot_destroyer(p_dest_table, 'ARCHIVE');
    -- Ensure destination indexes that are needed for efficient replication are created even if p_index is set false
    IF p_index = false THEN
        RAISE NOTICE 'Adding primary/unique key to table...';
        IF v_key_type = 'primary' THEN
            EXECUTE 'ALTER TABLE '||p_dest_table||' ADD PRIMARY KEY('||array_to_string(v_pk_name, ',')||')';
        ELSE
            EXECUTE 'CREATE UNIQUE INDEX ON '||p_dest_table||' ('||array_to_string(v_pk_name, ',')||')';
        END IF;    
    END IF;
ELSE
    RAISE NOTICE 'Destination table % already exists. No data or indexes were pulled from source', p_dest_table;
END IF;

v_insert_refresh_config := 'INSERT INTO @extschema@.refresh_config_dml(source_table, dest_table, dblink, control, pk_name, pk_type, last_run, filter, condition) VALUES('
    ||quote_literal(p_src_table)||', '||quote_literal(p_dest_table)||', '|| p_dblink_id||', '||quote_literal('@extschema@.'||v_src_table_name||'_q')||', '
    ||quote_literal(v_pk_name)||', '||quote_literal(v_pk_type)||', '||quote_literal(CURRENT_TIMESTAMP)||','||COALESCE(quote_literal(p_filter), 'NULL')||','
    ||COALESCE(quote_literal(p_condition), 'NULL')||')';
RAISE NOTICE 'Inserting data into config table';
EXECUTE v_insert_refresh_config;

PERFORM dblink_disconnect('mimeo_dml');

EXECUTE 'SELECT set_config(''search_path'','''||v_old_search_path||''',''false'')';

RAISE NOTICE 'Done';

EXCEPTION
    WHEN OTHERS THEN
        EXECUTE 'SELECT set_config(''search_path'',''@extschema@,'||v_dblink_schema||''',''false'')';
        -- Only cleanup remote objects if replication doesn't exist at all for source table
        EXECUTE 'SELECT count(*) FROM @extschema@.refresh_config_dml WHERE source_table = '||quote_literal(p_src_table) INTO v_exists;
        IF v_exists = 0 THEN
            PERFORM dblink_exec('mimeo_dml', 'DROP TABLE IF EXISTS @extschema@.'||v_src_table_name||'_q');
            PERFORM dblink_exec('mimeo_dml', 'DROP TRIGGER IF EXISTS '||v_src_table_name||'_mimeo_trig ON '||p_src_table);
            PERFORM dblink_exec('mimeo_dml', 'DROP FUNCTION IF EXISTS @extschema@.'||v_src_table_name||'_mimeo_queue()');
        END IF;
        IF dblink_get_connections() @> '{mimeo_dml}' THEN
            PERFORM dblink_disconnect('mimeo_dml');
        END IF;
        EXECUTE 'SELECT set_config(''search_path'','''||v_old_search_path||''',''false'')';
        IF v_exists = 0 THEN
            RAISE EXCEPTION 'dml_maker() failure. No mimeo configuration found for source %. Cleaned up source table mimeo objects (queue table, function & trigger) if they existed.  SQLERRM: %', p_src_table, SQLERRM;
        ELSE
            RAISE EXCEPTION 'dml_maker() failure. Check to see if dml configuration for % already exists. SQLERRM: % ', p_src_table, SQLERRM;
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
    , p_pk_type text[] DEFAULT NULL) 
RETURNS void
    LANGUAGE plpgsql
    AS $$
DECLARE

v_col_exists                int;
v_cols                      text[];
v_cols_n_types              text[];
v_cols_n_types_csv          text;
v_counter                   int := 1;
v_create_trig               text;
v_data_source               text;
v_dblink_schema             text;
v_dest_check                text;
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

v_remote_sql := 'SELECT array_agg(attname) as cols, array_agg(format_type(atttypid, atttypmod)::text) as types, array_agg(attname||'' ''||format_type(atttypid, atttypmod)::text) as cols_n_types FROM pg_attribute WHERE attrelid = ' || quote_literal(p_src_table) || '::regclass AND attnum > 0 AND attisdropped is false';
-- Apply column filters if used
IF p_filter IS NOT NULL THEN
    v_remote_sql := v_remote_sql || ' AND ARRAY[attname::text] <@ '||quote_literal(p_filter);
END IF;
v_remote_sql := 'SELECT cols, types, cols_n_types FROM dblink(''mimeo_logdel'', ' || quote_literal(v_remote_sql) || ') t (cols text[], types text[], cols_n_types text[])';
EXECUTE v_remote_sql INTO v_cols, v_types, v_cols_n_types;

v_cols_n_types_csv := array_to_string(v_cols_n_types, ',');

v_remote_q_table := 'CREATE TABLE @extschema@.'||v_src_table_name||'_q ('||v_cols_n_types_csv||', mimeo_source_deleted timestamptz, processed boolean)';

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

v_remote_q_index := 'CREATE INDEX '||v_src_table_name||'_q_'||array_to_string(v_pk_name, '_')||'_idx ON @extschema@.'||v_src_table_name||'_q ('||array_to_string(v_pk_name, ',')||')';

v_counter := 1;
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
PERFORM dblink_exec('mimeo_logdel', v_remote_q_table);
PERFORM dblink_exec('mimeo_logdel', v_remote_q_index);
PERFORM dblink_exec('mimeo_logdel', v_trigger_func);
-- Grant any current role with write privileges on source table INSERT on the queue table before the trigger is actually created
v_remote_grants_sql := 'SELECT DISTINCT grantee FROM information_schema.table_privileges WHERE table_schema ||''.''|| table_name = '||quote_literal(p_dest_table)||' and privilege_type IN (''INSERT'',''UPDATE'',''DELETE'')';
FOR v_row IN SELECT grantee FROM dblink('mimeo_logdel', v_remote_grants_sql) t (grantee text)
LOOP
    PERFORM dblink_exec('mimeo_logdel', 'GRANT USAGE ON SCHEMA @extschema@ TO '||v_row.grantee);
    PERFORM dblink_exec('mimeo_logdel', 'GRANT INSERT ON TABLE @extschema@.'||v_src_table_name||'_q TO '||v_row.grantee);
    PERFORM dblink_exec('mimeo_logdel', 'GRANT EXECUTE ON FUNCTION @extschema@.'||v_src_table_name||'_mimeo_queue() TO '||v_row.grantee);
END LOOP;
PERFORM dblink_exec('mimeo_logdel', v_create_trig);

-- Only create destination table if it doesn't already exist
SELECT schemaname||'.'||tablename INTO v_dest_check FROM pg_tables WHERE schemaname = v_dest_schema_name AND tablename = v_dest_table_name;
IF v_dest_check IS NULL THEN
    RAISE NOTICE 'Snapshotting source table to pull all current source data...';
    -- Snapshot the table after triggers have been created to ensure all new data after setup is replicated
    v_insert_refresh_config := 'INSERT INTO @extschema@.refresh_config_snap(source_table, dest_table, dblink, filter, condition) VALUES('
        ||quote_literal(p_src_table)||', '||quote_literal(p_dest_table)||', '|| p_dblink_id||','
        ||COALESCE(quote_literal(p_filter), 'NULL')||','||COALESCE(quote_literal(p_condition), 'NULL')||')';
    EXECUTE v_insert_refresh_config;

    EXECUTE 'SELECT @extschema@.refresh_snap('||quote_literal(p_dest_table)||', p_index := '||p_index||', p_pulldata := '||p_pulldata||')';
    PERFORM @extschema@.snapshot_destroyer(p_dest_table, 'ARCHIVE');
    -- Ensure destination indexes that are needed for efficient replication are created even if p_index is set false
    IF p_index = false THEN
        RAISE NOTICE 'Adding primary/unique key to table...';
        IF v_key_type = 'primary' THEN
            EXECUTE 'ALTER TABLE '||p_dest_table||' ADD PRIMARY KEY('||array_to_string(v_pk_name, ',')||')';
        ELSE
            EXECUTE 'CREATE UNIQUE INDEX ON '||p_dest_table||' ('||array_to_string(v_pk_name, ',')||')';
        END IF;    
    END IF;
ELSE
    RAISE NOTICE 'Destination table % already exists. No data or indexes were pulled from source', p_dest_table;
END IF;

SELECT count(*) INTO v_col_exists FROM pg_attribute 
    WHERE attrelid = p_dest_table::regclass AND attname = 'mimeo_source_deleted' AND attisdropped = false;
IF v_col_exists < 1 THEN
    EXECUTE 'ALTER TABLE '||p_dest_table||' ADD COLUMN mimeo_source_deleted timestamptz';
ELSE
    RAISE WARNING 'Special column (mimeo_source_deleted) already exists on destination table (%)', p_dest_table;
END IF;

PERFORM dblink_disconnect('mimeo_logdel');

v_insert_refresh_config := 'INSERT INTO @extschema@.refresh_config_logdel(source_table, dest_table, dblink, control, pk_name, pk_type, last_run, filter, condition) VALUES('
    ||quote_literal(p_src_table)||', '||quote_literal(p_dest_table)||', '|| p_dblink_id||', '||quote_literal('@extschema@.'||v_src_table_name||'_q')||', '
    ||quote_literal(v_pk_name)||', '||quote_literal(v_pk_type)||', '||quote_literal(CURRENT_TIMESTAMP)||','||COALESCE(quote_literal(p_filter), 'NULL')||','
    ||COALESCE(quote_literal(p_condition), 'NULL')||')';
RAISE NOTICE 'Inserting data into config table';

EXECUTE v_insert_refresh_config;

EXECUTE 'SELECT set_config(''search_path'','''||v_old_search_path||''',''false'')';

RAISE NOTICE 'Done';

EXCEPTION
    WHEN OTHERS THEN
        EXECUTE 'SELECT set_config(''search_path'',''@extschema@,'||v_dblink_schema||''',''false'')';
        -- Only cleanup remote objects if replication doesn't exist at all for source table
        EXECUTE 'SELECT count(*) FROM @extschema@.refresh_config_logdel WHERE source_table = '||quote_literal(p_src_table) INTO v_exists;
        IF v_exists = 0 THEN
            PERFORM dblink_exec('mimeo_logdel', 'DROP TABLE IF EXISTS @extschema@.'||v_src_table_name||'_q');
            PERFORM dblink_exec('mimeo_logdel', 'DROP TRIGGER IF EXISTS '||v_src_table_name||'_mimeo_trig ON '||p_src_table);
            PERFORM dblink_exec('mimeo_logdel', 'DROP FUNCTION IF EXISTS @extschema@.'||v_src_table_name||'_mimeo_queue()');
        END IF;
        IF dblink_get_connections() @> '{mimeo_logdel}' THEN
            PERFORM dblink_disconnect('mimeo_logdel');
        END IF;
        EXECUTE 'SELECT set_config(''search_path'','''||v_old_search_path||''',''false'')';
        IF v_exists = 0 THEN
            RAISE EXCEPTION 'logdel_maker() failure. No mimeo configuration found for source %. Cleaned up source table mimeo objects (queue table, function & trigger) if they existed.  SQLERRM: %', p_src_table, SQLERRM;
        ELSE
            RAISE EXCEPTION 'logdel_maker() failure. Check to see if logdel configuration for % already exists. SQLERRM: % ', p_src_table, SQLERRM;
        END IF;
END
$$;


/*
 *  Refresh based on DML (Insert, Update, Delete)
 */
CREATE OR REPLACE FUNCTION refresh_dml(p_destination text, p_limit int default NULL, p_repull boolean DEFAULT false, p_debug boolean DEFAULT false) RETURNS void
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
v_dest_table            text;
v_exec_status           text;
v_fetch_sql             text;
v_field                 text;
v_filter                text[];
v_insert_sql            text;
v_job_id                int;
v_jobmon_schema         text;
v_job_name              text;
v_limit                 int; 
v_old_search_path       text;
v_pk_counter            int;
v_pk_name_csv           text;
v_pk_name_type_csv      text := '';
v_pk_name               text[];
v_pk_type               text[];
v_pk_where              text := '';
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

v_job_name := 'Refresh DML: '||p_destination;
v_dblink_name := 'mimeo_dml_refresh_'||p_destination;

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
FROM refresh_config_dml 
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
v_adv_lock := pg_try_advisory_lock(hashtext('refresh_dml'), hashtext(v_job_name));
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
    RAISE EXCEPTION 'ERROR: primary key fields in refresh_config_dml must be defined';
END IF;

-- determine column list, column type list
IF v_filter IS NULL THEN 
    SELECT array_to_string(array_agg(attname),','), array_to_string(array_agg(attname||' '||format_type(atttypid, atttypmod)::text),',') 
        INTO v_cols, v_cols_n_types
        FROM pg_attribute WHERE attrelid = p_destination::regclass AND attnum > 0 AND attisdropped is false;
ELSE
    -- ensure all primary key columns are included in any column filters
    FOREACH v_field IN ARRAY v_pk_name LOOP
        IF v_field = ANY(v_filter) THEN
            CONTINUE;
        ELSE
            RAISE EXCEPTION 'ERROR: filter list did not contain all columns that compose primary/unique key for %',v_job_name; 
        END IF;
    END LOOP;
    SELECT array_to_string(array_agg(attname),','), array_to_string(array_agg(attname||' '||format_type(atttypid, atttypmod)::text),',') 
        INTO v_cols, v_cols_n_types
        FROM pg_attribute WHERE attrelid = p_destination::regclass AND ARRAY[attname::text] <@ v_filter AND attnum > 0 AND attisdropped is false;
END IF;    

IF p_limit IS NOT NULL THEN
    v_limit := p_limit;
END IF;

v_pk_name_csv := array_to_string(v_pk_name, ',');
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
v_with_update := 'WITH a AS (SELECT '||v_pk_name_csv||' FROM '|| v_control ||' ORDER BY '||v_pk_name_csv||' LIMIT '|| COALESCE(v_limit::text, 'ALL') ||') UPDATE '||v_control||' b SET processed = true FROM a WHERE '||v_pk_where;
v_trigger_update := 'SELECT dblink_exec('||quote_literal(v_dblink_name)||','|| quote_literal(v_with_update)||')';
PERFORM gdb(p_debug,v_trigger_update);
EXECUTE v_trigger_update INTO v_exec_status;    
PERFORM update_step(v_step_id, 'OK','Result was '||v_exec_status);

IF p_repull THEN
    -- Do truncate of remote queue table here before full data pull is actually started to ensure all new changes are recorded
    PERFORM update_step(v_step_id, 'OK','Request to repull ALL data from source. This could take a while...');
    PERFORM gdb(p_debug, 'Request to repull ALL data from source. This could take a while...');
    v_truncate_remote_q := 'SELECT dblink_exec('||quote_literal(v_dblink_name)||','||quote_literal('TRUNCATE TABLE '||v_control)||')';
    EXECUTE v_truncate_remote_q;

    v_step_id := add_step(v_job_id,'Truncating local table');
    PERFORM gdb(p_debug,'Truncating local table');
    EXECUTE 'TRUNCATE '||v_dest_table;
    PERFORM update_step(v_step_id, 'OK','Done');
    -- Define cursor query
    v_remote_f_sql := 'SELECT '||v_cols||' FROM '||v_source_table;
    IF v_condition IS NOT NULL THEN
        v_remote_f_sql := v_remote_f_sql || ' ' || v_condition;
    END IF;
ELSE
    EXECUTE 'CREATE TEMP TABLE '||v_tmp_table||'_queue ('||v_pk_name_type_csv||', PRIMARY KEY ('||v_pk_name_csv||'))';
    -- Copy queue locally for use in removing updated/deleted rows
    v_remote_q_sql := 'SELECT DISTINCT '||v_pk_name_csv||' FROM '||v_control||' WHERE processed = true';
    PERFORM dblink_open(v_dblink_name, 'mimeo_cursor', v_remote_q_sql);
    v_step_id := add_step(v_job_id, 'Creating local queue temp table');
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
    PERFORM gdb(p_debug,'Temp queue table row count '||v_total::text);

    v_step_id := add_step(v_job_id,'Deleting records from local table');
    v_delete_sql := 'DELETE FROM '||v_dest_table||' a USING '||v_tmp_table||'_queue b WHERE '|| v_pk_where; 
    PERFORM gdb(p_debug,v_delete_sql);
    EXECUTE v_delete_sql; 
    GET DIAGNOSTICS v_rowcount = ROW_COUNT;
    PERFORM gdb(p_debug,'Rows removed from local table before applying changes: '||v_rowcount::text);
    PERFORM update_step(v_step_id, 'OK','Removed '||v_rowcount||' records');
    -- Define cursor query
    v_remote_f_sql := 'SELECT '||v_cols||' FROM '||v_source_table||' JOIN ('||v_remote_q_sql||') x USING ('||v_pk_name_csv||')';
    IF v_condition IS NOT NULL THEN
        v_remote_f_sql := v_remote_f_sql || ' ' || v_condition;
    END IF;
END IF;

-- insert records to local table. Have to do temp table in case destination table is partitioned (returns 0 when inserting to parent)
PERFORM dblink_open(v_dblink_name, 'mimeo_cursor', v_remote_f_sql);
v_step_id := add_step(v_job_id, 'Inserting new records into local table');
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
PERFORM update_step(v_step_id, 'OK','Number of rows inserted: '||v_total);

IF p_repull = false AND v_total > (v_limit * .75) THEN
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
UPDATE refresh_config_dml SET last_run = CURRENT_TIMESTAMP WHERE dest_table = p_destination; 
PERFORM update_step(v_step_id, 'OK','Last run was '||CURRENT_TIMESTAMP);

EXECUTE 'DROP TABLE IF EXISTS '||v_tmp_table||'_full';
EXECUTE 'DROP TABLE IF EXISTS '||v_tmp_table||'_queue';

PERFORM dblink_disconnect(v_dblink_name);

IF v_batch_limit_reached = false THEN
    PERFORM close_job(v_job_id);
ELSE
    -- Set final job status to level 2 (WARNING) to bring notice that the batch limit was reached and may need adjusting.
    -- Preventive warning to keep replication from falling behind.
    PERFORM fail_job(v_job_id, 2);
END IF;

-- Ensure old search path is reset for the current session
EXECUTE 'SELECT set_config(''search_path'','''||v_old_search_path||''',''false'')';

PERFORM pg_advisory_unlock(hashtext('refresh_dml'), hashtext(v_job_name));

EXCEPTION
    WHEN QUERY_CANCELED THEN
        EXECUTE 'SELECT set_config(''search_path'',''@extschema@,'||v_jobmon_schema||','||v_dblink_schema||''',''false'')';
        IF dblink_get_connections() @> ARRAY[v_dblink_name] THEN
            PERFORM dblink_disconnect(v_dblink_name);
        END IF;
        EXECUTE 'SELECT set_config(''search_path'','''||v_old_search_path||''',''false'')';
        PERFORM pg_advisory_unlock(hashtext('refresh_dml'), hashtext(v_job_name));
        RAISE EXCEPTION '%', SQLERRM;  
    WHEN OTHERS THEN
        EXECUTE 'SELECT set_config(''search_path'',''@extschema@,'||v_jobmon_schema||','||v_dblink_schema||''',''false'')';
        IF v_job_id IS NULL THEN
                v_job_id := add_job('Refresh DML: '||p_destination);
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

        PERFORM pg_advisory_unlock(hashtext('refresh_dml'), hashtext(v_job_name));
        RAISE EXCEPTION '%', SQLERRM;
END
$$;


/*
 *  Refresh insert only table based on timestamp control field
 */
CREATE OR REPLACE FUNCTION refresh_inserter(p_destination text, p_limit integer DEFAULT NULL, p_repull boolean DEFAULT false, p_repull_start text DEFAULT NULL, p_repull_end text DEFAULT NULL, p_debug boolean DEFAULT false) RETURNS void
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
v_dest_table            text;
v_dst_active            boolean;
v_dst_check             boolean;
v_dst_start             int;
v_dst_end               int;
v_fetch_sql             text;
v_filter                text[]; 
v_full_refresh          boolean := false;
v_insert_sql            text;
v_job_id                int;
v_jobmon_schema         text;
v_job_name              text;
v_last_value            timestamptz;
v_last_value_new      timestamptz;
v_limit                 int;
v_now                   timestamptz := now();
v_old_search_path       text;
v_remote_sql            text;
v_rowcount              bigint := 0; 
v_source_table          text;
v_step_id               int;
v_tmp_table             text;
v_total                 bigint := 0;

BEGIN

IF p_debug IS DISTINCT FROM true THEN
    PERFORM set_config( 'client_min_messages', 'warning', true );
END IF;

v_job_name := 'Refresh Inserter: '||p_destination;
v_dblink_name := 'mimeo_inserter_refresh_'||p_destination;

SELECT nspname INTO v_dblink_schema FROM pg_namespace n, pg_extension e WHERE e.extname = 'dblink' AND e.extnamespace = n.oid;
SELECT nspname INTO v_jobmon_schema FROM pg_namespace n, pg_extension e WHERE e.extname = 'pg_jobmon' AND e.extnamespace = n.oid;

-- Set custom search path to allow easier calls to other functions, especially job logging
SELECT current_setting('search_path') INTO v_old_search_path;
EXECUTE 'SELECT set_config(''search_path'',''@extschema@,'||v_jobmon_schema||','||v_dblink_schema||',public'',''false'')';

v_job_id := add_job(v_job_name);
PERFORM gdb(p_debug,'Job ID: '||v_job_id::text);

-- Take advisory lock to prevent multiple calls to function overlapping
v_adv_lock := pg_try_advisory_lock(hashtext('refresh_inserter'), hashtext(v_job_name));
IF v_adv_lock = 'false' THEN
    v_step_id := add_step(v_job_id,'Obtaining advisory lock for job: '||v_job_name);
    PERFORM gdb(p_debug,'Obtaining advisory lock FAILED for job: '||v_job_name);
    PERFORM update_step(v_step_id, 'WARNING','Found concurrent job. Exiting gracefully');
    PERFORM fail_job(v_job_id, 2);
    EXECUTE 'SELECT set_config(''search_path'','''||v_old_search_path||''',''false'')';
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
    , condition
    , dst_active
    , dst_start
    , dst_end
    , batch_limit 
FROM refresh_config_inserter WHERE dest_table = p_destination 
INTO v_source_table
    , v_dest_table
    , v_tmp_table
    , v_dblink
    , v_control
    , v_last_value
    , v_boundary
    , v_filter
    , v_condition
    , v_dst_active
    , v_dst_start
    , v_dst_end
    , v_limit; 
IF NOT FOUND THEN
   RAISE EXCEPTION 'ERROR: no configuration found for %',v_job_name; 
END IF;  

-- Do not allow this function to run during DST time change if config option is true. Otherwise will miss data from source
IF v_dst_active THEN
    v_dst_check := @extschema@.dst_change(v_now);
    IF v_dst_check THEN 
        IF to_number(to_char(v_now, 'HH24MM'), '0000') > v_dst_start AND to_number(to_char(v_now, 'HH24MM'), '0000') < v_dst_end THEN
            v_step_id := add_step( v_job_id, 'DST Check');
            PERFORM update_step(v_step_id, 'OK', 'Job CANCELLED - Does not run during DST time change');
            PERFORM close_job(v_job_id);
            PERFORM gdb(p_debug, 'Cannot run during DST time change');
            UPDATE refresh_config_inserter SET last_run = CURRENT_TIMESTAMP WHERE dest_table = p_destination;  
            EXECUTE 'SELECT set_config(''search_path'','''||v_old_search_path||''',''false'')';
            PERFORM pg_advisory_unlock(hashtext('refresh_inserter'), hashtext(v_job_name));
            RETURN;
        END IF;
    END IF;
END IF;

v_step_id := add_step(v_job_id,'Building SQL');

IF v_filter IS NULL THEN 
    SELECT array_to_string(array_agg(attname),','), array_to_string(array_agg(attname||' '||format_type(atttypid, atttypmod)::text),',') 
        INTO v_cols, v_cols_n_types
        FROM pg_attribute WHERE attrelid = p_destination::regclass AND attnum > 0 AND attisdropped is false;
ELSE
    SELECT array_to_string(array_agg(attname),','), array_to_string(array_agg(attname||' '||format_type(atttypid, atttypmod)::text),',') 
        INTO v_cols, v_cols_n_types
        FROM pg_attribute WHERE attrelid = p_destination::regclass AND ARRAY[attname::text] <@ v_filter AND attnum > 0 AND attisdropped is false;
END IF;

PERFORM dblink_connect(v_dblink_name, auth(v_dblink));

IF p_limit IS NOT NULL THEN
    v_limit := p_limit;
END IF;

IF p_repull THEN
    -- Repull ALL data if no start and end values set
    IF p_repull_start IS NULL AND p_repull_end IS NULL THEN
        PERFORM update_step(v_step_id, 'OK','Request to repull ALL data from source. This could take a while...');
        EXECUTE 'TRUNCATE '||v_dest_table;
        v_remote_sql := 'SELECT '||v_cols||' FROM '||v_source_table;
        IF v_condition IS NOT NULL THEN
            v_remote_sql := v_remote_sql || ' ' || v_condition;
        END IF;
    ELSE
        PERFORM update_step(v_step_id, 'OK','Request to repull data from '||COALESCE(p_repull_start, '-infinity')||' to '||COALESCE(p_repull_end, 'infinity'));
        PERFORM gdb(p_debug,'Request to repull data from '||COALESCE(p_repull_start, '-infinity')||' to '||COALESCE(p_repull_end, 'infinity'));
        v_remote_sql := 'SELECT '||v_cols||' FROM '||v_source_table;
        IF v_condition IS NOT NULL THEN
            v_remote_sql := v_remote_sql || ' ' || v_condition || ' AND ';
        ELSE
            v_remote_sql := v_remote_sql || ' WHERE ';
        END IF;
        v_remote_sql := v_remote_sql ||v_control||' > '||quote_literal(COALESCE(p_repull_start, '-infinity'))||' AND '
            ||v_control||' < '||quote_literal(COALESCE(p_repull_end, 'infinity'));
        -- Delete the old local data
        v_delete_sql := 'DELETE FROM '||v_dest_table||' WHERE '||v_control||' > '||quote_literal(COALESCE(p_repull_start, '-infinity'))||' AND '
            ||v_control||' < '||quote_literal(COALESCE(p_repull_end, 'infinity'));
        v_step_id := add_step(v_job_id, 'Deleting current, local data');
        PERFORM gdb(p_debug,'Deleting current, local data: '||v_delete_sql);
        EXECUTE v_delete_sql;
        GET DIAGNOSTICS v_rowcount = ROW_COUNT;
        PERFORM update_step(v_step_id, 'OK', v_rowcount || 'rows removed');
    END IF;
ELSE
    -- does < for upper boundary to keep missing data from happening on rare edge case where a newly inserted row outside the transaction batch
    -- has the exact same timestamp as the previous batch's max timestamp
    v_remote_sql := 'SELECT '||v_cols||' FROM '||v_source_table;
    IF v_condition IS NOT NULL THEN
        v_remote_sql := v_remote_sql || ' ' || v_condition || ' AND ';
    ELSE
        v_remote_sql := v_remote_sql || ' WHERE ';
    END IF;
    v_remote_sql := v_remote_sql ||v_control||' > '||quote_literal(v_last_value)||' AND '||v_control||' < '||quote_literal(v_boundary)||' ORDER BY '||v_control||' ASC LIMIT '|| COALESCE(v_limit::text, 'ALL');

    PERFORM update_step(v_step_id, 'OK','Grabbing rows from '||v_last_value::text||' to '||v_boundary::text);
    PERFORM gdb(p_debug,'Grabbing rows from '||v_last_value::text||' to '||v_boundary::text);

END IF;

EXECUTE 'CREATE TEMP TABLE '||v_tmp_table||' ('||v_cols_n_types||')';
PERFORM gdb(p_debug,v_remote_sql);
PERFORM dblink_open(v_dblink_name, 'mimeo_cursor', v_remote_sql);
v_step_id := add_step(v_job_id, 'Inserting new records into local table');
v_rowcount := 0;
v_total := 0;
LOOP
    v_fetch_sql := 'INSERT INTO '||v_tmp_table||'('||v_cols||') 
        SELECT '||v_cols||' FROM dblink_fetch('||quote_literal(v_dblink_name)||', ''mimeo_cursor'', 50000) AS ('||v_cols_n_types||')';
    EXECUTE v_fetch_sql;
    GET DIAGNOSTICS v_rowcount = ROW_COUNT;
    v_total := v_total + coalesce(v_rowcount, 0);
    IF v_rowcount > 0 AND v_rowcount IS NOT NULL THEN -- otherwise would set max to NULL on last loop
        EXECUTE 'SELECT max('||v_control||') FROM '||v_tmp_table INTO v_last_value_new;
    END IF;
    IF v_limit IS NULL THEN -- insert into the real table in batches if no limit to avoid excessively large temp tables
        EXECUTE 'INSERT INTO '||v_dest_table||' ('||v_cols||') SELECT '||v_cols||' FROM '||v_tmp_table;
        EXECUTE 'TRUNCATE '||v_tmp_table;
    END IF;
    EXIT WHEN v_rowcount = 0;        
    PERFORM gdb(p_debug,'Fetching rows in batches: '||v_total||' done so far. New last_value: '||v_last_value_new);
    PERFORM update_step(v_step_id, 'PENDING', 'Fetching rows in batches: '||v_total||' done so far. New last_value: '||v_last_value_new);
END LOOP;
PERFORM dblink_close(v_dblink_name, 'mimeo_cursor');
PERFORM update_step(v_step_id, 'OK','Rows fetched: '||v_total);

IF v_limit IS NULL THEN
    -- nothing else to do
ELSE
    -- When using batch limits, entire batch must be pulled to temp table before inserting to real table to catch edge cases 
    v_step_id := add_step(v_job_id,'Checking for batch limit issues');
    PERFORM gdb(p_debug, 'Checking for batch limit issues');
    -- Not recommended that the batch actually equal the limit set if possible. Handle all edge cases to keep data consistent
    IF v_total >= v_limit THEN
        PERFORM update_step(v_step_id, 'WARNING','Row count fetched equal to or greater than limit set: '||v_limit||'. Recommend increasing batch limit if possible.');
        PERFORM gdb(p_debug, 'Row count fetched equal to or greater than limit set: '||v_limit||'. Recommend increasing batch limit if possible.'); 
        EXECUTE 'SELECT max('||v_control||') FROM '||v_tmp_table INTO v_last_value;
        v_step_id := add_step(v_job_id, 'Removing high boundary rows from this batch to avoid missing data');       
        EXECUTE 'DELETE FROM '||v_tmp_table||' WHERE '||v_control||' = '||quote_literal(v_last_value);
        GET DIAGNOSTICS v_rowcount = ROW_COUNT;
        PERFORM update_step(v_step_id, 'OK', 'Removed '||v_rowcount||' rows. Batch now contains '||v_limit - v_rowcount||' records');
        PERFORM gdb(p_debug, 'Removed '||v_rowcount||' rows from batch. Batch table now contains '||v_limit - v_rowcount||' records');
        v_batch_limit_reached = 2;
        v_total := v_total - v_rowcount;
        IF (v_limit - v_rowcount) < 1 THEN
            v_step_id := add_step(v_job_id, 'Reached inconsistent state');
            PERFORM update_step(v_step_id, 'CRITICAL', 'Batch contained max rows ('||v_limit||') or greater and all contained the same timestamp value. Unable to guarentee rows will ever be replicated consistently. Increase row limit parameter to allow a consistent batch.');
            PERFORM gdb(p_debug, 'Batch contained max rows desired ('||v_limit||') or greaer and all contained the same timestamp value. Unable to guarentee rows will be replicated consistently. Increase row limit parameter to allow a consistent batch.');
            v_batch_limit_reached = 3;
        END IF;
    ELSE
        PERFORM update_step(v_step_id, 'OK','No issues found');
        PERFORM gdb(p_debug, 'No issues found');
    END IF;

    IF v_batch_limit_reached <> 3 THEN
        v_step_id := add_step(v_job_id,'Inserting new records into local table');
        EXECUTE 'INSERT INTO '||v_dest_table||' ('||v_cols||') SELECT '||v_cols||' FROM '||v_tmp_table;
        PERFORM update_step(v_step_id, 'OK','Inserted '||v_total||' records');
        PERFORM gdb(p_debug, 'Inserted '||v_total||' records');
    END IF;

END IF; -- end v_limit IF

IF v_batch_limit_reached <> 3 THEN
    v_step_id := add_step(v_job_id, 'Setting next lower boundary');
    UPDATE refresh_config_inserter set last_value = coalesce(v_last_value_new, v_last_value), last_run = CURRENT_TIMESTAMP WHERE dest_table = p_destination;  
    PERFORM update_step(v_step_id, 'OK','Lower boundary value is: '||coalesce(v_last_value_new, v_last_value));
    PERFORM gdb(p_debug, 'Lower boundary value is: '||coalesce(v_last_value_new, v_last_value));
END IF;

EXECUTE 'DROP TABLE IF EXISTS ' || v_tmp_table;

PERFORM dblink_disconnect(v_dblink_name);

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

-- Ensure old search path is reset for the current session
EXECUTE 'SELECT set_config(''search_path'','''||v_old_search_path||''',''false'')';

PERFORM pg_advisory_unlock(hashtext('refresh_inserter'), hashtext(v_job_name));

EXCEPTION
    WHEN QUERY_CANCELED THEN
        PERFORM pg_advisory_unlock(hashtext('refresh_inserter'), hashtext(v_job_name));
        EXECUTE 'SELECT set_config(''search_path'',''@extschema@,'||v_jobmon_schema||','||v_dblink_schema||''',''false'')';
        IF dblink_get_connections() @> ARRAY[v_dblink_name] THEN
            PERFORM dblink_disconnect(v_dblink_name);
        END IF;
        EXECUTE 'SELECT set_config(''search_path'','''||v_old_search_path||''',''false'')';
        RAISE EXCEPTION '%', SQLERRM;    
    WHEN OTHERS THEN
        EXECUTE 'SELECT set_config(''search_path'',''@extschema@,'||v_jobmon_schema||','||v_dblink_schema||''',''false'')';
        IF v_job_id IS NULL THEN
            v_job_id := add_job('Refresh Inserter: '||p_destination);
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
        PERFORM pg_advisory_unlock(hashtext('refresh_inserter'), hashtext(v_job_name));
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

-- insert records to local table (inserts/updates). Have to do temp table in case destination table is partitioned (returns 0 when inserting to parent)
v_remote_f_sql := 'SELECT '||v_cols||' FROM '||v_source_table||' JOIN ('||v_remote_q_sql||') x USING ('||v_pk_name_csv||')';
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
v_match             boolean := 'f';
v_old_search_path   text;
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
v_table_exists      int;
v_total             bigint := 0;
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
    , post_script 
INTO v_source_table
    , v_dest_table
    , v_dblink
    , v_filter
    , v_condition
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

v_remote_sql := 'SELECT array_agg(attname) as cols, array_agg(attname||'' ''||format_type(atttypid, atttypmod)::text) as cols_n_types FROM pg_attribute WHERE attrelid = '||quote_literal(v_source_table)||'::regclass AND attnum > 0 AND attisdropped is false';

-- Apply column filters if used
IF v_filter IS NOT NULL THEN
    v_remote_sql := v_remote_sql || ' AND ARRAY[attname::text] <@ '||quote_literal(v_filter);
END IF;

v_remote_sql := 'SELECT cols, cols_n_types FROM dblink('||quote_literal(v_dblink_name)||', ' || quote_literal(v_remote_sql) || ') t (cols text[], cols_n_types text[])';
perform gdb(p_debug,'v_remote_sql: '||v_remote_sql);
EXECUTE v_remote_sql INTO v_cols, v_cols_n_types;  
perform gdb(p_debug,'v_cols: {'|| array_to_string(v_cols, ',') ||'}');
perform gdb(p_debug,'v_cols_n_types: {'|| array_to_string(v_cols_n_types, ',') ||'}');

PERFORM update_step(v_step_id, 'OK','Done');

v_step_id := add_step(v_job_id,'Truncate non-active snap table');

v_exists := strpos(v_view_definition, 'snap1');
  IF v_exists > 0 THEN
    v_snap := '_snap2';
    ELSE
    v_snap := '_snap1';
 END IF;
v_refresh_snap := v_dest_table||v_snap;
PERFORM gdb(p_debug,'v_refresh_snap: '||v_refresh_snap::text);

-- Create snap table if it doesn't exist
SELECT string_to_array(v_refresh_snap, '.') AS oparts INTO v_parts;
SELECT INTO v_table_exists count(1) FROM pg_tables
    WHERE  schemaname = v_parts.oparts[1] AND
           tablename = v_parts.oparts[2];
IF v_table_exists = 0 THEN
    PERFORM gdb(p_debug,'Snap table does not exist. Creating... ');    
    v_create_sql := 'CREATE TABLE ' || v_refresh_snap || ' (' || array_to_string(v_cols_n_types, ',') || ')';
    perform gdb(p_debug,'v_create_sql: '||v_create_sql::text);
    EXECUTE v_create_sql;
ELSE 

/* Check local column definitions against remote and recreate table if different. Allows automatic recreation of
        snap tables if columns change (add, drop type change)  */  
    v_local_sql := 'SELECT array_agg(attname||'' ''||format_type(atttypid, atttypmod)::text) as cols_n_types FROM pg_attribute WHERE attnum > 0 AND attisdropped is false AND attrelid = ' || quote_literal(v_refresh_snap) || '::regclass'; 
        
    PERFORM gdb(p_debug, v_local_sql);

    EXECUTE v_local_sql INTO v_lcols_array;

    -- Check to see if there's a change in the column structure on the remote
    FOREACH v_r IN ARRAY v_cols_n_types LOOP
        v_match := 'f';
        FOREACH v_l IN ARRAY v_lcols_array LOOP
            IF v_r = v_l THEN
                v_match := 't';
                EXIT;
            END IF;
        END LOOP;
    END LOOP;

    IF v_match = 'f' THEN
        EXECUTE 'DROP TABLE ' || v_refresh_snap;
        EXECUTE 'DROP VIEW ' || v_dest_table;
        v_create_sql := 'CREATE TABLE ' || v_refresh_snap || ' (' || array_to_string(v_cols_n_types, ',') || ')';
        PERFORM gdb(p_debug,'v_create_sql: '||v_create_sql::text);
        EXECUTE v_create_sql;
        v_step_id := add_step(v_job_id,'Source table structure changed.');
        PERFORM update_step(v_step_id, 'OK','Tables and view dropped and recreated. Please double-check snap table attributes (permissions, indexes, etc');
        PERFORM gdb(p_debug,'Source table structure changed. Tables and view dropped and recreated. Please double-check snap table attributes (permissions, indexes, etc)');

    END IF;
    -- truncate non-active snap table
    EXECUTE 'TRUNCATE TABLE ' || v_refresh_snap;

    PERFORM update_step(v_step_id, 'OK','Done');
END IF;

v_remote_sql := 'SELECT '|| array_to_string(v_cols, ',') ||' FROM '||v_source_table;
-- Used by p_pull options in all maker functions to be able to create a replication job but pull no data
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
IF (v_table_exists = 0 OR v_match = 'f') AND p_index = true THEN
    v_remote_index_sql := 'SELECT 
        CASE
            WHEN i.indisprimary IS true THEN ''primary''
            WHEN i.indisunique IS true THEN ''unique''
            ELSE ''index''
        END AS key_type,
        ( SELECT array_agg( a.attname ORDER by x.r ) 
            FROM pg_attribute a 
            JOIN ( SELECT k, row_number() over () as r 
                    FROM unnest(i.indkey) k ) as x 
            ON a.attnum = x.k AND a.attrelid = i.indrelid ';
    IF v_filter IS NOT NULL THEN
        v_remote_index_sql := v_remote_index_sql || ' WHERE ARRAY[a.attname::text] <@ '||quote_literal(v_filter);
    END IF;
    v_remote_index_sql := v_remote_index_sql || ') AS indkey_names
        FROM pg_index i
        WHERE i.indrelid = '||quote_literal(v_source_table)||'::regclass';

    FOR v_row IN EXECUTE 'SELECT key_type, indkey_names FROM dblink('||quote_literal(v_dblink_name)||', '||quote_literal(v_remote_index_sql)||') t (key_type text, indkey_names text[])' LOOP
        IF v_row.indkey_names IS NOT NULL THEN   -- If column filter is used, indkey_name column may be null
            IF v_row.key_type = 'primary' THEN
                RAISE NOTICE 'Creating primary key...';
                EXECUTE 'ALTER TABLE '||v_refresh_snap||' ADD CONSTRAINT '||v_parts.oparts[2]||'_'||array_to_string(v_row.indkey_names, '_')||'_idx PRIMARY KEY ('||array_to_string(v_row.indkey_names, ',')||')';
            ELSIF v_row.key_type = 'unique' THEN
                RAISE NOTICE 'Creating unique index...';
                EXECUTE 'CREATE UNIQUE INDEX '||v_parts.oparts[2]||'_'||array_to_string(v_row.indkey_names, '_')||'_idx ON '||v_refresh_snap|| '('||array_to_string(v_row.indkey_names, ',')||')';
            ELSE
                RAISE NOTICE 'Creating index...';
                EXECUTE 'CREATE INDEX '||v_parts.oparts[2]||'_'||array_to_string(v_row.indkey_names, '_')||'_idx ON '||v_refresh_snap|| '('||array_to_string(v_row.indkey_names, ',')||')';
            END IF;
        END IF;
    END LOOP;
END IF;

IF v_rowcount IS NOT NULL THEN
     EXECUTE 'ANALYZE ' ||v_refresh_snap;

    --SET statement_timeout='30 min';  
    
    -- swap view
    v_step_id := add_step(v_job_id,'Swap view to '||v_refresh_snap);
    PERFORM gdb(p_debug,'Swapping view to '||v_refresh_snap);
    EXECUTE 'CREATE OR REPLACE VIEW '||v_dest_table||' AS SELECT * FROM '||v_refresh_snap;
    PERFORM update_step(v_step_id, 'OK','View Swapped');

    v_step_id := add_step(v_job_id,'Updating last value');
    UPDATE refresh_config_snap set last_run = CURRENT_TIMESTAMP WHERE dest_table = p_destination;  
    PERFORM update_step(v_step_id, 'OK','Done');

    -- Runs special sql to fix indexes, permissions, etc on recreated objects
    IF v_match = 'f' AND v_post_script IS NOT NULL THEN
        v_step_id := add_step(v_job_id,'Applying post_script sql commands due to schema change');
        PERFORM @extschema@.post_script(v_dest_table);
        PERFORM update_step(v_step_id, 'OK','Done');
    END IF;

    PERFORM close_job(v_job_id);
ELSE
    RAISE EXCEPTION 'No rows found in source table';
END IF;

PERFORM dblink_disconnect(v_dblink_name);

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
 *  Refresh insert/update only table based on timestamp control field
 */
CREATE OR REPLACE FUNCTION refresh_updater(p_destination text, p_limit integer DEFAULT NULL, p_repull boolean DEFAULT false, p_repull_start text DEFAULT NULL, p_repull_end text DEFAULT NULL, p_debug boolean DEFAULT false) RETURNS void
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
v_dest_table            text;
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
v_jobmon_schema         text;
v_job_name              text;
v_last_value            timestamptz;
v_last_value_new      timestamptz;
v_limit                 int;
v_now                   timestamptz := now(); 
v_old_search_path       text;
v_pk_counter            int := 1;
v_pk_name               text[];
v_remote_boundry_sql    text;
v_remote_boundry        timestamptz;
v_remote_sql            text;
v_rowcount              bigint := 0; 
v_source_table          text;
v_step_id               int;
v_tmp_table             text;
v_total                 bigint := 0;

BEGIN

IF p_debug IS DISTINCT FROM true THEN
    PERFORM set_config( 'client_min_messages', 'warning', true );
END IF;

v_job_name := 'Refresh Updater: '||p_destination;
v_dblink_name := 'mimeo_updater_refresh_'||p_destination;

SELECT nspname INTO v_dblink_schema FROM pg_namespace n, pg_extension e WHERE e.extname = 'dblink' AND e.extnamespace = n.oid;
SELECT nspname INTO v_jobmon_schema FROM pg_namespace n, pg_extension e WHERE e.extname = 'pg_jobmon' AND e.extnamespace = n.oid;

-- Set custom search path to allow easier calls to other functions, especially job logging
SELECT current_setting('search_path') INTO v_old_search_path;
EXECUTE 'SELECT set_config(''search_path'',''@extschema@,'||v_jobmon_schema||','||v_dblink_schema||',public'',''false'')';


v_job_id := add_job(v_job_name);
PERFORM gdb(p_debug,'Job ID: '||v_job_id::text);

-- Take advisory lock to prevent multiple calls to function overlapping
v_adv_lock := pg_try_advisory_lock(hashtext('refresh_updater'), hashtext(v_job_name));
IF v_adv_lock = 'false' THEN
    v_step_id := add_step(v_job_id,'Obtaining advisory lock for job: '||v_job_name);
    PERFORM gdb(p_debug,'Obtaining advisory lock FAILED for job: '||v_job_name);
    PERFORM update_step(v_step_id, 'WARNING','Found concurrent job. Exiting gracefully');
    PERFORM fail_job(v_job_id, 2);
    EXECUTE 'SELECT set_config(''search_path'','''||v_old_search_path||''',''false'')';
    RETURN;
END IF;

SELECT source_table
    , dest_table
    , 'tmp_'||replace(dest_table,'.','_')
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
FROM refresh_config_updater
WHERE dest_table = p_destination INTO 
    v_source_table
    , v_dest_table
    , v_tmp_table
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
    , v_limit;
IF NOT FOUND THEN
   RAISE EXCEPTION 'ERROR: no configuration found for %',v_job_name;
END IF;

-- Do not allow this function to run during DST time change if config option is true. Otherwise will miss data from source
IF v_dst_active THEN
    v_dst_check := @extschema@.dst_change(v_now);
    IF v_dst_check THEN 
        IF to_number(to_char(v_now, 'HH24MM'), '0000') > v_dst_start AND to_number(to_char(v_now, 'HH24MM'), '0000') < v_dst_end THEN
            v_step_id := add_step( v_job_id, 'DST Check');
            PERFORM update_step(v_step_id, 'OK', 'Job CANCELLED - Does not run during DST time change');
            UPDATE refresh_config_updater SET last_run = CURRENT_TIMESTAMP WHERE dest_table = p_destination;  
            PERFORM close_job(v_job_id);
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
    SELECT array_to_string(array_agg(attname),','), array_to_string(array_agg(attname||' '||format_type(atttypid, atttypmod)::text),',')
        INTO v_cols, v_cols_n_types
        FROM pg_attribute WHERE attrelid = p_destination::regclass AND attnum > 0 AND attisdropped is false;
ELSE
    -- ensure all primary key columns are included in any column filters
    FOREACH v_field IN ARRAY v_pk_name LOOP
        IF v_field = ANY(v_filter) THEN
            CONTINUE;
        ELSE
            RAISE EXCEPTION 'ERROR: filter list did not contain all columns that compose primary/unique key for %',v_job_name; 
        END IF;
    END LOOP;
    SELECT array_to_string(array_agg(attname),','), array_to_string(array_agg(attname||' '||format_type(atttypid, atttypmod)::text),',') 
        INTO v_cols, v_cols_n_types
        FROM pg_attribute WHERE attrelid = p_destination::regclass AND ARRAY[attname::text] <@ v_filter AND attnum > 0 AND attisdropped is false;
END IF;

PERFORM dblink_connect(v_dblink_name, auth(v_dblink));

IF p_limit IS NOT NULL THEN
    v_limit := p_limit;
END IF;

-- Repull old data instead of normal new data pull
IF p_repull THEN
    -- Repull ALL data if no start and end values set
    IF p_repull_start IS NULL AND p_repull_end IS NULL THEN
        PERFORM update_step(v_step_id, 'OK','Request to repull ALL data from source. This could take a while...');
        EXECUTE 'TRUNCATE '||v_dest_table;
        v_remote_sql := 'SELECT '||v_cols||' FROM '||v_source_table;
        IF v_condition IS NOT NULL THEN
            v_remote_sql := v_remote_sql || ' ' || v_condition;
        END IF;
    ELSE
        PERFORM update_step(v_step_id, 'OK','Request to repull data from '||p_repull_start||' to '||p_repull_end);
        PERFORM gdb(p_debug,'Request to repull data from '||p_repull_start||' to '||p_repull_end);
        v_remote_sql := 'SELECT '||v_cols||' FROM '||v_source_table;
        IF v_condition IS NOT NULL THEN
            v_remote_sql := v_remote_sql || ' ' || v_condition || ' AND ';
        ELSE
            v_remote_sql := v_remote_sql || ' WHERE ';
        END IF;
        v_remote_sql := v_remote_sql ||v_control||' > '||quote_literal(COALESCE(p_repull_start, '-infinity'))||' AND '
            ||v_control||' < '||quote_literal(COALESCE(p_repull_end, 'infinity'));
        EXECUTE 'DELETE FROM '||v_dest_table||' WHERE '||v_control||' > '||quote_literal(COALESCE(p_repull_start, '-infinity'))||' AND '
            ||v_control||' < '||quote_literal(COALESCE(p_repull_end, 'infinity'));
    END IF;
ELSE
    -- does < for upper boundary to keep missing data from happening on rare edge case where a newly inserted row outside the transaction batch
    -- has the exact same timestamp as the previous batch's max timestamp
    v_remote_sql := 'SELECT '||v_cols||' FROM '||v_source_table;
    IF v_condition IS NOT NULL THEN
        v_remote_sql := v_remote_sql || ' ' || v_condition || ' AND ';
    ELSE
        v_remote_sql := v_remote_sql || ' WHERE ';
    END IF;
    v_remote_sql := v_remote_sql ||v_control||' > '||quote_literal(v_last_value)||' AND '||v_control||' < '||quote_literal(v_boundary)||' ORDER BY '||v_control||' ASC LIMIT '|| COALESCE(v_limit::text, 'ALL');

    v_delete_sql := 'DELETE FROM '||v_dest_table||' USING '||v_tmp_table||' t WHERE ';

    WHILE v_pk_counter <= array_length(v_pk_name,1) LOOP
        IF v_pk_counter > 1 THEN
            v_delete_sql := v_delete_sql ||' AND ';
        END IF;
        v_delete_sql := v_delete_sql ||v_dest_table||'.'||v_pk_name[v_pk_counter]||' = t.'||v_pk_name[v_pk_counter];
        v_pk_counter := v_pk_counter + 1;
    END LOOP;

    PERFORM update_step(v_step_id, 'OK','Grabbing rows from '||v_last_value::text||' to '||v_boundary::text);
    PERFORM gdb(p_debug,'Grabbing rows from '||v_last_value::text||' to '||v_boundary::text);
END IF;

v_insert_sql := 'INSERT INTO '||v_dest_table||' ('||v_cols||') SELECT '||v_cols||' FROM '||v_tmp_table; 

PERFORM gdb(p_debug,v_remote_sql);
PERFORM dblink_open(v_dblink_name, 'mimeo_cursor', v_remote_sql);
v_step_id := add_step(v_job_id, 'Inserting new/updated records into local table');
v_rowcount := 0;

EXECUTE 'CREATE TEMP TABLE '||v_tmp_table||' ('||v_cols_n_types||')'; 
LOOP            
    v_fetch_sql := 'INSERT INTO '||v_tmp_table||'('||v_cols||') 
        SELECT '||v_cols||' FROM dblink_fetch('||quote_literal(v_dblink_name)||', ''mimeo_cursor'', 50000) AS ('||v_cols_n_types||')';
    EXECUTE v_fetch_sql;
    GET DIAGNOSTICS v_rowcount = ROW_COUNT;
    v_total := v_total + coalesce(v_rowcount, 0);
    IF v_rowcount > 0 AND v_rowcount IS NOT NULL THEN -- otherwise would set max to NULL on last loop
        EXECUTE 'SELECT max('||v_control||') FROM '||v_tmp_table INTO v_last_value_new;
    END IF;
    IF v_limit IS NULL THEN -- insert into the real table in batches if no limit to avoid excessively large temp tables
        IF p_repull IS FALSE THEN   -- Delete any rows that exist in the current temp table batch. repull delete is done above.
            EXECUTE v_delete_sql;
        END IF;
        EXECUTE v_insert_sql;
        EXECUTE 'TRUNCATE '||v_tmp_table;
    END IF;
    EXIT WHEN v_rowcount = 0;
    PERFORM gdb(p_debug,'Fetching rows in batches: '||v_total||' done so far. New last_value: '||v_last_value_new);
    PERFORM update_step(v_step_id, 'PENDING', 'Fetching rows in batches: '||v_total||' done so far. New last_value: '||v_last_value_new);
END LOOP;
PERFORM dblink_close(v_dblink_name, 'mimeo_cursor');
PERFORM update_step(v_step_id, 'OK','Rows fetched: '||v_total);     
    
IF v_limit IS NULL THEN
    -- nothing else to do
ELSE 
    -- When using batch limits, entire batch must be pulled to temp table before inserting to real table to catch edge cases 
    v_step_id := add_step(v_job_id,'Checking for batch limit issues');     
    -- Not recommended that the batch actually equal the limit set if possible.
    IF v_total >= v_limit THEN
        PERFORM update_step(v_step_id, 'WARNING','Row count fetched equal to or greater than limit set: '||v_limit||'. Recommend increasing batch limit if possible.');
        PERFORM gdb(p_debug, 'Row count fetched equal to or greater than limit set: '||v_limit||'. Recommend increasing batch limit if possible.'); 
        EXECUTE 'SELECT max('||v_control||') FROM '||v_tmp_table INTO v_last_value;
        v_step_id := add_step(v_job_id, 'Removing high boundary rows from this batch to avoid missing data');       
        EXECUTE 'DELETE FROM '||v_tmp_table||' WHERE '||v_control||' = '||quote_literal(v_last_value);
        GET DIAGNOSTICS v_rowcount = ROW_COUNT;
        PERFORM update_step(v_step_id, 'OK', 'Removed '||v_rowcount||' rows. Batch now contains '||v_limit - v_rowcount||' records');
        PERFORM gdb(p_debug, 'Removed '||v_rowcount||' rows from batch. Batch table now contains '||v_limit - v_rowcount||' records');
        v_batch_limit_reached := 2;
        IF (v_limit - v_rowcount) < 1 THEN
            v_step_id := add_step(v_job_id, 'Reached inconsistent state');
            PERFORM update_step(v_step_id, 'CRITICAL', 'Batch contained max rows ('||v_limit||') or greater and all contained the same timestamp value. Unable to guarentee rows will ever be replicated consistently. Increase row limit parameter to allow a consistent batch.');
            PERFORM gdb(p_debug, 'Batch contained max rows ('||v_limit||') or greater and all contained the same timestamp value. Unable to guarentee rows will be replicated consistently. Increase row limit parameter to allow a consistent batch.');
            v_batch_limit_reached := 3;
        END IF;
    ELSE
        PERFORM update_step(v_step_id, 'OK','No issues found');
        PERFORM gdb(p_debug, 'No issues found');
    END IF;

    IF v_batch_limit_reached <> 3 THEN
        EXECUTE 'CREATE INDEX ON '||v_tmp_table||' ('||array_to_string(v_pk_name, ',')||')'; -- incase of large batch limit
        EXECUTE 'ANALYZE '||v_tmp_table;       
        v_step_id := add_step(v_job_id,'Deleting records marked for update in local table');
        perform gdb(p_debug,v_delete_sql);
        execute v_delete_sql; 
        GET DIAGNOSTICS v_rowcount = ROW_COUNT;
        PERFORM update_step(v_step_id, 'OK','Deleted '||v_rowcount||' records');

        v_step_id := add_step(v_job_id,'Inserting new records into local table');
        perform gdb(p_debug,v_insert_sql);
        EXECUTE v_insert_sql; 
        GET DIAGNOSTICS v_rowcount = ROW_COUNT;
        PERFORM update_step(v_step_id, 'OK','Inserted '||v_rowcount||' records');
    END IF;

END IF; -- end v_limit IF

IF v_batch_limit_reached <> 3 THEN
    v_step_id := add_step(v_job_id, 'Setting next lower boundary');
    UPDATE refresh_config_updater set last_value = coalesce(v_last_value_new, v_last_value), last_run = CURRENT_TIMESTAMP WHERE dest_table = p_destination;  
    PERFORM update_step(v_step_id, 'OK','Lower boundary value is: '||coalesce(v_last_value_new, v_last_value));
    PERFORM gdb(p_debug, 'Lower boundary value is: '||coalesce(v_last_value_new, v_last_value));
END IF;

EXECUTE 'DROP TABLE IF EXISTS '||v_tmp_table;

PERFORM dblink_disconnect(v_dblink_name);

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

-- Ensure old search path is reset for the current session
EXECUTE 'SELECT set_config(''search_path'','''||v_old_search_path||''',''false'')';

PERFORM pg_advisory_unlock(hashtext('refresh_updater'), hashtext(v_job_name));

EXCEPTION
    WHEN QUERY_CANCELED THEN
        PERFORM pg_advisory_unlock(hashtext('refresh_updater'), hashtext(v_job_name));
        EXECUTE 'SELECT set_config(''search_path'',''@extschema@,'||v_jobmon_schema||','||v_dblink_schema||''',''false'')';
        IF dblink_get_connections() @> ARRAY[v_dblink_name] THEN
            PERFORM dblink_disconnect(v_dblink_name);
        END IF;
        EXECUTE 'SELECT set_config(''search_path'','''||v_old_search_path||''',''false'')';
        RAISE EXCEPTION '%', SQLERRM;  
    WHEN OTHERS THEN
        EXECUTE 'SELECT set_config(''search_path'',''@extschema@,'||v_jobmon_schema||','||v_dblink_schema||''',''false'')';
        IF v_job_id IS NULL THEN
                v_job_id := add_job('Refresh Updater: '||p_destination);
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

        PERFORM pg_advisory_unlock(hashtext('refresh_updater'), hashtext(v_job_name));
        RAISE EXCEPTION '%', SQLERRM;
END
$$;
