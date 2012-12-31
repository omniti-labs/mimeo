-- Yet another bug with dml/logdel replication. Updates may not be applied to the destination. This bug was introduced in the new trigger created in 0.9.1. If you've not created any new replication jobs with 0.9.1, all dml/logdel replication jobs with a single primary/unique key column are fine. If you have created any, a new trigger will have to be made on the source table so the replication jobs should be recreated (run destroyer then maker functions. Note only the table owner can drop a trigger). 
-- Please note that if you use composite primary/unique keys (more than one column), you will still need to re-create your replication jobs for dml & logdel replication to get a new trigger installed on the source if you are using any version older than this one. (Backup your destination logdel tables first to preserve the deleted rows). Single column primary/unique keys only have an issue with triggers created with 0.9.1.
-- Made the source of the trigger functions more human readable.

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
v_remote_key_sql            text;
v_remote_q_index            text;
v_remote_q_table            text;
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
	RAISE EXCEPTION 'ERROR: database link ID is incorrect %', p_dblink_id; 
END IF;

IF p_dest_table IS NULL THEN
    p_dest_table := p_src_table;
END IF;

v_src_table_name := replace(p_src_table, '.', '_');

IF position('.' in p_dest_table) > 0 THEN
    v_dest_schema_name := split_part(p_dest_table, '.', 1); 
    v_dest_table_name := split_part(p_dest_table, '.', 2);
END IF;

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

v_remote_q_table := 'CREATE TABLE @extschema@.'||v_src_table_name||'_pgq (';
WHILE v_pk_counter <= array_length(v_pk_name,1) LOOP
    v_remote_q_table := v_remote_q_table || v_pk_name[v_pk_counter]||' '||v_pk_type[v_pk_counter];
    v_pk_counter := v_pk_counter + 1;
    IF v_pk_counter <= array_length(v_pk_name,1) THEN
        v_remote_q_table := v_remote_q_table || ', ';
    END IF;
END LOOP;
v_remote_q_table := v_remote_q_table || ', processed boolean)';

RAISE NOTICE 'v_remote_q_table: %', v_remote_q_table;

v_remote_q_index := 'CREATE INDEX '||v_src_table_name||'_pgq_'||array_to_string(v_pk_name, '_')||'_idx ON @extschema@.'||v_src_table_name||'_pgq ('||array_to_string(v_pk_name, ',')||')';

v_pk_counter := 1;
v_trigger_func := 'CREATE FUNCTION @extschema@.'||v_src_table_name||'_mimeo_queue() RETURNS trigger LANGUAGE plpgsql AS $_$ ';
    v_trigger_func := v_trigger_func || ' 
        BEGIN IF TG_OP = ''INSERT'' THEN ';
    v_pk_value := array_to_string(v_pk_name, ', NEW.');
    v_pk_value := 'NEW.'||v_pk_value;
    v_trigger_func := v_trigger_func || ' 
            INSERT INTO @extschema@.'||v_src_table_name||'_pgq ('||array_to_string(v_pk_name, ',')||') VALUES ('||v_pk_value||'); ';
    v_trigger_func := v_trigger_func || ' 
        ELSIF TG_OP = ''UPDATE'' THEN ';
    -- UPDATE needs to insert the NEW values so reuse v_pk_value from INSERT operation
    v_trigger_func := v_trigger_func || ' 
            INSERT INTO @extschema@.'||v_src_table_name||'_pgq ('||array_to_string(v_pk_name, ',')||') VALUES ('||v_pk_value||'); ';
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
                INSERT INTO @extschema@.'||v_src_table_name||'_pgq ('||array_to_string(v_pk_name, ',')||') VALUES ('||v_pk_value||'); ';
    v_trigger_func := v_trigger_func || ' 
            END IF;';
    v_trigger_func := v_trigger_func || ' 
        ELSIF TG_OP = ''DELETE'' THEN ';
    -- DELETE needs to insert the OLD values so reuse v_pk_value from UPDATE operation
    v_trigger_func := v_trigger_func || ' 
            INSERT INTO @extschema@.'||v_src_table_name||'_pgq ('||array_to_string(v_pk_name, ',')||') VALUES ('||v_pk_value||'); ';
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
    ||quote_literal(p_src_table)||', '||quote_literal(p_dest_table)||', '|| p_dblink_id||', '||quote_literal('@extschema@.'||v_src_table_name||'_pgq')||', '
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
            PERFORM dblink_exec('mimeo_dml', 'DROP TABLE IF EXISTS @extschema@.'||v_src_table_name||'_pgq');
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
v_remote_key_sql            text;
v_remote_sql                text;
v_remote_q_index            text;
v_remote_q_table            text;
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
	RAISE EXCEPTION 'ERROR: database link ID is incorrect %', p_dblink_id; 
END IF;

IF p_dest_table IS NULL THEN
    p_dest_table := p_src_table;
END IF;

v_src_table_name := replace(p_src_table, '.', '_');

IF position('.' in p_dest_table) > 0 THEN
    v_dest_schema_name := split_part(p_dest_table, '.', 1); 
    v_dest_table_name := split_part(p_dest_table, '.', 2);
END IF;

PERFORM dblink_connect('mimeo_logdel', @extschema@.auth(p_dblink_id));

v_remote_sql := 'SELECT array_agg(attname) as cols, array_agg(format_type(atttypid, atttypmod)::text) as types, array_agg(attname||'' ''||format_type(atttypid, atttypmod)::text) as cols_n_types FROM pg_attribute WHERE attrelid = ' || quote_literal(p_src_table) || '::regclass AND attnum > 0 AND attisdropped is false';
-- Apply column filters if used
IF p_filter IS NOT NULL THEN
    v_remote_sql := v_remote_sql || ' AND ARRAY[attname::text] <@ '||quote_literal(p_filter);
END IF;
v_remote_sql := 'SELECT cols, types, cols_n_types FROM dblink(''mimeo_logdel'', ' || quote_literal(v_remote_sql) || ') t (cols text[], types text[], cols_n_types text[])';
EXECUTE v_remote_sql INTO v_cols, v_types, v_cols_n_types;

v_cols_n_types_csv := array_to_string(v_cols_n_types, ',');

v_remote_q_table := 'CREATE TABLE @extschema@.'||v_src_table_name||'_pgq ('||v_cols_n_types_csv||', mimeo_source_deleted timestamptz, processed boolean)';

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

v_remote_q_index := 'CREATE INDEX '||v_src_table_name||'_pgq_'||array_to_string(v_pk_name, '_')||'_idx ON @extschema@.'||v_src_table_name||'_pgq ('||array_to_string(v_pk_name, ',')||')';

v_counter := 1;
v_trigger_func := 'CREATE FUNCTION @extschema@.'||v_src_table_name||'_mimeo_queue() RETURNS trigger LANGUAGE plpgsql AS $_$ DECLARE ';
    v_trigger_func := v_trigger_func || ' 
        v_del_time timestamptz := clock_timestamp(); ';
    v_trigger_func := v_trigger_func || ' 
        BEGIN IF TG_OP = ''INSERT'' THEN ';
    v_q_value := array_to_string(v_pk_name, ', NEW.');
    v_q_value := 'NEW.'||v_q_value;
    v_trigger_func := v_trigger_func || ' 
            INSERT INTO @extschema@.'||v_src_table_name||'_pgq ('||array_to_string(v_pk_name, ',')||') VALUES ('||v_q_value||');';
    v_trigger_func := v_trigger_func || ' 
        ELSIF TG_OP = ''UPDATE'' THEN  ';
    -- UPDATE needs to insert the NEW values so reuse v_q_value from INSERT operation
    v_trigger_func := v_trigger_func || ' 
            INSERT INTO @extschema@.'||v_src_table_name||'_pgq ('||array_to_string(v_pk_name, ',')||') VALUES ('||v_q_value||');';
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
                INSERT INTO @extschema@.'||v_src_table_name||'_pgq ('||array_to_string(v_pk_name, ',')||') VALUES ('||v_q_value||'); ';
    v_trigger_func := v_trigger_func || ' 
            END IF;';
    v_trigger_func := v_trigger_func || ' 
        ELSIF TG_OP = ''DELETE'' THEN  ';
    v_q_value := array_to_string(v_cols, ', OLD.');
    v_q_value := 'OLD.'||v_q_value;
    v_trigger_func := v_trigger_func || ' 
            INSERT INTO @extschema@.'||v_src_table_name||'_pgq ('||array_to_string(v_cols, ',')||', mimeo_source_deleted) VALUES ('||v_q_value||', v_del_time);';
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
    ||quote_literal(p_src_table)||', '||quote_literal(p_dest_table)||', '|| p_dblink_id||', '||quote_literal('@extschema@.'||v_src_table_name||'_pgq')||', '
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
            PERFORM dblink_exec('mimeo_logdel', 'DROP TABLE IF EXISTS @extschema@.'||v_src_table_name||'_pgq');
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
