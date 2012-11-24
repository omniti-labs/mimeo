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
                    array_agg( a.attname ) AS indkey_names,
                    array_agg(format_type(a.atttypid, a.atttypmod)::text) AS indkey_types,
                    array_agg(attname||'' ''||format_type(atttypid, atttypmod)::text) AS indkey_names_n_types
                FROM
                    pg_index i
                    JOIN pg_attribute a ON i.indrelid = a.attrelid AND a.attnum = any( i.indkey )
                WHERE
                    i.indrelid = '||quote_literal(p_src_table)||'::regclass
                    AND ( i.indisprimary OR i.indisunique )
                GROUP BY i.indexrelid::regclass, key_type
                HAVING bool_and( a.attnotnull ) 
                ORDER BY key_type LIMIT 1';
    EXECUTE 'SELECT key_type, indkey_names, indkey_types, indkey_names_n_types FROM dblink(''mimeo_dml'', '||quote_literal(v_remote_key_sql)||') t (key_type text, indkey_names text[], indkey_types text[], indkey_names_n_types text[])' 
        INTO v_key_type, v_pk_name, v_pk_type, v_pk_name_n_type;
END IF;

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

v_remote_q_table := 'CREATE TABLE @extschema@.'||v_src_table_name||'_pgq ('||array_to_string(v_pk_name_n_type, ',')|| ', processed boolean)';

v_remote_q_index := 'CREATE INDEX '||v_src_table_name||'_pgq_'||array_to_string(v_pk_name, '_')||'_idx ON @extschema@.'||v_src_table_name||'_pgq ('||array_to_string(v_pk_name, ',')||')';

v_pk_counter := 1;
v_trigger_func := 'CREATE FUNCTION @extschema@.'||v_src_table_name||'_mimeo_queue() RETURNS trigger LANGUAGE plpgsql AS $_$ DECLARE ';
    WHILE v_pk_counter <= array_length(v_pk_name,1) LOOP
        v_trigger_func := v_trigger_func||'v_'||v_pk_name[v_pk_counter]||' '||v_pk_type[v_pk_counter]||'; ';
        v_pk_counter := v_pk_counter + 1;
    END LOOP;
    v_pk_counter := 1;
    v_trigger_func := v_trigger_func || ' BEGIN IF TG_OP = ''INSERT'' THEN ';
    WHILE v_pk_counter <= array_length(v_pk_name,1) LOOP
        v_trigger_func := v_trigger_func||' v_'||v_pk_name[v_pk_counter]||' := NEW.'||v_pk_name[v_pk_counter]||'; ';
        v_pk_counter := v_pk_counter + 1;
    END LOOP;
    v_pk_counter := 1;
    v_trigger_func := v_trigger_func || ' ELSE ';
    WHILE v_pk_counter <= array_length(v_pk_name,1) LOOP
        v_trigger_func := v_trigger_func||' v_'||v_pk_name[v_pk_counter]||' := OLD.'||v_pk_name[v_pk_counter]||'; ';
        v_pk_counter := v_pk_counter + 1;
    END LOOP;
    v_pk_counter := 1;
    v_trigger_func := v_trigger_func || ' END IF; INSERT INTO @extschema@.'||v_src_table_name||'_pgq ('||array_to_string(v_pk_name, ',')||') ';
    v_trigger_func := v_trigger_func || ' VALUES (';
    WHILE v_pk_counter <= array_length(v_pk_name,1) LOOP
        IF v_pk_counter > 1 THEN
            v_trigger_func := v_trigger_func || ', ';
        END IF;
        v_trigger_func := v_trigger_func||'v_'||v_pk_name[v_pk_counter];
        v_pk_counter := v_pk_counter + 1;
    END LOOP;
    v_trigger_func := v_trigger_func || '); RETURN NULL; END $_$;';

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

v_insert_refresh_config := 'INSERT INTO @extschema@.refresh_config_dml(source_table, dest_table, dblink, control, pk_field, pk_type, last_run, filter, condition) VALUES('
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
