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
