-- IMPORTANT NOTE: Signatures on maker functions & refresh_snap() have changes so they were dropped and recreated. Check permissions if needed before and after update.
-- Automatic creation of indexes with maker functions. Does not automatically propogate future changes to indexes with refresh runs. Allows source and destination to be different (ex. often data warehouse destinations do not need indexes to save on space. Also prevents issues with partitioned destination tables).
-- Ensure primary or unique indexes are always made on destination tables when using dml/logdel_maker() (update_maker() was already properly doing this). Will do this even when new p_index option is set to false.
-- Changed funtion parameter 'p_pk_field' to 'p_pk_name' to be more consistent with other internal variable names.
-- update_maker() now checks that if the column filter option is used, all columns that are part of primary/unqiue key are included.
-- Fixed dml/logdel_destroyer() functions to actually remove the objects on the remote database.
-- Fixed manually setting the primary/unique key types with the maker function parameter p_pk_type.
-- Updated Makefile to allow setting of grep binary if needed during building.

DROP FUNCTION @extschema@.dml_maker(text, int, text, p_filter text[], text, boolean, text[], text[]);
DROP FUNCTION @extschema@.inserter_maker(text, text, int, interval,text, text[], text, boolean);
DROP FUNCTION @extschema@.logdel_maker(text, int, text, text[],text, boolean, text[], text[]);
DROP FUNCTION @extschema@.snapshot_maker(text, int, text, text[], text, boolean);
DROP FUNCTION @extschema@.updater_maker(text, text, int, interval, text, text[], text, boolean, text[], text[]);
DROP FUNCTION @extschema@.refresh_snap(text, boolean, boolean);

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


/*
 *  Inserter maker function. 
 */
CREATE FUNCTION inserter_maker(
    p_src_table text
    , p_control_field text
    , p_dblink_id int
    , p_boundary interval DEFAULT '00:10:00'
    , p_dest_table text DEFAULT NULL
    , p_index boolean DEFAULT true
    , p_filter text[] DEFAULT NULL
    , p_condition text DEFAULT NULL
    , p_pulldata boolean DEFAULT true) 
RETURNS void
    LANGUAGE plpgsql
    AS $$
DECLARE

v_data_source               text;
v_dest_check                text;
v_dest_schema_name          text;
v_dest_table_name           text;
v_dst_active                boolean;
v_insert_refresh_config     text;
v_max_timestamp             timestamptz;

BEGIN

SELECT data_source INTO v_data_source FROM @extschema@.dblink_mapping WHERE data_source_id = p_dblink_id; 
IF NOT FOUND THEN
	RAISE EXCEPTION 'ERROR: database link ID is incorrect %', p_dblink_id; 
END IF;  

IF p_dest_table IS NULL THEN
    p_dest_table := p_src_table;
END IF;

IF position('.' in p_dest_table) > 0 THEN
    v_dest_schema_name := split_part(p_dest_table, '.', 1); 
    v_dest_table_name := split_part(p_dest_table, '.', 2);
END IF;

-- Only create destination table if it doesn't already exist
SELECT schemaname||'.'||tablename INTO v_dest_check FROM pg_tables WHERE schemaname = v_dest_schema_name AND tablename = v_dest_table_name;
IF v_dest_check IS NULL THEN

    v_insert_refresh_config := 'INSERT INTO @extschema@.refresh_config_snap(source_table, dest_table, dblink, filter, condition) VALUES('
        ||quote_literal(p_src_table)||', '||quote_literal(p_dest_table)||', '||p_dblink_id||','
        ||COALESCE(quote_literal(p_filter), 'NULL')||','||COALESCE(quote_literal(p_condition), 'NULL')||')';

    RAISE NOTICE 'Snapshotting source table to pull all current source data...';
    EXECUTE v_insert_refresh_config;	

    EXECUTE 'SELECT @extschema@.refresh_snap('||quote_literal(p_dest_table)||', p_index := '||p_index||', p_pulldata := '||p_pulldata||')';
    PERFORM @extschema@.snapshot_destroyer(p_dest_table, 'ARCHIVE');
    	
    RAISE NOTICE 'Snapshot complete.';
ELSE
    RAISE NOTICE 'Destination table % already exists. No data or indexes were pulled from source', p_dest_table;
END IF;

RAISE NOTICE 'Getting the maximum destination timestamp...';
EXECUTE 'SELECT max('||p_control_field||') FROM '||p_dest_table||';' INTO v_max_timestamp;

v_dst_active := @extschema@.dst_utc_check();

v_insert_refresh_config := 'INSERT INTO @extschema@.refresh_config_inserter(source_table, dest_table, dblink, control, boundary, last_value, last_run, dst_active, filter, condition) VALUES('
    ||quote_literal(p_src_table)||','||quote_literal(p_dest_table)||','|| p_dblink_id||','
    ||quote_literal(p_control_field)||','||quote_literal(p_boundary)||','||quote_literal(COALESCE(v_max_timestamp, CURRENT_TIMESTAMP))||','
    ||quote_literal(CURRENT_TIMESTAMP)||','||v_dst_active||','||COALESCE(quote_literal(p_filter), 'NULL')||','||COALESCE(quote_literal(p_condition), 'NULL')||');';

RAISE NOTICE 'Inserting data into config table';
EXECUTE v_insert_refresh_config;

-- Remove temp snap from config
EXECUTE 'DELETE FROM @extschema@.refresh_config_snap WHERE source_table = '||quote_literal(p_src_table)||' AND dest_table = '||quote_literal(p_dest_table);

RAISE NOTICE 'Done';
END
$$;


/*
 *  Logdel maker function.
 */
CREATE FUNCTION logdel_maker(
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
v_cols_csv                  text;
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

v_cols_csv := array_to_string(v_cols, ',');
v_cols_n_types_csv := array_to_string(v_cols_n_types, ',');

v_remote_q_table := 'CREATE TABLE @extschema@.'||v_src_table_name||'_pgq ('||v_cols_n_types_csv||', mimeo_source_deleted timestamptz, processed boolean)';

IF p_pk_name IS NULL AND p_pk_type IS NULL THEN
    -- Either gets the primary key or it gets the first unique index in alphabetical order by index name
    v_remote_key_sql := 'SELECT
                    CASE
                        WHEN i.indisprimary IS true THEN ''primary''
                        WHEN i.indisunique IS true THEN ''unique''
                    END AS key_type,
                    array_agg( a.attname ) AS indkey_names,
                    array_agg(format_type(a.atttypid, a.atttypmod)::text) AS indkey_types
                FROM
                    pg_index i
                    JOIN pg_attribute a ON i.indrelid = a.attrelid AND a.attnum = any( i.indkey )
                WHERE
                    i.indrelid = '||quote_literal(p_src_table)||'::regclass
                    AND ( i.indisprimary OR i.indisunique )
                GROUP BY 1
                HAVING bool_and( a.attnotnull )
                ORDER BY 1 LIMIT 1';
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
    WHILE v_counter <= array_length(v_cols,1) LOOP
        v_trigger_func := v_trigger_func||'v_'||v_cols[v_counter]||' '||v_types[v_counter]||'; ';
        v_counter := v_counter + 1;
    END LOOP;
    v_trigger_func := v_trigger_func || 'v_del_time timestamptz; ';
    v_counter := 1;
    v_trigger_func := v_trigger_func || ' BEGIN 
        IF TG_OP = ''INSERT'' THEN ';
    WHILE v_counter <= array_length(v_pk_name,1) LOOP
        v_trigger_func := v_trigger_func||' v_'||v_pk_name[v_counter]||' := NEW.'||v_pk_name[v_counter]||'; ';
        v_counter := v_counter + 1;
    END LOOP;
    v_counter := 1;
    v_trigger_func := v_trigger_func || ' ELSIF TG_OP = ''UPDATE'' THEN  ';
    WHILE v_counter <= array_length(v_pk_name,1) LOOP
        v_trigger_func := v_trigger_func||' v_'||v_pk_name[v_counter]||' := OLD.'||v_pk_name[v_counter]||'; ';
        v_counter := v_counter + 1;
    END LOOP;
    v_counter := 1;
    v_trigger_func := v_trigger_func || ' ELSIF TG_OP = ''DELETE'' THEN  ';
    WHILE v_counter <= array_length(v_cols,1) LOOP
        v_trigger_func := v_trigger_func||' v_'||v_cols[v_counter]||' := OLD.'||v_cols[v_counter]||'; ';
        v_counter := v_counter + 1;
    END LOOP;
    v_trigger_func := v_trigger_func || 'v_del_time := clock_timestamp(); ';
    v_counter := 1;
    v_trigger_func := v_trigger_func || ' END IF; INSERT INTO @extschema@.'||v_src_table_name||'_pgq ('||v_cols_csv||', mimeo_source_deleted) ';
    v_trigger_func := v_trigger_func || ' VALUES (';
    WHILE v_counter <= array_length(v_cols,1) LOOP
        IF v_counter > 1 THEN
            v_trigger_func := v_trigger_func || ', ';
        END IF;
        v_trigger_func := v_trigger_func||'v_'||v_cols[v_counter];
        v_counter := v_counter + 1;
    END LOOP;
    v_trigger_func := v_trigger_func ||', v_del_time); RETURN NULL; END $_$;';

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

v_insert_refresh_config := 'INSERT INTO @extschema@.refresh_config_logdel(source_table, dest_table, dblink, control, pk_field, pk_type, last_run, filter, condition) VALUES('
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


/*
 *  Snapshot maker function. Optional custom destination table name.
 */
CREATE FUNCTION snapshot_maker(
    p_src_table text
    , p_dblink_id int
    , p_dest_table text DEFAULT NULL
    , p_index boolean DEFAULT true
    , p_filter text[] DEFAULT NULL
    , p_condition text DEFAULT NULL
    , p_pulldata boolean DEFAULT true) 
RETURNS void
    LANGUAGE plpgsql
    AS $$
DECLARE

v_data_source               text;
v_insert_refresh_config     text;

BEGIN

SELECT data_source INTO v_data_source FROM @extschema@.dblink_mapping WHERE data_source_id = p_dblink_id; 
IF NOT FOUND THEN
	RAISE EXCEPTION 'ERROR: Database link ID does not exist in @extschema@.dblink_mapping: %', p_dblink_id; 
END IF;  

IF p_dest_table IS NULL THEN
    p_dest_table := p_src_table;
END IF;

v_insert_refresh_config := 'INSERT INTO @extschema@.refresh_config_snap(source_table, dest_table, dblink, filter, condition) VALUES('||quote_literal(p_src_table)||', '||quote_literal(p_dest_table)||','||p_dblink_id||','||COALESCE(quote_literal(p_filter), 'NULL')||','||COALESCE(quote_literal(p_condition), 'NULL')||')';

RAISE NOTICE 'Inserting record in @extschema@.refresh_config';
EXECUTE v_insert_refresh_config;	
RAISE NOTICE 'Insert successful';	

RAISE NOTICE 'attempting first snapshot';
EXECUTE 'SELECT @extschema@.refresh_snap('||quote_literal(p_dest_table)||', p_index := '||p_index||', p_pulldata := '||p_pulldata||')'; 

RAISE NOTICE 'attempting second snapshot';
EXECUTE 'SELECT @extschema@.refresh_snap('||quote_literal(p_dest_table)||', p_index := '||p_index||', p_pulldata := '||p_pulldata||')';

RAISE NOTICE 'all done';

RETURN;
END
$$;


/*
 *  Updater maker function.
 */ 
CREATE FUNCTION updater_maker(
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
    , p_pk_type text[] DEFAULT NULL) 
RETURNS void
    LANGUAGE plpgsql
    AS $$
DECLARE

v_data_source               text;
v_dblink_schema             text;
v_dest_check                text;
v_dest_schema_name          text;
v_dest_table_name           text;
v_dst_active                boolean;
v_field                     text;
v_insert_refresh_config     text;
v_key_type                  text;
v_max_timestamp             timestamptz;
v_old_search_path           text;
v_pk_name                   text[] := p_pk_name;
v_pk_type                   text[] := p_pk_type;
v_remote_key_sql            text;
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

IF position('.' in p_dest_table) > 0 THEN
    v_dest_schema_name := split_part(p_dest_table, '.', 1); 
    v_dest_table_name := split_part(p_dest_table, '.', 2);
END IF;

PERFORM dblink_connect('mimeo_updater', @extschema@.auth(p_dblink_id));

IF p_pk_name IS NULL AND p_pk_type IS NULL THEN
    -- Either gets the primary key or it gets the first unique index in alphabetical order by index name
    v_remote_key_sql := 'SELECT
                    CASE
                        WHEN i.indisprimary IS true THEN ''primary''
                        WHEN i.indisunique IS true THEN ''unique''
                    END AS key_type,
                    array_agg( a.attname ) AS indkey_names,
                    array_agg(format_type(a.atttypid, a.atttypmod)::text) AS indkey_types
                FROM
                    pg_index i
                    JOIN pg_attribute a ON i.indrelid = a.attrelid AND a.attnum = any( i.indkey )
                WHERE
                    i.indrelid = '||quote_literal(p_src_table)||'::regclass
                    AND ( i.indisprimary OR i.indisunique )
                GROUP BY 1
                HAVING bool_and( a.attnotnull )
                ORDER BY 1 LIMIT 1';
    EXECUTE 'SELECT key_type, indkey_names, indkey_types FROM dblink(''mimeo_updater'', '||quote_literal(v_remote_key_sql)||') t (key_type text, indkey_names text[], indkey_types text[])' 
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

-- Only create destination table if it doesn't already exist
SELECT schemaname||'.'||tablename INTO v_dest_check FROM pg_tables WHERE schemaname = v_dest_schema_name AND tablename = v_dest_table_name;
IF v_dest_check IS NULL THEN

    v_insert_refresh_config := 'INSERT INTO @extschema@.refresh_config_snap(source_table, dest_table, dblink, filter, condition) VALUES('
        ||quote_literal(p_src_table)||', '||quote_literal(p_dest_table)||', '||p_dblink_id||','
        ||COALESCE(quote_literal(p_filter), 'NULL')||','||COALESCE(quote_literal(p_condition), 'NULL')||')';    

    RAISE NOTICE 'Snapshotting source table to pull all current source data...';
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
    RAISE NOTICE 'Destination table % already exists. No data or indexes was pulled from source', p_dest_table;
END IF;

PERFORM dblink_disconnect('mimeo_updater');

RAISE NOTICE 'Getting the maximum destination timestamp...';
EXECUTE 'SELECT max('||p_control_field||') FROM '||p_dest_table||';' INTO v_max_timestamp;

v_dst_active := @extschema@.dst_utc_check();

v_insert_refresh_config := 'INSERT INTO @extschema@.refresh_config_updater(source_table, dest_table, dblink, control, boundary, pk_field, pk_type, last_value, last_run, dst_active, filter, condition) VALUES('
    ||quote_literal(p_src_table)||', '||quote_literal(p_dest_table)||', '|| p_dblink_id||', '||quote_literal(p_control_field)||', '''
    ||p_boundary||'''::interval, '||quote_literal(v_pk_name)||', '||quote_literal(v_pk_type)||', '
    ||quote_literal(COALESCE(v_max_timestamp, CURRENT_TIMESTAMP))||','||quote_literal(CURRENT_TIMESTAMP)||','||v_dst_active||','
    ||COALESCE(quote_literal(p_filter), 'NULL')||','||COALESCE(quote_literal(p_condition), 'NULL')||')';

RAISE NOTICE 'Inserting data into config table';
EXECUTE v_insert_refresh_config;

-- Remove temp snap from config
EXECUTE 'DELETE FROM @extschema@.refresh_config_snap WHERE source_table = '||quote_literal(p_src_table)||' AND dest_table = '||quote_literal(p_dest_table);

EXECUTE 'SELECT set_config(''search_path'','''||v_old_search_path||''',''false'')';

RAISE NOTICE 'Done';

RETURN;

EXCEPTION
    WHEN OTHERS THEN
        EXECUTE 'SELECT set_config(''search_path'',''@extschema@,'||v_dblink_schema||''',''false'')';
        IF dblink_get_connections() @> '{mimeo_updater}' THEN
            PERFORM dblink_disconnect('mimeo_updater');
        END IF;
        EXECUTE 'SELECT set_config(''search_path'','''||v_old_search_path||''',''false'')';
        RAISE EXCEPTION '%', SQLERRM;  
END  
$$;


/*
 *  DML destroyer function. Pass ARCHIVE to keep table intact.
 */
CREATE OR REPLACE FUNCTION dml_destroyer(p_dest_table text, p_archive_option text) RETURNS void
    LANGUAGE plpgsql
    AS $$
    
DECLARE

v_dblink            int;
v_dblink_schema     text;
v_dest_table        text;
v_drop_function     text;
v_drop_q_table      text;
v_drop_trigger      text;
v_old_search_path   text;
v_src_table         text;
v_table_name        text;
    
BEGIN

SELECT nspname INTO v_dblink_schema FROM pg_namespace n, pg_extension e WHERE e.extname = 'dblink' AND e.extnamespace = n.oid;
SELECT current_setting('search_path') INTO v_old_search_path;
EXECUTE 'SELECT set_config(''search_path'',''@extschema@,'||v_dblink_schema||',public'',''false'')';

SELECT source_table, dest_table, dblink INTO v_src_table, v_dest_table, v_dblink
		FROM @extschema@.refresh_config_dml WHERE dest_table = p_dest_table;
IF NOT FOUND THEN
		RAISE NOTICE 'This table is not set up for dml replication: %', v_dest_table;
ELSE
    v_table_name := replace(v_src_table, '.', '_');

    v_drop_function := 'DROP FUNCTION IF EXISTS @extschema@.'||v_table_name||'_mimeo_queue()';
    v_drop_trigger := 'DROP TRIGGER IF EXISTS '||v_table_name||'_mimeo_trig ON '||v_src_table;
    v_drop_q_table := 'DROP TABLE IF EXISTS @extschema@.'||v_table_name||'_pgq';

    RAISE NOTICE 'Removing mimeo objects from source database if they exist (trigger, function, queue table)';
    PERFORM dblink_connect('mimeo_dml_destroy', @extschema@.auth(v_dblink));
    PERFORM dblink_exec('mimeo_dml_destroy', v_drop_trigger);
    PERFORM dblink_exec('mimeo_dml_destroy', v_drop_function);
    PERFORM dblink_exec('mimeo_dml_destroy', v_drop_q_table);
    PERFORM dblink_disconnect('mimeo_dml_destroy');

    IF p_archive_option != 'ARCHIVE' THEN 
        RAISE NOTICE 'Destination table destroyed: %', v_dest_table;
        EXECUTE 'DROP TABLE IF EXISTS ' || v_dest_table;
    ELSE
        RAISE NOTICE 'Archive option set. Destination table NOT destroyed: %', v_dest_table; 
    END IF;

    RAISE NOTICE 'Removing config data';
    EXECUTE 'DELETE FROM @extschema@.refresh_config_dml WHERE dest_table = ' || quote_literal(v_dest_table);

    RAISE NOTICE 'Done';
END IF;

EXECUTE 'SELECT set_config(''search_path'','''||v_old_search_path||''',''false'')';

EXCEPTION
    WHEN OTHERS THEN
        EXECUTE 'SELECT set_config(''search_path'',''@extschema@,'||v_dblink_schema||''',''false'')';
        IF dblink_get_connections() @> '{mimeo_dml_destroy}' THEN
            PERFORM dblink_disconnect('mimeo_dml_destroy');
        END IF;
        EXECUTE 'SELECT set_config(''search_path'','''||v_old_search_path||''',''false'')';
        RAISE EXCEPTION '%', SQLERRM;    
END
$$;


/*
 *  Logdel destroyer function. Pass ARCHIVE to keep table intact.
 */
CREATE OR REPLACE FUNCTION logdel_destroyer(p_dest_table text, p_archive_option text) RETURNS void
    LANGUAGE plpgsql
    AS $$
    
DECLARE

v_dblink            int;
v_dblink_schema     text;
v_dest_table        text;
v_drop_function     text;
v_drop_q_table      text;
v_drop_trigger      text;
v_old_search_path   text;
v_src_table         text;
v_table_name        text;
    
BEGIN

SELECT nspname INTO v_dblink_schema FROM pg_namespace n, pg_extension e WHERE e.extname = 'dblink' AND e.extnamespace = n.oid;
SELECT current_setting('search_path') INTO v_old_search_path;
EXECUTE 'SELECT set_config(''search_path'',''@extschema@,'||v_dblink_schema||',public'',''false'')';

SELECT source_table, dest_table, dblink INTO v_src_table, v_dest_table, v_dblink
		FROM @extschema@.refresh_config_logdel WHERE dest_table = p_dest_table;
IF NOT FOUND THEN
		RAISE NOTICE 'This table is not set up for logdel replication: %', v_dest_table;
ELSE
    v_table_name := replace(v_src_table, '.', '_');

    v_drop_function := 'DROP FUNCTION IF EXISTS @extschema@.'||v_table_name||'_mimeo_queue()';
    v_drop_trigger := 'DROP TRIGGER IF EXISTS '||v_table_name||'_mimeo_trig ON '||v_src_table;
    v_drop_q_table := 'DROP TABLE IF EXISTS @extschema@.'||v_table_name||'_pgq';

    RAISE NOTICE 'Removing mimeo objects from source database (trigger, function, queue table)';
    PERFORM dblink_connect('mimeo_logdel_destroy', @extschema@.auth(v_dblink));
    PERFORM dblink_exec('mimeo_logdel_destroy', v_drop_trigger);
    PERFORM dblink_exec('mimeo_logdel_destroy', v_drop_function);
    PERFORM dblink_exec('mimeo_logdel_destroy', v_drop_q_table);
    PERFORM dblink_disconnect('mimeo_logdel_destroy');

    IF p_archive_option != 'ARCHIVE' THEN 
        RAISE NOTICE 'Destination table destroyed: %', v_dest_table;
        EXECUTE 'DROP TABLE IF EXISTS ' || v_dest_table;
    ELSE
        RAISE NOTICE 'Archive option set. Destination table NOT destroyed: %', v_dest_table; 
    END IF;

    RAISE NOTICE 'Removing config data';
    EXECUTE 'DELETE FROM @extschema@.refresh_config_logdel WHERE dest_table = ' || quote_literal(v_dest_table);	

    RAISE NOTICE 'Done';
END IF;

EXECUTE 'SELECT set_config(''search_path'','''||v_old_search_path||''',''false'')';

EXCEPTION
    WHEN OTHERS THEN
        EXECUTE 'SELECT set_config(''search_path'',''@extschema@,'||v_dblink_schema||''',''false'')';
        IF dblink_get_connections() @> '{mimeo_logdel_destroy}' THEN
            PERFORM dblink_disconnect('mimeo_logdel_destroy');
        END IF;
        EXECUTE 'SELECT set_config(''search_path'','''||v_old_search_path||''',''false'')';
        RAISE EXCEPTION '%', SQLERRM;    

END
$$;


/*
 *  Snap refresh to repull all table data
 */
CREATE FUNCTION refresh_snap(p_destination text, p_index boolean DEFAULT true, p_debug boolean DEFAULT false, p_pulldata boolean DEFAULT true) RETURNS void
    LANGUAGE plpgsql SECURITY DEFINER
    AS $$
DECLARE

v_adv_lock          boolean; 
v_cols_n_types      text;
v_cols              text;
v_condition         text;
v_create_sql        text;
v_dblink_schema     text;
v_dblink            int;
v_dest_table        text;
v_exists            int;
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
v_rcols_array       text[];
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
v_view_definition   text;

BEGIN

IF p_debug IS DISTINCT FROM true THEN
    PERFORM set_config( 'client_min_messages', 'notice', true );
END IF;

v_job_name := 'Refresh Snap: '||p_destination;

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
    PERFORM update_step(v_step_id, 'OK','Found concurrent job. Exiting gracefully');
    PERFORM close_job(v_job_id);
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

v_exists := strpos(v_view_definition, 'snap1');
  IF v_exists > 0 THEN
    v_snap := '_snap2';
    ELSE
    v_snap := '_snap1';
 END IF;
v_refresh_snap := v_dest_table||v_snap;
PERFORM gdb(p_debug,'v_refresh_snap: '||v_refresh_snap::text);

PERFORM dblink_connect('mimeo_refresh_snap', @extschema@.auth(v_dblink));

v_remote_sql := 'SELECT array_to_string(array_agg(attname),'','') as cols, array_to_string(array_agg(attname||'' ''||format_type(atttypid, atttypmod)::text),'','') as cols_n_types FROM pg_attribute WHERE attrelid = '||quote_literal(v_source_table)||'::regclass AND attnum > 0 AND attisdropped is false';

-- Apply column filters if used
IF v_filter IS NOT NULL THEN
    v_remote_sql := v_remote_sql || ' AND ARRAY[attname::text] <@ '||quote_literal(v_filter);
END IF;

v_remote_sql := 'SELECT cols, cols_n_types FROM dblink(''mimeo_refresh_snap'', ' || quote_literal(v_remote_sql) || ') t (cols text, cols_n_types text)';
perform gdb(p_debug,'v_remote_sql: '||v_remote_sql);
EXECUTE v_remote_sql INTO v_cols, v_cols_n_types;  
perform gdb(p_debug,'v_cols: '||v_cols);
perform gdb(p_debug,'v_cols_n_types: '||v_cols_n_types);

v_remote_sql := 'SELECT '||v_cols||' FROM '||v_source_table;
-- Used by p_pull options in all maker functions to be able to create a replication job but pull no data
IF p_pulldata = false THEN
    v_remote_sql := v_remote_sql || ' LIMIT 0';
ELSIF v_condition IS NOT NULL THEN
    v_remote_sql := v_remote_sql || ' ' || v_condition;
END IF;

v_insert_sql := 'INSERT INTO ' || v_refresh_snap || ' SELECT '||v_cols||' FROM dblink(''mimeo_refresh_snap'','||quote_literal(v_remote_sql)||') t ('||v_cols_n_types||')';

PERFORM update_step(v_step_id, 'OK','Done');

v_step_id := add_step(v_job_id,'Truncate non-active snap table');

-- Create snap table if it doesn't exist
SELECT string_to_array(v_refresh_snap, '.') AS oparts INTO v_parts;
SELECT INTO v_table_exists count(1) FROM pg_tables
    WHERE  schemaname = v_parts.oparts[1] AND
           tablename = v_parts.oparts[2];
IF v_table_exists = 0 THEN

    PERFORM gdb(p_debug,'Snap table does not exist. Creating... ');
    
    v_create_sql := 'CREATE TABLE ' || v_refresh_snap || ' (' || v_cols_n_types || ')';
    perform gdb(p_debug,'v_create_sql: '||v_create_sql::text);
    EXECUTE v_create_sql;
ELSE 

/* Check local column definitions against remote and recreate table if different. Allows automatic recreation of
        snap tables if columns change (add, drop type change)  */  
    v_local_sql := 'SELECT array_agg(attname||'' ''||format_type(atttypid, atttypmod)::text) as cols_n_types FROM pg_attribute WHERE attnum > 0 AND attisdropped is false AND attrelid = ' || quote_literal(v_refresh_snap) || '::regclass'; 
        
    PERFORM gdb(p_debug,'v_local_sql: '||v_local_sql::text);

    EXECUTE v_local_sql INTO v_lcols_array;
    SELECT string_to_array(v_cols_n_types, ',') AS cols INTO v_rcols_array;

    -- Check to see if there's a change in the column structure on the remote
    FOREACH v_r IN ARRAY v_rcols_array LOOP
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
        v_create_sql := 'CREATE TABLE ' || v_refresh_snap || ' (' || v_cols_n_types || ')';
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

-- Create indexes if new table was created
IF (v_table_exists = 0 OR v_match = 'f') AND p_index = true THEN
    v_remote_index_sql := 'SELECT
                CASE
                    WHEN i.indisprimary IS true THEN ''primary''
                    WHEN i.indisunique IS true THEN ''unique''
                    ELSE ''index''
                END AS key_type,
                array_agg( a.attname ) AS indkey_names
            FROM
                pg_index i
                JOIN pg_attribute a ON i.indrelid = a.attrelid AND a.attnum = any( i.indkey )
            WHERE
                i.indrelid = '||quote_literal(v_source_table)||'::regclass';
    IF v_filter IS NOT NULL THEN
        v_remote_index_sql := v_remote_index_sql || ' AND ARRAY[a.attname::text] <@ '||quote_literal(v_filter);
    END IF;
    v_remote_index_sql := v_remote_index_sql || ' GROUP BY i.indexrelid::regclass, key_type';
    FOR v_row IN EXECUTE 'SELECT key_type, indkey_names FROM dblink(''mimeo_refresh_snap'', '||quote_literal(v_remote_index_sql)||') t (key_type text, indkey_names text[])' LOOP
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
    END LOOP;
END IF;

-- populating snap table
v_step_id := add_step(v_job_id,'Inserting records into local table');
PERFORM gdb(p_debug,'Inserting rows... '||v_insert_sql);
EXECUTE v_insert_sql; 
GET DIAGNOSTICS v_rowcount = ROW_COUNT;
PERFORM update_step(v_step_id, 'OK','Inserted '||v_rowcount||' records');

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

PERFORM dblink_disconnect('mimeo_refresh_snap');

-- Ensure old search path is reset for the current session
EXECUTE 'SELECT set_config(''search_path'','''||v_old_search_path||''',''false'')';

PERFORM pg_advisory_unlock(hashtext('refresh_snap'), hashtext(v_job_name));

EXCEPTION
    WHEN QUERY_CANCELED THEN
        EXECUTE 'SELECT set_config(''search_path'',''@extschema@,'||v_jobmon_schema||','||v_dblink_schema||''',''false'')';
        IF dblink_get_connections() @> '{mimeo_refresh_snap}' THEN
            PERFORM dblink_disconnect('mimeo_refresh_snap');
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
            v_step_id := jobmon.add_step(v_job_id, 'EXCEPTION before first step logged');
        END IF;
        PERFORM update_step(v_step_id, 'BAD', 'ERROR: '||coalesce(SQLERRM,'unknown'));
        PERFORM fail_job(v_job_id);
        IF dblink_get_connections() @> '{mimeo_refresh_snap}' THEN
            PERFORM dblink_disconnect('mimeo_refresh_snap');
        END IF;
        EXECUTE 'SELECT set_config(''search_path'','''||v_old_search_path||''',''false'')';
        PERFORM pg_advisory_unlock(hashtext('refresh_snap'), hashtext(v_job_name));
        RAISE EXCEPTION '%', SQLERRM;
END
$$;
