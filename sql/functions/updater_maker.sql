/*
 *  Updater maker function.
 */ 
CREATE FUNCTION updater_maker(p_src_table text, p_control_field text, p_dblink_id int, p_boundary interval DEFAULT '00:10:00', p_dest_table text DEFAULT NULL, p_filter text[] DEFAULT NULL, p_condition text DEFAULT NULL, p_pulldata boolean DEFAULT true, p_pk_field text[] DEFAULT NULL, p_pk_type text[] DEFAULT NULL) RETURNS void
    LANGUAGE plpgsql
    AS $$
DECLARE

v_alter_table               text;
v_data_source               text;
v_dblink_schema             text;
v_dest_check                text;
v_dest_schema_name          text;
v_dest_table_name           text;
v_dst_active                boolean;
v_exists                    int;
v_insert_refresh_config     text;
v_max_timestamp             timestamptz;
v_old_search_path           text;
v_pk_field                  text[] := p_pk_field;
v_pk_field_csv              text := '';
v_pk_type                   text[] := p_pk_field;
v_pk_type_csv               text := '';
v_remote_key_sql            text;
v_snap_suffix               text;
v_update_refresh_config     text;
v_view_definition           text;

BEGIN

SELECT nspname INTO v_dblink_schema FROM pg_namespace n, pg_extension e WHERE e.extname = 'dblink' AND e.extnamespace = n.oid;
SELECT current_setting('search_path') INTO v_old_search_path;
EXECUTE 'SELECT set_config(''search_path'',''@extschema@,'||v_dblink_schema||',public'',''false'')';

IF (p_pk_field IS NULL AND p_pk_type IS NOT NULL) OR (p_pk_field IS NOT NULL AND p_pk_type IS NULL) THEN
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

IF p_pk_field IS NULL AND p_pk_type IS NULL THEN
    PERFORM dblink_connect('mimeo_updater', @extschema@.auth(p_dblink_id));
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
    EXECUTE 'SELECT indkey_names, indkey_types FROM dblink(''mimeo_updater'', '||quote_literal(v_remote_key_sql)||') t (key_type text, indkey_names text[], indkey_types text[])' 
        INTO v_pk_field, v_pk_type;
    PERFORM dblink_disconnect('mimeo_updater');
END IF;

IF v_pk_field IS NULL OR v_pk_type IS NULL THEN
    RAISE EXCEPTION 'Source table has no valid primary key or unique index';
END IF;

v_pk_field_csv := array_to_string(v_pk_field,',');
v_pk_type_csv := array_to_string(v_pk_type,',');

-- Only create destination table if it doesn't already exist
SELECT schemaname||'.'||tablename INTO v_dest_check FROM pg_tables WHERE schemaname = v_dest_schema_name AND tablename = v_dest_table_name;
IF v_dest_check IS NULL THEN

    v_insert_refresh_config := 'INSERT INTO @extschema@.refresh_config_snap(source_table, dest_table, dblink, filter, condition) VALUES('
        ||quote_literal(p_src_table)||', '||quote_literal(p_dest_table)||', '||p_dblink_id||','
        ||COALESCE(quote_literal(p_filter), 'NULL')||','||COALESCE(quote_literal(p_condition), 'NULL')||')';    

    RAISE NOTICE 'Snapshotting source table to pull all current source data...';
    EXECUTE v_insert_refresh_config;	
    
    EXECUTE 'SELECT @extschema@.refresh_snap('||quote_literal(p_dest_table)||', p_pulldata := '||p_pulldata||')'; 
    PERFORM @extschema@.snapshot_destroyer(p_dest_table, 'ARCHIVE');

    v_alter_table := 'ALTER TABLE '||p_dest_table||' ADD PRIMARY KEY('||v_pk_field_csv||');';

    RAISE NOTICE 'Snapshot complete. Adding primary key constraint to table..';
    EXECUTE v_alter_table;
    RAISE NOTICE 'Added successfully';
ELSE
    RAISE NOTICE 'Destination table % already exists. No data was pulled from source', p_dest_table;
END IF;

RAISE NOTICE 'Getting the maximum destination timestamp...';
EXECUTE 'SELECT max('||p_control_field||') FROM '||p_dest_table||';' INTO v_max_timestamp;

v_dst_active := @extschema@.dst_utc_check();

v_insert_refresh_config := 'INSERT INTO @extschema@.refresh_config_updater(source_table, dest_table, dblink, control, boundary, pk_field, pk_type, last_value, dst_active, filter, condition) VALUES('
    ||quote_literal(p_src_table)||', '||quote_literal(p_dest_table)||', '|| p_dblink_id||', '||quote_literal(p_control_field)||', '''
    ||p_boundary||'''::interval, '||quote_literal(v_pk_field)||', '||quote_literal(v_pk_type)||', '
    ||quote_literal(COALESCE(v_max_timestamp, CURRENT_TIMESTAMP))||', '||v_dst_active||','
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
