-- New refresh_table option for just doing a straight truncate and repull for a regular table. Only adding as a minor feature because other changes do not affect API and it's not recommended as a regular refresh job if possible. Also refresh_table() does not currently log to pg_jobmon, so cannot be monitored and will not set off any alerts if/when this refresh type fails. What this is useful for is having a way to get data from production to a staging/dev database where you still want to be able to edit the destination table. Could do that with Incremental or DML, but this avoids requiring any primary keys, control columns or write access on the source database.
-- Removed custom enum type. Made things much more complicated than they needed to be. Enums are REALLY bad in extensions since you can never just add a new value in an update.
-- The commands to remove the batch limit defaults & set the default boundaries for incremental replication were missing from the mimeo--0.9.3--0.10.1 bypass update file. If that file was used to update mimeo, the defaults were never changed and limits were still being set for new refresh jobs. Run those here again.

ALTER TABLE @extschema@.refresh_config DROP "type";
ALTER TABLE @extschema@.refresh_config_snap DROP "type";
ALTER TABLE @extschema@.refresh_config_inserter DROP "type";
ALTER TABLE @extschema@.refresh_config_updater DROP "type";
ALTER TABLE @extschema@.refresh_config_dml DROP "type";
ALTER TABLE @extschema@.refresh_config_logdel DROP "type";
DROP TYPE @extschema@.refresh_type;
ALTER TABLE @extschema@.refresh_config ADD "type" text;
ALTER TABLE @extschema@.refresh_config_snap ALTER COLUMN type SET DEFAULT 'snap';
ALTER TABLE @extschema@.refresh_config_snap ADD CONSTRAINT refresh_config_snap_type_check CHECK (type = 'snap');
UPDATE @extschema@.refresh_config_snap SET type = 'snap';
ALTER TABLE @extschema@.refresh_config_inserter ALTER COLUMN type SET DEFAULT 'inserter';
ALTER TABLE @extschema@.refresh_config_inserter ADD CONSTRAINT refresh_config_inserter_type_check CHECK (type = 'inserter');
UPDATE @extschema@.refresh_config_inserter SET type = 'inserter';
ALTER TABLE @extschema@.refresh_config_updater ALTER COLUMN type SET DEFAULT 'updater';
ALTER TABLE @extschema@.refresh_config_updater ADD CONSTRAINT refresh_config_updater_type_check CHECK (type = 'updater');
UPDATE @extschema@.refresh_config_updater SET type = 'updater';
ALTER TABLE @extschema@.refresh_config_dml ALTER COLUMN type SET DEFAULT 'dml';
ALTER TABLE @extschema@.refresh_config_dml ADD CONSTRAINT refresh_config_dml_type_check CHECK (type = 'dml');
UPDATE @extschema@.refresh_config_dml SET type = 'dml';
ALTER TABLE @extschema@.refresh_config_logdel ALTER COLUMN type SET DEFAULT 'logdel';
ALTER TABLE @extschema@.refresh_config_logdel ADD CONSTRAINT refresh_config_logdel_type_check CHECK (type = 'logdel');
UPDATE @extschema@.refresh_config_logdel SET type = 'logdel';

CREATE TABLE refresh_config_table (LIKE @extschema@.refresh_config INCLUDING ALL) INHERITS (@extschema@.refresh_config);
SELECT pg_catalog.pg_extension_config_dump('refresh_config_table', '');
ALTER TABLE @extschema@.refresh_config_table ADD CONSTRAINT refresh_config_table_dblink_fkey FOREIGN KEY (dblink) REFERENCES @extschema@.dblink_mapping(data_source_id);
ALTER TABLE @extschema@.refresh_config_table ADD CONSTRAINT refresh_config_table_dest_table_pkey PRIMARY KEY (dest_table);
ALTER TABLE @extschema@.refresh_config_table ADD COLUMN source_table text NOT NULL;
ALTER TABLE @extschema@.refresh_config_table ALTER COLUMN type SET DEFAULT 'table';
ALTER TABLE @extschema@.refresh_config_table ADD CONSTRAINT refresh_config_snap_type_check CHECK (type = 'table');

-- Stuff that was missed in the 0.9.3 -> 0.10.1 bypass update
ALTER TABLE @extschema@.refresh_config ALTER batch_limit DROP DEFAULT;
UPDATE @extschema@.refresh_config_inserter SET boundary = '10 minutes'::interval WHERE boundary IS NULL;
UPDATE @extschema@.refresh_config_updater SET boundary = '10 minutes'::interval WHERE boundary IS NULL;
ALTER TABLE @extschema@.refresh_config_inserter ALTER boundary SET DEFAULT '10 minutes'::interval;
ALTER TABLE @extschema@.refresh_config_updater ALTER boundary SET DEFAULT '10 minutes'::interval;
ALTER TABLE @extschema@.refresh_config_inserter ALTER boundary SET NOT NULL;
ALTER TABLE @extschema@.refresh_config_updater ALTER boundary SET NOT NULL;


/*
 *  Plain table refresh maker function. 
 */
CREATE FUNCTION table_maker(
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

IF position('.' in p_dest_table) > 0 AND position('.' in p_src_table) > 0 THEN
    v_dest_schema_name := split_part(p_dest_table, '.', 1); 
    v_dest_table_name := split_part(p_dest_table, '.', 2);
ELSE
    RAISE EXCEPTION 'Source (and destination) table must be schema qualified';
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

v_insert_refresh_config := 'INSERT INTO @extschema@.refresh_config_table(source_table, dest_table, dblink, last_run, filter, condition) VALUES('
    ||quote_literal(p_src_table)||','||quote_literal(p_dest_table)||','|| p_dblink_id||','
    ||quote_literal(CURRENT_TIMESTAMP)||','||COALESCE(quote_literal(p_filter), 'NULL')||','||COALESCE(quote_literal(p_condition), 'NULL')||');';

RAISE NOTICE 'Inserting data into config table';
EXECUTE v_insert_refresh_config;

-- Remove temp snap from config
EXECUTE 'DELETE FROM @extschema@.refresh_config_snap WHERE source_table = '||quote_literal(p_src_table)||' AND dest_table = '||quote_literal(p_dest_table);

RAISE NOTICE 'Done';
END
$$;


/*
 *  Plain table refresh function. 
 */
CREATE FUNCTION refresh_table(p_destination text, p_debug boolean DEFAULT false) RETURNS void
    LANGUAGE plpgsql
    AS $$
DECLARE

v_adv_lock              boolean;
v_cols                  text[];
v_cols_n_types          text[];
v_dblink_name           text;
v_dblink_schema         text;
v_fetch_sql             text;
v_old_search_path       text;
v_source_table          text;
v_dest_table            text;
v_dblink                int;
v_filter                text;
v_condition             text;
v_post_script           text[];
v_rowcount              bigint := 0;
v_total                 bigint := 0;
v_remote_sql            text;

BEGIN

SELECT nspname INTO v_dblink_schema FROM pg_namespace n, pg_extension e WHERE e.extname = 'dblink' AND e.extnamespace = n.oid;

SELECT current_setting('search_path') INTO v_old_search_path;
EXECUTE 'SELECT set_config(''search_path'',''@extschema@,'||v_dblink_schema||',public'',''false'')';

v_adv_lock := pg_try_advisory_lock(hashtext('refresh_table'), hashtext(p_destination));
IF v_adv_lock = 'false' THEN
    RAISE NOTICE 'Found concurrent job. Exiting gracefully';
    EXECUTE 'SELECT set_config(''search_path'','''||v_old_search_path||''',''false'')';
    RETURN;
END IF;

SELECT source_table
    , dest_table
    , dblink
    , filter
    , condition
INTO v_source_table
    , v_dest_table
    , v_dblink
    , v_filter
    , v_condition
FROM refresh_config_table
WHERE dest_table = p_destination; 
IF NOT FOUND THEN
   RAISE EXCEPTION 'ERROR: This table is not set up for plain table replication: %',p_destination; 
END IF;

v_dblink_name := 'mimeo_table_refresh_'||v_dest_table;

EXECUTE 'TRUNCATE TABLE '||v_dest_table;

PERFORM dblink_connect(v_dblink_name, @extschema@.auth(v_dblink));

v_remote_sql := 'SELECT array_agg(attname) as cols, array_agg(attname||'' ''||format_type(atttypid, atttypmod)::text) as cols_n_types FROM pg_attribute WHERE attrelid = '||quote_literal(v_source_table)||'::regclass AND attnum > 0 AND attisdropped is false';
IF v_filter IS NOT NULL THEN
    v_remote_sql := v_remote_sql || ' AND ARRAY[attname::text] <@ '||quote_literal(v_filter);
END IF;
v_remote_sql := 'SELECT cols, cols_n_types FROM dblink('||quote_literal(v_dblink_name)||', ' || quote_literal(v_remote_sql) || ') t (cols text[], cols_n_types text[])';
EXECUTE v_remote_sql INTO v_cols, v_cols_n_types;

v_remote_sql := 'SELECT '|| array_to_string(v_cols, ',') ||' FROM '||v_source_table;
IF v_condition IS NOT NULL THEN
    v_remote_sql := v_remote_sql || ' ' || v_condition;
END IF;  
PERFORM dblink_open(v_dblink_name, 'mimeo_cursor', v_remote_sql);
v_rowcount := 0;
LOOP
    v_fetch_sql := 'INSERT INTO '|| v_dest_table ||' ('|| array_to_string(v_cols, ',') ||') 
        SELECT '||array_to_string(v_cols, ',')||' FROM dblink_fetch('||quote_literal(v_dblink_name)||', ''mimeo_cursor'', 50000) AS ('||array_to_string(v_cols_n_types, ',')||')';
    EXECUTE v_fetch_sql;
    GET DIAGNOSTICS v_rowcount = ROW_COUNT;
    EXIT WHEN v_rowcount = 0;
    v_total := v_total + coalesce(v_rowcount, 0);
    PERFORM gdb(p_debug,'Fetching rows in batches: '||v_total||' done so far.');
END LOOP;
PERFORM dblink_close(v_dblink_name, 'mimeo_cursor');

PERFORM dblink_disconnect(v_dblink_name);

PERFORM pg_advisory_unlock(hashtext('refresh_table'), hashtext(p_destination));

EXECUTE 'SELECT set_config(''search_path'','''||v_old_search_path||''',''false'')';

EXCEPTION
    WHEN QUERY_CANCELED THEN
        EXECUTE 'SELECT set_config(''search_path'',''@extschema@,'||v_dblink_schema||',public'',''false'')';
        IF dblink_get_connections() @> ARRAY[v_dblink_name] THEN
            PERFORM dblink_disconnect(v_dblink_name);
        END IF;
        PERFORM pg_advisory_unlock(hashtext('refresh_table'), hashtext(p_destination));
        EXECUTE 'SELECT set_config(''search_path'','''||v_old_search_path||''',''false'')';
        RAISE EXCEPTION '%', SQLERRM;
    WHEN OTHERS THEN
        EXECUTE 'SELECT set_config(''search_path'',''@extschema@,'||v_dblink_schema||',public'',''false'')';
        IF dblink_get_connections() @> ARRAY[v_dblink_name] THEN
            PERFORM dblink_disconnect(v_dblink_name);
        END IF;
        PERFORM pg_advisory_unlock(hashtext('refresh_table'), hashtext(p_destination));
        EXECUTE 'SELECT set_config(''search_path'','''||v_old_search_path||''',''false'')';
        RAISE EXCEPTION '%', SQLERRM;  
END
$$;


/*
 *  Plain table destroyer function. Pass archive to keep table intact.
 */
CREATE FUNCTION table_destroyer(p_dest_table text, p_archive_option text) RETURNS void
    LANGUAGE plpgsql
    AS $$
DECLARE

v_dest_table        text;
    
BEGIN

SELECT dest_table INTO v_dest_table
    FROM @extschema@.refresh_config_table WHERE dest_table = p_dest_table;
IF NOT FOUND THEN
    RAISE NOTICE 'This table is not set up for plain table replication: %', p_dest_table;
ELSE
    -- Keep destination table
    IF p_archive_option != 'ARCHIVE' THEN 
        EXECUTE 'DROP TABLE IF EXISTS ' || v_dest_table;
        RAISE NOTICE 'Destination table destroyed: %', v_dest_table;
    ELSE
        RAISE NOTICE 'Archive option set. Destination table NOT destroyed: %', v_dest_table; 
    END IF;

    EXECUTE 'DELETE FROM @extschema@.refresh_config_table WHERE dest_table = ' || quote_literal(v_dest_table);	
END IF;

END
$$;
