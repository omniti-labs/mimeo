-- Allow refresh_table() to handle if there are sequences in the destination table. See mimeo.md doc file for required configuration if this is needed.
-- Added p_sequences argument to table_maker() so they can be setup to be reset right away. Maker function does not reset them, just adds them to the config table so the refresh function can do so.

ALTER TABLE @extschema@.refresh_config_table ADD COLUMN sequences text[];
DROP FUNCTION @extschema@.table_maker(text, int, text, boolean, text[], text, boolean);

/*
 * Returns the highest value for the given sequence by checking all columns that use it as a default
 * Awesome query poached from http://stackoverflow.com/a/5943183
 */
CREATE FUNCTION sequence_max_value(oid) RETURNS bigint
    LANGUAGE plpgsql STRICT
    AS $$ 
DECLARE

v_tabrelid      oid;
v_colname       name;
v_row           record;
v_newmax        bigint;

BEGIN

FOR v_tabrelid, v_colname IN 
    SELECT attrelid, attname FROM pg_attribute WHERE (attrelid, attnum) IN (
        SELECT adrelid::regclass, adnum FROM pg_attrdef WHERE oid IN (
            SELECT objid FROM pg_depend WHERE refobjid = $1 AND classid = 'pg_attrdef'::regclass
        )
    ) 
LOOP
    FOR v_row IN EXECUTE 'SELECT max(' || quote_ident(v_colname) || ') FROM ' || v_tabrelid::regclass LOOP
        IF v_newmax IS NULL OR v_row.max > v_newmax THEN
            v_newmax := v_row.max;
        END IF;
    END LOOP;
END LOOP;

RETURN v_newmax;

END
$$;


/*
 *  Plain table refresh function. 
 */
CREATE OR REPLACE FUNCTION refresh_table(p_destination text, p_truncate_cascade boolean DEFAULT NULL, p_debug boolean DEFAULT false) RETURNS void
    LANGUAGE plpgsql
    AS $$
DECLARE

v_adv_lock              boolean;
v_cols                  text[];
v_cols_n_types          text[];
v_condition             text;
v_dblink                int;
v_dblink_name           text;
v_dblink_schema         text;
v_dest_table            text;
v_fetch_sql             text;
v_filter                text;
v_old_search_path       text;
v_post_script           text[];
v_remote_sql            text;
v_rowcount              bigint := 0;
v_seq                   text;
v_seq_max               bigint;
v_sequences             text[];
v_source_table          text;
v_total                 bigint := 0;
v_truncate_cascade      boolean;
v_truncate_sql          text;

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
    , sequences
    , truncate_cascade
INTO v_source_table
    , v_dest_table
    , v_dblink
    , v_filter
    , v_condition
    , v_sequences
    , v_truncate_cascade
FROM refresh_config_table
WHERE dest_table = p_destination; 
IF NOT FOUND THEN
   RAISE EXCEPTION 'ERROR: This table is not set up for plain table replication: %',p_destination; 
END IF;

v_dblink_name := 'mimeo_table_refresh_'||v_dest_table;

IF p_truncate_cascade IS NOT NULL THEN
    v_truncate_cascade := p_truncate_cascade;
END IF;

v_truncate_sql := 'TRUNCATE TABLE '||v_dest_table;
IF v_truncate_cascade THEN
    v_truncate_sql := v_truncate_sql || ' CASCADE';
    RAISE NOTICE 'WARNING! If this table had foreign keys, you have just truncated all referencing tables as well!';
END IF;
EXECUTE v_truncate_sql;

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

-- Reset any sequences given in the parameter to their new value. 
-- Checks all tables that use the given sequence to ensure it's the max for the entire database.
IF v_sequences IS NOT NULL THEN
    FOREACH v_seq IN ARRAY v_sequences LOOP
        SELECT sequence_max_value(c.oid) INTO v_seq_max FROM pg_class c JOIN pg_namespace n ON c.relnamespace = n.oid WHERE n.nspname ||'.'|| c.relname = v_seq;
        IF v_seq_max IS NOT NULL THEN
            PERFORM setval(v_seq, v_seq_max);
        END IF;
    END LOOP;
END IF;

UPDATE refresh_config_table set last_run = CURRENT_TIMESTAMP WHERE dest_table = v_dest_table;

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
 *  Plain table refresh maker function. 
 */
CREATE FUNCTION table_maker(
    p_src_table text
    , p_dblink_id int
    , p_dest_table text DEFAULT NULL
    , p_index boolean DEFAULT true
    , p_filter text[] DEFAULT NULL
    , p_condition text DEFAULT NULL
    , p_sequences text[] DEFAULT NULL
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
v_seq                       text;
v_seq_max                   bigint;

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

v_insert_refresh_config := 'INSERT INTO @extschema@.refresh_config_table(source_table, dest_table, dblink, last_run, filter, condition, sequences) VALUES('
    ||quote_literal(p_src_table)||','||quote_literal(p_dest_table)||','|| p_dblink_id||','
    ||quote_literal(CURRENT_TIMESTAMP)||','||COALESCE(quote_literal(p_filter), 'NULL')||','||COALESCE(quote_literal(p_condition), 'NULL')||','||COALESCE(quote_literal(p_sequences), 'NULL')||');';

RAISE NOTICE 'Inserting data into config table';
EXECUTE v_insert_refresh_config;

-- Remove temp snap from config
EXECUTE 'DELETE FROM @extschema@.refresh_config_snap WHERE source_table = '||quote_literal(p_src_table)||' AND dest_table = '||quote_literal(p_dest_table);

RAISE NOTICE 'Done';
END
$$;
