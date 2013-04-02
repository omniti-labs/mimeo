/*
 *  Plain table refresh function. 
 */
CREATE FUNCTION refresh_table(p_destination text, p_truncate_cascade boolean DEFAULT NULL, p_debug boolean DEFAULT false) RETURNS void
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
v_link_exists           boolean;
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

v_dblink_name := 'mimeo_table_refresh_'||p_destination;

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
   RAISE EXCEPTION 'No configuration found for Refresh Table: %',p_destination; 
END IF;

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
    WHEN QUERY_CANCELED OR OTHERS THEN
        EXECUTE 'SELECT '||v_dblink_schema||'.dblink_get_connections() @> ARRAY['||quote_literal(v_dblink_name)||']' INTO v_link_exists;
        IF v_link_exists THEN
            EXECUTE 'SELECT '||v_dblink_schema||'.dblink_disconnect('||quote_literal(v_dblink_name)||')';
        END IF;
        PERFORM pg_advisory_unlock(hashtext('refresh_table'), hashtext(p_destination));
        RAISE EXCEPTION '%', SQLERRM;
END
$$;
