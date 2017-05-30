/*
 * Function that can monitor if snapshot tables may be growing too large for full table replication
 * Pass a minimum row count and/or byte size to return any table that grows larger than that one the source
 * Checks all snapshot tables unless the p_destination parameter is set to check a specific one
 */
CREATE FUNCTION snapshot_monitor(p_rowcount bigint DEFAULT NULL::bigint, p_size bigint DEFAULT NULL::bigint, p_destination text DEFAULT NULL::text, p_debug boolean DEFAULT false)
    RETURNS TABLE(dest_tablename text, source_rowcount bigint, source_size bigint)
    LANGUAGE plpgsql
AS $$
DECLARE

v_dblink            int;
v_dblink_name       text;
v_dblink_schema     text;
v_dest_table        text;
v_remote_sql        text;
v_result            bigint;
v_source_table      text;
v_sql               text;
v_table             record;

BEGIN

SELECT nspname INTO v_dblink_schema FROM pg_catalog.pg_namespace n, pg_catalog.pg_extension e WHERE e.extname = 'dblink' AND e.extnamespace = n.oid;

IF p_destination IS NOT NULL THEN
    v_sql := format('SELECT dest_table, dblink, source_table FROM @extschema@.refresh_config_snap WHERE dest_table= %L;' , p_destination); 
ELSE
    v_sql:= 'SELECT dest_table, dblink, source_table FROM @extschema@.refresh_config_snap';
END IF;


FOR v_table IN EXECUTE v_sql
LOOP
    IF p_debug THEN
        RAISE NOTICE 'v_table: %', v_table;
    END IF;
    v_dblink_name := mimeo.check_name_length('mimeo_snap_validation_'||v_table.source_table);

    EXECUTE format('SELECT %I.dblink_connect(%L, @extschema@.auth(%L))', v_dblink_schema, v_dblink_name, v_table.dblink);

    v_remote_sql := format('SELECT pg_total_relation_size(%L);', v_table.source_table);
    v_remote_sql := format('SELECT table_size FROM dblink.dblink(%L, %L) t (table_size bigint)', v_dblink_name, v_remote_sql);

    EXECUTE v_remote_sql INTO v_result;

    source_size := v_result::bigint;

    v_remote_sql := format('SELECT count(*) FROM %s;', v_table.source_table);
    v_remote_sql := format('SELECT table_count FROM %I.dblink(%L, %L) t (table_count int)', v_dblink_schema, v_dblink_name, v_remote_sql);

    EXECUTE v_remote_sql INTO v_result;

    source_rowcount := v_result::bigint;
    dest_tablename:= v_table.source_table;

    EXECUTE format('SELECT %I.dblink_disconnect(%L)', v_dblink_schema, v_dblink_name);

    IF p_debug THEN
        RAISE NOTICE 'p_rowcount: %, source_rowcount: % ', p_rowcount, source_rowcount;
        RAISE NOTICE 'p_size: %, source_size: % ', p_size, source_size;
    END IF;

    IF (p_rowcount IS NULL AND p_size IS NULL) THEN
        RETURN NEXT;
    ELSIF (p_rowcount IS NULL AND p_size IS NOT NULL) 
    AND (source_size >= p_size) THEN 
        RETURN NEXT;
    ELSIF (p_rowcount IS NOT NULL AND p_size IS NULL) 
    AND (source_rowcount >= p_rowcount) THEN 
        RETURN NEXT;
    ELSIF (p_rowcount IS NOT NULL AND p_size IS NOT NULL) 
    AND (source_rowcount >= p_rowcount OR source_size >= p_size) THEN 
        RETURN NEXT; 
    END IF;

END LOOP;

EXCEPTION 
WHEN QUERY_CANCELED THEN
    EXECUTE format('SELECT %I.dblink_disconnect(%L)', v_dblink_schema, v_dblink_name);
    RAISE EXCEPTION '%', SQLERRM;
WHEN OTHERS THEN 
    EXECUTE format('SELECT %I.dblink_disconnect(%L)', v_dblink_schema, v_dblink_name);
    RAISE EXCEPTION '%', SQLERRM;
END
$$;
