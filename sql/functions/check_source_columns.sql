/*
 * Check tables on data sources to see if columns have been added or types changed.
 * Returns a record value so that a WHERE condition can be used to ignore tables that aren't desired.
 * If p_data_source_id value is not given, all configured sources are checked.
 */
CREATE FUNCTION check_source_columns(p_data_source_id int DEFAULT NULL
    , OUT dest_schemaname text
    , OUT dest_tablename text
    , OUT src_schemaname text
    , OUT src_tablename text
    , OUT missing_column_name text
    , OUT missing_column_type text
    , OUT data_source int) RETURNS SETOF record
    LANGUAGE plpgsql
    AS $$
DECLARE

v_dblink_schema         text;
v_exists                int;
v_row_dblink            record;
v_row_table             record;
v_row_col               record;

BEGIN

IF p_data_source_id IS NOT NULL THEN
    SELECT count(*) INTO v_exists FROM @extschema@.dblink_mapping_mimeo WHERE data_source_id = p_data_source_id;
    IF v_exists < 1 THEN
        RAISE EXCEPTION 'Given data_source_id (%) does not exist in @extschema@.dblink_mapping_mimeo config table', p_data_source_id;
    END IF;
END IF;

SELECT nspname INTO v_dblink_schema FROM pg_namespace n, pg_extension e WHERE e.extname = 'dblink' AND e.extnamespace = n.oid;

FOR v_row_dblink IN SELECT data_source_id FROM @extschema@.dblink_mapping_mimeo
LOOP
    -- Allow a parameter to choose which data sources are checked. If parameter is NULL, check them all.
    IF p_data_source_id IS NOT NULL THEN
        IF p_data_source_id <> v_row_dblink.data_source_id THEN
            CONTINUE;
        END IF;
    END IF;
    EXECUTE 'SELECT '||v_dblink_schema||'.dblink_connect(''mimeo_col_check'', @extschema@.auth('||v_row_dblink.data_source_id||'))';

    FOR v_row_table IN 
        ( SELECT source_table, dest_table, filter, type FROM @extschema@.refresh_config_snap WHERE dblink = v_row_dblink.data_source_id
        UNION
        SELECT source_table, dest_table, filter, type FROM @extschema@.refresh_config_inserter WHERE dblink = v_row_dblink.data_source_id
        UNION
        SELECT source_table, dest_table, filter, type FROM @extschema@.refresh_config_updater WHERE dblink = v_row_dblink.data_source_id
        UNION
        SELECT source_table, dest_table, filter, type FROM @extschema@.refresh_config_dml WHERE dblink = v_row_dblink.data_source_id
        UNION
        SELECT source_table, dest_table, filter, type FROM @extschema@.refresh_config_logdel WHERE dblink = v_row_dblink.data_source_id
        UNION
        SELECT source_table, dest_table, filter, type FROM @extschema@.refresh_config_table WHERE dblink = v_row_dblink.data_source_id )
        ORDER BY 1,2
    LOOP
        FOR v_row_col IN
            EXECUTE 'SELECT attname, atttypid, atttypmod, schemaname, tablename FROM '||v_dblink_schema||'.dblink(''mimeo_col_check'', 
                ''SELECT a.attname::text, a.atttypid, a.atttypmod, n.nspname AS schemaname, c.relname AS tablename
                FROM pg_attribute a
                JOIN pg_class c ON c.oid = a.attrelid
                JOIN pg_namespace n ON c.relnamespace = n.oid
                WHERE a.attrelid = '''''||v_row_table.source_table||'''''::regclass 
                AND a.attnum > 0
                AND attisdropped = false
                ORDER BY 1,2'') t (attname text, atttypid oid, atttypmod int, schemaname text, tablename text)'
        LOOP
            IF v_row_col.attname <> ANY (v_row_table.filter) THEN
                CONTINUE;
            END IF;
            SELECT count(*) INTO v_exists
            FROM pg_attribute
            WHERE attrelid = v_row_table.dest_table::regclass
            AND attnum > 0
            AND attisdropped = false
            AND attname = v_row_col.attname
            AND atttypid = v_row_col.atttypid
            AND atttypmod = v_row_col.atttypmod;

            -- if column doesn't exist, means it's missing on destination.
            IF v_exists < 1 THEN
                IF v_row_table.type = 'snap' THEN
                    SELECT schemaname, viewname INTO dest_schemaname, dest_tablename FROM pg_catalog.pg_views WHERE schemaname||'.'||viewname = v_row_table.dest_table;
                ELSE
                    SELECT schemaname, tablename INTO dest_schemaname, dest_tablename FROM pg_catalog.pg_tables WHERE schemaname||'.'||tablename = v_row_table.dest_table;
                END IF;
                src_schemaname := v_row_col.schemaname;
                src_tablename := v_row_col.tablename;
                missing_column_name := v_row_col.attname;
                missing_column_type := format_type(v_row_col.atttypid, v_row_col.atttypmod)::text;
                data_source := v_row_dblink.data_source_id;
                RETURN NEXT;
            END IF;

        END LOOP; -- end v_row_col

    END LOOP; -- end v_row_table

    EXECUTE 'SELECT '||v_dblink_schema||'.dblink_disconnect(''mimeo_col_check'')';

END LOOP; -- end v_row_dblink

END
$$;

