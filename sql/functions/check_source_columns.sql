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
v_sql                   text;
v_src_schema_name       text;
v_src_table_name        text;

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
    EXECUTE format('SELECT %I.dblink_connect(%L, @extschema@.auth(%s))', v_dblink_schema, 'mimeo_col_check', v_row_dblink.data_source_id);

    FOR v_row_table IN 
        SELECT source_table, dest_table, filter, type FROM @extschema@.refresh_config WHERE dblink = v_row_dblink.data_source_id ORDER BY 1,2
    LOOP
        v_sql := format('SELECT schemaname, tablename
            FROM (
                SELECT schemaname, tablename 
                FROM pg_catalog.pg_tables 
                WHERE schemaname ||''.''|| tablename = %L
                UNION
                SELECT schemaname, viewname AS tablename
                FROM pg_catalog.pg_views
                WHERE schemaname || ''.'' || viewname = %L
            ) tables LIMIT 1'
        , v_row_table.source_table, v_row_table.source_table);

        EXECUTE format('SELECT schemaname, tablename
                    FROM %I.dblink(%L, %L)
                    AS (schemaname text, tablename text)'
                    , v_dblink_schema, 'mimeo_col_check', v_sql)
                INTO v_src_schema_name, v_src_table_name;

        v_sql := format('SELECT a.attname::text, format_type(a.atttypid, a.atttypmod) AS formatted_type, n.nspname AS schemaname, c.relname AS tablename
                    FROM pg_catalog.pg_attribute a
                    JOIN pg_catalog.pg_class c ON c.oid = a.attrelid
                    JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid
                    WHERE n.nspname = %L
                    AND c.relname = %L
                    AND a.attnum > 0
                    AND attisdropped = false
                    ORDER BY 1,2'
                , v_src_schema_name, v_src_table_name);

        FOR v_row_col IN EXECUTE 
            format('SELECT attname, formatted_type, schemaname, tablename 
            FROM %I.dblink(%L, %L) 
            AS (attname text, formatted_type text, schemaname text, tablename text)'
            , v_dblink_schema, 'mimeo_col_check', v_sql)
        LOOP
            IF v_row_col.attname <> ANY (v_row_table.filter) THEN
                CONTINUE;
            END IF;

            IF v_row_table.type = 'snap' THEN
                SELECT schemaname, viewname INTO dest_schemaname, dest_tablename FROM pg_catalog.pg_views WHERE schemaname||'.'||viewname = v_row_table.dest_table;
            ELSE
                SELECT schemaname, tablename INTO dest_schemaname, dest_tablename FROM pg_catalog.pg_tables WHERE schemaname||'.'||tablename = v_row_table.dest_table;
            END IF;

            SELECT count(*) INTO v_exists
            FROM pg_catalog.pg_attribute a
            JOIN pg_catalog.pg_class c ON a.attrelid = c.oid
            JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid
            WHERE n.nspname = dest_schemaname
            AND c.relname = dest_tablename
            AND attnum > 0
            AND attisdropped = false
            AND attname = v_row_col.attname
            AND format_type(atttypid, atttypmod) = v_row_col.formatted_type;

            -- if column doesn't exist, means it's missing on destination.
            IF v_exists < 1 THEN
                src_schemaname := v_row_col.schemaname;
                src_tablename := v_row_col.tablename;
                missing_column_name := v_row_col.attname;
                missing_column_type := v_row_col.formatted_type;
                data_source := v_row_dblink.data_source_id;
                RETURN NEXT;
            ELSE
                -- Reset output variables used above
                dest_schemaname = NULL;
                dest_tablename = NULL;
            END IF;

        END LOOP; -- end v_row_col

    END LOOP; -- end v_row_table

    EXECUTE format('SELECT %I.dblink_disconnect(%L)', v_dblink_schema, 'mimeo_col_check');

END LOOP; -- end v_row_dblink

END
$$;

