/*
 *  Plain table destroyer function. 
 */
CREATE FUNCTION table_destroyer(p_dest_table text, p_keep_table boolean DEFAULT true, p_debug boolean DEFAULT false) RETURNS void
    LANGUAGE plpgsql
    AS $$
DECLARE

v_dest_schema_name  text;
v_dest_table        text;
v_dest_table_name   text;
v_drop_sql          text;

BEGIN

SELECT dest_table INTO v_dest_table
    FROM @extschema@.refresh_config_table WHERE dest_table = p_dest_table;
IF NOT FOUND THEN
    RAISE NOTICE 'This table is not set up for plain table replication: %', p_dest_table;
ELSE
    SELECT schemaname, tablename 
    INTO v_dest_schema_name, v_dest_table_name
    FROM pg_catalog.pg_tables
    WHERE schemaname||'.'||tablename = v_dest_table;

    IF p_keep_table THEN
        RAISE NOTICE 'Destination table NOT destroyed: %', v_dest_table; 
    ELSE
        v_drop_sql := format('DROP TABLE IF EXISTS %I.%I', v_dest_schema_name, v_dest_table_name);
        PERFORM @extschema@.gdb(p_debug, 'v_drop_sql: '||v_drop_sql);
        EXECUTE v_drop_sql;
        RAISE NOTICE 'Destination table destroyed: %', v_dest_table;
    END IF;

    EXECUTE 'DELETE FROM @extschema@.refresh_config_table WHERE dest_table = ' || quote_literal(v_dest_table);
END IF;

END
$$;


