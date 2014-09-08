/*
 *  Snapshot destroyer function. 
 */
CREATE FUNCTION snapshot_destroyer(p_dest_table text, p_keep_table boolean DEFAULT true, p_debug boolean DEFAULT false) RETURNS void
    LANGUAGE plpgsql
    AS $$
DECLARE

v_dest_schema_name  text;
v_dest_table        text;
v_dest_table_name   text;
v_drop_table        text;
v_drop_view         text;
v_exists            int;
v_rename_table      text;
v_snap_suffix       text;
v_src_table         text;
v_table_name        text;
v_view_definition   text;

BEGIN

SELECT source_table, dest_table INTO v_src_table, v_dest_table
    FROM @extschema@.refresh_config_snap WHERE dest_table = p_dest_table;
IF NOT FOUND THEN
    RAISE EXCEPTION 'This table is not set up for snapshot replication: %', v_dest_table;
END IF;

SELECT schemaname, viewname 
INTO v_dest_schema_name, v_dest_table_name
FROM pg_catalog.pg_views
WHERE schemaname||'.'||viewname = v_dest_table;

-- Keep one of the snap tables as a real table with the original view name
IF p_keep_table THEN
    SELECT definition INTO v_view_definition FROM pg_views WHERE schemaname = v_dest_schema_name AND viewname = v_dest_table_name;
    IF v_view_definition IS NULL THEN
        RAISE EXCEPTION 'Destination table view not found: %', v_dest_table;
    END IF;
    v_exists := strpos(v_view_definition, 'snap1');
    IF v_exists > 0 THEN
        v_snap_suffix := '_snap1';
    ELSE
        v_snap_suffix := '_snap2';
    END IF;

    v_drop_view := format('DROP VIEW %I.%I', v_dest_schema_name, v_dest_table_name);
    PERFORM @extschema@.gdb(p_debug, 'v_drop_view '||v_drop_view);
    EXECUTE v_drop_view;
    v_rename_table := format('ALTER TABLE %I.%I RENAME TO %I', v_dest_schema_name, v_dest_table_name||v_snap_suffix, v_dest_table_name);
    PERFORM @extschema@.gdb(p_debug, 'v_rename_table '||v_rename_table);
    EXECUTE v_rename_table;

    RAISE NOTICE 'Destination table NOT destroyed: %. Changed from a view into a plain table', v_dest_table; 
ELSE
    v_drop_view := format('DROP VIEW %I.%I', v_dest_schema_name, v_dest_table_name);
    PERFORM @extschema@.gdb(p_debug, 'v_drop_view '||v_drop_view);
    EXECUTE v_drop_view;
    RAISE NOTICE 'Destination table destroyed: %', v_dest_table;
END IF;

v_drop_table := format('DROP TABLE IF EXISTS %I.%I', v_dest_schema_name, v_dest_table_name||'_snap1');
PERFORM @extschema@.gdb(p_debug, 'v_drop_table '||v_drop_table);
EXECUTE v_drop_table;
v_drop_table := format('DROP TABLE IF EXISTS %I.%I', v_dest_schema_name, v_dest_table_name||'_snap2');
PERFORM @extschema@.gdb(p_debug, 'v_drop_table '||v_drop_table);
EXECUTE v_drop_table;

EXECUTE 'DELETE FROM @extschema@.refresh_config_snap WHERE dest_table = ' || quote_literal(v_dest_table);

END
$$;


