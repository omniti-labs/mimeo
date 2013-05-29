/*
 *  Snapshot destroyer function. 
 */
CREATE FUNCTION snapshot_destroyer(p_dest_table text, p_keep_table boolean DEFAULT true) RETURNS void
    LANGUAGE plpgsql
    AS $$
DECLARE

v_dest_table        text;
v_exists            int;
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

-- Keep one of the snap tables as a real table with the original view name
IF p_keep_table THEN
    SELECT definition INTO v_view_definition FROM pg_views WHERE schemaname || '.' || viewname = v_dest_table;
    v_exists := strpos(v_view_definition, 'snap1');
    IF v_exists > 0 THEN
        v_snap_suffix := '_snap1';
    ELSE
        v_snap_suffix := '_snap2';
    END IF;
    
    IF position('.' in p_dest_table) > 0 THEN 
        v_table_name := substring(p_dest_table from position('.' in p_dest_table)+1);
    ELSE
        v_table_name := p_dest_table;
    END IF;

    EXECUTE 'DROP VIEW ' || v_dest_table;
    EXECUTE 'ALTER TABLE '||v_dest_table||v_snap_suffix||' RENAME TO '||v_table_name;
    
    RAISE NOTICE 'Destination table NOT destroyed: %. Changed from a view into a plain table', v_dest_table; 
ELSE
    EXECUTE 'DROP VIEW ' || v_dest_table;    
    RAISE NOTICE 'Destination table destroyed: %', v_dest_table;
END IF;

EXECUTE 'DROP TABLE IF EXISTS ' || v_dest_table || '_snap1';
EXECUTE 'DROP TABLE IF EXISTS ' || v_dest_table || '_snap2';

EXECUTE 'DELETE FROM @extschema@.refresh_config_snap WHERE dest_table = ' || quote_literal(v_dest_table);

END
$$;
