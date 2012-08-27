/*
 *  Inserter destroyer function. Pass archive to keep table intact.
 */
CREATE FUNCTION inserter_destroyer(p_dest_table text, p_archive_option text) RETURNS void
    LANGUAGE plpgsql
    AS $$
    
DECLARE

v_dest_table        text;
v_src_table         text;
    
BEGIN

SELECT source_table, dest_table INTO v_src_table, v_dest_table
		FROM @extschema@.refresh_config_inserter WHERE dest_table = p_dest_table;
IF NOT FOUND THEN
		RAISE EXCEPTION 'This table is not set up for inserter replication: %', v_dest_table;
END IF;

-- Keep destination table
IF p_archive_option != 'ARCHIVE' THEN 
    EXECUTE 'DROP TABLE ' || v_dest_table;
END IF;

EXECUTE 'DELETE FROM @extschema@.refresh_config_inserter WHERE dest_table = ' || quote_literal(v_dest_table);	

END
$$;
