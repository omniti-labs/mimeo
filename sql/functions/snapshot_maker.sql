/*
 *  Snapshot maker function. Optional custom destination table name.
 */
CREATE FUNCTION snapshot_maker(p_src_table text, p_dblink_id int, p_dest_table text DEFAULT NULL) RETURNS void
    LANGUAGE plpgsql
    AS $$
DECLARE

v_data_source               text;
v_insert_refresh_config     text;

BEGIN

SELECT data_source INTO v_data_source FROM @extschema@.dblink_mapping WHERE data_source_id = p_dblink_id; 
IF NOT FOUND THEN
	RAISE EXCEPTION 'ERROR: Database link ID does not exist in @extschema@.dblink_mapping: %', p_dblink_id; 
END IF;  

IF p_dest_table IS NULL THEN
    p_dest_table := p_src_table;
END IF;

v_insert_refresh_config := 'INSERT INTO @extschema@.refresh_config_snap(source_table, dest_table, dblink) VALUES('
    ||quote_literal(p_src_table)||', '||quote_literal(p_dest_table)||', '|| p_dblink_id||');';


RAISE NOTICE 'Inserting record in @extschema@.refresh_config';
EXECUTE v_insert_refresh_config;	
RAISE NOTICE 'Insert successful';	

RAISE NOTICE 'attempting first snapshot';
PERFORM @extschema@.refresh_snap(p_dest_table);

RAISE NOTICE 'attempting second snapshot';
PERFORM @extschema@.refresh_snap(p_dest_table);

RAISE NOTICE 'all done';

RETURN;

END
$$;
