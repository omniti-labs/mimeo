/*
 *  Inserter maker function. Optional custom destination table name.
 */
CREATE FUNCTION inserter_maker(p_src_table text, p_control_field text, p_dblink_id int, p_boundary interval DEFAULT '00:10:00', p_dest_table text DEFAULT NULL) RETURNS void
    LANGUAGE plpgsql
    AS $$
DECLARE

v_data_source               text;
v_dst_active                boolean;
v_exists                    int;
v_insert_refresh_config     text;
v_max_timestamp             timestamptz;
v_snap_suffix               text;
v_view_definition           text;

BEGIN

SELECT data_source INTO v_data_source FROM @extschema@.dblink_mapping WHERE data_source_id = p_dblink_id; 
IF NOT FOUND THEN
	RAISE EXCEPTION 'ERROR: database link ID is incorrect %', p_dblink_id; 
END IF;  

IF p_dest_table IS NULL THEN
    p_dest_table := p_src_table;
END IF;

-- Temp snap config
v_insert_refresh_config := 'INSERT INTO @extschema@.refresh_config_snap(source_table, dest_table, dblink) VALUES('
    ||quote_literal(p_src_table)||', '||quote_literal(p_dest_table)||', '|| p_dblink_id||')';

RAISE NOTICE 'Snapshotting source table to pull all current source data...';
EXECUTE v_insert_refresh_config;	

PERFORM @extschema@.refresh_snap(p_dest_table);

PERFORM @extschema@.snapshot_destroyer(p_dest_table, 'ARCHIVE');
	
RAISE NOTICE 'Snapshot complete. Getting the maximum destination timestamp...';
EXECUTE 'SELECT max('||p_control_field||') FROM '||p_dest_table||';' INTO v_max_timestamp;

v_dst_active := @extschema@.dst_utc_check();

v_insert_refresh_config := 'INSERT INTO @extschema@.refresh_config_inserter(source_table, dest_table, dblink, control, boundary, last_value, dst_active) VALUES('
    ||quote_literal(p_src_table)||', '||quote_literal(p_dest_table)||', '|| p_dblink_id||', '
    ||quote_literal(p_control_field)||', '''||p_boundary||'''::interval, '''||v_max_timestamp||'''::timestamptz, '||v_dst_active||');';

RAISE NOTICE 'Inserting data into config table';
EXECUTE v_insert_refresh_config;

-- Remove temp snap from config
EXECUTE 'DELETE FROM @extschema@.refresh_config_snap WHERE source_table = '||quote_literal(p_src_table)||' AND dest_table = '||quote_literal(p_dest_table);

RAISE NOTICE 'Done';

RETURN;

END
$$;
