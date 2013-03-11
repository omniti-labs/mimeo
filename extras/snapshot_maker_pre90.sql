/*
 * !!!!!! READ THIS FIRST !!!!!!
 * Alternate function to provide a way to use a pre-9.0 version of PostgreSQL as the source.
 * There's no functional code changes in this version, it's just provided as a maker function for using the "refresh_snap_pre90" version.
 * It is not installed as part of the extension so can be safely added and removed without affecting it if you don't rename the function to its original name.
 * You must do a find-and-replace to set the proper schema that mimeo is installed to on the destination database.
 * I left "@extschema@" in here from the original extension code to provide an easy string to find and replace.
 * Just search for that and replace with your installation's schema.
 */
CREATE OR REPLACE FUNCTION @extschema@.snapshot_maker_pre90(
    p_src_table text
    , p_dblink_id int
    , p_dest_table text DEFAULT NULL
    , p_index boolean DEFAULT true
    , p_filter text[] DEFAULT NULL
    , p_condition text DEFAULT NULL
    , p_pulldata boolean DEFAULT true) 
RETURNS void
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

v_insert_refresh_config := 'INSERT INTO @extschema@.refresh_config_snap(source_table, dest_table, dblink, filter, condition) VALUES('||quote_literal(p_src_table)||', '||quote_literal(p_dest_table)||','||p_dblink_id||','||COALESCE(quote_literal(p_filter), 'NULL')||','||COALESCE(quote_literal(p_condition), 'NULL')||')';

RAISE NOTICE 'Inserting record in @extschema@.refresh_config';
EXECUTE v_insert_refresh_config;	
RAISE NOTICE 'Insert successful';	

RAISE NOTICE 'attempting first snapshot';
EXECUTE 'SELECT @extschema@.refresh_snap_pre90('||quote_literal(p_dest_table)||', p_index := '||p_index||', p_pulldata := '||p_pulldata||')'; 

RAISE NOTICE 'attempting second snapshot';
EXECUTE 'SELECT @extschema@.refresh_snap_pre90('||quote_literal(p_dest_table)||', p_index := '||p_index||', p_pulldata := '||p_pulldata||')';

RAISE NOTICE 'all done';

RETURN;
END
$$;
