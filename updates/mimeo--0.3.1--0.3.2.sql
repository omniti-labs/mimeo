-- Added new inserter_maker and inserter_destroyer functions

/*
 *  Inserter maker function. Accepts custom destination name.
 */
CREATE FUNCTION inserter_maker(p_src_table text, p_dest_table text, p_control_field text, p_dblink_id int) RETURNS void
    LANGUAGE plpgsql
    AS $_$
declare
v_insert_refresh_config          text;
v_update_refresh_config          text;
v_max_timestamp			 timestamptz;
v_data_source			 text;
v_exists            		 int;
v_snap_suffix       		 text;
v_view_definition   		 text;

BEGIN
	SELECT data_source INTO v_data_source FROM @extschema@.dblink_mapping WHERE data_source_id = p_dblink_id; 
	IF NOT FOUND THEN
   		RAISE EXCEPTION 'ERROR: database link ID is incorrect %', p_dblink_id; 
	END IF;  

	v_insert_refresh_config := 'INSERT INTO @extschema@.refresh_config(source_table, dest_table, type, dblink, control, boundary) VALUES('||quote_literal(p_src_table)||', '||quote_literal(p_dest_table)||',''snap'', '|| p_dblink_id||', '||quote_literal(p_control_field)||', ''10mins''::interval);';

	RAISE NOTICE 'Inserting record in @extschema@.refresh_config';
	EXECUTE v_insert_refresh_config;	
	RAISE NOTICE 'Insert successful';	

	RAISE NOTICE 'attempting snapshot';
	PERFORM @extschema@.refresh_snap(p_dest_table, FALSE);

	RAISE NOTICE 'attempting to destroy snapshot';

	SELECT definition INTO v_view_definition FROM pg_views WHERE schemaname || '.' || viewname = p_dest_table;
    	v_exists := strpos(v_view_definition, 'snap1');
    	IF v_exists > 0 THEN
        	v_snap_suffix := 'snap1';
    	END IF;
    
    	EXECUTE 'DROP VIEW ' || p_dest_table;
    	EXECUTE 'CREATE TABLE ' || p_dest_table || ' AS SELECT * FROM ' || p_dest_table || '_' || v_snap_suffix;
	EXECUTE 'DROP TABLE ' || p_dest_table || '_snap1';

	RAISE NOTICE 'Destroyed successfully';

	RAISE NOTICE 'Taking the maximum timestamp';
	EXECUTE 'SELECT max('||p_control_field||') FROM '||p_dest_table||';' INTO v_max_timestamp;
	RAISE NOTICE 'The select statement ran successfully.';

	v_update_refresh_config := 'UPDATE @extschema@.refresh_config SET (type, last_value) = (''inserter'', '||quote_literal(v_max_timestamp)||') WHERE dest_table = '||quote_literal(p_src_table)||';';

	RAISE NOTICE 'Updating config table with highest timestamp value';
	EXECUTE v_update_refresh_config;
	RAISE NOTICE 'Update successful';

	RAISE NOTICE 'all done';

	RETURN;
END
$_$;

/*
 *  Inserter maker function. Assumes source and destination are the same tablename.
 */
CREATE FUNCTION inserter_maker(p_src_table text, p_control_field text, p_dblink_id int) RETURNS void
    LANGUAGE plpgsql
    AS $_$
declare
v_insert_refresh_config          text;
v_update_refresh_config          text;
v_max_timestamp			 timestamptz;
v_data_source			 text;
v_exists            		 int;
v_snap_suffix       		 text;
v_view_definition   		 text;

BEGIN
	SELECT data_source INTO v_data_source FROM @extschema@.dblink_mapping WHERE data_source_id = p_dblink_id; 
	IF NOT FOUND THEN
   		RAISE EXCEPTION 'ERROR: database link ID is incorrect %', p_dblink_id; 
	END IF;  

	v_insert_refresh_config := 'INSERT INTO @extschema@.refresh_config(source_table, dest_table, type, dblink, control, boundary) VALUES('||quote_literal(p_src_table)||', '||quote_literal(p_src_table)||',''snap'', '|| p_dblink_id||', '||quote_literal(p_control_field)||', ''10mins''::interval);';

	RAISE NOTICE 'Inserting record in @extschema@.refresh_config';
	EXECUTE v_insert_refresh_config;	
	RAISE NOTICE 'Insert successful';	

	RAISE NOTICE 'attempting snapshot';
	PERFORM @extschema@.refresh_snap(p_src_table, FALSE);

	RAISE NOTICE 'attempting to destroy snapshot';

	SELECT definition INTO v_view_definition FROM pg_views WHERE schemaname || '.' || viewname = p_src_table;
    	v_exists := strpos(v_view_definition, 'snap1');
    	IF v_exists > 0 THEN
        	v_snap_suffix := 'snap1';
    	END IF;
    
    	EXECUTE 'DROP VIEW ' || p_src_table;
    	EXECUTE 'CREATE TABLE ' || p_src_table || ' AS SELECT * FROM ' || p_src_table || '_' || v_snap_suffix;
	EXECUTE 'DROP TABLE ' || p_src_table || '_snap1';

	RAISE NOTICE 'Destroyed successfully';

	RAISE NOTICE 'Taking the maximum timestamp';
	EXECUTE 'SELECT max('||p_control_field||') FROM '||p_src_table||';' INTO v_max_timestamp;
	RAISE NOTICE 'The select statement ran successfully.';

	v_update_refresh_config := 'UPDATE @extschema@.refresh_config SET (type, last_value) = (''inserter'', '||quote_literal(v_max_timestamp)||') WHERE dest_table = '||quote_literal(p_src_table)||';';

	RAISE NOTICE 'Updating config table with highest timestamp value';
	EXECUTE v_update_refresh_config;
	RAISE NOTICE 'Update successful';

	RAISE NOTICE 'all done';

	RETURN;
END
$_$;

/*
 *  Inserter destroyer function. Pass archive to keep table intact.
 */
CREATE FUNCTION inserter_destroyer(p_dest_table text, p_archive_option text) RETURNS void
    LANGUAGE plpgsql
    AS $_$
    
DECLARE
    v_dest_table        text;
    v_src_table         text;
    
BEGIN

	SELECT source_table, dest_table INTO v_src_table, v_dest_table
    		FROM @extschema@.refresh_config WHERE dest_table = p_dest_table;
	IF NOT FOUND THEN
    		RAISE EXCEPTION 'This table is not set up for inserter replication: %', v_dest_table;
	END IF;

	-- Deletes entry in config and keeps the replicated table intact.
	IF p_archive_option = 'ARCHIVE' THEN 

    		EXECUTE 'DELETE FROM @extschema@.refresh_config WHERE dest_table = ' || quote_literal(v_dest_table);

	ELSE

		EXECUTE 'DROP TABLE ' || v_dest_table;

		EXECUTE 'DELETE FROM @extschema@.refresh_config WHERE dest_table = ' || quote_literal(v_dest_table);

	END IF;

END
$_$;
