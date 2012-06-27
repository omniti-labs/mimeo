-- Made dblink_mapping.data_source_id column a real serial column (default is the next sequence ID) to make setup easier
-- Made non-existent database link ID error a little clearer
-- Made snapshot_destroyer parameter name clearer in what its use is. Required dropping function so please re-check your function permissions.
-- Documentation update.


ALTER TABLE dblink_mapping ALTER COLUMN data_source_id SET DEFAULT nextval('@extschema@.dblink_mapping_data_source_id_seq');


CREATE OR REPLACE FUNCTION snapshot_maker(p_src_table text, p_dblink_id int) RETURNS void
    LANGUAGE plpgsql
    AS $_$
declare
v_insert_refresh_config     text;
v_data_source               text;

BEGIN
	SELECT data_source INTO v_data_source FROM @extschema@.dblink_mapping WHERE data_source_id = p_dblink_id; 
	IF NOT FOUND THEN
   		RAISE EXCEPTION 'Database link ID does not exist in @extschema@.dblink_mapping: %', p_dblink_id; 
	END IF;  
doc
	v_insert_refresh_config := 'INSERT INTO @extschema@.refresh_config(source_table, dest_table, type, dblink) VALUES('||quote_literal(p_src_table)||', '||quote_literal(p_src_table)||',''snap'', '|| p_dblink_id||');';

	RAISE NOTICE 'Inserting record in @extschema@.refresh_config';
	EXECUTE v_insert_refresh_config;	
	RAISE NOTICE 'Insert successful';	

	RAISE NOTICE 'attempting first snapshot';
	PERFORM @extschema@.refresh_snap(p_src_table, FALSE);

	RAISE NOTICE 'attempting second snapshot';
	PERFORM @extschema@.refresh_snap(p_src_table, FALSE);

	RAISE NOTICE 'all done';

	RETURN;
END
$_$;


CREATE OR REPLACE FUNCTION snapshot_maker(p_src_table text, p_dest_table text, p_dblink_id int) RETURNS void
    LANGUAGE plpgsql
    AS $_$
declare
v_insert_refresh_config     text;
v_data_source	            text;

BEGIN
	SELECT data_source INTO v_data_source FROM @extschema@.dblink_mapping WHERE data_source_id = p_dblink_id; 
	IF NOT FOUND THEN
   		RAISE EXCEPTION 'ERROR: Database link ID does not exist in @extschema@.dblink_mapping: %', p_dblink_id; 
	END IF;  

	v_insert_refresh_config := 'INSERT INTO @extschema@.refresh_config(source_table, dest_table, type, dblink) VALUES('||quote_literal(p_src_table)||', '||quote_literal(p_dest_table)||',''snap'', '|| p_dblink_id||');';

	RAISE NOTICE 'Inserting record in @extschema@.refresh_config';
	EXECUTE v_insert_refresh_config;	
	RAISE NOTICE 'Insert successful';	

	RAISE NOTICE 'attempting first snapshot';
	PERFORM @extschema@.refresh_snap(p_dest_table, FALSE);

	RAISE NOTICE 'attempting second snapshot';
	PERFORM @extschema@.refresh_snap(p_dest_table, FALSE);

	RAISE NOTICE 'all done';

	RETURN;
END
$_$;

ALTER EXTENSION mimeo DROP FUNCTION snapshot_destroyer(text, text);
DROP FUNCTION snapshot_destroyer(text, text);
CREATE FUNCTION snapshot_destroyer(p_dest_table text, p_archive_option text) RETURNS void
    LANGUAGE plpgsql
    AS $_$
    
DECLARE
    v_dest_table        text;
    v_exists            int;
    v_snap_suffix       text;
    v_src_table         text;
    v_view_definition   text;
    
BEGIN

SELECT source_table, dest_table INTO v_src_table, v_dest_table
    FROM @extschema@.refresh_config WHERE dest_table = p_dest_table;
IF NOT FOUND THEN
    RAISE EXCEPTION 'This table is not set up for snapshot replication: %', v_dest_table;
END IF;

-- Make a brand new, real table to keep the data that is not part of the snap system anymore
IF p_archive_option = 'ARCHIVE' THEN

    SELECT definition INTO v_view_definition FROM pg_views WHERE schemaname || '.' || viewname = v_dest_table;
    v_exists := strpos(v_view_definition, 'snap1');
    IF v_exists > 0 THEN
        v_snap_suffix := 'snap1';
    ELSE
        v_snap_suffix := 'snap2';
    END IF;
    
    EXECUTE 'DROP VIEW ' || v_dest_table;
    EXECUTE 'CREATE TEMPORARY TABLE tmp_snapshot_destroy AS SELECT * FROM ' || v_dest_table || '_' || v_snap_suffix;
    EXECUTE 'CREATE TABLE ' || v_dest_table || ' AS SELECT * FROM tmp_snapshot_destroy';
    
ELSE

    EXECUTE 'DROP VIEW ' || v_dest_table;    

END IF;

EXECUTE 'DROP TABLE ' || v_dest_table || '_snap1';
EXECUTE 'DROP TABLE ' || v_dest_table || '_snap2';

EXECUTE 'DELETE FROM @extschema@.refresh_config WHERE dest_table = ' || quote_literal(v_dest_table);

EXECUTE 'DROP TABLE IF EXISTS tmp_snapshot_destroy';

END
$_$;
