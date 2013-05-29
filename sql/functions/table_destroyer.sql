/*
 *  Plain table destroyer function. 
 */
CREATE FUNCTION table_destroyer(p_dest_table text, p_keep_table boolean DEFAULT true) RETURNS void
    LANGUAGE plpgsql
    AS $$
DECLARE

v_dest_table        text;
    
BEGIN

SELECT dest_table INTO v_dest_table
    FROM @extschema@.refresh_config_table WHERE dest_table = p_dest_table;
IF NOT FOUND THEN
    RAISE NOTICE 'This table is not set up for plain table replication: %', p_dest_table;
ELSE
    IF p_keep_table THEN 
        RAISE NOTICE 'Destination table NOT destroyed: %', v_dest_table; 
    ELSE
        RAISE NOTICE 'Destination table destroyed: %', v_dest_table;
        EXECUTE 'DROP TABLE IF EXISTS ' || v_dest_table;
    END IF;

    EXECUTE 'DELETE FROM @extschema@.refresh_config_table WHERE dest_table = ' || quote_literal(v_dest_table);	
END IF;

END
$$;
