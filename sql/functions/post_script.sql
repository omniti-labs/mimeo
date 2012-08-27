/*
 *  Function to run any SQL after object recreation due to schema changes on source
 */
CREATE FUNCTION post_script(p_dest_table text) RETURNS void
    LANGUAGE plpgsql SECURITY DEFINER
    AS $$
DECLARE
    v_post_script   text[];
    v_sql           text;
BEGIN
    
     SELECT post_script INTO v_post_script FROM @extschema@.refresh_config_snap WHERE dest_table = p_dest_table;

    FOREACH v_sql IN ARRAY v_post_script LOOP
        RAISE NOTICE 'v_sql: %', v_sql;
        EXECUTE v_sql;
    END LOOP;
END
$$;
