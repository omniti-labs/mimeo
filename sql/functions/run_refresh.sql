/*
 * Function to run batches of refresh jobs.
 */
CREATE FUNCTION run_refresh(p_type text, p_batch int, p_debug boolean DEFAULT false) RETURNS void
    LANGUAGE plpgsql SECURITY DEFINER
    AS $$
DECLARE
    
    v_adv_lock      boolean;
    v_row           record;

BEGIN

-- Take advisory lock to only allow single batch of each type to run
v_adv_lock := pg_try_advisory_lock(hashtext('run_refresh'), hashtext(p_type));
IF v_adv_lock = 'false' THEN
    PERFORM @extschema@.gdb(p_debug,'Batch for type '||p_type||' already running');
    RETURN;
END IF;

FOR v_row IN 
    SELECT dest_table, batch_limit FROM @extschema@.refresh_config 
    WHERE type = p_type::@extschema@.refresh_type 
    AND period IS NOT NULL 
    AND (now() - last_value)::interval > period 
    ORDER BY last_value ASC        
    LIMIT p_batch
LOOP

    CASE p_type
        WHEN 'snap' THEN
            PERFORM @extschema@.refresh_snap(v_row.dest_table);
        WHEN 'inserter' THEN
            PERFORM @extschema@.refresh_inserter(v_row.dest_table, p_limit := v_row.batch_limit);
        WHEN 'updater' THEN
            PERFORM @extschema@.refresh_updater(v_row.dest_table, p_limit := v_row.batch_limit);
        WHEN 'dml' THEN
            PERFORM @extschema@.refresh_dml(v_row.dest_table, p_limit := v_row.batch_limit);
        WHEN 'logdel' THEN
            PERFORM @extschema@.refresh_logdel(v_row.dest_table, p_limit := v_row.batch_limit);
    END CASE;

    PERFORM @extschema@.gdb(p_debug, 'Running refresh for: '|| v_row.dest_table);

END LOOP;

PERFORM pg_advisory_unlock(hashtext('run_refresh'), hashtext(p_type));

EXCEPTION
    WHEN QUERY_CANCELED THEN
        PERFORM pg_advisory_unlock(hashtext('run_refresh'), hashtext(p_type));
        RAISE EXCEPTION '%', SQLERRM;  

END
$$;
