CREATE FUNCTION concurrent_lock_check(p_dest_table text) RETURNS boolean
    LANGUAGE plpgsql STABLE SECURITY DEFINER
    AS $$
DECLARE

v_adv_lock          boolean;
v_hash1             text;
v_hash2             text;
v_row               record;

BEGIN

FOR v_row IN
    SELECT dest_table, type FROM @extschema@.refresh_config WHERE dest_table = p_dest_table
LOOP
    CASE
        WHEN v_row.type = 'dml' THEN
            v_hash1 := 'refresh_dml';
            v_hash2 := 'Refresh DML: '||v_row.dest_table;
        WHEN v_row.type = 'inserter_serial' OR v_row.type = 'inserter_time' THEN
            v_hash1 := 'refresh_inserter';
            v_hash2 := 'Refresh Inserter: '||v_row.dest_table;
        WHEN v_row.type = 'logdel' THEN
            v_hash1 := 'refresh_logdel';
            v_hash2 := 'Refresh Log Del: '||v_row.dest_table;
        WHEN v_row.type = 'snap' THEN
            v_hash1 := 'refresh_snap';
            v_hash2 := 'Refresh Snap: '||v_row.dest_table;
        WHEN v_row.type = 'table' THEN
            v_hash1 := 'refresh_table';
            v_hash2 := 'Refresh Table: '||v_row.dest_table;
        WHEN v_row.type = 'updater_serial' OR v_row.type = 'updater_time' THEN
            v_hash1 := 'refresh_updater';
            v_hash2 := 'Refresh Updater: '||v_row.dest_table;
        ELSE
            RAISE EXCEPTION 'Unexpected condition in advisory lock creation check. Given table possibly not managed by mimeo.';
    END CASE;

    v_adv_lock := pg_try_advisory_xact_lock(hashtext(v_hash1), hashtext(v_hash2));
    -- First lock that fails to be obtained should immediately cause function to return false
    IF v_adv_lock = 'false' THEN
        EXIT;
    END IF;
END LOOP;

RETURN v_adv_lock;

END
$$;

