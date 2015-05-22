/*
 *  Refresh insert/update only table based on timestamp control field
 */
CREATE FUNCTION refresh_updater(p_destination text, p_limit integer DEFAULT NULL, p_repull boolean DEFAULT false, p_repull_start text DEFAULT NULL, p_repull_end text DEFAULT NULL, p_jobmon boolean DEFAULT NULL, p_lock_wait int DEFAULT NULL, p_debug boolean DEFAULT false) RETURNS void
    LANGUAGE plpgsql SECURITY DEFINER
    AS $$
DECLARE
    v_type      text;
BEGIN

SELECT type INTO v_type FROM @extschema@.refresh_config_updater WHERE dest_table = p_destination;
IF v_type = 'updater_time' THEN
    PERFORM @extschema@.refresh_updater_time(p_destination, p_limit, p_repull, p_repull_start::timestamp, p_repull_end::timestamp, p_jobmon, p_lock_wait, p_debug);
ELSIF v_type = 'updater_serial' THEN
    PERFORM @extschema@.refresh_updater_serial(p_destination, p_limit, p_repull, p_repull_start::bigint, p_repull_end::bigint, p_jobmon, p_lock_wait, p_debug);
ELSIF v_type IS NULL THEN
    RAISE EXCEPTION 'No configuration found for refresh_updater on table %', p_destination;
ELSE
    RAISE EXCEPTION 'Invalid value for control_type column in refresh_config_updater table: %. Must be "time" or "serial"', v_type;
END IF;

END
$$;


