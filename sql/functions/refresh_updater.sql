/*
 *  Refresh insert/update only table based on timestamp or id control field
 */
CREATE FUNCTION refresh_updater(p_destination text, p_limit integer DEFAULT NULL, p_repull boolean DEFAULT false, p_repull_start text DEFAULT NULL, p_repull_end text DEFAULT NULL, p_jobmon boolean DEFAULT NULL, p_lock_wait int DEFAULT NULL, p_debug boolean DEFAULT false) RETURNS void
    LANGUAGE plpgsql SECURITY DEFINER
    AS $$
DECLARE
    v_adv_lock          boolean;
    v_job_id            bigint;
    v_jobmon            boolean;
    v_jobmon_schema     text;
    v_job_name          text;
    v_step_id           bigint;
    v_type              text;
BEGIN

-- Take advisory lock to prevent multiple calls to function overlapping
v_adv_lock := @extschema@.concurrent_lock_check(p_destination, p_lock_wait);
IF v_adv_lock = 'false' THEN
    -- This code is known duplication of code found in more specific refresh functions below.
    -- This is done in order to keep advisory lock as early in the code as possible to avoid race conditions and still log if issues are encountered.
    v_job_name := 'Refresh Updater: '||p_destination;
    SELECT jobmon INTO v_jobmon FROM @extschema@.refresh_config_updater WHERE dest_table = p_destination;
    SELECT nspname INTO v_jobmon_schema FROM pg_namespace n, pg_extension e WHERE e.extname = 'pg_jobmon' AND e.extnamespace = n.oid;
    v_jobmon := COALESCE(p_jobmon, v_jobmon);
    IF v_jobmon IS TRUE AND v_jobmon_schema IS NULL THEN
        RAISE EXCEPTION 'jobmon config set to TRUE, but unable to determine if pg_jobmon extension is installed';
    END IF;

    IF v_jobmon THEN
        EXECUTE format('SELECT %I.add_job(%L)', v_jobmon_schema, v_job_name) INTO v_job_id;
        EXECUTE format('SELECT %I.add_step(%L, %L)', v_jobmon_schema, v_job_id, 'Obtaining advisory lock for job: '||v_job_name) INTO v_step_id;
        EXECUTE format('SELECT %I.update_step(%L, %L, %L)', v_jobmon_schema, v_step_id, 'WARNING', 'Found concurrent job. Exiting gracefully');
        EXECUTE format('SELECT %I.fail_job(%L, %L)', v_jobmon_schema, v_job_id, 2);
    END IF;
    PERFORM @extschema@.gdb(p_debug,'Obtaining advisory lock FAILED for job: '||v_job_name);
    RAISE DEBUG 'Found concurrent job. Exiting gracefully';
    RETURN;
END IF;

SELECT type INTO v_type FROM @extschema@.refresh_config_updater WHERE dest_table = p_destination;
IF v_type = 'updater_time' THEN
    PERFORM @extschema@.refresh_updater_time(p_destination, p_limit, p_repull, p_repull_start::timestamp, p_repull_end::timestamp, p_jobmon, p_lock_wait, p_debug);
ELSIF v_type = 'updater_serial' THEN
    PERFORM @extschema@.refresh_updater_serial(p_destination, p_limit, p_repull, p_repull_start::bigint, p_repull_end::bigint, p_jobmon, p_lock_wait, p_debug);
ELSIF v_type IS NULL THEN
    RAISE EXCEPTION 'Destination table given in argument (%) is not managed by mimeo.', p_destination; 
ELSE
    RAISE EXCEPTION 'Invalid value for control_type column in refresh_config_updater table: %. Must be "time" or "serial"', v_type;
END IF;

END
$$;

