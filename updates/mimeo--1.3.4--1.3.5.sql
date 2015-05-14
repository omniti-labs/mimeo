-- Added a lock wait argument to concurrent_lock_check() function to allow a specified period of time to wait for the advisory lock before giving up. The following are valid values:
    -- NULL (default value if not set): Do not wait at all for an advisory lock and immediately return FALSE if one cannot be obtained (this was the old behavior).
    -- > 0: Keep retrying to obtain an advisory lock for the given table for this number of seconds before giving up and returning FALSE. If lock is obtained in this time period, will immediately return TRUE.
    -- <= 0: Keep retrying indefinitely to obtain an advisory lock. Will only return when the lock is obtained and returns TRUE. Ensure your code handles this condition properly to avoid infinite waits.
-- Will keep retrying once per second until lock is obtained or wait period expires. 

-- Concurrent lock check code greatly simplified and now avoids possible race conditions discovered during code review. 
-- Thanks to https://github.com/bdunavant for assistance with this patch

DROP FUNCTION @extschema@.concurrent_lock_check(text);

CREATE FUNCTION concurrent_lock_check(p_dest_table text, p_lock_wait int DEFAULT NULL) RETURNS boolean
    LANGUAGE plpgsql SECURITY DEFINER
    AS $$
DECLARE

v_adv_lock          boolean;
v_dest_table        text;
v_lock_iter         int;

BEGIN

SELECT dest_table INTO v_dest_table FROM @extschema@.refresh_config WHERE dest_table = p_dest_table;
IF v_dest_table IS NULL THEN
    RAISE EXCEPTION 'Destination table given in argument (%) is not managed by mimeo.', p_dest_table;
END IF;

v_adv_lock := pg_try_advisory_xact_lock(hashtext('mimeo advisory lock'), hashtext(v_dest_table));

IF v_adv_lock = 'false' AND p_lock_wait IS NOT NULL THEN

    IF p_lock_wait > 0 THEN

        -- Use the "try" advisory lock since we want to check at intervals whether it's available
        v_lock_iter = 0;
        WHILE (v_adv_lock = 'false') AND (v_lock_iter >= p_lock_wait) LOOP
            v_lock_iter := v_lock_iter + 1;
            v_adv_lock := pg_try_advisory_xact_lock(hashtext('mimeo advisory lock'), hashtext(v_dest_table));
            PERFORM pg_sleep(1);
        END LOOP;

   ELSE
        -- Since we want to wait indefinitely, can just call normal transaction advisory lock since it waits forever for true
        PERFORM pg_advisory_xact_lock(hashtext('mimeo advisory lock'), hashtext(v_dest_table));
        v_adv_lock := 'true';
   END IF; -- END p_lock_wait > 0 check check

END IF; -- END v_adv_lock check

RETURN v_adv_lock;

END
$$;

