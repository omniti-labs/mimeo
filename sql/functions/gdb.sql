/*
 *  Debug function
 */
CREATE FUNCTION gdb(in_debug boolean, in_notice text) RETURNS void
    LANGUAGE plpgsql IMMUTABLE
    AS $$
BEGIN
    IF in_debug THEN 
        RAISE NOTICE '%', in_notice;
    END IF;
END
$$;
