/*
 *  Checks to see if the server is using UTC/GMT timezone. Returns TRUE if it is NOT (makes function logic easier)
 */
CREATE FUNCTION dst_utc_check() RETURNS boolean
    LANGUAGE sql IMMUTABLE
    AS $$
    SELECT to_char( date_trunc('day', now()) , 'TZ' ) <> 'UTC' AND to_char( date_trunc('day', now()) , 'TZ' ) <> 'GMT';
$$;
