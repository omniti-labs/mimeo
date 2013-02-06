/*
 *  Checks if DST time change has occured
 */
CREATE FUNCTION dst_change(date timestamp with time zone) RETURNS boolean
    LANGUAGE sql IMMUTABLE
    AS $$ 
    SELECT to_char( date_trunc('day', $1) , 'TZ' ) <> to_char( date_trunc( 'day', $1 ) + '1 day'::interval, 'TZ' ); 
$$;
