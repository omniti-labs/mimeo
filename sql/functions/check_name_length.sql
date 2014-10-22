/*
 * Truncate the name of the given object if it is greater than the postgres default max (63 characters). Otherwise returns given name.
 * Also appends given suffix if given and truncates the name so that the entire suffix will fit.
 */
CREATE FUNCTION check_name_length(p_table_name text, p_suffix text DEFAULT NULL) RETURNS text
    LANGUAGE SQL IMMUTABLE
    AS $$
SELECT
    CASE WHEN char_length($1) + char_length(COALESCE($2, '')) >= 63 THEN
        substring($1 from 1 for 63 - char_length(COALESCE($2, ''))) || COALESCE($2, '')
    ELSE
        $1 ||COALESCE($2, '')
    END;
$$;


