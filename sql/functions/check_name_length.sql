/* 
 * Truncate the name of the given object if it is greater than the postgres default max (63 characters). Otherwise returns given name.
 * Also appends given suffix if given and truncates the name so that the entire suffix will fit.
 */
CREATE FUNCTION check_name_length(p_table_name text, p_suffix text DEFAULT NULL) RETURNS text
    LANGUAGE SQL IMMUTABLE
    AS $$
SELECT
    CASE WHEN char_length(p_table_name) + char_length(COALESCE(p_suffix, '')) >= 63 THEN
        substring(p_table_name from 1 for 63 - char_length(COALESCE(p_suffix, ''))) || COALESCE(p_suffix, '')
    ELSE
        p_table_name ||COALESCE(p_suffix, '')
    END;
$$;


