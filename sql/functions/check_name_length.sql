/* 
 * Truncate the name of the given object if it is greater than the postgres default max (63 characters).
 * Also appends given suffix and schema if given and truncates the name so that the entire suffix will fit.
 * Returns original name with schema given if it doesn't require truncation
 */
CREATE FUNCTION check_name_length(p_table_name text, p_suffix text DEFAULT NULL, p_schema text DEFAULT NULL) RETURNS text 
    LANGUAGE SQL IMMUTABLE
    AS $$
SELECT
    CASE WHEN char_length(p_table_name) + char_length(COALESCE(p_suffix, '')) >= 63 THEN
        COALESCE(p_schema ||'.', '') || substring(p_table_name from 1 for 63 - char_length(COALESCE(p_suffix, ''))) || COALESCE(p_suffix, '')
    ELSE
        COALESCE(p_schema ||'.', '') || p_table_name ||COALESCE(p_suffix, '')
    END;
$$;
