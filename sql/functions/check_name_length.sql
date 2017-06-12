/*
 * Truncate the name of the given object if it is greater than the postgres default max (63 characters). Otherwise returns given name.
 * Also appends suffix if given and truncates the name so that the entire suffix will fit.
 * Setting p_convert_standard will change any non-standard character in the object name to an underscore. Allows for new object names that use 
 *   existing object names to be more widely usable.
 */
CREATE FUNCTION check_name_length(p_table_name text, p_suffix text DEFAULT NULL, p_convert_standard boolean DEFAULT false) RETURNS text
    LANGUAGE plpgsql IMMUTABLE
    AS $$
DECLARE

v_result    text;

BEGIN

IF ( char_length(p_table_name) + char_length(COALESCE(p_suffix, '')) ) >= 63 THEN
    v_result := substring(p_table_name from 1 for 63 - char_length(COALESCE(p_suffix, ''))) || COALESCE(p_suffix, '') ;
ELSE
    v_result := p_table_name || COALESCE(p_suffix, '');
END IF;

IF p_convert_standard THEN
    v_result := regexp_replace(v_result, '\W', '_', 'g');
END IF;

RETURN v_result;

END
$$;


