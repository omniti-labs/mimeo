-- Fix check_name_length() function to work with PostgreSQL 9.1. Installation would fail with syntax errors. Named parameter support was not added for SQL functions until 9.2.
    -- Function was added in version 1.1.0, so any updates from version 1.0.1 to later would fail on PostgreSQL 9.1. If anyone needs an update script for 1.0.1 to 1.3.2, please create an issue on github.
    -- Upgrading PostgreSQL to >= 9.2 will also allow mimeo to be updated to 1.1.0 and greater (Recommended fix).

/*
 * Truncate the name of the given object if it is greater than the postgres default max (63 characters). Otherwise returns given name.
 * Also appends given suffix if given and truncates the name so that the entire suffix will fit.
 */
CREATE OR REPLACE FUNCTION check_name_length(p_table_name text, p_suffix text DEFAULT NULL) RETURNS text
    LANGUAGE SQL IMMUTABLE
    AS $$
SELECT
    CASE WHEN char_length($1) + char_length(COALESCE($2, '')) >= 63 THEN
        substring($1 from 1 for 63 - char_length(COALESCE($2, ''))) || COALESCE($2, '')
    ELSE
        $1 ||COALESCE($2, '')
    END;
$$;


