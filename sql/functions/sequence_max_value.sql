/*
 * Returns the highest value for the given sequence by checking all columns that use it as a default
 * Awesome query poached and fixed from http://stackoverflow.com/a/5943183
 */
CREATE FUNCTION sequence_max_value(oid) RETURNS bigint
    LANGUAGE plpgsql STRICT
    AS $$ 
DECLARE

v_colname       name;
v_newmax        bigint;
v_row           record;
v_tabrelid      oid;

BEGIN

FOR v_tabrelid, v_colname IN 
    SELECT attrelid, attname FROM pg_attribute WHERE (attrelid, attnum) IN (
        SELECT adrelid::regclass, adnum FROM pg_attrdef WHERE oid IN (
            SELECT objid FROM pg_depend WHERE refobjid = $1 AND classid = 'pg_attrdef'::regclass
        )
    ) 
LOOP
    FOR v_row IN EXECUTE 'SELECT max(' || quote_ident(v_colname) || ')::bigint FROM ' || v_tabrelid::regclass LOOP
        IF v_newmax IS NULL OR v_row.max > v_newmax THEN
            v_newmax := v_row.max;
        END IF;
    END LOOP;
END LOOP;

RETURN v_newmax;

END
$$;
