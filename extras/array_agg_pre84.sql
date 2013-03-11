/*
 * array_agg() function for source database versions less than 8.4 but greater than 8.1. Ensure this is installed in the public schema
 */

CREATE AGGREGATE array_agg(anyelement) (
    SFUNC=array_append,
    STYPE=anyarray,
    INITCOND=’{}’
);

