/*
 * array_agg() function for source database version 8.1. Ensure this is installed in the public schema
 */

CREATE AGGREGATE array_agg (
    SFUNC = array_append,
    BASETYPE = anyelement,
    STYPE = anyarray,
    INITCOND = '{}'
);
