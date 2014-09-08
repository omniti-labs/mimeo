/*
 *  Fetches either the primary key or a valid, not-null unique index. Primary key is always preferred over unique key. 
 */
CREATE FUNCTION fetch_replication_key(p_src_schemaname text, p_src_tablename text, p_dblink_name text, p_debug boolean DEFAULT false, OUT indkey_names text[], OUT indkey_types text[], OUT key_type text, OUT indexrelid oid, OUT statement text) RETURNS record
    LANGUAGE plpgsql
    AS $$
DECLARE

v_dblink_schema         text;
v_remote_sql            text;
v_sql                   text;

BEGIN

IF p_src_schemaname IS NULL OR p_src_tablename IS NULL OR p_dblink_name IS NULL THEN
    RAISE EXCEPTION 'p_src_schemaname, p_src_tablename, and p_dblink_name parameters cannot be null in fetch_replication_key() call';
END IF;

SELECT nspname INTO v_dblink_schema FROM pg_namespace n, pg_extension e WHERE e.extname = 'dblink' AND e.extnamespace = n.oid;

v_remote_sql := format('SELECT indexrelid,
    pg_get_indexdef(indexrelid) AS statement,
    CASE
        WHEN i.indisprimary IS true THEN ''primary''
        WHEN i.indisunique IS true THEN ''unique''
    END AS key_type,
    ( SELECT array_agg( a.attname ORDER by x.r ) 
        FROM pg_catalog.pg_attribute a 
        JOIN ( SELECT k, row_number() over () as r 
                FROM unnest(i.indkey) k ) as x 
        ON a.attnum = x.k AND a.attrelid = i.indrelid
        WHERE a.attnotnull
    ) AS indkey_names,
    ( SELECT array_agg( format_type(a.atttypid, a.atttypmod)::text ORDER by x.r ) 
        FROM pg_catalog.pg_attribute a 
        JOIN ( SELECT k, row_number() over () as r 
                FROM unnest(i.indkey) k ) as x 
        ON a.attnum = x.k AND a.attrelid = i.indrelid
        WHERE a.attnotnull 
    ) AS indkey_types
    FROM pg_catalog.pg_index i
    WHERE i.indrelid = ''%I.%I''::regclass
        AND (i.indisprimary OR i.indisunique)
        AND i.indisvalid
    ORDER BY key_type, indexrelid LIMIT 1', p_src_schemaname, p_src_tablename);
PERFORM gdb(p_debug, 'fetch_rep_key: '||v_remote_sql);

v_sql := format('SELECT indexrelid, statement, key_type, indkey_names, indkey_types FROM %I.dblink(%L,%L) t (indexrelid oid, statement text, key_type text, indkey_names text[], indkey_types text[])'
    , v_dblink_schema, p_dblink_name, v_remote_sql);
PERFORM gdb(p_debug, 'fetch_rep_key: '||v_sql);

EXECUTE v_sql INTO indexrelid, statement, key_type, indkey_names, indkey_types;

END
$$;


