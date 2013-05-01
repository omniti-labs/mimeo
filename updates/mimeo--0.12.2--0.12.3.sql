-- Fixed function that determines replication key to always get the oldest unique key when no primary key is available (lowest index oid value). Ensures it's more consistent when it is reused elsewhere to determine which key to use.

/*
 *  Fetches either the primary key or a valid, not-null unique index. Primary key is always preferred over unique key. 
 */
CREATE OR REPLACE FUNCTION fetch_replication_key(p_src_table text, p_dblink_name text, OUT indkey_names text[], OUT indkey_types text[], OUT key_type text, OUT indexrelid oid, OUT statement text) RETURNS record
    LANGUAGE plpgsql
    AS $$
DECLARE

v_dblink_schema         text;
v_remote_sql            text;

BEGIN

IF p_src_table IS NULL OR p_dblink_name IS NULL THEN
    RAISE EXCEPTION 'p_src_table and p_dblink_name parameters cannot be null in fetch_replication_key() call';
END IF;

SELECT nspname INTO v_dblink_schema FROM pg_namespace n, pg_extension e WHERE e.extname = 'dblink' AND e.extnamespace = n.oid;

v_remote_sql := 'SELECT indexrelid,
    pg_get_indexdef(indexrelid) AS statement,
    CASE
        WHEN i.indisprimary IS true THEN ''primary''
        WHEN i.indisunique IS true THEN ''unique''
    END AS key_type,
    ( SELECT array_agg( a.attname ORDER by x.r ) 
        FROM pg_attribute a 
        JOIN ( SELECT k, row_number() over () as r 
                FROM unnest(i.indkey) k ) as x 
        ON a.attnum = x.k AND a.attrelid = i.indrelid
        WHERE a.attnotnull
    ) AS indkey_names,
    ( SELECT array_agg( a.atttypid::regtype::text ORDER by x.r ) 
        FROM pg_attribute a 
        JOIN ( SELECT k, row_number() over () as r 
                FROM unnest(i.indkey) k ) as x 
        ON a.attnum = x.k AND a.attrelid = i.indrelid
        WHERE a.attnotnull 
    ) AS indkey_types
    FROM pg_index i
    WHERE i.indrelid = '||quote_literal(p_src_table)||'::regclass
        AND (i.indisprimary OR i.indisunique)
        AND i.indisvalid
    ORDER BY key_type, indexrelid LIMIT 1';

EXECUTE 'SELECT indexrelid, statement, key_type, indkey_names, indkey_types FROM '||v_dblink_schema||'.dblink('||quote_literal(p_dblink_name)||','||quote_literal(v_remote_sql)||') t (indexrelid oid, statement text, key_type text, indkey_names text[], indkey_types text[])' INTO indexrelid, statement, key_type, indkey_names, indkey_types;

END
$$;


