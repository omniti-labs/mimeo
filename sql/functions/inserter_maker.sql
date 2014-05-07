/*
 *  Inserter maker function. 
 */
CREATE FUNCTION inserter_maker(
    p_src_table text
    , p_type text
    , p_control_field text
    , p_dblink_id int
    , p_boundary text DEFAULT NULL
    , p_dest_table text DEFAULT NULL
    , p_index boolean DEFAULT true
    , p_filter text[] DEFAULT NULL
    , p_condition text DEFAULT NULL
    , p_pulldata boolean DEFAULT true
    , p_debug boolean DEFAULT false) 
RETURNS void
    LANGUAGE plpgsql
    AS $$
DECLARE

v_boundary_serial           int;
v_boundary_time             interval;
v_data_source               text;
v_dest_schema_name          text;
v_dest_table_name           text;
v_dst_active                boolean;
v_jobmon                    boolean;
v_insert_refresh_config     text;
v_max_id                    bigint;
v_max_timestamp             timestamptz;
v_table_exists              boolean;

BEGIN

SELECT data_source INTO v_data_source FROM @extschema@.dblink_mapping_mimeo WHERE data_source_id = p_dblink_id; 
IF NOT FOUND THEN
    RAISE EXCEPTION 'Database link ID is incorrect %', p_dblink_id; 
END IF;

IF (p_type <> 'time' AND p_type <> 'serial') OR p_type IS NULL THEN
    RAISE EXCEPTION 'Invalid inserter type: %. Must be either "time" or "serial"', p_type;
END IF;

IF p_dest_table IS NULL THEN
    p_dest_table := p_src_table;
END IF;

IF position('.' in p_dest_table) > 0 AND position('.' in p_src_table) > 0 THEN
    v_dest_schema_name := split_part(p_dest_table, '.', 1); 
    v_dest_table_name := split_part(p_dest_table, '.', 2);
ELSE
    RAISE EXCEPTION 'Source (and destination) table must be schema qualified';
END IF;

-- Determine if pg_jobmon is installed to set config table option below
SELECT 
    CASE 
        WHEN count(nspname) > 0 THEN true
        ELSE false
    END AS jobmon_schema
INTO v_jobmon 
FROM pg_namespace n, pg_extension e WHERE e.extname = 'pg_jobmon' AND e.extnamespace = n.oid;

IF p_type = 'time' THEN
    v_dst_active := @extschema@.dst_utc_check();
    IF p_boundary IS NULL THEN
        v_boundary_time = '10 minutes'::interval;
    ELSE
        v_boundary_time = p_boundary::interval;
    END IF;
    v_insert_refresh_config := 'INSERT INTO @extschema@.refresh_config_inserter_time (
            source_table
            , type
            , dest_table
            , dblink
            , control
            , boundary
            , last_value
            , last_run
            , dst_active
            , filter
            , condition
            , jobmon ) 
        VALUES('
            ||quote_literal(p_src_table)
            ||', '||quote_literal('inserter_time')
            ||', '||quote_literal(p_dest_table)
            ||', '||quote_literal(p_dblink_id)
            ||', '||quote_literal(p_control_field)
            ||', '||quote_literal(v_boundary_time)
            ||', '||quote_literal('0001-01-01'::date)
            ||', '||quote_literal(CURRENT_TIMESTAMP)
            ||', '||v_dst_active
            ||', '||COALESCE(quote_literal(p_filter), 'NULL')
            ||', '||COALESCE(quote_literal(p_condition), 'NULL')
            ||', '||v_jobmon||')';
ELSIF p_type = 'serial' THEN
    IF p_boundary IS NULL THEN
        v_boundary_serial = 10;
    ELSE
        v_boundary_serial = p_boundary::int;
    END IF;
    v_insert_refresh_config := 'INSERT INTO @extschema@.refresh_config_inserter_serial (
            source_table
            , type
            , dest_table
            , dblink
            , control
            , boundary
            , last_value
            , last_run
            , filter
            , condition
            , jobmon ) 
        VALUES('
            ||quote_literal(p_src_table)
            ||', '||quote_literal('inserter_serial')
            ||', '||quote_literal(p_dest_table)
            ||', '||quote_literal(p_dblink_id)
            ||', '||quote_literal(p_control_field)
            ||', '||quote_literal(v_boundary_serial)
            ||', '||quote_literal(0)
            ||', '||quote_literal(CURRENT_TIMESTAMP)
            ||', '||COALESCE(quote_literal(p_filter), 'NULL')
            ||', '||COALESCE(quote_literal(p_condition), 'NULL')
            ||', '||v_jobmon||')';
ELSE
    RAISE EXCEPTION 'Invalid inserter type: %. Must be either "time" or "serial"', p_type;
END IF;

PERFORM @extschema@.gdb(p_debug, 'v_insert_refresh_config: '||v_insert_refresh_config);
EXECUTE v_insert_refresh_config;

SELECT p_table_exists FROM @extschema@.manage_dest_table(p_dest_table, NULL, p_debug) INTO v_table_exists;

IF p_pulldata AND v_table_exists = false THEN
    RAISE NOTICE 'Pulling all data from source...';
    EXECUTE 'SELECT @extschema@.refresh_inserter('||quote_literal(p_dest_table)||', p_repull := true, p_debug := '||p_debug||')';
END IF;

IF p_index AND v_table_exists = false THEN
    PERFORM @extschema@.create_index(p_dest_table, NULL, p_debug);
END IF;

IF v_table_exists THEN
    RAISE NOTICE 'Destination table % already exists. No data or indexes were pulled from source', p_dest_table;
END IF;

IF p_type = 'time' THEN
    RAISE NOTICE 'Getting the maximum destination timestamp...';
    EXECUTE 'SELECT max('||p_control_field||') FROM '||p_dest_table||';' INTO v_max_timestamp;
    EXECUTE 'UPDATE @extschema@.refresh_config_inserter_time SET last_value = '||quote_literal(COALESCE(v_max_timestamp, CURRENT_TIMESTAMP))||' WHERE dest_table = '||quote_literal(p_dest_table);
ELSIF p_type = 'serial' THEN
    RAISE NOTICE 'Getting the maximum destination id...';
    EXECUTE 'SELECT max('||p_control_field||') FROM '||p_dest_table||';' INTO v_max_id;
    EXECUTE 'UPDATE @extschema@.refresh_config_inserter_serial SET last_value = '||COALESCE(v_max_id, 0)||' WHERE dest_table = '||quote_literal(p_dest_table);
END IF;


RAISE NOTICE 'Done';
END
$$;


