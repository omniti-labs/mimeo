/*
 *  Snapshot maker function.
 */
CREATE FUNCTION snapshot_maker(
    p_src_table text
    , p_dblink_id int
    , p_dest_table text DEFAULT NULL
    , p_index boolean DEFAULT true
    , p_filter text[] DEFAULT NULL
    , p_condition text DEFAULT NULL
    , p_pulldata boolean DEFAULT true
    , p_jobmon boolean DEFAULT NULL
    , p_debug boolean DEFAULT false) 
RETURNS void
    LANGUAGE plpgsql
    AS $$
DECLARE

v_data_source               text;
v_dest_exists               text;
v_insert_refresh_config     text;
v_job_id                    bigint;
v_job_name                  text;
v_jobmon                    boolean;
v_jobmon_schema             text;
v_old_search_path           text;
v_step_id                   bigint;

BEGIN

SELECT nspname INTO v_jobmon_schema FROM pg_namespace n, pg_extension e WHERE e.extname = 'pg_jobmon' AND e.extnamespace = n.oid;
SELECT current_setting('search_path') INTO v_old_search_path;
EXECUTE 'SELECT set_config(''search_path'',''@extschema@,'||COALESCE(v_jobmon_schema||',', '')||'public'',''false'')';

SELECT data_source INTO v_data_source FROM @extschema@.dblink_mapping_mimeo WHERE data_source_id = p_dblink_id; 
IF NOT FOUND THEN
	RAISE EXCEPTION 'ERROR: Database link ID does not exist in @extschema@.dblink_mapping_mimeo: %', p_dblink_id; 
END IF;  

IF p_dest_table IS NULL THEN
    p_dest_table := p_src_table;
END IF;

IF position('.' in p_dest_table) = 0 AND position('.' in p_src_table) = 0 THEN
    RAISE EXCEPTION 'Source (and destination) table must be schema qualified';
END IF;

SELECT tablename INTO v_dest_exists
FROM pg_catalog.pg_tables
WHERE schemaname = split_part(p_dest_table, '.', 1)::name
AND tablename = split_part(p_dest_table, '.', 2)::name;
    IF v_dest_exists IS NOT NULL THEN
        RAISE EXCEPTION 'Destination table cannot exist before running snapshot_maker(): %', p_dest_table;
    END IF;

IF p_jobmon IS TRUE AND v_jobmon_schema IS NULL THEN
    RAISE EXCEPTION 'p_jobmon parameter set to TRUE, but unable to determine if pg_jobmon extension is installed';
ELSIF (p_jobmon IS TRUE OR p_jobmon IS NULL) AND v_jobmon_schema IS NOT NULL THEN
    v_jobmon := true;
ELSE
    v_jobmon := false;
END IF;

v_job_name := 'Snapshot Maker: '||p_src_table;

IF v_jobmon THEN
    v_job_id := add_job(v_job_name);
    PERFORM gdb(p_debug,'Job ID: '||v_job_id::text);
    v_step_id := add_step(v_job_id,'Inserting config data');
END IF;

v_insert_refresh_config := 'INSERT INTO @extschema@.refresh_config_snap(
        source_table
        , dest_table
        , dblink
        , filter
        , condition
        , jobmon) 
    VALUES('
        ||quote_literal(p_src_table)
        ||', '||quote_literal(p_dest_table)
        ||', '||p_dblink_id
        ||', '||COALESCE(quote_literal(p_filter), 'NULL')
        ||', '||COALESCE(quote_literal(p_condition), 'NULL')
        ||', '||v_jobmon||')';

EXECUTE v_insert_refresh_config;

IF v_jobmon THEN
    PERFORM update_step(v_step_id, 'OK','Done');
END IF;

IF v_jobmon THEN
    v_step_id := add_step(v_job_id,'Running first snapshot. See separate refresh job for more details.');
END IF;

RAISE NOTICE 'attempting first snapshot';
EXECUTE 'SELECT @extschema@.refresh_snap('||quote_literal(p_dest_table)||', p_index := '||p_index||', p_pulldata := '||p_pulldata||', p_debug := '||p_debug||')'; 

IF v_jobmon THEN
    PERFORM update_step(v_step_id, 'OK','Done');
    v_step_id := add_step(v_job_id,'Running second snapshot. See separate refresh job for more details.');
END IF;

RAISE NOTICE 'attempting second snapshot';
EXECUTE 'SELECT @extschema@.refresh_snap('||quote_literal(p_dest_table)||', p_index := '||p_index||', p_pulldata := '||p_pulldata||', p_debug := '||p_debug||')';

IF v_jobmon THEN
    PERFORM update_step(v_step_id, 'OK','Done');
END IF;

IF v_jobmon THEN
    PERFORM close_job(v_job_id);
END IF;

EXECUTE 'SELECT set_config(''search_path'','''||v_old_search_path||''',''false'')';

RAISE NOTICE 'Done';

RETURN;

EXCEPTION
    WHEN OTHERS THEN
        SELECT nspname INTO v_jobmon_schema FROM pg_namespace n, pg_extension e WHERE e.extname = 'pg_jobmon' AND e.extnamespace = n.oid;
        SELECT jobmon INTO v_jobmon FROM @extschema@.refresh_config_snap WHERE dest_table = p_src_table;
        v_jobmon := COALESCE(p_jobmon, v_jobmon);
        IF v_jobmon_schema IS NULL THEN
            v_jobmon := false;
        END IF;
        IF v_jobmon THEN
            IF v_job_id IS NULL THEN
                EXECUTE format('SELECT %I.add_job(%L)', v_jobmon_schema, 'Snapshot Maker: '||p_src_table) INTO v_job_id;
                EXECUTE format('SELECT %I.add_step(%L, %L)', v_jobmon_schema, v_job_id, 'EXCEPTION before job logging started') INTO v_step_id;
            END IF;
            IF v_step_id IS NULL THEN
                EXECUTE format('SELECT %I.add_step(%L, %L)', v_jobmon_schema, v_job_id, 'EXCEPTION before first step logged') INTO v_step_id;
            END IF;
                  EXECUTE format('SELECT %I.update_step(%L, %L, %L)', v_jobmon_schema, v_step_id, 'CRITICAL', 'ERROR: '||COALESCE(SQLERRM,'unknown'));
            EXECUTE format('SELECT %I.fail_job(%L)', v_jobmon_schema, v_job_id);
        END IF;
        RAISE EXCEPTION '%', SQLERRM;
END
$$;

