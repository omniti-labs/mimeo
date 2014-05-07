/*
 * Do not allow both serial & time updater replication for the same dest table
 */
CREATE FUNCTION unique_updater_dest_trig() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
DECLARE
v_exists    boolean;
BEGIN
    SELECT 
        CASE 
            WHEN count(*) = 1 THEN true
            ELSE false 
        END 
    INTO v_exists
    FROM @extschema@.refresh_config_updater
    WHERE dest_table = NEW.dest_table;

    IF v_exists THEN
        RAISE EXCEPTION 'Updater replication already defined for %', NEW.dest_table;
    END IF;
    RETURN NEW;
END
$$;

CREATE TRIGGER unique_updater_dest_trig
BEFORE INSERT OR UPDATE OF dest_table
ON @extschema@.refresh_config_updater_serial
FOR EACH ROW EXECUTE PROCEDURE @extschema@.unique_updater_dest_trig();

CREATE TRIGGER unique_updater_dest_trig
BEFORE INSERT OR UPDATE OF dest_table
ON @extschema@.refresh_config_updater_time
FOR EACH ROW EXECUTE PROCEDURE @extschema@.unique_updater_dest_trig();

