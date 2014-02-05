/*
 *  Authentication for dblink
 */
CREATE FUNCTION auth(integer) RETURNS text
    LANGUAGE plpgsql STABLE
    AS $$
DECLARE

    v_auth          text;
    v_data_source   text;
    v_pwd           text;
    v_username      text;
    
BEGIN
    
SELECT data_source, username, pwd INTO v_data_source, v_username, v_pwd FROM @extschema@.dblink_mapping_mimeo WHERE data_source_id = $1;

IF v_pwd IS NOT NULL THEN
    v_auth := v_data_source||' user='||v_username||' password='||v_pwd;
ELSE
    v_auth := v_data_source||' user='||v_username;
END IF;

RETURN v_auth;

END
$$;


