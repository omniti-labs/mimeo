-- ########## mimeo table definitions ##########
CREATE SEQUENCE dblink_mapping_data_source_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;
CREATE TABLE dblink_mapping (
    data_source_id integer NOT NULL DEFAULT nextval('@extschema@.dblink_mapping_data_source_id_seq'),
    data_source text NOT NULL,
    username text NOT NULL,
    pwd text,
    dbh_attr text,
    CONSTRAINT dblink_mapping_data_source_id_pkey PRIMARY KEY (data_source_id)
);
SELECT pg_catalog.pg_extension_config_dump('dblink_mapping', '');
ALTER SEQUENCE dblink_mapping_data_source_id_seq OWNED BY dblink_mapping.data_source_id;

CREATE TABLE refresh_config (
    dest_table text NOT NULL,
    type text NOT NULL,
    dblink integer NOT NULL,
    last_run timestamp with time zone,
    filter text[],
    condition text,
    period interval,
    batch_limit int,
    jobmon boolean DEFAULT false
);
SELECT pg_catalog.pg_extension_config_dump('refresh_config', '');
CREATE RULE refresh_config_parent_nodata AS ON INSERT TO @extschema@.refresh_config DO INSTEAD NOTHING;

CREATE TABLE refresh_config_snap (LIKE @extschema@.refresh_config INCLUDING ALL) INHERITS (@extschema@.refresh_config);
SELECT pg_catalog.pg_extension_config_dump('refresh_config_snap', '');
ALTER TABLE @extschema@.refresh_config_snap ADD CONSTRAINT refresh_config_snap_dblink_fkey FOREIGN KEY (dblink) REFERENCES @extschema@.dblink_mapping(data_source_id);
ALTER TABLE @extschema@.refresh_config_snap ADD CONSTRAINT refresh_config_snap_dest_table_pkey PRIMARY KEY (dest_table);
ALTER TABLE @extschema@.refresh_config_snap ADD COLUMN source_table text NOT NULL;
ALTER TABLE @extschema@.refresh_config_snap ADD COLUMN n_tup_ins bigint;
ALTER TABLE @extschema@.refresh_config_snap ADD COLUMN n_tup_upd bigint;
ALTER TABLE @extschema@.refresh_config_snap ADD COLUMN n_tup_del bigint;
ALTER TABLE @extschema@.refresh_config_snap ADD COLUMN post_script text[];
ALTER TABLE @extschema@.refresh_config_snap ALTER COLUMN type SET DEFAULT 'snap';
ALTER TABLE @extschema@.refresh_config_snap ADD CONSTRAINT refresh_config_snap_type_check CHECK (type = 'snap');

CREATE TABLE refresh_config_inserter (LIKE @extschema@.refresh_config INCLUDING ALL) INHERITS (@extschema@.refresh_config);
SELECT pg_catalog.pg_extension_config_dump('refresh_config_inserter', '');
ALTER TABLE @extschema@.refresh_config_inserter ADD CONSTRAINT refresh_config_inserter_dblink_fkey FOREIGN KEY (dblink) REFERENCES @extschema@.dblink_mapping(data_source_id);
ALTER TABLE @extschema@.refresh_config_inserter ADD CONSTRAINT refresh_config_inserter_dest_table_pkey PRIMARY KEY (dest_table);
ALTER TABLE @extschema@.refresh_config_inserter ADD COLUMN source_table text NOT NULL; 
ALTER TABLE @extschema@.refresh_config_inserter ADD COLUMN control text NOT NULL;   
ALTER TABLE @extschema@.refresh_config_inserter ADD COLUMN boundary interval NOT NULL DEFAULT '10 minutes'::interval;
ALTER TABLE @extschema@.refresh_config_inserter ADD COLUMN last_value timestamptz NOT NULL;
ALTER TABLE @extschema@.refresh_config_inserter ADD COLUMN dst_active boolean NOT NULL DEFAULT true;
ALTER TABLE @extschema@.refresh_config_inserter ADD COLUMN dst_start int NOT NULL DEFAULT 30;
ALTER TABLE @extschema@.refresh_config_inserter ADD COLUMN dst_end int NOT NULL DEFAULT 230;
ALTER TABLE @extschema@.refresh_config_inserter ALTER COLUMN type SET DEFAULT 'inserter';
ALTER TABLE @extschema@.refresh_config_inserter ADD CONSTRAINT refresh_config_inserter_type_check CHECK (type = 'inserter');

CREATE TABLE refresh_config_updater (LIKE @extschema@.refresh_config INCLUDING ALL) INHERITS (@extschema@.refresh_config);
SELECT pg_catalog.pg_extension_config_dump('refresh_config_updater', '');
ALTER TABLE @extschema@.refresh_config_updater ADD CONSTRAINT refresh_config_updater_dblink_fkey FOREIGN KEY (dblink) REFERENCES @extschema@.dblink_mapping(data_source_id);
ALTER TABLE @extschema@.refresh_config_updater ADD CONSTRAINT refresh_config_updater_dest_table_pkey PRIMARY KEY (dest_table);
ALTER TABLE @extschema@.refresh_config_updater ADD COLUMN source_table text NOT NULL;
ALTER TABLE @extschema@.refresh_config_updater ADD COLUMN control text NOT NULL;  
ALTER TABLE @extschema@.refresh_config_updater ADD COLUMN boundary interval NOT NULL DEFAULT '10 minutes'::interval;
ALTER TABLE @extschema@.refresh_config_updater ADD COLUMN last_value timestamptz NOT NULL;
ALTER TABLE @extschema@.refresh_config_updater ADD COLUMN pk_name text[] NOT NULL; 
ALTER TABLE @extschema@.refresh_config_updater ADD COLUMN pk_type text[] NOT NULL;
ALTER TABLE @extschema@.refresh_config_updater ADD COLUMN dst_active boolean NOT NULL DEFAULT true;
ALTER TABLE @extschema@.refresh_config_updater ADD COLUMN dst_start int NOT NULL DEFAULT 30;
ALTER TABLE @extschema@.refresh_config_updater ADD COLUMN dst_end int NOT NULL DEFAULT 230;
ALTER TABLE @extschema@.refresh_config_updater ALTER COLUMN type SET DEFAULT 'updater';
ALTER TABLE @extschema@.refresh_config_updater ADD CONSTRAINT refresh_config_updater_type_check CHECK (type = 'updater');  

CREATE TABLE refresh_config_dml (LIKE @extschema@.refresh_config INCLUDING ALL) INHERITS (@extschema@.refresh_config);
SELECT pg_catalog.pg_extension_config_dump('refresh_config_dml', '');
ALTER TABLE @extschema@.refresh_config_dml ADD CONSTRAINT refresh_config_dml_dblink_fkey FOREIGN KEY (dblink) REFERENCES @extschema@.dblink_mapping(data_source_id);
ALTER TABLE @extschema@.refresh_config_dml ADD CONSTRAINT refresh_config_dml_dest_table_pkey PRIMARY KEY (dest_table);
ALTER TABLE @extschema@.refresh_config_dml ADD COLUMN source_table text NOT NULL;
ALTER TABLE @extschema@.refresh_config_dml ADD COLUMN control text NOT NULL;  
ALTER TABLE @extschema@.refresh_config_dml ADD COLUMN pk_name text[] NOT NULL; 
ALTER TABLE @extschema@.refresh_config_dml ADD COLUMN pk_type text[] NOT NULL;
ALTER TABLE @extschema@.refresh_config_dml ALTER COLUMN type SET DEFAULT 'dml';
ALTER TABLE @extschema@.refresh_config_dml ADD CONSTRAINT refresh_config_dml_type_check CHECK (type = 'dml');
ALTER TABLE @extschema@.refresh_config_dml ADD CONSTRAINT refresh_config_dml_source_table_unique UNIQUE (source_table);

CREATE TABLE refresh_config_logdel (LIKE @extschema@.refresh_config INCLUDING ALL) INHERITS (@extschema@.refresh_config);
SELECT pg_catalog.pg_extension_config_dump('refresh_config_logdel', '');
ALTER TABLE @extschema@.refresh_config_logdel ADD CONSTRAINT refresh_config_logdel_dblink_fkey FOREIGN KEY (dblink) REFERENCES @extschema@.dblink_mapping(data_source_id);
ALTER TABLE @extschema@.refresh_config_logdel ADD CONSTRAINT refresh_config_logdel_dest_table_pkey PRIMARY KEY (dest_table);
ALTER TABLE @extschema@.refresh_config_logdel ADD COLUMN source_table text NOT NULL;
ALTER TABLE @extschema@.refresh_config_logdel ADD COLUMN control text NOT NULL;  
ALTER TABLE @extschema@.refresh_config_logdel ADD COLUMN pk_name text[] NOT NULL; 
ALTER TABLE @extschema@.refresh_config_logdel ADD COLUMN pk_type text[] NOT NULL;
ALTER TABLE @extschema@.refresh_config_logdel ALTER COLUMN type SET DEFAULT 'logdel';
ALTER TABLE @extschema@.refresh_config_logdel ADD CONSTRAINT refresh_config_logdel_type_check CHECK (type = 'logdel');
ALTER TABLE @extschema@.refresh_config_logdel ADD CONSTRAINT refresh_config_logdel_source_table_unique UNIQUE (source_table);

CREATE TABLE refresh_config_table (LIKE @extschema@.refresh_config INCLUDING ALL) INHERITS (@extschema@.refresh_config);
SELECT pg_catalog.pg_extension_config_dump('refresh_config_table', '');
ALTER TABLE @extschema@.refresh_config_table ADD CONSTRAINT refresh_config_table_dblink_fkey FOREIGN KEY (dblink) REFERENCES @extschema@.dblink_mapping(data_source_id);
ALTER TABLE @extschema@.refresh_config_table ADD CONSTRAINT refresh_config_table_dest_table_pkey PRIMARY KEY (dest_table);
ALTER TABLE @extschema@.refresh_config_table ADD COLUMN source_table text NOT NULL;
ALTER TABLE @extschema@.refresh_config_table ADD COLUMN truncate_cascade boolean NOT NULL DEFAULT false;
ALTER TABLE @extschema@.refresh_config_table ADD COLUMN sequences text[];
ALTER TABLE @extschema@.refresh_config_table ALTER COLUMN type SET DEFAULT 'table';
ALTER TABLE @extschema@.refresh_config_table ADD CONSTRAINT refresh_config_snap_type_check CHECK (type = 'table');
