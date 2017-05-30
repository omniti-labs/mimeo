-- ########## mimeo table definitions ##########
CREATE SEQUENCE dblink_mapping_mimeo_data_source_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;
CREATE TABLE dblink_mapping_mimeo (
    data_source_id integer NOT NULL DEFAULT nextval('@extschema@.dblink_mapping_mimeo_data_source_id_seq'),
    data_source text NOT NULL,
    username text NOT NULL,
    pwd text,
    CONSTRAINT dblink_mapping_mimeo_data_source_id_pkey PRIMARY KEY (data_source_id)
);
SELECT pg_catalog.pg_extension_config_dump('dblink_mapping_mimeo', '');
ALTER SEQUENCE dblink_mapping_mimeo_data_source_id_seq OWNED BY dblink_mapping_mimeo.data_source_id;

CREATE TABLE refresh_config (
    dest_table text NOT NULL,
    source_table text NOT NULL,
    type text NOT NULL,
    dblink integer NOT NULL,
    last_run timestamp with time zone,
    filter text[],
    condition text,
    period interval,
    batch_limit int,
    jobmon boolean DEFAULT false NOT NULL
);
SELECT pg_catalog.pg_extension_config_dump('refresh_config', '');
CREATE RULE refresh_config_parent_nodata AS ON INSERT TO @extschema@.refresh_config DO INSTEAD NOTHING;

CREATE TABLE refresh_config_snap (LIKE @extschema@.refresh_config INCLUDING ALL) INHERITS (@extschema@.refresh_config);
SELECT pg_catalog.pg_extension_config_dump('refresh_config_snap', '');
ALTER TABLE @extschema@.refresh_config_snap ADD CONSTRAINT refresh_config_snap_dblink_fkey FOREIGN KEY (dblink) REFERENCES @extschema@.dblink_mapping_mimeo(data_source_id);
ALTER TABLE @extschema@.refresh_config_snap ADD CONSTRAINT refresh_config_snap_dest_table_pkey PRIMARY KEY (dest_table);
ALTER TABLE @extschema@.refresh_config_snap ADD COLUMN check_stats boolean DEFAULT true NOT NULL;
ALTER TABLE @extschema@.refresh_config_snap ADD COLUMN n_tup_ins bigint;
ALTER TABLE @extschema@.refresh_config_snap ADD COLUMN n_tup_upd bigint;
ALTER TABLE @extschema@.refresh_config_snap ADD COLUMN n_tup_del bigint;
ALTER TABLE @extschema@.refresh_config_snap ADD COLUMN post_script text[];
ALTER TABLE @extschema@.refresh_config_snap ALTER COLUMN type SET DEFAULT 'snap';
ALTER TABLE @extschema@.refresh_config_snap ADD CONSTRAINT refresh_config_snap_type_check CHECK (type = 'snap');

CREATE TABLE refresh_config_inserter (LIKE @extschema@.refresh_config INCLUDING ALL) INHERITS (@extschema@.refresh_config);
ALTER TABLE @extschema@.refresh_config_inserter ADD COLUMN control text NOT NULL;
CREATE RULE refresh_config_inserter_parent_nodata AS ON INSERT TO @extschema@.refresh_config_inserter DO INSTEAD NOTHING;

CREATE TABLE refresh_config_inserter_time (LIKE @extschema@.refresh_config INCLUDING ALL) INHERITS (@extschema@.refresh_config_inserter);
SELECT pg_catalog.pg_extension_config_dump('refresh_config_inserter_time', '');
ALTER TABLE @extschema@.refresh_config_inserter_time ADD CONSTRAINT refresh_config_inserter_time_dblink_fkey FOREIGN KEY (dblink) REFERENCES @extschema@.dblink_mapping_mimeo(data_source_id);
ALTER TABLE @extschema@.refresh_config_inserter_time ADD CONSTRAINT refresh_config_inserter_time_dest_table_pkey PRIMARY KEY (dest_table);
ALTER TABLE @extschema@.refresh_config_inserter_time ADD COLUMN boundary interval NOT NULL DEFAULT '10 minutes'::interval;
ALTER TABLE @extschema@.refresh_config_inserter_time ADD COLUMN last_value timestamptz NOT NULL;
ALTER TABLE @extschema@.refresh_config_inserter_time ADD COLUMN dst_active boolean NOT NULL DEFAULT true;
ALTER TABLE @extschema@.refresh_config_inserter_time ADD COLUMN dst_start int NOT NULL DEFAULT 30;
ALTER TABLE @extschema@.refresh_config_inserter_time ADD COLUMN dst_end int NOT NULL DEFAULT 230;
ALTER TABLE @extschema@.refresh_config_inserter_time ALTER COLUMN type SET DEFAULT 'inserter_time';
ALTER TABLE @extschema@.refresh_config_inserter_time ADD CONSTRAINT refresh_config_inserter_type_check CHECK (type = 'inserter_time');

CREATE TABLE refresh_config_inserter_serial (LIKE @extschema@.refresh_config_inserter INCLUDING ALL) INHERITS (@extschema@.refresh_config_inserter);
SELECT pg_catalog.pg_extension_config_dump('refresh_config_inserter_serial', '');
ALTER TABLE @extschema@.refresh_config_inserter_serial ADD CONSTRAINT refresh_config_inserter_serial_dest_table_pkey PRIMARY KEY (dest_table);
ALTER TABLE @extschema@.refresh_config_inserter_serial ADD CONSTRAINT refresh_config_inserter_serial_dblink_fkey FOREIGN KEY (dblink) REFERENCES @extschema@.dblink_mapping_mimeo(data_source_id);
ALTER TABLE @extschema@.refresh_config_inserter_serial ADD COLUMN boundary int NOT NULL DEFAULT 0;
ALTER TABLE @extschema@.refresh_config_inserter_serial ADD COLUMN last_value bigint NOT NULL DEFAULT 0;
ALTER TABLE @extschema@.refresh_config_inserter_serial ALTER COLUMN type SET DEFAULT 'inserter_serial';
ALTER TABLE @extschema@.refresh_config_inserter_serial ADD CONSTRAINT refresh_config_inserter_type_chk CHECK (type = 'inserter_serial');

CREATE TABLE refresh_config_updater (LIKE @extschema@.refresh_config INCLUDING ALL) INHERITS (@extschema@.refresh_config);
SELECT pg_catalog.pg_extension_config_dump('refresh_config_updater', '');
ALTER TABLE @extschema@.refresh_config_updater ADD COLUMN control text NOT NULL;
ALTER TABLE @extschema@.refresh_config_updater ADD COLUMN pk_name text[] NOT NULL; 
ALTER TABLE @extschema@.refresh_config_updater ADD COLUMN pk_type text[] NOT NULL;
CREATE RULE refresh_config_updater_parent_nodata AS ON INSERT TO @extschema@.refresh_config_updater DO INSTEAD NOTHING;

CREATE TABLE refresh_config_updater_time (LIKE @extschema@.refresh_config INCLUDING ALL) INHERITS (@extschema@.refresh_config_updater);
SELECT pg_catalog.pg_extension_config_dump('refresh_config_updater_time', '');
ALTER TABLE @extschema@.refresh_config_updater_time ADD CONSTRAINT refresh_config_updater_time_dblink_fkey FOREIGN KEY (dblink) REFERENCES @extschema@.dblink_mapping_mimeo(data_source_id);
ALTER TABLE @extschema@.refresh_config_updater_time ADD CONSTRAINT refresh_config_updater_time_dest_table_pkey PRIMARY KEY (dest_table);
ALTER TABLE @extschema@.refresh_config_updater_time ADD COLUMN boundary interval NOT NULL DEFAULT '10 minutes'::interval;
ALTER TABLE @extschema@.refresh_config_updater_time ADD COLUMN last_value timestamptz NOT NULL;
ALTER TABLE @extschema@.refresh_config_updater_time ADD COLUMN dst_active boolean NOT NULL DEFAULT true;
ALTER TABLE @extschema@.refresh_config_updater_time ADD COLUMN dst_start int NOT NULL DEFAULT 30;
ALTER TABLE @extschema@.refresh_config_updater_time ADD COLUMN dst_end int NOT NULL DEFAULT 230;
ALTER TABLE @extschema@.refresh_config_updater_time ALTER COLUMN type SET DEFAULT 'updater_time';
ALTER TABLE @extschema@.refresh_config_updater_time ADD CONSTRAINT refresh_config_updater_type_check CHECK (type = 'updater_time');

CREATE TABLE refresh_config_updater_serial (LIKE @extschema@.refresh_config_updater INCLUDING ALL) INHERITS (@extschema@.refresh_config_updater);
SELECT pg_catalog.pg_extension_config_dump('refresh_config_updater_serial', '');
ALTER TABLE @extschema@.refresh_config_updater_serial ADD CONSTRAINT refresh_config_updater_serial_dest_table_pkey PRIMARY KEY (dest_table);
ALTER TABLE @extschema@.refresh_config_updater_serial ADD CONSTRAINT refresh_config_updater_serial_dblink_fkey FOREIGN KEY (dblink) REFERENCES @extschema@.dblink_mapping_mimeo(data_source_id);
ALTER TABLE @extschema@.refresh_config_updater_serial ADD COLUMN boundary int NOT NULL DEFAULT 0;
ALTER TABLE @extschema@.refresh_config_updater_serial ADD COLUMN last_value bigint NOT NULL DEFAULT 0;
ALTER TABLE @extschema@.refresh_config_updater_serial ALTER COLUMN type SET DEFAULT 'updater_serial';
ALTER TABLE @extschema@.refresh_config_updater_serial ADD CONSTRAINT refresh_config_updater_type_chk CHECK (type = 'updater_serial');

CREATE TABLE refresh_config_dml (LIKE @extschema@.refresh_config INCLUDING ALL) INHERITS (@extschema@.refresh_config);
SELECT pg_catalog.pg_extension_config_dump('refresh_config_dml', '');
ALTER TABLE @extschema@.refresh_config_dml ADD CONSTRAINT refresh_config_dml_dblink_fkey FOREIGN KEY (dblink) REFERENCES @extschema@.dblink_mapping_mimeo(data_source_id);
ALTER TABLE @extschema@.refresh_config_dml ADD CONSTRAINT refresh_config_dml_dest_table_pkey PRIMARY KEY (dest_table);
ALTER TABLE @extschema@.refresh_config_dml ADD COLUMN control text NOT NULL;  
ALTER TABLE @extschema@.refresh_config_dml ADD COLUMN pk_name text[] NOT NULL; 
ALTER TABLE @extschema@.refresh_config_dml ADD COLUMN pk_type text[] NOT NULL;
ALTER TABLE @extschema@.refresh_config_dml ADD COLUMN insert_on_fetch boolean NOT NULL DEFAULT true;

ALTER TABLE @extschema@.refresh_config_dml ALTER COLUMN type SET DEFAULT 'dml';
ALTER TABLE @extschema@.refresh_config_dml ADD CONSTRAINT refresh_config_dml_type_check CHECK (type = 'dml');
ALTER TABLE @extschema@.refresh_config_dml ADD CONSTRAINT refresh_config_dml_source_dest_unique UNIQUE (source_table, dest_table);

CREATE TABLE refresh_config_logdel (LIKE @extschema@.refresh_config INCLUDING ALL) INHERITS (@extschema@.refresh_config);
SELECT pg_catalog.pg_extension_config_dump('refresh_config_logdel', '');
ALTER TABLE @extschema@.refresh_config_logdel ADD CONSTRAINT refresh_config_logdel_dblink_fkey FOREIGN KEY (dblink) REFERENCES @extschema@.dblink_mapping_mimeo(data_source_id);
ALTER TABLE @extschema@.refresh_config_logdel ADD CONSTRAINT refresh_config_logdel_dest_table_pkey PRIMARY KEY (dest_table);
ALTER TABLE @extschema@.refresh_config_logdel ADD COLUMN control text NOT NULL;  
ALTER TABLE @extschema@.refresh_config_logdel ADD COLUMN pk_name text[] NOT NULL; 
ALTER TABLE @extschema@.refresh_config_logdel ADD COLUMN pk_type text[] NOT NULL;
ALTER TABLE @extschema@.refresh_config_logdel ADD COLUMN insert_on_fetch boolean NOT NULL DEFAULT true;
ALTER TABLE @extschema@.refresh_config_logdel ALTER COLUMN type SET DEFAULT 'logdel';
ALTER TABLE @extschema@.refresh_config_logdel ADD CONSTRAINT refresh_config_logdel_type_check CHECK (type = 'logdel');
ALTER TABLE @extschema@.refresh_config_logdel ADD CONSTRAINT refresh_config_logdel_source_dest_unique UNIQUE (source_table, dest_table);

CREATE TABLE refresh_config_table (LIKE @extschema@.refresh_config INCLUDING ALL) INHERITS (@extschema@.refresh_config);
SELECT pg_catalog.pg_extension_config_dump('refresh_config_table', '');
ALTER TABLE @extschema@.refresh_config_table ADD CONSTRAINT refresh_config_table_dblink_fkey FOREIGN KEY (dblink) REFERENCES @extschema@.dblink_mapping_mimeo(data_source_id);
ALTER TABLE @extschema@.refresh_config_table ADD CONSTRAINT refresh_config_table_dest_table_pkey PRIMARY KEY (dest_table);
ALTER TABLE @extschema@.refresh_config_table ADD COLUMN truncate_cascade boolean NOT NULL DEFAULT false;
ALTER TABLE @extschema@.refresh_config_table ADD COLUMN sequences text[];
ALTER TABLE @extschema@.refresh_config_table ALTER COLUMN type SET DEFAULT 'table';
ALTER TABLE @extschema@.refresh_config_table ADD CONSTRAINT refresh_config_snap_type_check CHECK (type = 'table');
