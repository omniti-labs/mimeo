CREATE DATABASE mimeo_source;
CREATE ROLE mimeo_test WITH LOGIN SUPERUSER PASSWORD 'mimeo_test';
CREATE SCHEMA mimeo_source;
CREATE SCHEMA mimeo_dest;
INSERT INTO mimeo.dblink_mapping (data_source, username, pwd) VALUES ('host=localhost port=5432 dbname=mimeo_source', 'mimeo_test', 'mimeo_test');
