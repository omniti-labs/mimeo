DROP SCHEMA mimeo_source CASCADE;
DROP SCHEMA mimeo_dest CASCADE;
DROP DATABASE mimeo_source;
DELETE FROM mimeo.dblink_mapping WHERE username = 'mimeo_test';
DROP ROLE mimeo_test;
DROP FUNCTION IF EXISTS test_mimeo_maker();
DROP FUNCTION IF EXISTS test_mimeo_refresh();
DROP FUNCTION IF EXISTS test_mimeo_destroyer(text);


