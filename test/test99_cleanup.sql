\set ON_ERROR_ROLLBACK 1
\set ON_ERROR_STOP true

SELECT set_config('search_path','mimeo, dblink, public',false);
SELECT plan(7);

DROP SCHEMA IF EXISTS mimeo_source CASCADE;
SELECT hasnt_schema('mimeo_source', 'Cleanup schema ''mimeo_source''');
DROP SCHEMA IF EXISTS mimeo_dest CASCADE;
SELECT hasnt_schema('mimeo_dest', 'Cleanup schema ''mimeo_dest''');
DROP DATABASE IF EXISTS mimeo_source;
DELETE FROM mimeo.dblink_mapping_mimeo WHERE username = 'mimeo_test';
SELECT is_empty('SELECT * FROM dblink_mapping_mimeo WHERE username = ''mimeo_test''', 'Cleanup mimeo_test role from dblink_mapping_mimeo');
DROP ROLE IF EXISTS mimeo_owner;
SELECT hasnt_role('mimeo_owner', 'Drop ''mimeo_owner'' role');
DROP ROLE IF EXISTS mimeo_test;
SELECT hasnt_role('mimeo_test', 'Drop ''mimeo_test'' role');
DROP ROLE IF EXISTS mimeo_dumb_role;
SELECT hasnt_role('mimeo_dumb_role', 'Drop ''mimeo_dumb_role'' role');
DROP ROLE IF EXISTS "mimeo-dumber-role";
SELECT hasnt_role('mimeo-dumber-role', 'Drop ''mimeo-dumb-role'' role');

SELECT * FROM finish();
