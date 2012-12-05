SELECT set_config('search_path','mimeo, dblink, tap',false);
SELECT plan(4);

DROP SCHEMA IF EXISTS mimeo_source CASCADE;
SELECT hasnt_schema('mimeo_source', 'Cleanup schema ''mimeo_source''');
DROP SCHEMA IF EXISTS mimeo_dest CASCADE;
SELECT hasnt_schema('mimeo_dest', 'Cleanup schema ''mimeo_dest''');
DROP DATABASE IF EXISTS mimeo_source;
DELETE FROM mimeo.dblink_mapping WHERE username = 'mimeo_test';
SELECT is_empty('SELECT * FROM dblink_mapping WHERE username = ''mimeo_test''', 'Cleanup mimeo_test role from dblink_mapping');
DROP ROLE IF EXISTS mimeo_test;
SELECT hasnt_role('mimeo_test', 'Drop ''mimeo_test'' role');

SELECT * FROM finish();
