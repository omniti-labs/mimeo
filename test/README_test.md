Mimeo Test Suite
==============
The scripts in this folder can be used to test mimeo and make sure it is working as it should. All sql files and functions must be run as a super user in the database since it creates a new test super user and a database.

Run the files in this order:

    db=# \i test_mimeo_setup.sql

This will create a test "source" database, another test role, and schemas needed on the local database.

    db=# \i test_mimeo_maker.sql
    db=# select test_mimeo_maker();

This will create the objects in the "source" database above, run all maker functions and insert intial data in source tables
A 30 second sleep is forced to ensure enough time passes for new data to be picked up in the following step

    db=# \i test_mimeo_refresh.sql
    db=# select test_mimeo_refresh();

Test all the refresh functions. Should pull the new data inserted a the end of the maker test.
Currently the test suite does not automatically validate the data (working on it).
For now, check all the tables/views created in both the mimeo_source and mimeo_dest schemas that were created on the database running the test.
There should be at minimum 3 rows in all tables if things ran successfully.
You can also check the pg_jobmon logs to ensure everything ran as it should.

    db=# \i test_mimeo_destroyer.sql
    db=# select test_mimeo_destroyer('archive option');

Tests all the destroyer functions. Pass 'ARCHIVE' to test and make sure the archive option works (applies to all calls). Pass anything else to have it drop all objects in the test schemas.

    db=# \i test_mimeo_cleanup.sql

Clean up everything. Drop the test role, test schemas and test database.
