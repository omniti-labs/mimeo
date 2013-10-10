Mimeo Test Suite
================

The pgTAP testing suite is used to provide an extensive and easily maintainable set of tests. Please see the pgTAP home page for more details on its installation and use.

http://pgTAP.org/

Since mimeo uses dblink, testing cannot be done as recommended in the pgTAP documenation by putting everything into transactions that can be rolled back. The tests have been split into different logical groups in their own files and MUST be run in numerical order. They assume that the required extensions have been installed in the following schemas:

    dblink: dblink
    mimeo: mimeo
    pgTAP: public 

If you've installed any of the above extensions in a different schema and would like to run the test suite, simply change the configuration option found at the top of each testing file to match your setup.

    SELECT set_config('search_path','mimeo, dblink, public',false);

You will also need to ensure your pg_hba.conf file has a trust or md5 entry for the **mimeo_test** role connecting via the localhost. One or all of the entries below should work.
    
    host    all         mimeo_test      localhost           trust
    host    all         mimeo_test      127.0.0.1/32        trust
    host    all         mimeo_test      ::1/128             trust
    
Once that's done, it's best to use the **pg_prove** script that pgTAP comes with to run all the tests. I like using the -f -v options to get more useful feedback.

    pg_prove -f -v /path/to/mimeo/test/*sql

The tests must be run by a superuser since roles & databases are created & dropped as part of the test. The tests are not required to run mimeo, so if you don't feel safe doing this you don't need to run the tests. But if you are running into problems and report any issues without a clear explanation of what is wrong, I will ask that you run the test suite so you can try and narrow down where the problem may be. You are free to look through to tests to see exactly what they're doing. The final two test scripts should clean up everything and leave your database as it was before.
