[![PGXN version](https://badge.fury.io/pg/mimeo.svg)](https://badge.fury.io/pg/mimeo)

Mimeo
=====

Mimeo is an extension that provides specialized, per-table replication between PostgreSQL instances. It currently provides snapshot (whole table copy), incremental (based on an incrementing timestamp or id), and DML (inserts, updates and deletes).

Also installing the pg_jobmon extension (see other repositories in omniti-labs) to log all replication activity and provide monitoring is highly recommended. 

In addition to the documentation, some additional information about this extension is discussed on the author's blog http://www.keithf4.com/tag/mimeo/

INSTALLATION
------------

Requirements: PostgreSQL 9.1+, dblink extension 

Recommendations: pg_jobmon (>= 1.3.2) extension (https://github.com/omniti-labs/pg_jobmon)

In directory where you downloaded mimeo to run

    make
    make install

Log into PostgreSQL and run the following commands. Schema can be whatever you wish, but it cannot be changed after installation.

    CREATE SCHEMA mimeo;
    CREATE EXTENSION mimeo SCHEMA mimeo;

See the doc folder for more usage information. The howto.md file provides a quickstart guide. The mimeo.md file contains a full reference guide.


UPGRADE
-------

Make sure all the upgrade scripts for the version you have installed up to the most recent version are in the $BASEDIR/share/extension folder. 

    ALTER EXTENSION mimeo UPDATE TO '<latest version>';


AUTHOR
------

Keith Fiske  
OmniTI, Inc - http://www.omniti.com  
keith@omniti.com


LICENSE AND COPYRIGHT
---------------------

Mimeo is released under the PostgreSQL License, a liberal Open Source license, similar to the BSD or MIT licenses.

Copyright (c) 2016 OmniTI, Inc.

Permission to use, copy, modify, and distribute this software and its documentation for any purpose, without fee, and without a written agreement is hereby granted, provided that the above copyright notice and this paragraph and the following two paragraphs appear in all copies.

IN NO EVENT SHALL THE AUTHOR BE LIABLE TO ANY PARTY FOR DIRECT, INDIRECT, SPECIAL, INCIDENTAL, OR CONSEQUENTIAL DAMAGES, INCLUDING LOST PROFITS, ARISING OUT OF THE USE OF THIS SOFTWARE AND ITS DOCUMENTATION, EVEN IF THE AUTHOR HAS BEEN ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

THE AUTHOR SPECIFICALLY DISCLAIMS ANY WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE. THE SOFTWARE PROVIDED HEREUNDER IS ON AN "AS IS" BASIS, AND THE AUTHOR HAS NO OBLIGATIONS TO PROVIDE MAINTENANCE, SUPPORT, UPDATES, ENHANCEMENTS, OR MODIFICATIONS.
