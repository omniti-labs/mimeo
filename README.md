mimeo
=====

mimeo is an extension that provides specialized replication between PostgreSQL instances. It currently provides snapshot (whole table copy), incremental (based on an incrementing value like timestamp, serial (coming soon), etc), and DML (inserts, updates and deletes).

It currently requires the pg_jobmon (see my other repositories) extension to log all replication activity. May see about making this optional in the future, but I would hope that anyone using a replication scheme like this would want it logged in detail!

Still in very early testing. Would appreciate any feedback!

INSTALLATION
------------

Requirements: dblink & pg_jobmon extensions

In directory where you downloaded mimeo to run

    make
    make install

Log into PostgreSQL and run the following commands. Schema can be whatever you wish, but it cannot be changed after installation.

    CREATE SCHEMA mimeo;
    CREATE EXTENSION mimeo SCHEMA mimeo;


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

mimeo is released under the PostgreSQL License, a liberal Open Source license, similar to the BSD or MIT licenses.

Copyright (c) 2012 OmniTI, Inc.

Permission to use, copy, modify, and distribute this software and its documentation for any purpose, without fee, and without a written agreement is hereby granted, provided that the above copyright notice and this paragraph and the following two paragraphs appear in all copies.

IN NO EVENT SHALL THE AUTHOR BE LIABLE TO ANY PARTY FOR DIRECT, INDIRECT, SPECIAL, INCIDENTAL, OR CONSEQUENTIAL DAMAGES, INCLUDING LOST PROFITS, ARISING OUT OF THE USE OF THIS SOFTWARE AND ITS DOCUMENTATION, EVEN IF THE AUTHOR HAS BEEN ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

THE AUTHOR SPECIFICALLY DISCLAIMS ANY WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE. THE SOFTWARE PROVIDED HEREUNDER IS ON AN "AS IS" BASIS, AND THE AUTHOR HAS NO OBLIGATIONS TO PROVIDE MAINTENANCE, SUPPORT, UPDATES, ENHANCEMENTS, OR MODIFICATIONS.
