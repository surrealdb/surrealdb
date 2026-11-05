/**
[test]
reason = """
	Setup for a_filter_over_records_reads_index_arguments_against_the_record_record_user:
	a record access to sign in with, a table the record user reads in full, and
	a table whose rows the user reads only where `n >= 5`, whose field `lim`
	is denied to the user and whose computed field `cl` reads it. Runs as
	owner.
"""
run = false
*/

DEFINE ACCESS user ON DATABASE TYPE RECORD SIGNIN ( $rid ) DURATION FOR TOKEN 1h, FOR SESSION 1h;
DEFINE TABLE person SCHEMALESS PERMISSIONS FULL;
CREATE person:bob;

DEFINE TABLE open SCHEMALESS PERMISSIONS FULL;
CREATE open:1 SET n = 1, lim = 1;
CREATE open:2 SET n = 5, lim = 1;
CREATE open:3 SET n = 10, lim = 1;

DEFINE TABLE guarded SCHEMALESS PERMISSIONS FOR select WHERE n >= 5;
DEFINE FIELD lim ON guarded PERMISSIONS FOR select NONE;
DEFINE FIELD cl ON guarded COMPUTED lim;
CREATE guarded:1 SET n = 1, lim = 1;
CREATE guarded:2 SET n = 10, lim = 1;
CREATE guarded:3 SET n = 20, lim = 1;
