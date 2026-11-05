/**
[test]
reason = """
	Setup for a_write_clause_element_filter_reads_index_and_method_arguments_against_the_record_record_user:
	a record access to sign in with, and tables the record user updates, whose
	rows and update permissions hold an element filter reading an index argument
	or a method argument of the record, with and without a closure. Runs as
	owner.
"""
run = false
*/

DEFINE ACCESS user ON DATABASE TYPE RECORD SIGNIN ( $rid ) DURATION FOR TOKEN 1h, FOR SESSION 1h;
DEFINE TABLE person SCHEMALESS PERMISSIONS FULL;
CREATE person:bob;

DEFINE TABLE pu_cl_i SCHEMALESS PERMISSIONS FOR select FULL FOR update WHERE [3][WHERE array::any([$this], |$e| $e >= [0, 8, 100][idx])] != [] FOR create, delete FULL;
CREATE pu_cl_i:1 SET idx = 1, lim = 8;
CREATE pu_cl_i:2 SET idx = 2, lim = 1;
DEFINE TABLE uw_cl_i SCHEMALESS PERMISSIONS FULL;
CREATE uw_cl_i:1 SET idx = 1, lim = 8;
CREATE uw_cl_i:2 SET idx = 2, lim = 1;
DEFINE TABLE pu_cf_i SCHEMALESS PERMISSIONS FOR select FULL FOR update WHERE [3][WHERE $this >= [0, 8, 100][idx]] != [] FOR create, delete FULL;
CREATE pu_cf_i:1 SET idx = 1, lim = 8;
CREATE pu_cf_i:2 SET idx = 2, lim = 1;
DEFINE TABLE uw_cf_i SCHEMALESS PERMISSIONS FULL;
CREATE uw_cf_i:1 SET idx = 1, lim = 8;
CREATE uw_cf_i:2 SET idx = 2, lim = 1;
DEFINE TABLE pu_cl_m SCHEMALESS PERMISSIONS FOR select FULL FOR update WHERE [3][WHERE array::any([$this], |$e| $e >= [0].concat([lim]).max())] != [] FOR create, delete FULL;
CREATE pu_cl_m:1 SET idx = 1, lim = 8;
CREATE pu_cl_m:2 SET idx = 2, lim = 1;
DEFINE TABLE uw_cl_m SCHEMALESS PERMISSIONS FULL;
CREATE uw_cl_m:1 SET idx = 1, lim = 8;
CREATE uw_cl_m:2 SET idx = 2, lim = 1;
DEFINE TABLE pu_cf_m SCHEMALESS PERMISSIONS FOR select FULL FOR update WHERE [3][WHERE $this >= [0].concat([lim]).max()] != [] FOR create, delete FULL;
CREATE pu_cf_m:1 SET idx = 1, lim = 8;
CREATE pu_cf_m:2 SET idx = 2, lim = 1;
DEFINE TABLE uw_cf_m SCHEMALESS PERMISSIONS FULL;
CREATE uw_cf_m:1 SET idx = 1, lim = 8;
CREATE uw_cf_m:2 SET idx = 2, lim = 1;
