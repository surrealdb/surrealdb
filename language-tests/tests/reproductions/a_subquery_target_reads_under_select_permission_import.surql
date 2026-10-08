/**
[test]
reason = """
Import: the tables `a_subquery_target_reads_under_select_permission` needs — a
table a record user may write but not read, a table it may do both to, and one
witness table per side effect the test looks for.

Each witness is its own table so that reading it back never depends on the
order two records of one table come out of a scan in.
"""
run = false
*/

DEFINE TABLE user SCHEMALESS;

DEFINE TABLE t SCHEMALESS
	PERMISSIONS
		FOR select NONE
		FOR create FULL
		FOR update FULL
		FOR delete FULL;

DEFINE TABLE u SCHEMALESS PERMISSIONS FULL;

DEFINE TABLE leak SCHEMALESS PERMISSIONS FULL;
DEFINE TABLE read_ok SCHEMALESS PERMISSIONS FULL;
DEFINE TABLE write_ok SCHEMALESS PERMISSIONS FULL;

CREATE user:1;
CREATE t:1 SET secret = 'alpha';
CREATE t:2 SET secret = 'bravo';
CREATE u:1 SET n = 1;
