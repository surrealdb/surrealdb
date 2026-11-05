/**
[test]
reason = """
	Setup for a_registered_method_can_still_reach_a_closure: a table
	whose `secret` field no caller may select, two rows that differ only in
	that field, and the record user the test runs as. Runs as owner.
"""
run = false
*/

DEFINE TABLE user SCHEMALESS;
DEFINE ACCESS user ON DATABASE TYPE RECORD SIGNIN ( $rid );
CREATE user:probe;

DEFINE TABLE t SCHEMAFULL PERMISSIONS FULL;
DEFINE FIELD secret ON t TYPE string PERMISSIONS FOR select NONE;
DEFINE FIELD ok ON t TYPE int;
CREATE t:1 SET secret = 'yes', ok = 1;
CREATE t:2 SET secret = 'no', ok = 2;
