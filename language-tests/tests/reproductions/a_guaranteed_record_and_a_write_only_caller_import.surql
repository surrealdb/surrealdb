/**
[test]
reason = """
Import for a_guaranteed_record_and_a_write_only_caller.surql: a table a record
user may write every record of and read none of, holding one record for a
conditional `UPSERT` to match, and an event that records each write on a table
the same user may read. The event is the only way the test can see what was
written, since the caller may select nothing on the table itself. Runs as
owner.
"""
run = false
*/

DEFINE TABLE trace SCHEMALESS PERMISSIONS FULL;

DEFINE TABLE hidden_writable SCHEMALESS
	PERMISSIONS
		FOR select NONE
		FOR create, update, delete FULL;

DEFINE EVENT traced ON hidden_writable WHEN true THEN {
	CREATE trace SET did = $event;
};

DEFINE TABLE user SCHEMALESS PERMISSIONS FULL;
CREATE user:1;
CREATE hidden_writable:1 SET n = 0;
DELETE trace;
