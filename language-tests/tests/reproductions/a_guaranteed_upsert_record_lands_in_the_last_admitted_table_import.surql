/**
[test]
reason = """
Import for a_guaranteed_upsert_record_lands_in_the_last_admitted_table.surql:
three pairs of bare tables a multi-target `UPSERT` can name, each pair covering
one arrangement of the record user's write permission — both halves writable,
the first half denied, the second half denied. Every table stays selectable so
the test can read back which one received the record. Runs as owner.
"""
run = false
*/

-- Both halves writable: the record belongs to the one written last.
DEFINE TABLE alpha SCHEMALESS PERMISSIONS FULL;
DEFINE TABLE beta SCHEMALESS PERMISSIONS FULL;

-- The first half is not writable by this caller.
DEFINE TABLE denied_first SCHEMALESS
	PERMISSIONS
		FOR select FULL
		FOR create, update, delete NONE;
DEFINE TABLE allowed_second SCHEMALESS PERMISSIONS FULL;

-- The second half is not writable by this caller.
DEFINE TABLE allowed_first SCHEMALESS PERMISSIONS FULL;
DEFINE TABLE denied_second SCHEMALESS
	PERMISSIONS
		FOR select FULL
		FOR create, update, delete NONE;

-- The record the session authenticates as.
DEFINE TABLE user SCHEMALESS PERMISSIONS FULL;
CREATE user:1;
