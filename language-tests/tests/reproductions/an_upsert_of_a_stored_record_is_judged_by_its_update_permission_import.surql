/**
[test]
reason = """
Import for an_upsert_of_a_stored_record_is_judged_by_its_update_permission.surql:
a table a record user may create on and select but whose update permission
refuses every record, and a table the same user may update and select but not
create on, each holding one record. Runs as owner.
"""
run = false
*/

DEFINE TABLE user SCHEMALESS PERMISSIONS FULL;
CREATE user:1;

DEFINE TABLE locked SCHEMALESS
	PERMISSIONS
		FOR select, create FULL
		FOR update WHERE false;
CREATE locked:1 SET n = 1;

DEFINE TABLE update_only SCHEMALESS
	PERMISSIONS
		FOR select, update FULL
		FOR create NONE;
CREATE update_only:1 SET n = 1;
