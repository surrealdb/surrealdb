/**
[test]
reason = """
	Setup for unique_permissions, the tables of permissions under a `UNIQUE` index: a table whose
	select permission hides the rows that hold both extremes
	of an indexed `int` field, a table that nobody may read, a table nothing governs, a table
	whose rows an owner field judges, a table whose field `n` has a select permission of its own
	beside one that has it on another field, a table whose permission hides a row of unreadable
	kind, and the record user the test runs as. Runs as owner.
"""
run = false
*/

DEFINE ACCESS user ON DATABASE TYPE RECORD SIGNIN ( $rid );
DEFINE TABLE user SCHEMALESS PERMISSIONS FOR select FULL;
CREATE user:alice SET name = 'alice' RETURN NONE;
DEFINE TABLE pub SCHEMAFULL PERMISSIONS FOR select WHERE public = true;
DEFINE FIELD public ON pub TYPE bool;
DEFINE FIELD n ON pub TYPE int;
DEFINE INDEX n ON pub FIELDS n UNIQUE;
CREATE pub:1 SET public = false, n = 100 RETURN NONE;
CREATE pub:2 SET public = true, n = 50 RETURN NONE;
CREATE pub:3 SET public = false, n = -100 RETURN NONE;
CREATE pub:4 SET public = true, n = -10 RETURN NONE;
CREATE pub:5 SET public = true, n = 51 RETURN NONE;
CREATE pub:6 SET public = false, n = 101 RETURN NONE;
DEFINE TABLE denied SCHEMAFULL PERMISSIONS FOR select NONE;
DEFINE FIELD n ON denied TYPE int;
DEFINE INDEX n ON denied FIELDS n UNIQUE;
CREATE denied:1 SET n = 3 RETURN NONE;
CREATE denied:2 SET n = 8 RETURN NONE;
DEFINE TABLE open SCHEMAFULL PERMISSIONS FOR select FULL;
DEFINE FIELD n ON open TYPE int;
DEFINE INDEX n ON open FIELDS n UNIQUE;
CREATE open:1 SET n = 6 RETURN NONE;
CREATE open:2 SET n = -2 RETURN NONE;
CREATE open:3 SET n = 7 RETURN NONE;
DEFINE TABLE owned SCHEMAFULL PERMISSIONS FOR select WHERE owner = $auth.id;
DEFINE FIELD owner ON owned TYPE record;
DEFINE FIELD n ON owned TYPE int;
DEFINE INDEX n ON owned FIELDS n UNIQUE;
CREATE owned:1 SET owner = user:bob, n = 90 RETURN NONE;
CREATE owned:2 SET owner = user:alice, n = 20 RETURN NONE;
CREATE owned:3 SET owner = user:alice, n = 21 RETURN NONE;
CREATE owned:4 SET owner = user:bob, n = -90 RETURN NONE;
CREATE owned:5 SET owner = user:alice, n = -5 RETURN NONE;
DEFINE TABLE guarded SCHEMAFULL PERMISSIONS FOR select FULL;
DEFINE FIELD owner ON guarded TYPE record;
DEFINE FIELD n ON guarded TYPE int PERMISSIONS FOR select WHERE owner = $auth.id;
DEFINE FIELD m ON guarded TYPE int;
DEFINE INDEX n ON guarded FIELDS n UNIQUE;
DEFINE INDEX m ON guarded FIELDS m UNIQUE;
CREATE guarded:1 SET owner = user:bob, n = 70, m = 1 RETURN NONE;
CREATE guarded:2 SET owner = user:alice, n = 30, m = 2 RETURN NONE;
CREATE guarded:3 SET owner = user:bob, n = -70, m = 3 RETURN NONE;
CREATE guarded:4 SET owner = user:alice, n = -30, m = 4 RETURN NONE;
DEFINE TABLE mixed SCHEMALESS PERMISSIONS FOR select WHERE public = true;
CREATE mixed:1 SET public = false, n = 'zzz' RETURN NONE;
CREATE mixed:2 SET public = true, n = 5 RETURN NONE;
CREATE mixed:3 SET public = true, n = 'aaa' RETURN NONE;
CREATE mixed:4 SET public = true, n = 12 RETURN NONE;
DEFINE FIELD n ON mixed TYPE int;
DEFINE INDEX n ON mixed FIELDS n UNIQUE;
