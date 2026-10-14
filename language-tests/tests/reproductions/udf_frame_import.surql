/**
[test]
reason = """
	Setup for udf_frame_write and udf_frame_perms. Two tables
	whose `select` guard calls a user-defined function, and a record user to
	trigger the guards (an owner session bypasses them).

	`secret` is reached only through a record link, so its guard is evaluated
	by a record dereference rather than by a scan: the function it calls
	writes, and that write has to be refused in that frame too.

	`doc` is readable by whoever `fn::is_member` admits. The function reads a
	field whose own `select` permission is `NONE`; inside a permission guard
	that field is readable, so the member is admitted.
"""
run = false
*/

-- `audit` is readable so the test can assert which guards wrote.
DEFINE TABLE audit SCHEMALESS PERMISSIONS FOR select FULL;

-- Idempotent marker, so the assertion does not depend on how many times a
-- guard is evaluated per statement.
DEFINE FUNCTION fn::mark_read() { RETURN true; } PERMISSIONS FULL;

-- A `select` guard reached through a record link. Its write must be refused.
DEFINE TABLE secret SCHEMALESS PERMISSIONS FOR select WHERE fn::mark_read();
DEFINE TABLE pub SCHEMALESS PERMISSIONS FULL;

-- The guard's function writes only after the guard is defined, so the
-- definition-time check accepts it and the write meets the runtime frame.
DEFINE FUNCTION OVERWRITE fn::mark_read() { UPSERT audit:reader SET hit = true; RETURN true; } PERMISSIONS FULL;

-- A membership table no record user may read directly; its `active` field
-- denies `select` on its own as well.
DEFINE TABLE membership SCHEMAFULL PERMISSIONS NONE;
DEFINE FIELD who ON membership TYPE record<user>;
DEFINE FIELD active ON membership TYPE bool PERMISSIONS FOR select NONE;
DEFINE FUNCTION fn::is_member($u: record<user>) {
	RETURN (SELECT VALUE active FROM ONLY membership WHERE who = $u LIMIT 1) ?? false;
} PERMISSIONS FULL;
DEFINE TABLE doc SCHEMALESS PERMISSIONS FOR select WHERE fn::is_member($auth.id);

DEFINE ACCESS user ON DATABASE TYPE RECORD SIGNIN ( $rid );

CREATE user:reader;
CREATE secret:1 SET name = 's';
CREATE pub:1 SET link = secret:1;
CREATE membership:1 SET who = user:reader, active = true;
CREATE doc:1 SET title = 'd';
