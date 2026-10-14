/**
[test]
reason = """
	Setup for perm_predicate_runtime_block. Two tables whose permission guards
	call a writing function, one on the clause a read evaluates and one on the
	clause a write evaluates, plus a record user to trigger them (an owner
	session bypasses permission guards entirely).

	Every DEFINE here succeeds: each function only reads when the guard that
	calls it is defined, and is redefined to write afterwards, so it is the
	runtime frame that meets the write. The assertions live in the test that
	imports this.
"""
run = false
*/

-- `audit` is readable so the test can assert exactly which guards wrote.
DEFINE TABLE audit SCHEMALESS PERMISSIONS FOR select FULL;

-- Idempotent markers, so the assertions do not depend on how many times a
-- permission guard is evaluated per operation.
DEFINE FUNCTION fn::mark_read() { RETURN true; } PERMISSIONS FULL;
DEFINE FUNCTION fn::mark_write() { RETURN true; } PERMISSIONS FULL;

-- A `select` guard is evaluated on a read, with permission enforcement already
-- disabled, so its write must be refused (GHSA-66r2-5gwj-gxm2).
DEFINE TABLE readable SCHEMALESS PERMISSIONS FOR select WHERE fn::mark_read();

-- An `update` guard is a schema clause too, so its write is refused as well.
DEFINE TABLE writable SCHEMALESS
	PERMISSIONS
		FOR select FULL
		FOR update WHERE fn::mark_write() OR true;

-- Each guard's function writes only after the guard is defined.
DEFINE FUNCTION OVERWRITE fn::mark_read() { UPSERT audit:reader SET hit = true; RETURN true; } PERMISSIONS FULL;
DEFINE FUNCTION OVERWRITE fn::mark_write() { UPSERT audit:writer SET hit = true; RETURN true; } PERMISSIONS FULL;

DEFINE ACCESS user ON DATABASE TYPE RECORD SIGNIN ( $rid );

CREATE user:test;
CREATE readable:1 SET visible = true;
CREATE writable:1 SET n = 0;
