/**
[test]
reason = """
	Setup for ddl_write_refusing_frame. A `select` PERMISSIONS guard and a
	COMPUTED body, each reaching a schema statement through a function, plus a
	record user to trigger them — an owner session bypasses permission guards
	entirely.

	Every DEFINE here succeeds. Each callee is read-only when the clause that
	calls it is defined, so the definition-time check accepts the clause, and is
	redefined with its schema statement afterwards, so the read reaches the
	runtime frame.

	The row `computed_holder:1` is created before the COMPUTED field is defined.
	A COMPUTED field is derived on every read, so a write to the table after the
	field exists evaluates the body — and the body is refused, which is what the
	importing test asserts rather than something this file may trip over.
"""
run = false
*/

-- Each function is read-only when the clause that calls it is defined.
DEFINE FUNCTION fn::define_from_guard() {
	RETURN true;
} PERMISSIONS FULL;

DEFINE FUNCTION fn::define_from_computed() {
	RETURN 'computed';
} PERMISSIONS FULL;

-- A `select` guard is evaluated on a read, with permission enforcement already
-- disabled, so the schema statement it reaches must be refused
-- (GHSA-66r2-5gwj-gxm2).
DEFINE TABLE guarded SCHEMALESS PERMISSIONS FOR select WHERE fn::define_from_guard();

-- A COMPUTED body runs on every read of the field, under the definer's auth,
-- so the schema statement it reaches must be refused as well.
DEFINE TABLE computed_holder SCHEMALESS PERMISSIONS FOR select FULL;
CREATE computed_holder:1 SET n = 1;
DEFINE FIELD computed ON computed_holder TYPE string
	COMPUTED fn::define_from_computed()
	PERMISSIONS FULL;

-- Each callee reaches its schema statement only after its clause is defined.
DEFINE FUNCTION OVERWRITE fn::define_from_guard() {
	DEFINE TABLE smuggled_by_guard SCHEMALESS;
	RETURN true;
} PERMISSIONS FULL;

DEFINE FUNCTION OVERWRITE fn::define_from_computed() {
	DEFINE TABLE smuggled_by_computed SCHEMALESS;
	RETURN 'computed';
} PERMISSIONS FULL;

DEFINE ACCESS user ON DATABASE TYPE RECORD SIGNIN ( $rid );

CREATE user:test;
CREATE guarded:1 SET visible = true;
