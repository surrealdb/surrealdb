/**
[env]
namespace = true
database = true

[test]
reason = """
	A function's own PERMISSIONS guard is evaluated on every call under the
	no-write frame. If the guard reaches the function's body and that body is
	redefined to write, the function becomes uncallable — so the redefinition is
	refused at definition time. The guard-vs-new-body check must resolve the
	self-call against the body about to be stored, not the old stored one, and
	must not rely on the consumer scan (which skips the target's own stored
	guard). Covers DEFINE OVERWRITE and ALTER.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: log:seed }]"

[[test.results]]
error = "Cannot define function `fn::f`: it modifies data, but it must remain read-only because it is used by its own PERMISSIONS clause (fn::f)"

[[test.results]]
error = "Cannot define function `fn::f`: it modifies data, but it must remain read-only because it is used by its own PERMISSIONS clause (fn::f)"
*/

DEFINE TABLE log SCHEMALESS;

-- Read-only body with a self-referential guard: defines fine.
DEFINE FUNCTION fn::f() { RETURN 1; } PERMISSIONS WHERE fn::f();

CREATE log:seed;

-- Redefining the body to write is refused: the guard would evaluate the new
-- writing body under the no-write frame on every call.
DEFINE FUNCTION OVERWRITE fn::f() { CREATE log; RETURN 1; } PERMISSIONS WHERE fn::f();

-- Same refusal through ALTER (which reuses the stored guard).
ALTER FUNCTION fn::f() { CREATE log; RETURN 1; };
