/**
[env]
namespace = true
database = true
auth = { level = "owner" }

# `mutable_permissions` off (the production default) so create/update/delete
# write clauses are refused; `files` stays on for DEFINE BUCKET below.
[env.capabilities]
allow-experimental = ["files"]

[test]
reason = """
	ALTER stores the same definition shape DEFINE does, so it must enforce the
	same rule: a PERMISSIONS clause cannot modify data (GHSA-66r2-5gwj-gxm2),
	directly or through a function call. Previously only DEFINE (and ALTER
	FUNCTION) checked, so a writing guard could be installed via ALTER. One
	refusal is pinned per ALTER statement kind that can set permissions
	(module is checked by the same code path but needs the surrealism
	capability, so it is exercised in unit coverage rather than here). With
	`mutable_permissions` off, a create/update write clause is refused with the
	capability-specific message; a SELECT clause is refused unconditionally.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Found a non-read-only expression in the PERMISSIONS clause for table `t`, but a PERMISSIONS clause must not modify data"

[[test.results]]
error = "Cannot define field `f`: a create/update/delete PERMISSIONS clause modifies data, which requires the `mutable_permissions` capability to be enabled; otherwise move the side effect to a DEFINE EVENT"

[[test.results]]
error = "Found a non-read-only expression in the PERMISSIONS clause for param `p`, but a PERMISSIONS clause must not modify data"

[[test.results]]
error = "Found a non-read-only expression in the PERMISSIONS clause for bucket `b`, but a PERMISSIONS clause must not modify data"

[[test.results]]
error = "The PERMISSIONS clause for table `t` calls `fn::writer`, which modifies data, but a PERMISSIONS clause must not modify data"

[[test.results]]
value = "NONE"
*/

-- Setup: everything defined read-only first.
DEFINE TABLE t SCHEMALESS;
DEFINE FIELD f ON t TYPE option<number>;
DEFINE PARAM $p VALUE 1;
DEFINE BUCKET b BACKEND 'memory';
DEFINE FUNCTION fn::writer() { CREATE audit SET marker = true; RETURN true; };

-- A direct write in the altered permission guard is refused, one per kind.
ALTER TABLE t PERMISSIONS FOR select WHERE (CREATE audit) OR true;
ALTER FIELD f ON t PERMISSIONS FOR update WHERE (UPDATE t SET x = 1) OR true;
ALTER PARAM $p PERMISSIONS WHERE (CREATE audit) OR true;
ALTER BUCKET b PERMISSIONS WHERE (CREATE audit) OR true;

-- A write reached through a function call is refused too, naming the function.
ALTER TABLE t PERMISSIONS FOR select WHERE fn::writer();

-- Control: a read-only guard alters fine.
ALTER TABLE t PERMISSIONS FOR select WHERE published = true;
