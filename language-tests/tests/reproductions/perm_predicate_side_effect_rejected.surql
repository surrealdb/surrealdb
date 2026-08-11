/**
[env]
namespace = true
database = true
auth = { level = "owner" }

# The production default: `mutable_permissions` off, so create/update/delete
# write clauses are refused at definition time like SELECT clauses.
[env.capabilities]
allow-experimental = []

[test]
reason = "A side-effecting PERMISSIONS predicate must be rejected at definition time (GHSA-66r2-5gwj-gxm2). Read-only predicates, helper-function calls and read-only subqueries remain valid. A SELECT clause is refused unconditionally; a create/update/delete write clause is refused with the capability-specific message when `mutable_permissions` is off."

[[test.results]]
value = "NONE"

[[test.results]]
error = "Found a non-read-only expression in the PERMISSIONS clause for table `bad`, but a PERMISSIONS clause must not modify data"

[[test.results]]
error = "Cannot define field `f`: a create/update/delete PERMISSIONS clause modifies data, which requires the `mutable_permissions` capability to be enabled; otherwise move the side effect to a DEFINE EVENT"

[[test.results]]
error = "Found a non-read-only expression in the PERMISSIONS clause for function `fn::bad`, but a PERMISSIONS clause must not modify data"

[[test.results]]
error = "Found a non-read-only expression in the PERMISSIONS clause for param `bad`, but a PERMISSIONS clause must not modify data"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"
*/

-- 0: control — a read-only table permission predicate is accepted
DEFINE TABLE ok PERMISSIONS FOR select WHERE published = true;

-- 1: a table permission predicate that performs a direct write is rejected
DEFINE TABLE bad PERMISSIONS FOR select, update WHERE (CREATE audit SET marker = true) OR true;

-- 2: a field permission predicate that performs a direct write is rejected
DEFINE FIELD f ON ok TYPE option<number> PERMISSIONS FOR update WHERE (UPDATE ok SET x = 1) OR true;

-- 3: a function permission predicate that performs a direct write is rejected
DEFINE FUNCTION fn::bad() { RETURN true; } PERMISSIONS WHERE (DELETE ok) OR true;

-- 4: a param permission predicate that performs a direct write is rejected
DEFINE PARAM $bad VALUE 1 PERMISSIONS WHERE (CREATE audit) OR true;

-- 5: control — a read-only helper function defines fine
DEFINE FUNCTION fn::is_ok() { RETURN true; } PERMISSIONS FULL;

-- 6: control — calling a function from a permission predicate is allowed (opaque at definition; writes inside it are blocked at runtime)
DEFINE TABLE okfn PERMISSIONS FOR select WHERE fn::is_ok();

-- 7: control — a read-only subquery in a permission predicate is allowed
DEFINE TABLE oksub PERMISSIONS FOR select WHERE id IN (SELECT VALUE id FROM ok);
