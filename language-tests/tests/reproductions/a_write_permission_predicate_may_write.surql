/**
[env]
imports = ["reproductions/a_write_permission_predicate_may_write_import.surql"]
auth = { namespace = "test", database = "test", access = "test", rid = "user:1" }

[test]
reason = """
A `PERMISSIONS FOR create`, `update` or `delete` predicate may write. It is
reached from a statement that is already writing, so a guard that records the
attempt performs its write rather than being refused, and the statement it
guards goes on to admit the record.

A `PERMISSIONS FOR select` predicate may not, and the refusal fails the read
rather than denying the row: a guard that could not be judged is not a guard
that answered no.

The two clauses below differ only in which action they guard, so the
difference between them is the frame each is judged in and nothing else.
"""

[[test.results]]
value = "false"

[[test.results]]
value = "[1]"

[[test.results]]
value = "true"

[[test.results]]
error = "Failed to check permission: A PERMISSIONS clause cannot contain a statement that modifies data"
*/

-- Nothing has judged a write yet.
RETURN (SELECT * FROM audit) != [];

-- The guard writes while it judges, and admits the record.
UPDATE doc:1 SET n = 1 RETURN VALUE n;
RETURN (SELECT * FROM audit) != [];

-- The same expression under a read guard is refused, and the read fails.
SELECT VALUE n FROM watched:1;
