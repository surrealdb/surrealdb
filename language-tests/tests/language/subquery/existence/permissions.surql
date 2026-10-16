/**
[env]
imports = ["./permissions_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "user:alice" }

[test]
reason = """
	A subquery over a table a permission governs is read through that permission. A predicate that
	cannot raise leaves the existence read cut to a row; one that raises on a row the cut would
	skip, or that reads the caller, keeps the whole read, and the record user sees the error or
	the rows the whole read gives.
"""

[[test.results]]
error = "An error occurred: denied by function"

[[test.results]]
error = "An error occurred: denied by function"

[[test.results]]
value = "[outer:1, outer:2]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[outer:1, outer:2]"

[[test.results]]
value = "[outer:1, outer:2]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[outer:1, outer:2]"

[[test.results]]
value = "[[{ id: flagged:3 }], [{ id: flagged:2 }]]"
*/

SELECT VALUE id FROM outer WHERE (SELECT id FROM box WHERE x = $parent.x);
SELECT VALUE id FROM outer WHERE (SELECT id FROM box);
SELECT VALUE id FROM outer WHERE (SELECT id FROM flagged WHERE x = $parent.x);
SELECT VALUE id FROM outer WHERE (SELECT id FROM flagged WHERE x = 9);
SELECT VALUE id FROM outer WHERE !(SELECT id FROM flagged WHERE x = 9);
SELECT VALUE id FROM outer WHERE (SELECT id FROM mine WHERE x = $parent.x);
SELECT VALUE id FROM outer WHERE (SELECT id FROM mine WHERE x = 9);
SELECT VALUE id FROM outer WHERE (SELECT id FROM hidden);
SELECT VALUE id FROM outer WHERE !(SELECT id FROM hidden);
SELECT VALUE (SELECT id FROM flagged WHERE x = $parent.x) FROM outer;
