/**
[env]
imports = ["language/planner/id_components_permission_bounds_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "user:alice" }

[test]
reason = """
	A key range over a table whose select permission is part of what the
	access path is chosen from is read for a record user as for anyone, and
	its pinned components, range and range count are the definer's to see: the
	caller's EXPLAIN shows the scan with `<permission>` in their place. The
	rows are those the permission and the caller's predicate both admit:
	London's for alice, whatever city she asks for.
"""

[[test.results]]
match = '''string::contains($result, "KeyRangeScan") AND string::contains($result, "key: <permission>") AND string::contains($result, "range: <permission>") AND !string::contains($result, "TableScan")'''
error = false

[[test.results]]
value = "[2, 3]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[1, 2, 3]"

[[test.results]]
value = "[1]"
*/

EXPLAIN SELECT temp FROM reading WHERE city = 'London' AND time >= d'2024-01-01T00:00:01Z';
SELECT VALUE temp FROM reading WHERE city = 'London' AND time >= d'2024-01-01T00:00:01Z' ORDER BY temp;
SELECT VALUE temp FROM reading WHERE city = 'Paris';
SELECT VALUE temp FROM reading ORDER BY temp;
SELECT VALUE temp FROM reading WHERE city = 'London' AND time < d'2024-01-01T00:00:01.500Z';
