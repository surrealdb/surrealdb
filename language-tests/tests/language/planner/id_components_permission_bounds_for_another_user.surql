/**
[env]
imports = ["language/planner/id_components_permission_bounds_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "user:bob" }

[test]
reason = """
	The companion of id_components_permission_bounds for the user of the other
	city: the same statements read the key ranges they select, and the
	permission admits Paris's rows alone.
"""

[[test.results]]
match = '''string::contains($result, "KeyRangeScan") AND string::contains($result, "key: <permission>") AND !string::contains($result, "TableScan")'''
error = false

[[test.results]]
value = "[]"

[[test.results]]
value = "[5]"

[[test.results]]
value = "[4, 5]"
*/

EXPLAIN SELECT temp FROM reading WHERE city = 'Paris' AND time > d'2024-01-01T00:00:00Z';
SELECT VALUE temp FROM reading WHERE city = 'London';
SELECT VALUE temp FROM reading WHERE city = 'Paris' AND time > d'2024-01-01T00:00:00Z';
SELECT VALUE temp FROM reading ORDER BY temp;
