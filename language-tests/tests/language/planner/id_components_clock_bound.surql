/**
[env]
namespace = true
database = true
auth = { level = "owner" }
imports = ["language/planner/id_components_key_range_import.surql"]

[test]
reason = """
	A bound over a datetime component that reads the clock is pushed where the
	statement folds the clock to one instant before it plans, which a SELECT
	does: the range and the predicate above it then hold the same instant. A
	user-defined function's argument bounds the range its body reads, and a
	call of a function as a bound gives the rows the predicate admits whether
	or not the range takes it. The readings are of 2024, so a bound an hour
	before now admits none of them and one at now admits every one.
"""

[[test.results]]
match = '''string::contains($result, "KeyRangeScan") AND string::contains($result, "key: city = 'London'") AND string::contains($result, "range: time > ") AND !string::contains($result, "TableScan")'''
error = false

[[test.results]]
value = "[]"

[[test.results]]
value = "[0, 3, 6, 9, 12, 15, 18, 21, 24, 27, 30, 33, 36, 39, 42, 45, 48, 51, 54, 57]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[51, 54, 57]"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
match = '''string::contains($result, "KeyRangeScan") AND string::contains($result, "key: city = 'London'") AND !string::contains($result, "TableScan")'''
error = false

[[test.results]]
value = "[51, 54, 57]"
*/

EXPLAIN SELECT temp FROM reading WHERE city = 'London' AND time > time::now() - 1h;
SELECT VALUE temp FROM reading WHERE city = 'London' AND time > time::now() - 1h;
SELECT VALUE temp FROM reading WHERE city = 'London' AND time < time::now() ORDER BY temp;

DEFINE FUNCTION fn::since($city: string, $from: datetime) {
	RETURN SELECT VALUE temp FROM reading WHERE city = $city AND time >= $from ORDER BY temp;
};
RETURN fn::since('London', d'2024-01-01T00:00:20Z');
RETURN fn::since('Lisbon', d'2024-01-01T00:00:00Z');

DEFINE FUNCTION fn::cutoff() {
	RETURN d'2024-01-01T00:00:20Z';
};
EXPLAIN SELECT temp FROM reading WHERE city = 'London' AND time >= fn::cutoff();
SELECT VALUE temp FROM reading WHERE city = 'London' AND time >= fn::cutoff() ORDER BY temp;
