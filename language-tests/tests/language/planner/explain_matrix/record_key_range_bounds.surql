/**
[env]
namespace = true
database = true
auth = { level = "owner" }
imports = ["language/planner/id_components_key_range_import.surql"]

[test]
reason = """
	A range over the component after the pinned ones reads the key range it
	selects (KeyRangeScan): `<`, `<=`, `>`, `>=`, both sides at once, a range
	on the leading component alone, and a bound only the run knows, evaluated
	when the scan opens. A datetime component's keys order by the whole
	second only, so its bounds widen to their seconds (`granularity: second`)
	and the predicate above the scan, which is always kept, drops the rows of
	those seconds it refuses. An int component's fractional bound rounds
	inwards. A range no value satisfies reads nothing. Every case returns
	what a scan of the whole table returns.
"""

[[test.results]]
match = '''string::contains($result, "KeyRangeScan") AND string::contains($result, "key: city = 'London'") AND string::contains($result, "range: time > d'2024-01-01T00:00:06") AND string::contains($result, "granularity: second") AND string::contains($result, "residual: yes") AND !string::contains($result, "TableScan")'''
error = false

[[test.results]]
value = "[18, 21, 24, 27, 30, 33, 36, 39, 42, 45, 48, 51, 54, 57]"

[[test.results]]
value = "[15, 18, 21, 24, 27, 30, 33, 36, 39, 42, 45, 48, 51, 54, 57]"

[[test.results]]
value = "[0, 3, 6, 9, 12]"

[[test.results]]
value = "[0, 3, 6, 9, 12, 15]"

[[test.results]]
match = '''string::contains($result, "KeyRangeScan") AND string::contains($result, "range: time >= d'2024-01-01T00:00:02") AND string::contains($result, " AND time < d'2024-01-01T00:00:07.2") AND !string::contains($result, "TableScan")'''
error = false

[[test.results]]
value = "[6, 9, 12, 15]"

# Bounds within a second: the whole second is read, and the predicate decides.
[[test.results]]
value = "[15, 18, 21]"

[[test.results]]
value = "true"

# A range on the leading component alone.
[[test.results]]
match = '''string::contains($result, "KeyRangeScan") AND string::contains($result, "range: city > 'Paris'") AND !string::contains($result, "key: ") AND !string::contains($result, "TableScan")'''
error = false

[[test.results]]
value = "[2, 5, 8, 11, 14, 17, 20, 23, 26, 29, 32, 35, 38, 41, 44, 47, 50, 53, 56, 59]"

# A conjunct over no component stays in the predicate above the scan.
[[test.results]]
match = '''string::contains($result, "KeyRangeScan") AND string::contains($result, "residual: yes") AND string::contains($result, "temp > 50") AND !string::contains($result, "TableScan")'''
error = false

[[test.results]]
value = "[51, 54, 57]"

# A bound held by a parameter.
[[test.results]]
value = "NONE"

[[test.results]]
match = '''string::contains($result, "KeyRangeScan") AND string::contains($result, "range: time >= ") AND !string::contains($result, "TableScan")'''
error = false

[[test.results]]
value = "[51, 54, 57]"

# A fractional bound on an int component rounds inwards.
[[test.results]]
match = '''string::contains($result, "KeyRangeScan") AND string::contains($result, "key: site = 'b'") AND string::contains($result, "range: n >= 3") AND !string::contains($result, "granularity") AND !string::contains($result, "TableScan")'''
error = false

[[test.results]]
value = "[9, 11, 13, 15, 17, 19]"

[[test.results]]
value = "[1, 3]"

# A range no value satisfies reads nothing.
[[test.results]]
match = '''string::contains($result, "KeyRangeScan") AND !string::contains($result, "TableScan")'''
error = false

[[test.results]]
value = "[]"
*/

EXPLAIN SELECT temp FROM reading WHERE city = 'London' AND time > d'2024-01-01T00:00:06Z';
SELECT VALUE temp FROM reading WHERE city = 'London' AND time > d'2024-01-01T00:00:06Z' ORDER BY temp;
SELECT VALUE temp FROM reading WHERE city = 'London' AND time >= d'2024-01-01T00:00:06Z' ORDER BY temp;
SELECT VALUE temp FROM reading WHERE city = 'London' AND time < d'2024-01-01T00:00:06Z' ORDER BY temp;
SELECT VALUE temp FROM reading WHERE city = 'London' AND d'2024-01-01T00:00:06Z' >= time ORDER BY temp;

EXPLAIN SELECT temp FROM reading WHERE city = 'London' AND time >= d'2024-01-01T00:00:02Z' AND time < d'2024-01-01T00:00:07.200Z';
SELECT VALUE temp FROM reading WHERE city = 'London' AND time >= d'2024-01-01T00:00:02Z' AND time < d'2024-01-01T00:00:07.200Z' ORDER BY temp;

SELECT VALUE temp FROM reading WHERE city = 'London' AND time > d'2024-01-01T00:00:04.800Z' AND time <= d'2024-01-01T00:00:08.400Z' ORDER BY temp;
RETURN (SELECT VALUE temp FROM reading WHERE city = 'London' AND time > d'2024-01-01T00:00:04.800Z' AND time <= d'2024-01-01T00:00:08.400Z' ORDER BY temp) == (SELECT VALUE temp FROM reading WITH NOINDEX WHERE city = 'London' AND time > d'2024-01-01T00:00:04.800Z' AND time <= d'2024-01-01T00:00:08.400Z' ORDER BY temp);

EXPLAIN SELECT temp FROM reading WHERE city > 'Paris';
SELECT VALUE temp FROM reading WHERE city > 'Paris' ORDER BY temp;

EXPLAIN SELECT temp FROM reading WHERE city = 'London' AND temp > 50;
SELECT VALUE temp FROM reading WHERE city = 'London' AND temp > 50 ORDER BY temp;

LET $from = d'2024-01-01T00:00:20Z';
EXPLAIN SELECT temp FROM reading WHERE city = 'London' AND time >= $from;
SELECT VALUE temp FROM reading WHERE city = 'London' AND time >= $from ORDER BY temp;

EXPLAIN SELECT hits FROM counter WHERE site = 'b' AND n > 2.5;
SELECT VALUE hits FROM counter WHERE site = 'b' AND n > 2.5 ORDER BY hits;
SELECT VALUE hits FROM counter WHERE site = 'b' AND n <= -0.5 ORDER BY hits;

EXPLAIN SELECT hits FROM counter WHERE site = 'a' AND n > 5 AND n < 3;
SELECT VALUE hits FROM counter WHERE site = 'a' AND n > 5 AND n < 3;
