/**
[env]
namespace = true
database = true
auth = { level = "owner" }
imports = ["language/planner/id_components_key_range_import.surql"]

[test]
reason = """
	Equality over the components a record id names reads the key ranges the
	pinned values select (KeyRangeScan) rather than the table: one range per
	run of pinned values, one per value of an IN or of an OR of equalities on
	one component, deduplicated. A component is addressed by its name or as
	`id[i]`, and an int component reads a float or decimal literal equal to an
	int as that int. The predicate stays above the scan (`residual: yes`), and
	every case returns what the same query under WITH NOINDEX, a scan of the
	whole table, returns.
"""

[[test.results]]
match = '''string::contains($result, "KeyRangeScan") AND string::contains($result, "table: reading") AND string::contains($result, "key: city = 'London'") AND string::contains($result, "residual: yes") AND !string::contains($result, "range: ") AND !string::contains($result, "TableScan")'''
error = false

[[test.results]]
value = "true"

[[test.results]]
value = "[0, 3, 6, 9, 12, 15, 18, 21, 24, 27, 30, 33, 36, 39, 42, 45, 48, 51, 54, 57]"

# An IN reads one range per distinct value.
[[test.results]]
match = '''string::contains($result, "KeyRangeScan") AND string::contains($result, "key: city IN [") AND string::contains($result, "ranges: 2") AND !string::contains($result, "TableScan")'''
error = false

[[test.results]]
value = "true"

# An OR of equalities on one component is read as an IN.
[[test.results]]
match = '''string::contains($result, "KeyRangeScan") AND string::contains($result, "ranges: 2") AND !string::contains($result, "TableScan")'''
error = false

[[test.results]]
value = "true"

# Every component pinned: one key.
[[test.results]]
match = '''string::contains($result, "KeyRangeScan") AND string::contains($result, "key: city = 'Paris', time = d'2024-01-01T00:00:01.6") AND !string::contains($result, "TableScan")'''
error = false

[[test.results]]
value = "[4]"

# A component addressed by its position.
[[test.results]]
match = '''string::contains($result, "KeyRangeScan") AND string::contains($result, "key: city = 'Rome'") AND !string::contains($result, "TableScan")'''
error = false

[[test.results]]
value = "true"

# A float or a decimal equal to an int pins the int.
[[test.results]]
match = '''string::contains($result, "KeyRangeScan") AND string::contains($result, "key: site = 'a', n = 3") AND !string::contains($result, "TableScan")'''
error = false

[[test.results]]
value = "[8]"

[[test.results]]
value = "[8]"
*/

EXPLAIN SELECT temp FROM reading WHERE city = 'London';
RETURN array::sort((SELECT VALUE temp FROM reading WHERE city = 'London')) == array::sort((SELECT VALUE temp FROM reading WITH NOINDEX WHERE city = 'London'));
SELECT VALUE temp FROM reading WHERE city = 'London' ORDER BY temp;

EXPLAIN SELECT temp FROM reading WHERE city IN ['Rome', 'London', 'Rome'];
RETURN array::sort((SELECT VALUE temp FROM reading WHERE city IN ['Rome', 'London', 'Rome'])) == array::sort((SELECT VALUE temp FROM reading WITH NOINDEX WHERE city IN ['Rome', 'London', 'Rome']));

EXPLAIN SELECT temp FROM reading WHERE city = 'London' OR city = 'Rome';
RETURN array::sort((SELECT VALUE temp FROM reading WHERE city = 'London' OR city = 'Rome')) == array::sort((SELECT VALUE temp FROM reading WITH NOINDEX WHERE city = 'London' OR city = 'Rome'));

EXPLAIN SELECT temp FROM reading WHERE city = 'Paris' AND time = d'2024-01-01T00:00:01.600Z';
SELECT VALUE temp FROM reading WHERE city = 'Paris' AND time = d'2024-01-01T00:00:01.600Z';

EXPLAIN SELECT temp FROM reading WHERE id[0] = 'Rome';
RETURN array::sort((SELECT VALUE temp FROM reading WHERE id[0] = 'Rome')) == array::sort((SELECT VALUE temp FROM reading WITH NOINDEX WHERE id[0] = 'Rome'));

EXPLAIN SELECT hits FROM counter WHERE site = 'a' AND n = 3.0;
SELECT VALUE hits FROM counter WHERE site = 'a' AND n = 3.0;
SELECT VALUE hits FROM counter WHERE site = 'a' AND n = 3dec;
