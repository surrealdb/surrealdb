/**
[env]
namespace = true
database = true
auth = { level = "owner" }
imports = ["language/planner/id_components_key_range_import.surql"]

[test]
reason = """
	A predicate that pins no leading component and bounds none reads the
	whole table (TableScan): a range on a component that is not the first,
	a literal the component cannot hold, an IN holding a value no component
	holds, `!=`, `NONE`, and any read the statement's `WITH NOINDEX` makes.
	A comparison no row can satisfy may be planned as no read at all; it is
	never a key range. The rows are those the predicate admits all the same.
"""

# A range on the second component alone.
[[test.results]]
match = '''string::contains($result, "TableScan") AND string::contains($result, "table: reading") AND !string::contains($result, "KeyRangeScan")'''
error = false

[[test.results]]
value = "[56, 57, 58, 59]"

# A literal of another kind than the component's.
[[test.results]]
match = '''!string::contains($result, "KeyRangeScan")'''
error = false

[[test.results]]
value = "[]"

# An IN with an element no component holds is not pushed at all.
[[test.results]]
match = '''string::contains($result, "TableScan") AND string::contains($result, "table: reading") AND !string::contains($result, "KeyRangeScan")'''
error = false

[[test.results]]
value = "[0, 3, 6, 9, 12, 15, 18, 21, 24, 27, 30, 33, 36, 39, 42, 45, 48, 51, 54, 57]"

# Neither `!=` nor `NONE` is a bound.
[[test.results]]
match = '''string::contains($result, "TableScan") AND string::contains($result, "table: reading") AND !string::contains($result, "KeyRangeScan")'''
error = false

[[test.results]]
match = '''!string::contains($result, "KeyRangeScan")'''
error = false

[[test.results]]
value = "[]"

# WITH NOINDEX reads the table.
[[test.results]]
match = '''string::contains($result, "TableScan") AND string::contains($result, "table: reading") AND !string::contains($result, "KeyRangeScan")'''
error = false
*/

EXPLAIN SELECT temp FROM reading WHERE time > d'2024-01-01T00:00:22Z';
SELECT VALUE temp FROM reading WHERE time > d'2024-01-01T00:00:22Z' ORDER BY temp;

EXPLAIN SELECT temp FROM reading WHERE city = 5;
SELECT VALUE temp FROM reading WHERE city = 5;

EXPLAIN SELECT temp FROM reading WHERE city IN ['London', NONE];
SELECT VALUE temp FROM reading WHERE city IN ['London', NONE] ORDER BY temp;

EXPLAIN SELECT temp FROM reading WHERE city != 'London';
EXPLAIN SELECT temp FROM reading WHERE city = NONE;
SELECT VALUE temp FROM reading WHERE city = NONE;

EXPLAIN SELECT temp FROM reading WITH NOINDEX WHERE city = 'London';
