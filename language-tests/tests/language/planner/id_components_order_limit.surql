/**
[env]
namespace = true
database = true
auth = { level = "owner" }
imports = ["language/planner/id_components_key_range_import.surql"]

[test]
reason = """
	A key-range scan reads its rows in key order. Where the components an
	ordering reads order exactly (an int, a string) and the components before
	them are pinned to one value, the scan delivers the ordering and the sort
	is removed, reading backwards for a descending one. A datetime component
	orders its keys by the whole second only, so its sort stays: under a LIMIT
	it keeps the rows the page needs, and the scan stops once it reads a
	second past the last row the sort keeps (`topk_cutoff: second`). Pages
	inside one second and across two are the true order's, as is the latest
	row by `id`.
"""

# An int component after a pinned one: the scan delivers the order.
[[test.results]]
match = '''string::contains($result, "KeyRangeScan") AND string::contains($result, "ordered: exact") AND string::contains($result, "direction: Forward") AND !string::contains($result, "Sort [")'''
error = false

[[test.results]]
value = "[0, 2, 4, 6, 8, 10, 12, 14, 16, 18]"

[[test.results]]
match = '''string::contains($result, "KeyRangeScan") AND string::contains($result, "ordered: exact") AND string::contains($result, "direction: Backward") AND !string::contains($result, "Sort [")'''
error = false

[[test.results]]
value = "[18, 16, 14]"

[[test.results]]
value = "[13, 11, 9]"

# A datetime component: the sort stays, and cuts the scan by the second.
[[test.results]]
match = '''string::contains($result, "KeyRangeScan") AND string::contains($result, "ordered: second") AND string::contains($result, "topk_cutoff: second") AND string::contains($result, "Sort [")'''
error = false

[[test.results]]
value = "[0, 3, 6]"

[[test.results]]
value = "[57, 54]"

[[test.results]]
value = "[57]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

# A page inside one second, and one across two.
[[test.results]]
value = "[5, 6, 7]"

[[test.results]]
value = "[8, 9, 10, 11]"

[[test.results]]
value = "[4, 3, 2]"
*/

EXPLAIN SELECT hits FROM counter WHERE site = 'a' ORDER BY n;
SELECT VALUE hits FROM counter WHERE site = 'a' ORDER BY n;
EXPLAIN SELECT hits FROM counter WHERE site = 'a' ORDER BY n DESC LIMIT 3;
SELECT VALUE hits FROM counter WHERE site = 'a' ORDER BY n DESC LIMIT 3;
SELECT VALUE hits FROM counter WHERE site = 'b' AND n < 9 ORDER BY id[1] DESC LIMIT 3;

EXPLAIN SELECT temp FROM reading WHERE city = 'London' ORDER BY time LIMIT 3;
SELECT VALUE temp FROM reading WHERE city = 'London' ORDER BY time LIMIT 3;
SELECT VALUE temp FROM reading WHERE city = 'London' ORDER BY time DESC LIMIT 2;
SELECT VALUE temp FROM reading WHERE city = 'London' ORDER BY id DESC LIMIT 1;

DEFINE TABLE pulse SCHEMALESS;
DEFINE FIELD id ON pulse TYPE [src: string, at: datetime];
FOR $i IN 0..14 {
	CREATE pulse SET
		src = 's',
		at = time::from_millis(1704067200000 + [0, 90, 180, 270, 360, 450, 540, 630, 720, 810, 1000, 1300, 1600, 1900][$i]),
		i = $i
		RETURN NONE;
};
SELECT VALUE i FROM pulse WHERE src = 's' ORDER BY at LIMIT 3 START 5;
SELECT VALUE i FROM pulse WHERE src = 's' ORDER BY at LIMIT 4 START 8;
SELECT VALUE i FROM pulse WHERE src = 's' ORDER BY at DESC LIMIT 3 START 9;
