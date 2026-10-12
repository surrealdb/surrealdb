/**
[env]
namespace = true
database = true
auth = { level = "owner" }
imports = ["language/planner/id_components_key_range_import.surql"]

[test]
reason = """
	A statement over a keyed table nested in another reads, for each row of the
	outer statement, the rows its predicate admits for that row: a bound that
	reads the outer row is evaluated per row, never once for every row, and a
	key range whose bounds read no outer row is read the same for each. The
	readings of London from 20s are temps 51, 54 and 57, of Rome from 22s 56
	and 59, and of London from 22s 57.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: site:a, temps: [51, 54, 57] }, { id: site:b, temps: [56, 59] }]"

[[test.results]]
value = "[{ id: site:a, temps: [51, 54, 57] }, { id: site:b, temps: [57] }]"

[[test.results]]
value = "[{ id: site:a, latest: [57] }, { id: site:b, latest: [59] }]"
*/

DEFINE TABLE site SCHEMALESS;
CREATE site:a SET city = 'London', since = d'2024-01-01T00:00:20Z' RETURN NONE;
CREATE site:b SET city = 'Rome', since = d'2024-01-01T00:00:22Z' RETURN NONE;

SELECT id, (SELECT VALUE temp FROM reading WHERE city = $parent.city AND time >= $parent.since ORDER BY temp) AS temps FROM site ORDER BY id;
SELECT id, (SELECT VALUE temp FROM reading WHERE city = 'London' AND time >= $parent.since ORDER BY temp) AS temps FROM site ORDER BY id;
SELECT id, (SELECT VALUE temp FROM reading WHERE city = $parent.city ORDER BY time DESC LIMIT 1) AS latest FROM site ORDER BY id;
