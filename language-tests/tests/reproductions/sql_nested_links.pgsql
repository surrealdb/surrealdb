/**
[env]
namespace = true
database = true
imports = ["reproductions/sql_nested_links_import.surql"]

[test]
reason = "Links retain SQL key semantics through nested queries and wildcard projections."

[[test.results]]
value = "[{ id: 1 }]"

[[test.results]]
value = "[{ id: 1 }]"

[[test.results]]
value = "[{ id: 1 }]"

[[test.results]]
value = "[{ id: 1 }]"

[[test.results]]
value = "[{ id: 1 }]"

[[test.results]]
value = "[{ n: 2 }, { n: 3 }]"

[[test.results]]
value = "[{ id: 1 }]"

[[test.results]]
value = "[{ id: 2 }]"

[[test.results]]
value = "[{ id: 1, n: 1 }, { id: 2, n: 2 }, { id: 3, n: NULL }]"
[[test.results]]
value = "[{ n: NULL, p: NULL }]"
*/
SELECT id FROM nested_staff WHERE dept = (SELECT dept FROM nested_staff WHERE id = 1);
SELECT id FROM nested_staff WHERE dept IN (SELECT dept FROM nested_staff WHERE id = 1);
SELECT id FROM (SELECT id, dept FROM nested_staff) q WHERE q.dept = 1;
SELECT id FROM (SELECT * FROM nested_staff) q WHERE q.dept = 1;
WITH q AS (SELECT * FROM nested_staff) SELECT id FROM q WHERE dept = 1;
SELECT q.dept + 1 AS n FROM (SELECT * FROM nested_staff) q WHERE q.dept IS NOT NULL ORDER BY n;
SELECT id FROM nested_dept WHERE id = (SELECT dept FROM nested_staff WHERE id = 1);
SELECT id FROM nested_staff WHERE dept NOT IN (SELECT dept FROM nested_staff WHERE id = 1) ORDER BY id;
UPDATE nested_staff SET n = abs(dept) RETURNING id, n;
SELECT abs(NULL) AS n, power(2, NULL) AS p;
